//! TCP port forwarding shared by the CLI and both language SDKs.
//!
//! Tokio owns the sockets and TLS state. Language bindings only own a handle;
//! they never read or write a TLS socket from separate language threads.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use reqwest::header::HeaderMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_tungstenite::{
    Connector, WebSocketStream,
    tungstenite::{self, Message, client::IntoClientRequest},
};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::error::SdkError;

const BUFFER_SIZE: usize = 16 * 1024;
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

pub struct TcpTunnelOptions {
    pub base_url: String,
    pub headers: HeaderMap,
    pub remote_port: u16,
    pub local_host: String,
    /// Zero asks the OS to choose an available port.
    pub local_port: u16,
    pub connect_timeout: Duration,
}

/// A local listener and its active connections. Dropping the handle cancels
/// them; `close().await` also waits until all sockets have been released.
pub struct TcpTunnel {
    local_addr: SocketAddr,
    cancel: CancellationToken,
    done: CancellationToken,
    errors: broadcast::Sender<TunnelConnectionError>,
}

#[derive(Clone, Debug)]
pub struct TunnelConnectionError {
    pub peer_addr: SocketAddr,
    pub message: String,
}

impl TcpTunnel {
    pub async fn listen(options: TcpTunnelOptions) -> Result<Self, SdkError> {
        Self::listen_with_connector(options, None).await
    }

    async fn listen_with_connector(
        options: TcpTunnelOptions,
        connector: Option<Connector>,
    ) -> Result<Self, SdkError> {
        if options.remote_port == 0 {
            return Err(SdkError::ClientError(
                "remote port must be between 1 and 65535".into(),
            ));
        }
        let mut url = Url::parse(&options.base_url)
            .map_err(|_| SdkError::ClientError("invalid sandbox proxy URL".into()))?;
        let scheme = match url.scheme() {
            "https" | "wss" => "wss",
            "http" | "ws" => "ws",
            _ => {
                return Err(SdkError::ClientError(
                    "sandbox proxy URL must use HTTP or WebSocket".into(),
                ));
            }
        };
        url.set_scheme(scheme)
            .map_err(|_| SdkError::ClientError("invalid sandbox proxy URL".into()))?;
        url.set_path("/api/v1/tunnels/tcp");
        url.set_query(Some(&format!("port={}", options.remote_port)));
        url.set_fragment(None);
        let mut request = url.as_str().into_client_request().map_err(ws_error)?;
        for (name, value) in &options.headers {
            request.headers_mut().insert(name, value.clone());
        }
        // Also support constructing a tunnel before any HTTP client exists.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let listener = TcpListener::bind((options.local_host.as_str(), options.local_port))
            .await
            .map_err(|error| {
                SdkError::ClientError(format!(
                    "failed to bind local tunnel listener on {}:{}: {error}",
                    options.local_host, options.local_port
                ))
            })?;
        let local_addr = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let done = CancellationToken::new();
        let (errors, _) = broadcast::channel(16);
        let task_errors = errors.clone();
        let task_cancel = cancel.clone();
        // Guards also signal completion if the listener task unwinds.
        let done_guard = done.clone().drop_guard();
        tokio::spawn(async move {
            let _done_guard = done_guard;
            let _cancel_guard = task_cancel.clone().drop_guard();
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = task_cancel.cancelled() => break,
                    Some(_) = connections.join_next() => {},
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, peer_addr)) => {
                                let request = request.clone();
                                let connector = connector.clone();
                                let errors = task_errors.clone();
                                connections.spawn(async move {
                                    let _ = stream.set_nodelay(true);
                                    let connect = tokio_tungstenite::connect_async_tls_with_config(
                                        request, None, true, connector,
                                    );
                                    let result = match tokio::time::timeout(options.connect_timeout, connect).await {
                                        Ok(Ok((ws, _))) => relay_connection(stream, ws).await,
                                        Ok(Err(error)) => Err(ws_error(error)),
                                        Err(_) => Err(SdkError::ClientError("tunnel websocket connection timed out".into())),
                                    };
                                    // A failed connection closes only its own local socket.
                                    if let Err(error) = result {
                                        let _ = errors.send(TunnelConnectionError { peer_addr, message: error.to_string() });
                                    }
                                });
                            }
                            Err(error) => {
                                if matches!(error.kind(), std::io::ErrorKind::InvalidInput | std::io::ErrorKind::NotConnected | std::io::ErrorKind::PermissionDenied) {
                                    break;
                                }
                                // Resource exhaustion and aborted accepts must not kill the
                                // listener or spin. Cancellation remains immediate during retry.
                                tokio::select! {
                                    _ = task_cancel.cancelled() => break,
                                    _ = tokio::time::sleep(Duration::from_millis(500)) => {},
                                }
                            }
                        }
                    }
                }
            }
            drop(listener);
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Ok(Self {
            local_addr,
            cancel,
            done,
            errors,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn is_closed(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Subscribe to per-connection failures without interrupting other clients.
    pub fn errors(&self) -> broadcast::Receiver<TunnelConnectionError> {
        self.errors.subscribe()
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn closed(&self) {
        self.done.cancelled().await;
    }

    pub async fn close(&self) {
        self.cancel();
        self.closed().await;
    }
}

impl Drop for TcpTunnel {
    fn drop(&mut self) {
        self.cancel();
    }
}

async fn relay_connection<S>(
    stream: TcpStream,
    websocket: WebSocketStream<S>,
) -> Result<(), SdkError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut tcp_read, mut tcp_write) = stream.into_split();
    // futures' split serializes access to the one WebSocket/TLS state machine.
    // No independent native or language threads can enter the TLS engine.
    let (mut ws_write, mut ws_read) = websocket.split();
    let local_closed = AtomicBool::new(false);
    let result = {
        let outgoing = async {
            let mut buffer = [0u8; BUFFER_SIZE];
            let mut ping = tokio::time::interval(KEEPALIVE_INTERVAL);
            ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ping.tick().await;
            loop {
                tokio::select! {
                    read = tcp_read.read(&mut buffer) => {
                        let n = read?;
                        if n == 0 {
                            local_closed.store(true, Ordering::Relaxed);
                            // Bound the send too: a non-reading peer can block a close frame.
                            let _ = tokio::time::timeout(CLOSE_TIMEOUT, ws_write.send(Message::Close(None))).await;
                            return Ok::<(), SdkError>(());
                        }
                        ws_write.send(Message::Binary(buffer[..n].to_vec().into())).await.map_err(ws_error)?;
                    }
                    _ = ping.tick() => ws_write.send(Message::Ping(bytes::Bytes::new())).await.map_err(ws_error)?,
                }
            }
        };
        let incoming = async {
            while let Some(message) = ws_read.next().await {
                match message {
                    Ok(Message::Binary(data)) => tcp_write.write_all(&data).await?,
                    Ok(Message::Text(_)) => {
                        return Err(SdkError::ClientError(
                            "received unexpected text frame from tunnel".into(),
                        ));
                    }
                    Ok(Message::Close(_)) => break,
                    // Tungstenite automatically queues and flushes Pong replies.
                    Ok(_) => {}
                    Err(error)
                        if expected_shutdown(&error, local_closed.load(Ordering::Relaxed)) =>
                    {
                        break;
                    }
                    Err(error) => return Err(ws_error(error)),
                }
            }
            Ok(())
        };
        tokio::pin!(outgoing, incoming);
        tokio::select! {
            result = &mut incoming => result,
            result = &mut outgoing => {
                match result {
                    Ok(()) => tokio::time::timeout(CLOSE_TIMEOUT, &mut incoming).await.unwrap_or(Ok(())),
                    Err(error) => Err(error),
                }
            }
        }
    };
    // Deliver all received tail bytes before EOF; close never waits indefinitely.
    let _ = tcp_write.shutdown().await;
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, ws_write.close()).await;
    result
}

fn expected_shutdown(error: &tungstenite::Error, local_closed: bool) -> bool {
    use std::io::ErrorKind;
    match error {
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => true,
        tungstenite::Error::Protocol(
            tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => local_closed,
        tungstenite::Error::Io(error) => {
            local_closed
                && matches!(
                    error.kind(),
                    ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::UnexpectedEof
                        | ErrorKind::NotConnected
                )
        }
        _ => false,
    }
}

fn ws_error(error: tungstenite::Error) -> SdkError {
    match error {
        tungstenite::Error::Http(response) => SdkError::ClientError(format!(
            "tunnel websocket handshake failed with HTTP {}",
            response.status()
        )),
        tungstenite::Error::Io(error) => SdkError::Io(error),
        // Don't include the request: it contains authentication headers.
        _ => SdkError::ClientError(format!("tunnel websocket failed: {error}")),
    }
}

#[cfg(test)]
mod tests;
