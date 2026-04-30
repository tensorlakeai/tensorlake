use std::io::ErrorKind;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, anyhow};
use futures::{SinkExt, StreamExt};
use reqwest::header::{AUTHORIZATION, HOST, HeaderMap, HeaderName, HeaderValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{self, Message, client::IntoClientRequest};
use url::Url;

use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_proxy_base;
use crate::error::{CliError, Result};

const TUNNEL_BUFFER_SIZE: usize = 16 * 1024;

pub async fn run(
    ctx: &CliContext,
    sandbox_id: &str,
    remote_port: u16,
    listen_port: Option<u16>,
) -> Result<()> {
    let listen_port = listen_port.unwrap_or(remote_port);
    let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);
    let ws_url = build_tunnel_url(&proxy_base, remote_port)?;
    let headers = build_proxy_headers(ctx, sandbox_id, host_override.as_deref())?;

    let listen_addr = format!("127.0.0.1:{listen_port}");
    let listener = TcpListener::bind(&listen_addr).await.map_err(|error| {
        CliError::Other(anyhow!(
            "failed to bind local tunnel listener on {}: {}",
            listen_addr,
            error
        ))
    })?;
    let local_addr = listener.local_addr()?;

    eprintln!(
        "Listening on {} and forwarding to sandbox {} port {}. Press Ctrl-C to stop.",
        local_addr, sandbox_id, remote_port
    );

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, peer_addr) = accept_result.map_err(CliError::Io)?;
                let ws_url = ws_url.clone();
                let headers = headers.clone();

                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, &ws_url, &headers).await {
                        eprintln!(
                            "Tunnel connection from {} failed: {}",
                            peer_addr,
                            error
                        );
                    }
                });
            }
            signal_result = tokio::signal::ctrl_c() => {
                signal_result.map_err(CliError::Io)?;
                eprintln!("Stopping tunnel listener on {}.", local_addr);
                break;
            }
        }
    }

    Ok(())
}

fn build_tunnel_url(proxy_base: &str, remote_port: u16) -> Result<String> {
    let mut url = Url::parse(proxy_base)
        .with_context(|| format!("invalid sandbox proxy base URL: {}", proxy_base))
        .map_err(CliError::Other)?;
    url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
        .map_err(|_| CliError::Other(anyhow!("failed to convert proxy URL to websocket URL")))?;
    url.set_path("/api/v1/tunnels/tcp");
    url.set_query(Some(&format!("port={remote_port}")));
    Ok(url.to_string())
}

fn build_proxy_headers(
    ctx: &CliContext,
    sandbox_id: &str,
    host_override: Option<&str>,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", ctx.bearer_token()?))
            .map_err(|error| CliError::Other(anyhow!("invalid bearer token header: {}", error)))?,
    );

    if let Some(org_id) = ctx.effective_organization_id() {
        headers.insert(
            HeaderName::from_static("x-forwarded-organization-id"),
            HeaderValue::from_str(&org_id).map_err(|error| {
                CliError::Other(anyhow!("invalid organization id header: {}", error))
            })?,
        );
    }

    if let Some(project_id) = ctx.effective_project_id() {
        headers.insert(
            HeaderName::from_static("x-forwarded-project-id"),
            HeaderValue::from_str(&project_id).map_err(|error| {
                CliError::Other(anyhow!("invalid project id header: {}", error))
            })?,
        );
    }

    if let Some(host) = host_override {
        headers.insert(
            HOST,
            HeaderValue::from_str(host)
                .map_err(|error| CliError::Other(anyhow!("invalid host header: {}", error)))?,
        );
    } else {
        headers.insert(
            HeaderName::from_static("x-tensorlake-sandbox-id"),
            HeaderValue::from_str(sandbox_id).map_err(|error| {
                CliError::Other(anyhow!("invalid sandbox id header: {}", error))
            })?,
        );
    }

    Ok(headers)
}

async fn handle_connection(
    stream: TcpStream,
    ws_url: &str,
    headers: &HeaderMap,
) -> anyhow::Result<()> {
    let mut request = ws_url
        .into_client_request()
        .context("failed to build websocket request")?;

    for (key, value) in headers {
        request.headers_mut().insert(key.clone(), value.clone());
    }

    let (ws_stream, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(map_ws_connect_error)?;

    relay_connection(stream, ws_stream).await
}

fn map_ws_connect_error(error: tungstenite::Error) -> anyhow::Error {
    match error {
        tungstenite::Error::Http(response) => {
            anyhow!("websocket handshake failed with HTTP {}", response.status())
        }
        other => anyhow!("websocket connection failed: {}", other),
    }
}

async fn relay_connection(
    stream: TcpStream,
    ws_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> anyhow::Result<()> {
    let (mut tcp_read, mut tcp_write) = stream.into_split();
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let (ws_sender, mut ws_receiver) = mpsc::channel::<Message>(32);
    let close_started = Arc::new(AtomicBool::new(false));

    let writer_close_started = close_started.clone();
    let writer = tokio::spawn(async move {
        while let Some(message) = ws_receiver.recv().await {
            if matches!(message, Message::Close(_)) {
                writer_close_started.store(true, Ordering::Relaxed);
            }
            match ws_write.send(message).await {
                Ok(()) => {}
                Err(error)
                    if is_expected_tunnel_shutdown_error(
                        &error,
                        writer_close_started.load(Ordering::Relaxed),
                    ) =>
                {
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }

        if let Err(error) = ws_write.close().await
            && !is_expected_tunnel_shutdown_error(
                &error,
                writer_close_started.load(Ordering::Relaxed),
            )
        {
            return Err(error.into());
        }

        Ok::<(), anyhow::Error>(())
    });

    let tcp_sender = ws_sender.clone();
    let tcp_close_started = close_started.clone();
    let mut tcp_to_ws = tokio::spawn(async move {
        let mut buffer = [0u8; TUNNEL_BUFFER_SIZE];
        loop {
            let read = tcp_read.read(&mut buffer).await?;
            if read == 0 {
                tcp_close_started.store(true, Ordering::Relaxed);
                let _ = tcp_sender.send(Message::Close(None)).await;
                return Ok::<(), anyhow::Error>(());
            }

            if tcp_sender
                .send(Message::Binary(buffer[..read].to_vec().into()))
                .await
                .is_err()
            {
                if tcp_close_started.load(Ordering::Relaxed) {
                    return Ok(());
                }
                return Err(anyhow!("websocket writer closed"));
            }
        }
    });

    let ws_sender_for_control = ws_sender.clone();
    let ws_close_started = close_started.clone();
    let mut ws_to_tcp = tokio::spawn(async move {
        while let Some(message) = ws_read.next().await {
            match message {
                Ok(Message::Binary(data)) => {
                    if let Err(error) = tcp_write.write_all(data.as_ref()).await {
                        if ws_close_started.load(Ordering::Relaxed)
                            && is_expected_tcp_shutdown_error(&error)
                        {
                            break;
                        }
                        return Err(error.into());
                    }
                }
                Ok(Message::Text(_)) => {
                    return Err(anyhow!("received unexpected text frame from tunnel"));
                }
                Ok(Message::Ping(payload)) => {
                    if ws_sender_for_control
                        .send(Message::Pong(payload))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(Message::Pong(_)) => {}
                Ok(Message::Close(_)) => {
                    ws_close_started.store(true, Ordering::Relaxed);
                    break;
                }
                Err(error)
                    if is_expected_tunnel_shutdown_error(
                        &error,
                        ws_close_started.load(Ordering::Relaxed),
                    ) =>
                {
                    break;
                }
                Err(error) => return Err(error.into()),
                _ => {}
            }
        }

        if let Err(error) = tcp_write.shutdown().await
            && !(ws_close_started.load(Ordering::Relaxed) && is_expected_tcp_shutdown_error(&error))
        {
            return Err(error.into());
        }

        Ok::<(), anyhow::Error>(())
    });

    let relay_result = tokio::select! {
        tcp_result = &mut tcp_to_ws => {
            let tcp_result = join_task(tcp_result)?;
            match tcp_result {
                Ok(()) => join_task(ws_to_tcp.await)?,
                Err(error) => {
                    ws_to_tcp.abort();
                    Err(error)
                }
            }
        }
        ws_result = &mut ws_to_tcp => {
            let ws_result = join_task(ws_result)?;
            match ws_result {
                Ok(()) => {
                    tcp_to_ws.abort();
                    Ok(())
                }
                Err(error) => {
                    tcp_to_ws.abort();
                    Err(error)
                }
            }
        }
    };

    drop(ws_sender);
    let writer_result = join_task(writer.await)?;

    if relay_result.is_ok() {
        writer_result?;
    }

    relay_result
}

fn join_task(
    result: std::result::Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> anyhow::Result<anyhow::Result<()>> {
    result.map_err(|error| anyhow!("tunnel task failed: {}", error))
}

fn is_expected_tunnel_shutdown_error(error: &tungstenite::Error, closing_initiated: bool) -> bool {
    match error {
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => true,
        tungstenite::Error::Protocol(
            tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => closing_initiated,
        tungstenite::Error::Io(io_error) => {
            closing_initiated && is_expected_tcp_shutdown_error(io_error)
        }
        _ => false,
    }
}

fn is_expected_tcp_shutdown_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::BrokenPipe
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::UnexpectedEof
            | ErrorKind::NotConnected
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::{
        accept_async, connect_async,
        tungstenite::{self, Message, error::ProtocolError},
    };

    use super::{is_expected_tunnel_shutdown_error, relay_connection};

    #[test]
    fn reset_without_close_is_only_expected_after_shutdown_starts() {
        let error = tungstenite::Error::Protocol(ProtocolError::ResetWithoutClosingHandshake);

        assert!(!is_expected_tunnel_shutdown_error(&error, false));
        assert!(is_expected_tunnel_shutdown_error(&error, true));
    }

    #[tokio::test]
    async fn relay_connection_handles_remote_close_cleanly() {
        let app_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let app_addr = app_listener.local_addr().unwrap();
        let app_client = tokio::spawn(async move { TcpStream::connect(app_addr).await.unwrap() });
        let (mut app_server, _) = app_listener.accept().await.unwrap();
        let app_client = app_client.await.unwrap();

        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let ws_server = tokio::spawn(async move {
            let (stream, _) = ws_listener.accept().await.unwrap();
            let mut ws_server = accept_async(stream).await.unwrap();
            ws_server.send(Message::Close(None)).await.unwrap();
        });

        let (ws_client, _) = connect_async(format!("ws://{}", ws_addr)).await.unwrap();
        let relay = tokio::spawn(async move { relay_connection(app_client, ws_client).await });

        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), app_server.read(&mut buf))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read, 0);
        assert!(relay.await.unwrap().is_ok());
        ws_server.await.unwrap();
    }

    #[allow(deprecated)]
    #[tokio::test]
    async fn relay_connection_treats_reset_after_local_close_as_clean_shutdown() {
        let app_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let app_addr = app_listener.local_addr().unwrap();
        let app_relay = tokio::spawn(async move { TcpStream::connect(app_addr).await.unwrap() });
        let (mut app_server, _) = app_listener.accept().await.unwrap();
        let app_relay = app_relay.await.unwrap();

        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let ws_server = tokio::spawn(async move {
            let (stream, _) = ws_listener.accept().await.unwrap();
            let mut ws_server = accept_async(stream).await.unwrap();

            let _ = tokio::time::timeout(Duration::from_secs(1), ws_server.next()).await;
            ws_server
                .get_mut()
                .set_linger(Some(Duration::ZERO))
                .unwrap();
            drop(ws_server);
        });

        let (ws_client, _) = connect_async(format!("ws://{}", ws_addr)).await.unwrap();
        let relay = tokio::spawn(async move { relay_connection(app_relay, ws_client).await });

        app_server.shutdown().await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        ws_server.await.unwrap();
    }
}
