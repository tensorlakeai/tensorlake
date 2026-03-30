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
    let headers = build_proxy_headers(ctx, host_override.as_deref())?;

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

fn build_proxy_headers(ctx: &CliContext, host_override: Option<&str>) -> Result<HeaderMap> {
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

    let writer = tokio::spawn(async move {
        while let Some(message) = ws_receiver.recv().await {
            ws_write.send(message).await?;
        }
        let _ = ws_write.close().await;
        Ok::<(), anyhow::Error>(())
    });

    let tcp_sender = ws_sender.clone();
    let mut tcp_to_ws = tokio::spawn(async move {
        let mut buffer = [0u8; TUNNEL_BUFFER_SIZE];
        loop {
            let read = tcp_read.read(&mut buffer).await?;
            if read == 0 {
                let _ = tcp_sender.send(Message::Close(None)).await;
                return Ok::<(), anyhow::Error>(());
            }

            tcp_sender
                .send(Message::Binary(buffer[..read].to_vec().into()))
                .await
                .map_err(|_| anyhow!("websocket writer closed"))?;
        }
    });

    let ws_sender_for_control = ws_sender.clone();
    let mut ws_to_tcp = tokio::spawn(async move {
        while let Some(message) = ws_read.next().await {
            match message? {
                Message::Binary(data) => {
                    tcp_write.write_all(data.as_ref()).await?;
                }
                Message::Text(_) => {
                    return Err(anyhow!("received unexpected text frame from tunnel"));
                }
                Message::Ping(payload) => {
                    if ws_sender_for_control
                        .send(Message::Pong(payload))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Message::Pong(_) => {}
                Message::Close(_) => break,
                _ => {}
            }
        }

        tcp_write.shutdown().await?;
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
