use std::time::Duration;

use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_proxy_base;
use crate::error::{CliError, Result};
use crate::http;

const OP_DATA: u8 = 0x00;
const OP_RESIZE: u8 = 0x01;
const OP_READY: u8 = 0x02;
const OP_EXIT: u8 = 0x03;
const PTY_CLOSE_WAIT_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, PartialEq, Eq)]
enum PtyBinaryFrame {
    Data(Vec<u8>),
    Exit(i32),
}

pub async fn run(ctx: &CliContext, sandbox_id: &str, shell: &str) -> Result<()> {
    if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        return Err(CliError::usage("ssh requires an interactive terminal"));
    }

    let (proxy_base, host_override) = sandbox_proxy_base(ctx, sandbox_id);

    // Build a client with auth headers and optional Host override
    let mut headers = reqwest::header::HeaderMap::new();
    if let Ok(token) = ctx.bearer_token() {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", token).parse().map_err(|e| {
                CliError::Other(anyhow::anyhow!("invalid bearer token header: {}", e))
            })?,
        );
    }
    if let Some(org_id) = ctx.effective_organization_id() {
        headers.insert(
            "X-Forwarded-Organization-Id",
            org_id.parse().map_err(|e| {
                CliError::Other(anyhow::anyhow!("invalid organization id header: {}", e))
            })?,
        );
    }
    if let Some(proj_id) = ctx.effective_project_id() {
        headers.insert(
            "X-Forwarded-Project-Id",
            proj_id.parse().map_err(|e| {
                CliError::Other(anyhow::anyhow!("invalid project id header: {}", e))
            })?,
        );
    }
    if let Some(ref host) = host_override {
        headers.insert(
            reqwest::header::HOST,
            host.parse()
                .map_err(|e| CliError::Other(anyhow::anyhow!("invalid host header: {}", e)))?,
        );
    }
    let client = http::client_builder()
        .default_headers(headers.clone())
        .build()
        .map_err(|e| CliError::Other(anyhow::anyhow!("{}", e)))?;

    // Get terminal size
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));

    // Forward the user's TERM value so applications inside the PTY see the
    // correct terminal type (e.g. tmux-256color inside tmux).
    let term_val = std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".to_string());

    // Let the first PTY request trigger the existing server-side wake-up path
    // for suspended sandboxes instead of resuming from the CLI first.
    // Create PTY session via proxy API
    let pty_resp = client
        .post(format!("{}/api/v1/pty", proxy_base))
        .json(&serde_json::json!({
            "command": shell,
            "rows": rows,
            "cols": cols,
            "env": {
                "TERM": term_val,
                "COLORTERM": "truecolor",
            },
        }))
        .send()
        .await
        .map_err(CliError::Http)?;

    if !pty_resp.status().is_success() {
        let status = pty_resp.status();
        let body = pty_resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to create PTY session (HTTP {}): {}",
            status,
            body
        )));
    }

    let pty_info: serde_json::Value = pty_resp.json().await.map_err(CliError::Http)?;
    let session_id = pty_info
        .get("session_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Other(anyhow::anyhow!("missing session_id in PTY response")))?;
    let token = pty_info
        .get("token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Other(anyhow::anyhow!("missing token in PTY response")))?;

    // Include the PTY token in both the header and query string for now. The
    // daemon accepts either form, and the query parameter keeps production
    // proxies that don't forward the custom header from breaking SSH.
    let ws_base = proxy_base
        .replace("https://", "wss://")
        .replace("http://", "ws://");
    let ws_url = format!("{}/api/v1/pty/{}/ws?token={}", ws_base, session_id, token);

    // Connect WebSocket
    use tokio_tungstenite::tungstenite;

    let mut request = tungstenite::client::IntoClientRequest::into_client_request(ws_url.as_str())
        .map_err(|e| {
            CliError::Other(anyhow::anyhow!("failed to build WebSocket request: {}", e))
        })?;

    // Add auth headers and PTY token to WebSocket request
    for (key, value) in &headers {
        request.headers_mut().insert(key.clone(), value.clone());
    }
    request.headers_mut().insert(
        "X-PTY-Token",
        token
            .parse()
            .map_err(|e| CliError::Other(anyhow::anyhow!("invalid pty token header: {}", e)))?,
    );

    let (ws_stream, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| CliError::Other(anyhow::anyhow!("WebSocket connection failed: {}", e)))?;

    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use tokio::io::AsyncReadExt;

    let (mut ws_write, mut ws_read) = ws_stream.split();

    // Enter raw mode with a Drop guard so the terminal is restored even on
    // panic or early return. Must happen before READY so output flushed by the
    // server is received while already in raw mode (avoids staircase \n without \r).
    struct RawModeGuard;
    impl Drop for RawModeGuard {
        fn drop(&mut self) {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
    crossterm::terminal::enable_raw_mode()?;
    let _raw_guard = RawModeGuard;

    // Send READY to tell the server to flush buffered output.
    ws_write
        .send(tungstenite::Message::Binary(vec![OP_READY].into()))
        .await
        .map_err(|e| CliError::Other(anyhow::anyhow!("failed to send READY: {}", e)))?;

    let result = async {
        // Spawn reader task (WebSocket -> stdout)
        let mut reader_handle = tokio::spawn(async move {
            let mut exit_code: Option<i32> = None;
            while let Some(msg) = ws_read.next().await {
                match msg {
                    Ok(tungstenite::Message::Binary(data)) => {
                        match parse_pty_binary_frame(&data) {
                            Some(PtyBinaryFrame::Data(payload)) => {
                                let mut stdout = tokio::io::stdout();
                                use tokio::io::AsyncWriteExt;
                                let _ = stdout.write_all(&payload).await;
                                let _ = stdout.flush().await;
                            }
                            Some(PtyBinaryFrame::Exit(code)) => {
                                exit_code = Some(code);
                                break;
                            }
                            None => {}
                        }
                    }
                    Ok(tungstenite::Message::Close(Some(frame))) => {
                        exit_code = exit_code.or_else(|| parse_legacy_exit_code(frame.reason.as_ref()));
                        break;
                    }
                    Ok(tungstenite::Message::Close(None)) | Err(_) => break,
                    _ => {}
                }
            }
            exit_code
        });

        // Listen for Ctrl+C on every platform. Window resize notifications are
        // only available through SIGWINCH on Unix, so Windows builds skip that
        // branch and keep the session functional without dynamic resize events.
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

        #[cfg(unix)]
        let mut sigwinch = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::window_change(),
        )
        .expect("failed to register SIGWINCH handler");

        // Main loop: stdin -> WebSocket
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 4096];
        let mut server_exit_code: Option<i32> = None;

        #[cfg(unix)]
        loop {
            tokio::select! {
                result = stdin.read(&mut buf) => {
                    match result {
                        Ok(0) => break,
                        Ok(n) => {
                            let mut msg = vec![OP_DATA];
                            msg.extend_from_slice(&buf[..n]);
                            if ws_write.send(tungstenite::Message::Binary(msg.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                exit_code = &mut reader_handle => {
                    server_exit_code = exit_code.unwrap_or(None);
                    break;
                }
                _ = sigwinch.recv() => {
                    let (new_cols, new_rows) = crossterm::terminal::size().unwrap_or((80, 24));
                    let mut msg = vec![OP_RESIZE];
                    msg.extend_from_slice(&new_cols.to_be_bytes());
                    msg.extend_from_slice(&new_rows.to_be_bytes());
                    if ws_write.send(tungstenite::Message::Binary(msg.into())).await.is_err() {
                        break;
                    }
                }
                _ = &mut ctrl_c => {
                    break;
                }
            }
        }

        #[cfg(not(unix))]
        loop {
            tokio::select! {
                result = stdin.read(&mut buf) => {
                    match result {
                        Ok(0) => break,
                        Ok(n) => {
                            let mut msg = vec![OP_DATA];
                            msg.extend_from_slice(&buf[..n]);
                            if ws_write.send(tungstenite::Message::Binary(msg.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                exit_code = &mut reader_handle => {
                    server_exit_code = exit_code.unwrap_or(None);
                    break;
                }
                _ = &mut ctrl_c => {
                    break;
                }
            }
        }

        // Send close frame so the server decrements client_count immediately
        // instead of waiting for the ping/pong timeout.
        let _ = ws_write
            .send(tungstenite::Message::Close(None))
            .await;

        // If the reader hasn't finished yet, give it a short grace period to
        // receive a server-side exit signal or close frame. Abort afterwards so
        // local disconnects do not hang indefinitely on a broken close handshake.
        if server_exit_code.is_none() {
            server_exit_code = wait_for_reader_shutdown(&mut reader_handle).await?;
        }

        Ok::<Option<i32>, CliError>(server_exit_code)
    }
    .await?;

    // Terminal is restored by _raw_guard Drop.
    drop(_raw_guard);

    // Print a newline so the outer shell prompt starts on a clean line.
    eprintln!();

    if let Some(code) = result
        && code != 0
    {
        return Err(CliError::ExitCode(code));
    }

    Ok(())
}

fn parse_pty_binary_frame(data: &[u8]) -> Option<PtyBinaryFrame> {
    match data.first().copied() {
        Some(OP_DATA) => Some(PtyBinaryFrame::Data(data[1..].to_vec())),
        Some(OP_EXIT) if data.len() >= 5 => {
            let exit_code = i32::from_be_bytes(data[1..5].try_into().ok()?);
            Some(PtyBinaryFrame::Exit(exit_code))
        }
        _ => None,
    }
}

fn parse_legacy_exit_code(reason: &str) -> Option<i32> {
    let code = reason.strip_prefix("exit:")?;
    code.parse::<i32>().ok()
}

async fn wait_for_reader_shutdown(
    reader_handle: &mut tokio::task::JoinHandle<Option<i32>>,
) -> Result<Option<i32>> {
    match tokio::time::timeout(PTY_CLOSE_WAIT_TIMEOUT, &mut *reader_handle).await {
        Ok(join_result) => join_result
            .map_err(|error| CliError::Other(anyhow::anyhow!("PTY reader task failed: {}", error))),
        Err(_) => {
            reader_handle.abort();
            let _ = reader_handle.await;
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OP_DATA, OP_EXIT, PTY_CLOSE_WAIT_TIMEOUT, PtyBinaryFrame, parse_legacy_exit_code,
        parse_pty_binary_frame, wait_for_reader_shutdown,
    };
    use std::future::pending;
    use std::time::Duration;

    #[test]
    fn parse_pty_binary_frame_reads_data_frames() {
        assert_eq!(
            parse_pty_binary_frame(&[OP_DATA, b'h', b'i']),
            Some(PtyBinaryFrame::Data(b"hi".to_vec()))
        );
    }

    #[test]
    fn parse_pty_binary_frame_reads_exit_frames() {
        assert_eq!(
            parse_pty_binary_frame(&[OP_EXIT, 0, 0, 0, 7]),
            Some(PtyBinaryFrame::Exit(7))
        );
    }

    #[test]
    fn parse_pty_binary_frame_ignores_malformed_exit_frames() {
        assert_eq!(parse_pty_binary_frame(&[OP_EXIT, 0, 0]), None);
    }

    #[test]
    fn parse_legacy_exit_code_reads_close_reason() {
        assert_eq!(parse_legacy_exit_code("exit:23"), Some(23));
        assert_eq!(parse_legacy_exit_code("bye"), None);
    }

    #[tokio::test]
    async fn wait_for_reader_shutdown_aborts_hung_reader() {
        let mut reader_handle = tokio::spawn(async move {
            pending::<()>().await;
            None
        });

        let result = tokio::time::timeout(
            PTY_CLOSE_WAIT_TIMEOUT + Duration::from_secs(1),
            wait_for_reader_shutdown(&mut reader_handle),
        )
        .await
        .expect("reader shutdown should be bounded")
        .unwrap();

        assert_eq!(result, None);
        assert!(reader_handle.is_finished());
    }
}
