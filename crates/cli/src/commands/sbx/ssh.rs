use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_proxy_base;
use crate::error::{CliError, Result};

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
            format!("Bearer {}", token).parse().unwrap(),
        );
    }
    if let Some(org_id) = ctx.effective_organization_id() {
        headers.insert("X-Forwarded-Organization-Id", org_id.parse().unwrap());
    }
    if let Some(proj_id) = ctx.effective_project_id() {
        headers.insert("X-Forwarded-Project-Id", proj_id.parse().unwrap());
    }
    if let Some(ref host) = host_override {
        headers.insert(reqwest::header::HOST, host.parse().unwrap());
    }
    let client = reqwest::Client::builder()
        .default_headers(headers.clone())
        .build()
        .map_err(|e| CliError::Other(anyhow::anyhow!("{}", e)))?;

    // Get terminal size
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));

    // Create PTY session via proxy API
    let pty_resp = client
        .post(format!("{}/api/v1/pty", proxy_base))
        .json(&serde_json::json!({
            "command": shell,
            "rows": rows,
            "cols": cols,
            "env": {
                "TERM": "xterm-256color",
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

    // Build WebSocket URL from proxy base
    let ws_base = proxy_base.replace("https://", "wss://").replace("http://", "ws://");
    let ws_url = format!(
        "{}/api/v1/pty/{}/ws?token={}",
        ws_base, session_id, token
    );

    // Connect WebSocket
    use tokio_tungstenite::tungstenite;

    let mut request = tungstenite::client::IntoClientRequest::into_client_request(ws_url.as_str())
        .map_err(|e| CliError::Other(anyhow::anyhow!("failed to build WebSocket request: {}", e)))?;

    // Add auth headers to WebSocket request
    for (key, value) in &headers {
        request.headers_mut().insert(key.clone(), value.clone());
    }

    let (ws_stream, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| CliError::Other(anyhow::anyhow!("WebSocket connection failed: {}", e)))?;

    // PTY protocol opcodes
    const OP_DATA: u8 = 0x00;
    const _OP_RESIZE: u8 = 0x01;
    const OP_READY: u8 = 0x02;

    use futures::stream::StreamExt;
    use futures::sink::SinkExt;
    use tokio::io::AsyncReadExt;

    let (mut ws_write, mut ws_read) = ws_stream.split();

    // Send READY
    ws_write
        .send(tungstenite::Message::Binary(vec![OP_READY].into()))
        .await
        .map_err(|e| CliError::Other(anyhow::anyhow!("failed to send READY: {}", e)))?;

    // Enter raw mode
    crossterm::terminal::enable_raw_mode()?;

    let result = async {
        // Spawn reader task (WebSocket -> stdout)
        let mut reader_handle = tokio::spawn(async move {
            let mut exit_code: Option<i32> = None;
            while let Some(msg) = ws_read.next().await {
                match msg {
                    Ok(tungstenite::Message::Binary(data)) => {
                        if !data.is_empty() && data[0] == OP_DATA {
                            let mut stdout = tokio::io::stdout();
                            use tokio::io::AsyncWriteExt;
                            let _ = stdout.write_all(&data[1..]).await;
                            let _ = stdout.flush().await;
                        }
                    }
                    Ok(tungstenite::Message::Close(Some(frame))) => {
                        let reason = frame.reason.to_string();
                        if reason.starts_with("exit:") {
                            if let Ok(code) = reason[5..].parse::<i32>() {
                                exit_code = Some(code);
                            }
                        }
                        break;
                    }
                    Ok(tungstenite::Message::Close(None)) | Err(_) => break,
                    _ => {}
                }
            }
            exit_code
        });

        // Main loop: stdin -> WebSocket
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 4096];

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
                    return exit_code.unwrap_or(None::<i32>);
                }
            }
        }

        // Wait for reader to finish
        reader_handle.await.unwrap_or(None)
    }
    .await;

    // Restore terminal
    crossterm::terminal::disable_raw_mode()?;

    if let Some(code) = result {
        if code != 0 {
            return Err(CliError::ExitCode(code));
        }
    }

    Ok(())
}
