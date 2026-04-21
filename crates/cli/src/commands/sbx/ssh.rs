use std::io::Read;
use std::time::Duration;

use crate::auth::context::CliContext;
use crate::commands::sbx::{parse_env_vars, sandbox_proxy_base};
use crate::error::{CliError, Result};
use crate::http;
use tokio::sync::mpsc;

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

pub async fn run(
    ctx: &CliContext,
    sandbox_id: &str,
    shell: &str,
    shell_args: &[String],
    workdir: Option<&str>,
    env: &[String],
) -> Result<()> {
    if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        return Err(CliError::usage("ssh requires an interactive terminal"));
    }

    let env_dict = parse_env_vars(env)?;
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

    // Forward the user's TERM when it maps to a type the remote VM is
    // guaranteed to have in its terminfo database. Emulator-specific values
    // like "xterm-ghostty" or "xterm-kitty" are not shipped by standard Linux
    // distributions; readline falls back to dumb mode for unknown types,
    // which breaks Ctrl+L and other escape-sequence-driven features.
    let term_val = {
        let raw = std::env::var("TERM").unwrap_or_default();
        let portable = matches!(
            raw.as_str(),
            "xterm"
                | "xterm-256color"
                | "xterm-color"
                | "screen"
                | "screen-256color"
                | "tmux"
                | "tmux-256color"
                | "vt100"
                | "vt220"
                | "ansi"
                | "linux"
        );
        if portable {
            raw
        } else {
            "xterm-256color".to_string()
        }
    };

    // Let the first PTY request trigger the existing server-side wake-up path
    // for suspended sandboxes instead of resuming from the CLI first.
    // Create PTY session via proxy API
    let pty_payload =
        build_pty_create_payload(shell, shell_args, workdir, &term_val, rows, cols, env_dict)?;

    let pty_resp = client
        .post(format!("{}/api/v1/pty", proxy_base))
        .json(&pty_payload)
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
    let mut raw_guard = Some(RawModeGuard);

    // Send READY to tell the server to flush buffered output.
    ws_write
        .send(tungstenite::Message::Binary(vec![OP_READY].into()))
        .await
        .map_err(|e| CliError::Other(anyhow::anyhow!("failed to send READY: {}", e)))?;

    let (result, needs_prompt_newline) = async {
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

        // Read stdin on a detached OS thread instead of `tokio::io::stdin()`.
        // Tokio backs TTY stdin with a blocking read that cannot be cancelled,
        // so aborting that task can leave the CLI hung until the user presses
        // Enter again. A detached thread may stay blocked, but it does not keep
        // the process alive once the SSH command finishes.
        let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<std::io::Result<Vec<u8>>>();
        std::thread::spawn(move || {
            let mut stdin = std::io::stdin().lock();
            let mut buf = [0u8; 4096];

            loop {
                match stdin.read(&mut buf) {
                    Ok(0) => {
                        let _ = stdin_tx.send(Ok(Vec::new()));
                        break;
                    }
                    Ok(n) => {
                        if stdin_tx.send(Ok(buf[..n].to_vec())).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = stdin_tx.send(Err(error));
                        break;
                    }
                }
            }
        });

        // All keyboard-generated bytes are forwarded as OP_DATA — raw mode clears
        // ISIG and ICANON so control characters (Ctrl+C=0x03, Ctrl+L=0x0C, etc.)
        // become plain bytes that the remote PTY's line discipline handles.
        //
        // Process-level signals are consolidated into a single typed channel so
        // the main select loop is platform-neutral. On non-Unix the channel is
        // never sent to, so the signal arm in select! blocks indefinitely.
        #[derive(Clone, Copy)]
        enum PosixSignal {
            WindowChange,
            Hangup,
            Terminate,
        }

        let (sig_tx, mut sig_rx) = mpsc::unbounded_channel::<PosixSignal>();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            macro_rules! forward_signal {
                ($kind:expr, $variant:expr) => {{
                    let tx = sig_tx.clone();
                    let mut stream = signal($kind).expect("failed to register signal handler");
                    tokio::spawn(async move {
                        loop {
                            if stream.recv().await.is_none() || tx.send($variant).is_err() {
                                break;
                            }
                        }
                    });
                }};
            }

            forward_signal!(SignalKind::window_change(), PosixSignal::WindowChange);
            forward_signal!(SignalKind::hangup(), PosixSignal::Hangup);
            forward_signal!(SignalKind::terminate(), PosixSignal::Terminate);
        }

        // Hold one sender so the channel stays open until the loop exits.
        let _sig_tx = sig_tx;

        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

        // Main loop: stdin -> WebSocket
        let mut server_exit_code: Option<i32> = None;
        let mut terminated_by_remote = false;

        loop {
            tokio::select! {
                maybe_stdin = stdin_rx.recv() => {
                    match maybe_stdin {
                        Some(Ok(data)) if data.is_empty() => break,
                        Some(Ok(data)) => {
                            let mut msg = vec![OP_DATA];
                            msg.extend_from_slice(&data);
                            if ws_write.send(tungstenite::Message::Binary(msg.into())).await.is_err() {
                                break;
                            }
                        }
                        Some(Err(_)) | None => break,
                    }
                }
                exit_code = &mut reader_handle => {
                    server_exit_code = exit_code.unwrap_or(None);
                    terminated_by_remote = true;
                    break;
                }
                posix_signal = sig_rx.recv() => match posix_signal {
                    Some(PosixSignal::WindowChange) => {
                        let (new_cols, new_rows) = crossterm::terminal::size().unwrap_or((80, 24));
                        let mut msg = vec![OP_RESIZE];
                        msg.extend_from_slice(&new_cols.to_be_bytes());
                        msg.extend_from_slice(&new_rows.to_be_bytes());
                        if ws_write.send(tungstenite::Message::Binary(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(PosixSignal::Hangup | PosixSignal::Terminate) => break,
                    None => {}
                },
                _ = &mut ctrl_c => {
                    // External SIGINT (e.g. kill -INT from another terminal).
                    server_exit_code = Some(130);
                    break;
                }
            }
        }

        // Send close frame so the server decrements client_count immediately
        // instead of waiting for the ping/pong timeout.
        let _ = ws_write
            .send(tungstenite::Message::Close(None))
            .await;

        // Restore cooked mode before any shutdown wait so typed input is not
        // swallowed if the process takes a brief moment to finish closing.
        drop(raw_guard.take());

        // If the reader hasn't finished yet, give it a short grace period to
        // receive a server-side exit signal or close frame. Abort afterwards so
        // local disconnects do not hang indefinitely on a broken close handshake.
        if server_exit_code.is_none() && !terminated_by_remote {
            let (exit_code, reader_completed) = wait_for_reader_shutdown(&mut reader_handle).await?;
            server_exit_code = exit_code;
            terminated_by_remote = terminated_by_remote || reader_completed;
        }

        Ok::<(Option<i32>, bool), CliError>((server_exit_code, !terminated_by_remote))
    }
    .await?;

    drop(raw_guard.take());

    if needs_prompt_newline {
        eprintln!();
    }

    if let Some(code) = result
        && code != 0
    {
        return Err(CliError::ExitCode(code));
    }

    Ok(())
}

fn build_pty_create_payload(
    shell: &str,
    shell_args: &[String],
    workdir: Option<&str>,
    term: &str,
    rows: u16,
    cols: u16,
    user_env: Option<serde_json::Value>,
) -> Result<serde_json::Value> {
    let mut env_map = serde_json::Map::new();
    env_map.insert(
        "TERM".to_string(),
        serde_json::Value::String(term.to_string()),
    );
    env_map.insert(
        "COLORTERM".to_string(),
        serde_json::Value::String("truecolor".to_string()),
    );

    if let Some(user_env_value) = user_env {
        let serde_json::Value::Object(user_env_map) = user_env_value else {
            return Err(CliError::Other(anyhow::anyhow!(
                "invalid env payload for PTY session"
            )));
        };
        for (key, value) in user_env_map {
            env_map.insert(key, value);
        }
    }

    let mut payload = serde_json::json!({
        "command": shell,
        "rows": rows,
        "cols": cols,
        "env": serde_json::Value::Object(env_map),
    });

    if !shell_args.is_empty() {
        payload["args"] = serde_json::json!(shell_args);
    }
    if let Some(path) = workdir {
        payload["working_dir"] = serde_json::Value::String(path.to_string());
    }

    Ok(payload)
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
) -> Result<(Option<i32>, bool)> {
    match tokio::time::timeout(PTY_CLOSE_WAIT_TIMEOUT, &mut *reader_handle).await {
        Ok(join_result) => join_result
            .map(|exit_code| (exit_code, true))
            .map_err(|error| CliError::Other(anyhow::anyhow!("PTY reader task failed: {}", error))),
        Err(_) => {
            reader_handle.abort();
            let _ = reader_handle.await;
            Ok((None, false))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OP_DATA, OP_EXIT, PTY_CLOSE_WAIT_TIMEOUT, PtyBinaryFrame, build_pty_create_payload,
        parse_legacy_exit_code, parse_pty_binary_frame, wait_for_reader_shutdown,
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

    #[test]
    fn build_pty_create_payload_includes_shell_args_and_workdir() {
        let payload = build_pty_create_payload(
            "/bin/zsh",
            &[String::from("-l"), String::from("-c")],
            Some("/tmp/work"),
            "xterm-256color",
            40,
            120,
            None,
        )
        .unwrap();

        assert_eq!(payload["command"], "/bin/zsh");
        assert_eq!(payload["args"], serde_json::json!(["-l", "-c"]));
        assert_eq!(payload["working_dir"], "/tmp/work");
        assert_eq!(payload["rows"], 40);
        assert_eq!(payload["cols"], 120);
    }

    #[test]
    fn build_pty_create_payload_merges_env_and_allows_overrides() {
        let payload = build_pty_create_payload(
            "/bin/bash",
            &[],
            None,
            "xterm-256color",
            24,
            80,
            Some(serde_json::json!({
                "FOO": "bar",
                "TERM": "screen-256color",
                "COLORTERM": "24bit",
            })),
        )
        .unwrap();

        assert_eq!(payload["env"]["FOO"], "bar");
        assert_eq!(payload["env"]["TERM"], "screen-256color");
        assert_eq!(payload["env"]["COLORTERM"], "24bit");
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

        assert_eq!(result, (None, false));
        assert!(reader_handle.is_finished());
    }
}
