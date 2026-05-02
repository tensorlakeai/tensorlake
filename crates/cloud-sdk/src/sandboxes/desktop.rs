use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use des::Des;
use des::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
use eventsource_stream::Eventsource;
use futures::{SinkExt, StreamExt};
use png::{BitDepth, ColorType};
use reqwest::Method;
use reqwest::header::{ACCEPT, HOST, HeaderMap};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::tungstenite::{self, Message, client::IntoClientRequest};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use url::Url;

use crate::{Client, error::SdkError};

/// How long each individual `bash`-builtin port probe is allowed to run inside
/// the sandbox before the daemon kills it. Two seconds comfortably covers a
/// successful TCP connect on a healthy port and bounds the cost of each retry
/// when the port is still refusing connections.
const PORT_PROBE_PROCESS_TIMEOUT_SECS: u64 = 2;

/// Pause between port-probe attempts. The probe itself is cheap (~5–10 ms when
/// the port is up), so the cap on probe rate is more about avoiding daemon
/// chatter than throughput.
const PORT_PROBE_RETRY_INTERVAL: Duration = Duration::from_millis(250);

const SECURITY_TYPE_NONE: u8 = 1;
const SECURITY_TYPE_VNC_AUTH: u8 = 2;
const ENCODING_RAW: i32 = 0;
const ENCODING_DESKTOP_SIZE: i32 = -223;
const BUTTON_LEFT_MASK: u8 = 1;
const BUTTON_MIDDLE_MASK: u8 = 1 << 1;
const BUTTON_RIGHT_MASK: u8 = 1 << 2;
const BUTTON_SCROLL_UP_MASK: u8 = 1 << 3;
const BUTTON_SCROLL_DOWN_MASK: u8 = 1 << 4;

#[derive(Clone)]
pub struct SandboxDesktopClient {
    session: Arc<Mutex<DesktopSession<TunnelConnection>>>,
}

impl SandboxDesktopClient {
    pub async fn connect(
        client: Client,
        host_override: Option<String>,
        port: u16,
        password: Option<String>,
        shared: bool,
        connect_timeout: Duration,
    ) -> Result<Self, SdkError> {
        // Wait for the VNC port to be reachable inside the sandbox before
        // attempting the WebSocket tunnel handshake. Without this, freshly
        // created sandboxes (where the in-VM `vncserver` systemd unit is
        // still starting) race the tunnel: the dataplane gets `Connection
        // refused` on 127.0.0.1:<port> and the proxy returns 502 before
        // VNC has had a chance to bind. The wait is bounded by
        // `connect_timeout` along with the WS handshake and VNC negotiation
        // that follow — total wall-clock is what the caller asked for.
        let session = timeout(connect_timeout, async move {
            wait_for_port_ready(&client, port).await?;
            let transport = TunnelConnection::connect(client, host_override, port).await?;
            DesktopSession::connect(transport, password, shared).await
        })
        .await
        .map_err(|_| {
            SdkError::ClientError(format!(
                "timed out while connecting desktop session after {:.2}s",
                connect_timeout.as_secs_f64()
            ))
        })??;

        Ok(Self {
            session: Arc::new(Mutex::new(session)),
        })
    }

    pub async fn close(&self) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.close().await
    }

    pub async fn screenshot(&self, timeout: Duration) -> Result<Vec<u8>, SdkError> {
        let mut session = self.session.lock().await;
        session.screenshot(timeout).await
    }

    pub async fn move_mouse(&self, x: u16, y: u16) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.move_mouse(x, y).await
    }

    pub async fn mouse_press(
        &self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
    ) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.mouse_press(button, x, y).await
    }

    pub async fn mouse_release(
        &self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
    ) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.mouse_release(button, x, y).await
    }

    pub async fn click(
        &self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
    ) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.click(button, x, y).await
    }

    pub async fn double_click(
        &self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
        delay_ms: u64,
    ) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.double_click(button, x, y, delay_ms).await
    }

    pub async fn scroll(&self, steps: i32, x: Option<u16>, y: Option<u16>) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.scroll(steps, x, y).await
    }

    pub async fn key_down(&self, key: &str) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.key_down(key).await
    }

    pub async fn key_up(&self, key: &str) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.key_up(key).await
    }

    pub async fn press(&self, keys: &[String]) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.press(keys).await
    }

    pub async fn type_text(&self, text: &str) -> Result<(), SdkError> {
        let mut session = self.session.lock().await;
        session.type_text(text).await
    }

    pub async fn dimensions(&self) -> Result<(u16, u16), SdkError> {
        let session = self.session.lock().await;
        Ok((session.width, session.height))
    }
}

/// Poll the in-sandbox daemon until the requested port accepts a TCP connection
/// from `127.0.0.1`. The probe runs `bash -c 'exec 3<>/dev/tcp/127.0.0.1/<port>'`
/// via the daemon's `/api/v1/processes/run` endpoint; bash's `/dev/tcp` builtin
/// returns exit 0 when the connect succeeds and non-zero on `Connection
/// refused` / timeout. `bash` is present on every sandbox image we ship.
///
/// Loops forever; the caller is expected to wrap this in an outer
/// `tokio::time::timeout` (see [`SandboxDesktopClient::connect`]). Treats
/// transient errors (e.g. SSE stream hiccups) as "not ready yet" and retries;
/// only persistent SDK errors that bubble out of the underlying request will
/// surface here.
async fn wait_for_port_ready(client: &Client, port: u16) -> Result<(), SdkError> {
    loop {
        match probe_port_once(client, port).await {
            Ok(true) => return Ok(()),
            Ok(false) | Err(_) => sleep(PORT_PROBE_RETRY_INTERVAL).await,
        }
    }
}

/// One probe attempt. Returns `Ok(true)` if `bash`'s `/dev/tcp` connect
/// succeeded (exit 0), `Ok(false)` otherwise. Errors are returned for
/// non-process-level failures (transport, JSON shape) so the caller can decide
/// whether to retry.
async fn probe_port_once(client: &Client, port: u16) -> Result<bool, SdkError> {
    let payload = json!({
        "command": "/bin/bash",
        "args": ["-c", format!("exec 3<>/dev/tcp/127.0.0.1/{port}")],
        "timeout_secs": PORT_PROBE_PROCESS_TIMEOUT_SECS,
    });
    let req = client
        .request(Method::POST, "/api/v1/processes/run")
        .header(ACCEPT, "text/event-stream")
        .json(&payload)
        .build()?;
    let response = client.execute(req).await?;
    let mut stream = response.bytes_stream().eventsource();
    while let Some(event) = stream.next().await {
        let msg = match event {
            Ok(m) => m,
            // Mid-stream transport errors should not be fatal — treat as
            // "not ready, try again".
            Err(_) => return Ok(false),
        };
        let parsed: serde_json::Value = match serde_json::from_str(&msg.data) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(exit_code) = parsed.get("exit_code").and_then(|v| v.as_i64()) {
            return Ok(exit_code == 0);
        }
        if parsed.get("signal").is_some() {
            // Killed by signal (e.g. SIGTERM from timeout) — treat as not ready.
            return Ok(false);
        }
    }
    // Stream ended without an Exited event — daemon hiccup, retry.
    Ok(false)
}

struct TunnelConnection {
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    read_buffer: Vec<u8>,
}

impl TunnelConnection {
    async fn connect(
        client: Client,
        host_override: Option<String>,
        remote_port: u16,
    ) -> Result<Self, SdkError> {
        let ws_url = build_tunnel_url(client.base_url(), remote_port)?;
        let headers = build_tunnel_headers(client.default_headers(), host_override.as_deref())?;

        let mut request = ws_url.into_client_request().map_err(|error| {
            SdkError::ClientError(format!("failed to build tunnel request: {error}"))
        })?;

        for (name, value) in &headers {
            request.headers_mut().insert(name, value.clone());
        }

        let (ws_stream, _) = connect_async(request).await.map_err(map_ws_connect_error)?;

        Ok(Self {
            ws_stream,
            read_buffer: Vec::new(),
        })
    }

    async fn refill_buffer(&mut self) -> Result<(), SdkError> {
        loop {
            let message = self.ws_stream.next().await.ok_or_else(|| {
                SdkError::ClientError("desktop tunnel closed unexpectedly".to_string())
            })?;
            match message {
                Ok(Message::Binary(data)) => {
                    self.read_buffer.extend_from_slice(data.as_ref());
                    return Ok(());
                }
                Ok(Message::Ping(payload)) => {
                    self.ws_stream
                        .send(Message::Pong(payload))
                        .await
                        .map_err(map_ws_error)?;
                }
                Ok(Message::Pong(_)) => {}
                Ok(Message::Close(_)) => {
                    return Err(SdkError::ClientError(
                        "desktop tunnel closed by remote peer".to_string(),
                    ));
                }
                Ok(Message::Text(_)) => {
                    return Err(SdkError::ClientError(
                        "desktop tunnel received unexpected text frame".to_string(),
                    ));
                }
                Ok(Message::Frame(_)) => {}
                Err(error) => return Err(map_ws_error(error)),
            }
        }
    }
}

trait DesktopTransport {
    async fn read_exact(&mut self, len: usize) -> Result<Vec<u8>, SdkError>;
    async fn write_all(&mut self, data: &[u8]) -> Result<(), SdkError>;
    async fn close(&mut self) -> Result<(), SdkError>;
}

impl DesktopTransport for TunnelConnection {
    async fn read_exact(&mut self, len: usize) -> Result<Vec<u8>, SdkError> {
        while self.read_buffer.len() < len {
            self.refill_buffer().await?;
        }

        let data: Vec<u8> = self.read_buffer.drain(..len).collect();
        Ok(data)
    }

    async fn write_all(&mut self, data: &[u8]) -> Result<(), SdkError> {
        self.ws_stream
            .send(Message::Binary(data.to_vec().into()))
            .await
            .map_err(map_ws_error)
    }

    async fn close(&mut self) -> Result<(), SdkError> {
        self.ws_stream.close(None).await.map_err(map_ws_error)
    }
}

struct DesktopSession<T> {
    transport: T,
    width: u16,
    height: u16,
    pixel_format: PixelFormat,
    framebuffer: Vec<u8>,
    pointer_x: u16,
    pointer_y: u16,
    button_mask: u8,
}

impl<T> DesktopSession<T>
where
    T: DesktopTransport,
{
    async fn connect(
        mut transport: T,
        password: Option<String>,
        shared: bool,
    ) -> Result<Self, SdkError> {
        let server_version = ProtocolVersion::read(&mut transport).await?;
        let client_version = server_version.negotiated();
        transport
            .write_all(client_version.render().as_bytes())
            .await?;

        let selected_security =
            negotiate_security(&mut transport, client_version, password).await?;

        transport.write_all(&[u8::from(shared)]).await?;

        let init = ServerInit::read(&mut transport).await?;
        if !init.pixel_format.true_color {
            return Err(SdkError::ClientError(
                "desktop sessions require a true-color VNC pixel format".to_string(),
            ));
        }

        let pixel_format = PixelFormat::preferred();
        send_set_pixel_format(&mut transport, &pixel_format).await?;
        send_set_encodings(&mut transport, &[ENCODING_RAW, ENCODING_DESKTOP_SIZE]).await?;

        let framebuffer = allocate_framebuffer(init.width, init.height)?;
        let _ = selected_security;
        Ok(Self {
            transport,
            width: init.width,
            height: init.height,
            pixel_format,
            framebuffer,
            pointer_x: 0,
            pointer_y: 0,
            button_mask: 0,
        })
    }

    async fn close(&mut self) -> Result<(), SdkError> {
        self.transport.close().await
    }

    async fn screenshot(&mut self, timeout_duration: Duration) -> Result<Vec<u8>, SdkError> {
        let deadline = Instant::now() + timeout_duration;
        let mut needs_refresh = true;

        loop {
            if Instant::now() >= deadline {
                return Err(SdkError::ClientError(format!(
                    "timed out waiting for desktop screenshot after {:.2}s",
                    timeout_duration.as_secs_f64()
                )));
            }

            if needs_refresh {
                send_framebuffer_update_request(
                    &mut self.transport,
                    false,
                    0,
                    0,
                    self.width,
                    self.height,
                )
                .await?;
                needs_refresh = false;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let batch = timeout(remaining, self.read_server_message())
                .await
                .map_err(|_| {
                    SdkError::ClientError(format!(
                        "timed out waiting for desktop screenshot after {:.2}s",
                        timeout_duration.as_secs_f64()
                    ))
                })??;

            match batch {
                ServerMessageOutcome::FramebufferUpdate {
                    saw_resize,
                    saw_raw,
                } => {
                    if saw_resize && !saw_raw {
                        needs_refresh = true;
                        continue;
                    }
                    return self.encode_png();
                }
                ServerMessageOutcome::Bell | ServerMessageOutcome::ServerCutText => {}
            }
        }
    }

    async fn move_mouse(&mut self, x: u16, y: u16) -> Result<(), SdkError> {
        self.ensure_pointer_in_bounds(x, y)?;
        self.pointer_x = x;
        self.pointer_y = y;
        send_pointer_event(&mut self.transport, self.button_mask, x, y).await
    }

    async fn mouse_press(
        &mut self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
    ) -> Result<(), SdkError> {
        self.move_if_requested(x, y).await?;
        self.button_mask |= button_mask(button)?;
        send_pointer_event(
            &mut self.transport,
            self.button_mask,
            self.pointer_x,
            self.pointer_y,
        )
        .await
    }

    async fn mouse_release(
        &mut self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
    ) -> Result<(), SdkError> {
        self.move_if_requested(x, y).await?;
        self.button_mask &= !button_mask(button)?;
        send_pointer_event(
            &mut self.transport,
            self.button_mask,
            self.pointer_x,
            self.pointer_y,
        )
        .await
    }

    async fn click(
        &mut self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
    ) -> Result<(), SdkError> {
        self.mouse_press(button, x, y).await?;
        self.mouse_release(button, None, None).await
    }

    async fn double_click(
        &mut self,
        button: &str,
        x: Option<u16>,
        y: Option<u16>,
        delay_ms: u64,
    ) -> Result<(), SdkError> {
        self.click(button, x, y).await?;
        sleep(Duration::from_millis(delay_ms)).await;
        self.click(button, None, None).await
    }

    async fn scroll(&mut self, steps: i32, x: Option<u16>, y: Option<u16>) -> Result<(), SdkError> {
        self.move_if_requested(x, y).await?;

        if steps == 0 {
            return Ok(());
        }

        let (wheel_mask, step_count) = if steps > 0 {
            (
                BUTTON_SCROLL_UP_MASK,
                usize::try_from(steps).expect("positive i32 fits into usize"),
            )
        } else {
            (
                BUTTON_SCROLL_DOWN_MASK,
                usize::try_from(steps.unsigned_abs()).expect("u32 fits into usize"),
            )
        };

        for _ in 0..step_count {
            let pressed_mask = self.button_mask | wheel_mask;
            send_pointer_event(
                &mut self.transport,
                pressed_mask,
                self.pointer_x,
                self.pointer_y,
            )
            .await?;
            send_pointer_event(
                &mut self.transport,
                self.button_mask,
                self.pointer_x,
                self.pointer_y,
            )
            .await?;
        }

        Ok(())
    }

    async fn key_down(&mut self, key: &str) -> Result<(), SdkError> {
        send_key_event(&mut self.transport, true, keysym_from_key_name(key)?).await
    }

    async fn key_up(&mut self, key: &str) -> Result<(), SdkError> {
        send_key_event(&mut self.transport, false, keysym_from_key_name(key)?).await
    }

    async fn press(&mut self, keys: &[String]) -> Result<(), SdkError> {
        if keys.is_empty() {
            return Err(SdkError::ClientError(
                "desktop press requires at least one key".to_string(),
            ));
        }

        if keys.len() == 1 {
            let key = keysym_from_key_name(&keys[0])?;
            send_key_event(&mut self.transport, true, key).await?;
            send_key_event(&mut self.transport, false, key).await?;
            return Ok(());
        }

        let mut keysyms = Vec::with_capacity(keys.len());
        for key in keys {
            keysyms.push(keysym_from_key_name(key)?);
        }

        for keysym in &keysyms[..keysyms.len() - 1] {
            send_key_event(&mut self.transport, true, *keysym).await?;
        }

        let last = *keysyms.last().ok_or_else(|| {
            SdkError::ClientError("desktop press requires at least one key".to_string())
        })?;
        send_key_event(&mut self.transport, true, last).await?;
        send_key_event(&mut self.transport, false, last).await?;

        for keysym in keysyms[..keysyms.len() - 1].iter().rev() {
            send_key_event(&mut self.transport, false, *keysym).await?;
        }

        Ok(())
    }

    async fn type_text(&mut self, text: &str) -> Result<(), SdkError> {
        for ch in text.chars() {
            let keysym = keysym_from_char(ch)?;
            send_key_event(&mut self.transport, true, keysym).await?;
            send_key_event(&mut self.transport, false, keysym).await?;
        }
        Ok(())
    }

    async fn move_if_requested(&mut self, x: Option<u16>, y: Option<u16>) -> Result<(), SdkError> {
        match (x, y) {
            (Some(x), Some(y)) => self.move_mouse(x, y).await,
            (None, None) => Ok(()),
            _ => Err(SdkError::ClientError(
                "desktop pointer actions require both x and y when specifying coordinates"
                    .to_string(),
            )),
        }
    }

    fn ensure_pointer_in_bounds(&self, x: u16, y: u16) -> Result<(), SdkError> {
        if self.width > 0 && x >= self.width {
            return Err(SdkError::ClientError(format!(
                "mouse x coordinate {x} is outside desktop width {}",
                self.width
            )));
        }
        if self.height > 0 && y >= self.height {
            return Err(SdkError::ClientError(format!(
                "mouse y coordinate {y} is outside desktop height {}",
                self.height
            )));
        }
        Ok(())
    }

    async fn read_server_message(&mut self) -> Result<ServerMessageOutcome, SdkError> {
        let message_type = read_u8(&mut self.transport).await?;
        match message_type {
            0 => self.read_framebuffer_update().await,
            1 => {
                self.read_set_color_map_entries().await?;
                Ok(ServerMessageOutcome::Bell)
            }
            2 => Ok(ServerMessageOutcome::Bell),
            3 => {
                self.read_server_cut_text().await?;
                Ok(ServerMessageOutcome::ServerCutText)
            }
            other => Err(SdkError::ClientError(format!(
                "unsupported VNC server message type {other}"
            ))),
        }
    }

    async fn read_framebuffer_update(&mut self) -> Result<ServerMessageOutcome, SdkError> {
        let _padding = read_u8(&mut self.transport).await?;
        let rectangle_count = read_u16(&mut self.transport).await?;
        let mut saw_raw = false;
        let mut saw_resize = false;

        for _ in 0..rectangle_count {
            let x = read_u16(&mut self.transport).await?;
            let y = read_u16(&mut self.transport).await?;
            let width = read_u16(&mut self.transport).await?;
            let height = read_u16(&mut self.transport).await?;
            let encoding = read_i32(&mut self.transport).await?;
            match encoding {
                ENCODING_RAW => {
                    let bytes_per_pixel = self.pixel_format.bytes_per_pixel();
                    let length = usize::from(width)
                        .checked_mul(usize::from(height))
                        .and_then(|count| count.checked_mul(bytes_per_pixel))
                        .ok_or_else(|| {
                            SdkError::ClientError(
                                "desktop raw rectangle size exceeds supported bounds".to_string(),
                            )
                        })?;
                    let data = self.transport.read_exact(length).await?;
                    self.blit_raw_rectangle(x, y, width, height, &data)?;
                    saw_raw = true;
                }
                ENCODING_DESKTOP_SIZE => {
                    self.resize_framebuffer(width, height)?;
                    saw_resize = true;
                }
                other => {
                    return Err(SdkError::ClientError(format!(
                        "unsupported VNC rectangle encoding {other}"
                    )));
                }
            }
        }

        Ok(ServerMessageOutcome::FramebufferUpdate {
            saw_resize,
            saw_raw,
        })
    }

    async fn read_set_color_map_entries(&mut self) -> Result<(), SdkError> {
        let _padding = read_u8(&mut self.transport).await?;
        let _first_color = read_u16(&mut self.transport).await?;
        let color_count = read_u16(&mut self.transport).await?;
        let byte_count = usize::from(color_count).checked_mul(6).ok_or_else(|| {
            SdkError::ClientError("desktop color map update exceeds supported bounds".to_string())
        })?;
        let _ = self.transport.read_exact(byte_count).await?;
        Err(SdkError::ClientError(
            "desktop sessions do not support color-map VNC pixel formats".to_string(),
        ))
    }

    async fn read_server_cut_text(&mut self) -> Result<(), SdkError> {
        let _ = self.transport.read_exact(3).await?;
        let len = read_u32(&mut self.transport).await?;
        let _ = self.transport.read_exact(len as usize).await?;
        Ok(())
    }

    fn resize_framebuffer(&mut self, width: u16, height: u16) -> Result<(), SdkError> {
        self.width = width;
        self.height = height;
        self.framebuffer = allocate_framebuffer(width, height)?;
        if width > 0 {
            self.pointer_x = self.pointer_x.min(width - 1);
        } else {
            self.pointer_x = 0;
        }
        if height > 0 {
            self.pointer_y = self.pointer_y.min(height - 1);
        } else {
            self.pointer_y = 0;
        }
        Ok(())
    }

    fn blit_raw_rectangle(
        &mut self,
        x: u16,
        y: u16,
        width: u16,
        height: u16,
        data: &[u8],
    ) -> Result<(), SdkError> {
        if x.checked_add(width).is_none_or(|value| value > self.width)
            || y.checked_add(height)
                .is_none_or(|value| value > self.height)
        {
            return Err(SdkError::ClientError(
                "desktop raw rectangle exceeds framebuffer bounds".to_string(),
            ));
        }

        let bytes_per_pixel = self.pixel_format.bytes_per_pixel();
        for row in 0..usize::from(height) {
            for col in 0..usize::from(width) {
                let src_index = (row * usize::from(width) + col) * bytes_per_pixel;
                let rgba = self
                    .pixel_format
                    .decode_pixel(&data[src_index..src_index + bytes_per_pixel])?;
                let dst_index =
                    ((usize::from(y) + row) * usize::from(self.width) + usize::from(x) + col) * 4;
                self.framebuffer[dst_index..dst_index + 4].copy_from_slice(&rgba);
            }
        }
        Ok(())
    }

    fn encode_png(&self) -> Result<Vec<u8>, SdkError> {
        let mut out = Vec::new();
        {
            let mut encoder =
                png::Encoder::new(&mut out, u32::from(self.width), u32::from(self.height));
            encoder.set_color(ColorType::Rgba);
            encoder.set_depth(BitDepth::Eight);
            let mut writer = encoder.write_header().map_err(|error| {
                SdkError::ClientError(format!("failed to encode desktop screenshot: {error}"))
            })?;
            writer
                .write_image_data(&self.framebuffer)
                .map_err(|error| {
                    SdkError::ClientError(format!("failed to encode desktop screenshot: {error}"))
                })?;
        }
        Ok(out)
    }
}

enum ServerMessageOutcome {
    FramebufferUpdate { saw_resize: bool, saw_raw: bool },
    Bell,
    ServerCutText,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProtocolVersion {
    major: u16,
    minor: u16,
}

impl ProtocolVersion {
    async fn read<T: DesktopTransport>(transport: &mut T) -> Result<Self, SdkError> {
        let raw = transport.read_exact(12).await?;
        let text = String::from_utf8(raw).map_err(|error| {
            SdkError::ClientError(format!("invalid VNC protocol banner: {error}"))
        })?;
        let parts: Vec<&str> = text.trim_end_matches('\n').split([' ', '.']).collect();
        if parts.len() != 3 || parts[0] != "RFB" {
            return Err(SdkError::ClientError(format!(
                "invalid VNC protocol banner `{text}`"
            )));
        }
        let major = parts[1].parse::<u16>().map_err(|error| {
            SdkError::ClientError(format!("invalid VNC protocol major version: {error}"))
        })?;
        let minor = parts[2].parse::<u16>().map_err(|error| {
            SdkError::ClientError(format!("invalid VNC protocol minor version: {error}"))
        })?;
        Ok(Self { major, minor })
    }

    fn negotiated(self) -> Self {
        if self.major != 3 || self.minor >= 8 {
            Self { major: 3, minor: 8 }
        } else if self.minor >= 7 {
            Self { major: 3, minor: 7 }
        } else {
            Self { major: 3, minor: 3 }
        }
    }

    fn render(self) -> String {
        format!("RFB {:03}.{:03}\n", self.major, self.minor)
    }
}

#[derive(Clone, Copy)]
struct PixelFormat {
    bits_per_pixel: u8,
    depth: u8,
    big_endian: bool,
    true_color: bool,
    red_max: u16,
    green_max: u16,
    blue_max: u16,
    red_shift: u8,
    green_shift: u8,
    blue_shift: u8,
}

impl PixelFormat {
    fn preferred() -> Self {
        Self {
            bits_per_pixel: 32,
            depth: 24,
            big_endian: false,
            true_color: true,
            red_max: 255,
            green_max: 255,
            blue_max: 255,
            red_shift: 16,
            green_shift: 8,
            blue_shift: 0,
        }
    }

    fn bytes_per_pixel(self) -> usize {
        usize::from(self.bits_per_pixel / 8)
    }

    fn decode_pixel(self, bytes: &[u8]) -> Result<[u8; 4], SdkError> {
        let bytes_per_pixel = self.bytes_per_pixel();
        if bytes.len() != bytes_per_pixel {
            return Err(SdkError::ClientError(
                "desktop pixel buffer has an unexpected size".to_string(),
            ));
        }

        let mut value = 0u32;
        if self.big_endian {
            for byte in bytes {
                value = (value << 8) | u32::from(*byte);
            }
        } else {
            for (index, byte) in bytes.iter().enumerate() {
                value |= u32::from(*byte) << (index * 8);
            }
        }

        let red = scale_channel(
            (value >> self.red_shift) & u32::from(self.red_max),
            self.red_max,
        )?;
        let green = scale_channel(
            (value >> self.green_shift) & u32::from(self.green_max),
            self.green_max,
        )?;
        let blue = scale_channel(
            (value >> self.blue_shift) & u32::from(self.blue_max),
            self.blue_max,
        )?;
        Ok([red, green, blue, 255])
    }

    fn encode(self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0] = self.bits_per_pixel;
        bytes[1] = self.depth;
        bytes[2] = u8::from(self.big_endian);
        bytes[3] = u8::from(self.true_color);
        bytes[4..6].copy_from_slice(&self.red_max.to_be_bytes());
        bytes[6..8].copy_from_slice(&self.green_max.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.blue_max.to_be_bytes());
        bytes[10] = self.red_shift;
        bytes[11] = self.green_shift;
        bytes[12] = self.blue_shift;
        bytes
    }
}

struct ServerInit {
    width: u16,
    height: u16,
    pixel_format: PixelFormat,
}

impl ServerInit {
    async fn read<T: DesktopTransport>(transport: &mut T) -> Result<Self, SdkError> {
        let width = read_u16(transport).await?;
        let height = read_u16(transport).await?;
        let pixel_format_bytes = transport.read_exact(16).await?;
        let pixel_format = parse_pixel_format(&pixel_format_bytes)?;
        let name_len = read_u32(transport).await?;
        let _name = transport.read_exact(name_len as usize).await?;
        Ok(Self {
            width,
            height,
            pixel_format,
        })
    }
}

async fn negotiate_security<T: DesktopTransport>(
    transport: &mut T,
    version: ProtocolVersion,
    password: Option<String>,
) -> Result<u8, SdkError> {
    let security_types = if version.minor == 3 {
        let security_type = read_u32(transport).await?;
        if security_type == 0 {
            let reason_len = read_u32(transport).await?;
            let reason = String::from_utf8(transport.read_exact(reason_len as usize).await?)
                .unwrap_or_else(|_| "unknown reason".to_string());
            return Err(SdkError::ClientError(format!(
                "VNC security negotiation failed: {reason}"
            )));
        }
        vec![security_type as u8]
    } else {
        let security_type_count = read_u8(transport).await?;
        if security_type_count == 0 {
            let reason_len = read_u32(transport).await?;
            let reason = String::from_utf8(transport.read_exact(reason_len as usize).await?)
                .unwrap_or_else(|_| "unknown reason".to_string());
            return Err(SdkError::ClientError(format!(
                "VNC security negotiation failed: {reason}"
            )));
        }
        transport
            .read_exact(usize::from(security_type_count))
            .await?
    };

    let selected = if password.is_some() && security_types.contains(&SECURITY_TYPE_VNC_AUTH) {
        SECURITY_TYPE_VNC_AUTH
    } else if security_types.contains(&SECURITY_TYPE_NONE) {
        SECURITY_TYPE_NONE
    } else if security_types.contains(&SECURITY_TYPE_VNC_AUTH) {
        return Err(SdkError::ClientError(
            "VNC server requires password authentication but no password was provided".to_string(),
        ));
    } else {
        let advertised = security_types
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(SdkError::ClientError(format!(
            "unsupported VNC security types advertised by server: [{advertised}]"
        )));
    };

    if version.minor >= 7 {
        transport.write_all(&[selected]).await?;
    }

    if selected == SECURITY_TYPE_VNC_AUTH {
        let password = password.ok_or_else(|| {
            SdkError::ClientError(
                "VNC server requires password authentication but no password was provided"
                    .to_string(),
            )
        })?;
        let challenge = transport.read_exact(16).await?;
        let response = encrypt_vnc_challenge(password.as_bytes(), &challenge)?;
        transport.write_all(&response).await?;
        read_security_result(transport, version.minor >= 8).await?;
    } else if version.minor >= 8 {
        read_security_result(transport, true).await?;
    }

    Ok(selected)
}

fn encrypt_vnc_challenge(password: &[u8], challenge: &[u8]) -> Result<[u8; 16], SdkError> {
    if challenge.len() != 16 {
        return Err(SdkError::ClientError(
            "VNC authentication challenge must be 16 bytes".to_string(),
        ));
    }

    let mut key = [0u8; 8];
    for (index, byte) in password.iter().take(8).enumerate() {
        key[index] = reverse_bits(*byte);
    }
    let cipher = Des::new_from_slice(&key).map_err(|error| {
        SdkError::ClientError(format!(
            "failed to initialize VNC authentication cipher: {error}"
        ))
    })?;

    let mut output = [0u8; 16];
    for block_index in 0..2 {
        let start = block_index * 8;
        output[start..start + 8].copy_from_slice(&challenge[start..start + 8]);
        let mut block = GenericArray::clone_from_slice(&output[start..start + 8]);
        cipher.encrypt_block(&mut block);
        output[start..start + 8].copy_from_slice(&block);
    }
    Ok(output)
}

fn reverse_bits(value: u8) -> u8 {
    let mut reversed = 0u8;
    for bit in 0..8 {
        reversed |= ((value >> bit) & 1) << (7 - bit);
    }
    reversed
}

async fn read_security_result<T: DesktopTransport>(
    transport: &mut T,
    has_reason_string: bool,
) -> Result<(), SdkError> {
    let status = read_u32(transport).await?;
    if status == 0 {
        return Ok(());
    }

    let reason = if has_reason_string {
        let reason_len = read_u32(transport).await?;
        String::from_utf8(transport.read_exact(reason_len as usize).await?)
            .unwrap_or_else(|_| "authentication failed".to_string())
    } else if status == 1 {
        "authentication failed".to_string()
    } else {
        format!("security handshake failed with status {status}")
    };
    Err(SdkError::ClientError(format!(
        "VNC security negotiation failed: {reason}"
    )))
}

async fn send_set_pixel_format<T: DesktopTransport>(
    transport: &mut T,
    pixel_format: &PixelFormat,
) -> Result<(), SdkError> {
    let mut message = Vec::with_capacity(20);
    message.push(0);
    message.extend_from_slice(&[0, 0, 0]);
    message.extend_from_slice(&pixel_format.encode());
    transport.write_all(&message).await
}

async fn send_set_encodings<T: DesktopTransport>(
    transport: &mut T,
    encodings: &[i32],
) -> Result<(), SdkError> {
    let mut message = Vec::with_capacity(4 + encodings.len() * 4);
    message.push(2);
    message.push(0);
    message.extend_from_slice(&(encodings.len() as u16).to_be_bytes());
    for encoding in encodings {
        message.extend_from_slice(&encoding.to_be_bytes());
    }
    transport.write_all(&message).await
}

async fn send_framebuffer_update_request<T: DesktopTransport>(
    transport: &mut T,
    incremental: bool,
    x: u16,
    y: u16,
    width: u16,
    height: u16,
) -> Result<(), SdkError> {
    let mut message = Vec::with_capacity(10);
    message.push(3);
    message.push(u8::from(incremental));
    message.extend_from_slice(&x.to_be_bytes());
    message.extend_from_slice(&y.to_be_bytes());
    message.extend_from_slice(&width.to_be_bytes());
    message.extend_from_slice(&height.to_be_bytes());
    transport.write_all(&message).await
}

async fn send_pointer_event<T: DesktopTransport>(
    transport: &mut T,
    button_mask: u8,
    x: u16,
    y: u16,
) -> Result<(), SdkError> {
    let mut message = Vec::with_capacity(6);
    message.push(5);
    message.push(button_mask);
    message.extend_from_slice(&x.to_be_bytes());
    message.extend_from_slice(&y.to_be_bytes());
    transport.write_all(&message).await
}

async fn send_key_event<T: DesktopTransport>(
    transport: &mut T,
    down: bool,
    keysym: u32,
) -> Result<(), SdkError> {
    let mut message = Vec::with_capacity(8);
    message.push(4);
    message.push(u8::from(down));
    message.extend_from_slice(&[0, 0]);
    message.extend_from_slice(&keysym.to_be_bytes());
    transport.write_all(&message).await
}

async fn read_u8<T: DesktopTransport>(transport: &mut T) -> Result<u8, SdkError> {
    Ok(transport.read_exact(1).await?[0])
}

async fn read_u16<T: DesktopTransport>(transport: &mut T) -> Result<u16, SdkError> {
    let bytes = transport.read_exact(2).await?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

async fn read_u32<T: DesktopTransport>(transport: &mut T) -> Result<u32, SdkError> {
    let bytes = transport.read_exact(4).await?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

async fn read_i32<T: DesktopTransport>(transport: &mut T) -> Result<i32, SdkError> {
    let bytes = transport.read_exact(4).await?;
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn parse_pixel_format(bytes: &[u8]) -> Result<PixelFormat, SdkError> {
    if bytes.len() != 16 {
        return Err(SdkError::ClientError(
            "invalid VNC pixel format payload length".to_string(),
        ));
    }
    Ok(PixelFormat {
        bits_per_pixel: bytes[0],
        depth: bytes[1],
        big_endian: bytes[2] != 0,
        true_color: bytes[3] != 0,
        red_max: u16::from_be_bytes([bytes[4], bytes[5]]),
        green_max: u16::from_be_bytes([bytes[6], bytes[7]]),
        blue_max: u16::from_be_bytes([bytes[8], bytes[9]]),
        red_shift: bytes[10],
        green_shift: bytes[11],
        blue_shift: bytes[12],
    })
}

fn scale_channel(value: u32, max: u16) -> Result<u8, SdkError> {
    if max == 0 {
        return Err(SdkError::ClientError(
            "invalid VNC pixel format with zero channel range".to_string(),
        ));
    }
    Ok(((value * 255) / u32::from(max)) as u8)
}

fn allocate_framebuffer(width: u16, height: u16) -> Result<Vec<u8>, SdkError> {
    let len = usize::from(width)
        .checked_mul(usize::from(height))
        .and_then(|size| size.checked_mul(4))
        .ok_or_else(|| {
            SdkError::ClientError("desktop framebuffer size exceeds supported bounds".to_string())
        })?;
    Ok(vec![0; len])
}

fn build_tunnel_url(base_url: &str, remote_port: u16) -> Result<String, SdkError> {
    let mut url = Url::parse(base_url).map_err(|error| {
        SdkError::ClientError(format!(
            "invalid sandbox proxy base URL `{base_url}`: {error}"
        ))
    })?;
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    url.set_scheme(scheme).map_err(|_| {
        SdkError::ClientError("failed to convert proxy URL to websocket URL".to_string())
    })?;
    url.set_path("/api/v1/tunnels/tcp");
    url.set_query(Some(&format!("port={remote_port}")));
    Ok(url.to_string())
}

fn build_tunnel_headers(
    mut headers: HeaderMap,
    host_override: Option<&str>,
) -> Result<HeaderMap, SdkError> {
    if let Some(host) = host_override {
        headers.insert(
            HOST,
            host.parse::<reqwest::header::HeaderValue>()
                .map_err(|error| SdkError::InvalidHeaderValue(error.to_string()))?,
        );
    }
    Ok(headers)
}

fn map_ws_connect_error(error: tungstenite::Error) -> SdkError {
    match error {
        tungstenite::Error::Http(response) => SdkError::ClientError(format!(
            "desktop tunnel websocket handshake failed with HTTP {}",
            response.status()
        )),
        other => map_ws_error(other),
    }
}

fn map_ws_error(error: tungstenite::Error) -> SdkError {
    match error {
        tungstenite::Error::Io(io_error) => SdkError::Io(io_error),
        other => SdkError::ClientError(format!("desktop tunnel websocket error: {other}")),
    }
}

fn button_mask(button: &str) -> Result<u8, SdkError> {
    match button.to_ascii_lowercase().as_str() {
        "left" => Ok(BUTTON_LEFT_MASK),
        "middle" => Ok(BUTTON_MIDDLE_MASK),
        "right" => Ok(BUTTON_RIGHT_MASK),
        other => Err(SdkError::ClientError(format!(
            "unsupported mouse button `{other}`; expected left, middle, or right"
        ))),
    }
}

fn keysym_from_key_name(key: &str) -> Result<u32, SdkError> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Err(SdkError::ClientError(
            "desktop key name cannot be empty".to_string(),
        ));
    }

    if trimmed.chars().count() == 1 {
        return keysym_from_char(trimmed.chars().next().expect("single char"));
    }

    let normalized = trimmed.to_ascii_lowercase();
    if let Some(value) = special_keysyms().get(normalized.as_str()) {
        return Ok(*value);
    }

    if let Some(function_number) = normalized.strip_prefix('f')
        && let Ok(number) = function_number.parse::<u8>()
        && (1..=12).contains(&number)
    {
        return Ok(0xffbd + u32::from(number));
    }

    Err(SdkError::ClientError(format!(
        "unsupported desktop key `{trimmed}`"
    )))
}

fn special_keysyms() -> &'static HashMap<&'static str, u32> {
    static SPECIAL_KEYS: OnceLock<HashMap<&'static str, u32>> = OnceLock::new();
    SPECIAL_KEYS.get_or_init(|| {
        HashMap::from([
            ("enter", 0xff0d),
            ("tab", 0xff09),
            ("escape", 0xff1b),
            ("backspace", 0xff08),
            ("delete", 0xffff),
            ("space", 0x0020),
            ("up", 0xff52),
            ("down", 0xff54),
            ("left", 0xff51),
            ("right", 0xff53),
            ("home", 0xff50),
            ("end", 0xff57),
            ("page_up", 0xff55),
            ("page_down", 0xff56),
            ("shift", 0xffe1),
            ("ctrl", 0xffe3),
            ("control", 0xffe3),
            ("alt", 0xffe9),
            ("meta", 0xffe7),
        ])
    })
}

fn keysym_from_char(ch: char) -> Result<u32, SdkError> {
    match ch {
        '\n' | '\r' => Ok(0xff0d),
        '\t' => Ok(0xff09),
        '\u{8}' => Ok(0xff08),
        ch if (' '..='~').contains(&ch) => Ok(ch as u32),
        ch if ch.is_ascii_control() => Err(SdkError::ClientError(format!(
            "unsupported control character U+{:04X} for desktop typing",
            ch as u32
        ))),
        ch => Ok(0x0100_0000 | (ch as u32)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    struct MockTransport {
        read_cursor: Cursor<Vec<u8>>,
        writes: Vec<u8>,
        closed: bool,
    }

    impl MockTransport {
        fn new(read_bytes: Vec<u8>) -> Self {
            Self {
                read_cursor: Cursor::new(read_bytes),
                writes: Vec::new(),
                closed: false,
            }
        }
    }

    impl DesktopTransport for MockTransport {
        async fn read_exact(&mut self, len: usize) -> Result<Vec<u8>, SdkError> {
            let mut out = vec![0; len];
            self.read_cursor
                .read_exact(&mut out)
                .map_err(SdkError::Io)?;
            Ok(out)
        }

        async fn write_all(&mut self, data: &[u8]) -> Result<(), SdkError> {
            self.writes.extend_from_slice(data);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), SdkError> {
            self.closed = true;
            Ok(())
        }
    }

    fn server_init_bytes(width: u16, height: u16, true_color: bool) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        let mut pixel_format = PixelFormat::preferred().encode();
        pixel_format[3] = u8::from(true_color);
        bytes.extend_from_slice(&pixel_format);
        bytes.extend_from_slice(&(4u32).to_be_bytes());
        bytes.extend_from_slice(b"Test");
        bytes
    }

    fn raw_framebuffer_update(width: u16, height: u16, pixels: &[[u8; 4]]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(0);
        bytes.push(0);
        bytes.extend_from_slice(&(1u16).to_be_bytes());
        bytes.extend_from_slice(&(0u16).to_be_bytes());
        bytes.extend_from_slice(&(0u16).to_be_bytes());
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&ENCODING_RAW.to_be_bytes());
        for [r, g, b, _] in pixels {
            bytes.extend_from_slice(&[*b, *g, *r, 0]);
        }
        bytes
    }

    fn desktop_size_update(width: u16, height: u16) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(0);
        bytes.push(0);
        bytes.extend_from_slice(&(1u16).to_be_bytes());
        bytes.extend_from_slice(&(0u16).to_be_bytes());
        bytes.extend_from_slice(&(0u16).to_be_bytes());
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&ENCODING_DESKTOP_SIZE.to_be_bytes());
        bytes
    }

    async fn connect_session(
        read_bytes: Vec<u8>,
        password: Option<&str>,
    ) -> Result<DesktopSession<MockTransport>, SdkError> {
        let transport = MockTransport::new(read_bytes);
        DesktopSession::connect(transport, password.map(str::to_string), true).await
    }

    #[tokio::test]
    async fn handshake_supports_none_security() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(1);
        bytes.push(SECURITY_TYPE_NONE);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&server_init_bytes(2, 1, true));

        let session = connect_session(bytes, None).await.unwrap();
        let writes = &session.transport.writes;
        assert!(writes.starts_with(b"RFB 003.008\n"));
        assert_eq!(session.width, 2);
        assert_eq!(session.height, 1);
    }

    #[tokio::test]
    async fn handshake_supports_vnc_password_auth() {
        let challenge = [0x11u8; 16];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(2);
        bytes.extend_from_slice(&[SECURITY_TYPE_NONE, SECURITY_TYPE_VNC_AUTH]);
        bytes.extend_from_slice(&challenge);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&server_init_bytes(1, 1, true));

        let session = connect_session(bytes, Some("secret")).await.unwrap();
        let writes = &session.transport.writes;
        assert_eq!(writes[12], SECURITY_TYPE_VNC_AUTH);
        let expected = encrypt_vnc_challenge(b"secret", &challenge).unwrap();
        assert_eq!(&writes[13..29], &expected);
    }

    #[tokio::test]
    async fn missing_password_is_rejected_when_vnc_auth_is_required() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(1);
        bytes.push(SECURITY_TYPE_VNC_AUTH);

        let error = match connect_session(bytes, None).await {
            Err(error) => error,
            Ok(_) => panic!("expected VNC auth failure"),
        };
        assert!(
            error
                .to_string()
                .contains("requires password authentication")
        );
    }

    #[tokio::test]
    async fn unsupported_security_types_are_reported() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(1);
        bytes.push(19);

        let error = match connect_session(bytes, None).await {
            Err(error) => error,
            Ok(_) => panic!("expected unsupported security type failure"),
        };
        assert!(error.to_string().contains("unsupported VNC security types"));
    }

    #[tokio::test]
    async fn color_map_pixel_formats_are_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(1);
        bytes.push(SECURITY_TYPE_NONE);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&server_init_bytes(1, 1, false));

        let error = match connect_session(bytes, None).await {
            Err(error) => error,
            Ok(_) => panic!("expected color-map pixel format failure"),
        };
        assert!(error.to_string().contains("true-color"));
    }

    #[tokio::test]
    async fn screenshot_decodes_raw_framebuffer_and_returns_png() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(1);
        bytes.push(SECURITY_TYPE_NONE);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&server_init_bytes(2, 1, true));
        bytes.extend_from_slice(&raw_framebuffer_update(
            2,
            1,
            &[[255, 0, 0, 255], [0, 255, 0, 255]],
        ));

        let mut session = connect_session(bytes, None).await.unwrap();
        let png = session.screenshot(Duration::from_secs(1)).await.unwrap();
        let decoder = png::Decoder::new(Cursor::new(png));
        let mut reader = decoder.read_info().unwrap();
        let mut buffer = vec![0; reader.output_buffer_size().unwrap_or(0)];
        let info = reader.next_frame(&mut buffer).unwrap();
        assert_eq!(info.width, 2);
        assert_eq!(info.height, 1);
        assert_eq!(&buffer[..8], &[255, 0, 0, 255, 0, 255, 0, 255]);
    }

    #[tokio::test]
    async fn screenshot_handles_desktop_resize() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(1);
        bytes.push(SECURITY_TYPE_NONE);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&server_init_bytes(1, 1, true));
        bytes.extend_from_slice(&desktop_size_update(2, 1));
        bytes.extend_from_slice(&raw_framebuffer_update(
            2,
            1,
            &[[10, 20, 30, 255], [40, 50, 60, 255]],
        ));

        let mut session = connect_session(bytes, None).await.unwrap();
        let _ = session.screenshot(Duration::from_secs(1)).await.unwrap();
        assert_eq!(session.width, 2);
        assert_eq!(session.height, 1);
        assert!(
            session
                .transport
                .writes
                .windows(10)
                .filter(|chunk| chunk[0] == 3)
                .count()
                >= 2
        );
    }

    #[tokio::test]
    async fn pointer_events_cover_left_middle_right_and_double_click() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(1);
        bytes.push(SECURITY_TYPE_NONE);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&server_init_bytes(10, 10, true));

        let mut session = connect_session(bytes, None).await.unwrap();
        session.move_mouse(4, 5).await.unwrap();
        session.click("middle", None, None).await.unwrap();
        session
            .double_click("left", Some(7), Some(8), 0)
            .await
            .unwrap();
        session.click("right", None, None).await.unwrap();
        session.scroll(2, None, None).await.unwrap();
        session.scroll(-1, Some(9), Some(6)).await.unwrap();

        let writes = &session.transport.writes;
        assert!(writes.windows(6).any(|chunk| chunk == [5, 2, 0, 4, 0, 5]));
        assert!(writes.windows(6).any(|chunk| chunk == [5, 1, 0, 7, 0, 8]));
        assert!(writes.windows(6).any(|chunk| chunk == [5, 4, 0, 7, 0, 8]));
        assert_eq!(
            writes
                .windows(6)
                .filter(|chunk| *chunk == [5, 8, 0, 7, 0, 8])
                .count(),
            2
        );
        assert!(writes.windows(6).any(|chunk| chunk == [5, 16, 0, 9, 0, 6]));
        assert!(writes.windows(6).any(|chunk| chunk == [5, 0, 0, 9, 0, 6]));
    }

    #[tokio::test]
    async fn key_events_cover_printable_named_and_combo_keys() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RFB 003.008\n");
        bytes.push(1);
        bytes.push(SECURITY_TYPE_NONE);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&server_init_bytes(10, 10, true));

        let mut session = connect_session(bytes, None).await.unwrap();
        session.key_down("a").await.unwrap();
        session.key_up("a").await.unwrap();
        session
            .press(&["ctrl".to_string(), "c".to_string()])
            .await
            .unwrap();
        session.key_down("enter").await.unwrap();
        session.type_text("Aé").await.unwrap();

        let writes = &session.transport.writes;
        assert!(
            writes
                .windows(8)
                .any(|chunk| chunk == [4, 1, 0, 0, 0, 0, 0, b'a'])
        );
        assert!(
            writes
                .windows(8)
                .any(|chunk| chunk == [4, 1, 0, 0, 0, 0, 0xff, 0xe3])
        );
        assert!(
            writes
                .windows(8)
                .any(|chunk| chunk == [4, 1, 0, 0, 0, 0, 0xff, 0x0d])
        );
        assert!(
            writes
                .windows(8)
                .any(|chunk| chunk == [4, 1, 0, 0, 0x01, 0x00, 0x00, 0xe9])
        );
    }
}
