//! Minimal HTTP/1.1 framing helpers for hand-rolled mock servers in tests.
//!
//! Hand-rolled rather than a mock-server crate because these tests assert on the raw
//! request bytes the client puts on the wire (header casing, framing, multipart body
//! content), which parsed-request mocks don't expose.
//!
//! Compiled twice on purpose: as `crate::test_support` for in-crate unit tests, and via
//! a `#[path]` module in `tests/common.rs` for integration tests.
#![allow(dead_code)]

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Read one HTTP/1.1 request — headers through CRLFCRLF plus a `content-length` body —
/// from a connection carrying a single request. If the peer closes early, returns the
/// partial bytes so the caller's assertions fail with what actually arrived.
pub async fn read_http_request(socket: &mut TcpStream) -> Vec<u8> {
    let mut carry = Vec::new();
    match read_http_request_keep_alive(socket, &mut carry).await {
        Some(request) => request,
        None => carry,
    }
}

/// Read one HTTP/1.1 request from a keep-alive connection. `carry` holds bytes beyond
/// the returned request between calls. Returns `None` once the peer closes or errors
/// before a complete request arrives; partial bytes remain in `carry`.
pub async fn read_http_request_keep_alive(
    socket: &mut TcpStream,
    carry: &mut Vec<u8>,
) -> Option<Vec<u8>> {
    let head_end = loop {
        if let Some(pos) = carry.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
        let mut buf = [0_u8; 4096];
        match socket.read(&mut buf).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => carry.extend_from_slice(&buf[..read]),
        }
    };

    let headers = String::from_utf8_lossy(&carry[..head_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);

    while carry.len() < head_end + content_length {
        let mut buf = [0_u8; 4096];
        match socket.read(&mut buf).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => carry.extend_from_slice(&buf[..read]),
        }
    }

    Some(carry.drain(..head_end + content_length).collect())
}

/// Write a bodyless 204 and close the connection.
pub async fn write_empty_response(socket: &mut TcpStream) -> std::io::Result<()> {
    socket
        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await
}

/// Write `body` as a 200 JSON response and close the connection, so pooled clients open
/// a fresh connection for their next request.
pub async fn write_json_response(socket: &mut TcpStream, body: &str) -> std::io::Result<()> {
    write_json(socket, "200 OK", body, false).await
}

/// Write `body` as a JSON response with the given status line (e.g. "202 Accepted"),
/// leaving the connection open for further requests.
pub async fn write_json_response_keep_alive(
    socket: &mut TcpStream,
    status: &str,
    body: &str,
) -> std::io::Result<()> {
    write_json(socket, status, body, true).await
}

async fn write_json(
    socket: &mut TcpStream,
    status: &str,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    let connection = if keep_alive {
        ""
    } else {
        "Connection: close\r\n"
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{connection}\r\n{body}",
        body.len(),
    );
    socket.write_all(response.as_bytes()).await
}
