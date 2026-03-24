use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::header::ACCEPT;
use sha2::{Digest, Sha256};

use crate::auth::context::CliContext;
use crate::cache::KvCache;
use crate::error::{CliError, Result};

pub async fn run(
    ctx: &CliContext,
    path_or_url: &str,
    pages: Option<&str>,
    ignore_cache: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let base = format!("{}/documents/v2", ctx.api_url.trim_end_matches('/'));
    let page_key = pages
        .map(|p| p.trim().to_string())
        .unwrap_or_else(|| "all".to_string());

    let is_url = path_or_url.starts_with("http://") || path_or_url.starts_with("https://");

    // Determine cache identity and, for local files, read contents now (needed for hash).
    let (cache_identity, file_upload) = if is_url {
        (format!("url:{}", path_or_url), None)
    } else {
        let path = std::path::Path::new(path_or_url);
        if !path.exists() || !path.is_file() {
            return Err(CliError::usage(format!("File not found: {}", path_or_url)));
        }
        let contents = tokio::fs::read(path).await.map_err(CliError::Io)?;
        let hash = hex::encode(Sha256::digest(&contents));
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("document")
            .to_string();
        (format!("file:{}", hash), Some((filename, contents)))
    };

    let cache_key = format!("{}|pages:{}", cache_identity, page_key);
    let cache = KvCache::new("parse");

    if !ignore_cache {
        if let Some(cached) = cache.get(&cache_key).await {
            print!("{}", cached);
            return Ok(());
        }
    }

    // Resolve to file_id (upload) or keep as file_url.
    let source = if let Some((filename, contents)) = file_upload {
        eprintln!("Uploading {}...", filename);
        let form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(contents).file_name(filename),
        );
        let resp = client
            .put(format!("{}/files", base))
            .multipart(form)
            .send()
            .await
            .map_err(CliError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let msg = resp.text().await.unwrap_or_default();
            return Err(CliError::usage(format!(
                "Upload failed ({}): {}",
                status, msg
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
        let file_id = body["file_id"]
            .as_str()
            .ok_or_else(|| CliError::usage("Upload response missing file_id"))?
            .to_string();
        ParseSource::FileId(file_id)
    } else {
        ParseSource::Url(path_or_url.to_string())
    };

    eprintln!("Parsing...");

    // Build parse request body.
    let mut parse_body = serde_json::json!({
        "parsing_options": {
            "chunking_strategy": "fragment",
            "table_output_mode": "markdown"
        }
    });
    match &source {
        ParseSource::FileId(id) => {
            parse_body["file_id"] = serde_json::Value::String(id.clone());
        }
        ParseSource::Url(url) => {
            parse_body["file_url"] = serde_json::Value::String(url.clone());
        }
    }
    if let Some(pages) = pages {
        let trimmed = pages.trim();
        if !trimmed.is_empty() {
            parse_body["page_range"] = serde_json::Value::String(trimmed.to_string());
        }
    }

    let parse_resp = client
        .post(format!("{}/parse", base))
        .json(&parse_body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !parse_resp.status().is_success() {
        let status = parse_resp.status();
        let msg = parse_resp.text().await.unwrap_or_default();
        return Err(CliError::usage(format!(
            "Parse request failed ({}): {}",
            status, msg
        )));
    }

    let parse_resp_body: serde_json::Value = parse_resp.json().await.map_err(CliError::Http)?;
    let parse_id = parse_resp_body["parse_id"]
        .as_str()
        .ok_or_else(|| CliError::usage("Parse response missing parse_id"))?
        .to_string();

    // Stream SSE events until parse_done or parse_failed.
    let sse_resp = client
        .get(format!("{}/parse/{}", base, parse_id))
        .header(ACCEPT, "text/event-stream")
        .send()
        .await
        .map_err(CliError::Http)?;

    if !sse_resp.status().is_success() {
        let status = sse_resp.status();
        let msg = sse_resp.text().await.unwrap_or_default();
        return Err(CliError::usage(format!(
            "SSE stream failed ({}): {}",
            status, msg
        )));
    }

    let stream = sse_resp.bytes_stream().eventsource();
    futures::pin_mut!(stream);

    let mut result: Option<String> = None;

    while let Some(event) = stream.next().await {
        let event = event.map_err(|e| CliError::usage(format!("SSE stream error: {}", e)))?;

        match event.event.as_str() {
            "parse_done" => {
                let data: serde_json::Value =
                    serde_json::from_str(&event.data).unwrap_or(serde_json::Value::Null);
                let markdown = data["chunks"]
                    .as_array()
                    .map(|chunks| {
                        chunks
                            .iter()
                            .filter_map(|c| c["content"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n\n")
                    })
                    .unwrap_or_default();
                result = Some(markdown);
                break;
            }
            "parse_failed" => {
                let data: serde_json::Value =
                    serde_json::from_str(&event.data).unwrap_or(serde_json::Value::Null);
                let err = data["error"].as_str().unwrap_or("unknown error");
                return Err(CliError::usage(format!("Parse failed: {}", err)));
            }
            _ => {} // parse_update, parse_queued — ignore
        }
    }

    let markdown =
        result.ok_or_else(|| CliError::usage("Parse stream ended without a result event"))?;

    cache.set(&cache_key, &markdown).await;
    print!("{}", markdown);

    Ok(())
}

enum ParseSource {
    FileId(String),
    Url(String),
}
