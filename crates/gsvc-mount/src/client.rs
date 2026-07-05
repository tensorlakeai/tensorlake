//! HTTP client for the artifact-storage native filesystem API.
//!
//! Every method maps 1:1 onto a `gsvc-server` product endpoint; the response types here mirror the
//! server's wire shapes (`crates/gsvc-server/src/http.rs`) and must stay in sync with them. All
//! content addressed by a commit oid is immutable, so callers cache responses keyed by
//! `(commit, path)` without invalidation; the only mutable read is [`FsClient::ref_status`].

use std::time::Duration;

use bytes::Bytes;
use serde::Deserialize;

use crate::MountError;

/// One page of a directory listing, as served by `GET .../tree[/{path}]?version=&after=&limit=`.
#[derive(Clone, Debug, Deserialize)]
pub struct TreePage {
    pub entries: Vec<TreeEntry>,
    pub truncated: bool,
    #[serde(default)]
    pub next_after: Option<String>,
    /// The listed tree's own oid (hex). `None` against servers predating the field.
    #[serde(default)]
    pub tree_oid: Option<String>,
}

/// One directory entry. `size` is present for blobs when cheaply known server-side; trees and
/// not-yet-indexed blobs carry `None` (a not-yet-indexed blob's size resolves via [`FsClient::stat`]).
#[derive(Clone, Debug, Deserialize)]
pub struct TreeEntry {
    pub name: String,
    pub oid: String,
    /// Git mode: `0o40000` dir, `0o100644`/`0o100755` file, `0o120000` symlink.
    pub mode: u32,
    #[serde(default)]
    pub size: Option<u64>,
}

impl TreeEntry {
    pub fn is_dir(&self) -> bool {
        self.mode == 0o40000
    }
    pub fn is_symlink(&self) -> bool {
        self.mode == 0o120000
    }
}

/// File metadata from `HEAD .../files/{path}?version=` response headers.
#[derive(Clone, Debug)]
pub struct FileStat {
    pub oid: String,
    pub mode: u32,
    pub size: u64,
}

/// A ref's head and movement generation, from `GET .../ref-status?ref=`.
#[derive(Clone, Debug, Deserialize)]
pub struct RefStatus {
    pub ref_name: String,
    #[serde(default)]
    pub oid: Option<String>,
    pub generation: u64,
}

/// Client for one repo's native filesystem API.
///
/// `base` is the server origin (e.g. `https://git.tensorlake.ai`); `project`/`repo` scope every
/// request. Auth is the git-style HTTP Basic token minted by the platform (`t:<token>`), matching
/// what the fast-clone client already sends.
#[derive(Clone)]
pub struct FsClient {
    http: reqwest::Client,
    base: String,
    project: String,
    repo: String,
    /// Basic-auth token, behind a shared lock so a long-lived mount can rotate a minted
    /// credential in place: kernel-held inodes make rebuilding the client (and the core above
    /// it) impossible once a mount is live. Clones share the slot, so rotation reaches every
    /// user of the client.
    token: std::sync::Arc<std::sync::RwLock<Option<String>>>,
}

impl FsClient {
    pub fn new(
        base: impl Into<String>,
        project: impl Into<String>,
        repo: impl Into<String>,
        token: Option<String>,
    ) -> Result<FsClient, MountError> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(MountError::Http)?;
        Ok(FsClient {
            http,
            base: base.into().trim_end_matches('/').to_string(),
            project: project.into(),
            repo: repo.into(),
            token: std::sync::Arc::new(std::sync::RwLock::new(token)),
        })
    }

    /// Replace the basic-auth token. Requests started before the swap finish with the old
    /// credential; everything after uses the new one.
    pub fn set_token(&self, token: Option<String>) {
        *self.token.write().expect("token lock") = token;
    }

    fn control(&self, tail: &str) -> String {
        format!(
            "{}/project/{}/repos/{}/{tail}",
            self.base, self.project, self.repo
        )
    }

    fn get(&self, url: String) -> reqwest::RequestBuilder {
        self.with_auth(self.http.get(url))
    }

    fn with_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.token.read().expect("token lock").as_deref() {
            Some(token) => req.basic_auth("t", Some(token)),
            None => req,
        }
    }

    /// Map a non-success response to a [`MountError`], consuming the body as the message.
    async fn error_for(resp: reqwest::Response) -> MountError {
        let status = resp.status().as_u16();
        let message = resp.text().await.unwrap_or_default();
        match status {
            404 => MountError::NotFound(message),
            425 => MountError::IndexNotReady(message),
            _ => MountError::Status { status, message },
        }
    }

    /// The ref's current head and movement generation — the only mutable read in the API.
    pub async fn ref_status(&self, refspec: &str) -> Result<RefStatus, MountError> {
        let url = format!("{}?ref={}", self.control("ref-status"), urlencode(refspec));
        let resp = self.get(url).send().await.map_err(MountError::Http)?;
        if !resp.status().is_success() {
            return Err(Self::error_for(resp).await);
        }
        resp.json().await.map_err(MountError::Http)
    }

    /// One page of a directory's entries **by tree oid** — no version resolution or path walk
    /// server-side. The oid comes from a parent listing already read.
    pub async fn tree_page_by_oid(
        &self,
        tree_oid: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<TreePage, MountError> {
        let mut url = format!(
            "{}?tree={}&limit={limit}",
            self.control("tree"),
            urlencode(tree_oid)
        );
        if let Some(after) = after {
            url.push_str("&after=");
            url.push_str(&urlencode(after));
        }
        let resp = self.get(url).send().await.map_err(MountError::Http)?;
        if !resp.status().is_success() {
            return Err(Self::error_for(resp).await);
        }
        resp.json().await.map_err(MountError::Http)
    }

    /// One page of `dir_path`'s entries at `version`, starting strictly after cursor `after`.
    pub async fn tree_page(
        &self,
        version: &str,
        dir_path: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<TreePage, MountError> {
        let tail = if dir_path.is_empty() {
            "tree".to_string()
        } else {
            format!("tree/{}", urlencode_path(dir_path))
        };
        let mut url = format!(
            "{}?version={}&limit={limit}",
            self.control(&tail),
            urlencode(version)
        );
        if let Some(after) = after {
            url.push_str("&after=");
            url.push_str(&urlencode(after));
        }
        let resp = self.get(url).send().await.map_err(MountError::Http)?;
        if !resp.status().is_success() {
            return Err(Self::error_for(resp).await);
        }
        resp.json().await.map_err(MountError::Http)
    }

    /// File metadata without the body (`HEAD .../files/{path}`).
    pub async fn stat(&self, version: &str, path: &str) -> Result<FileStat, MountError> {
        let url = format!(
            "{}?version={}",
            self.control(&format!("files/{}", urlencode_path(path))),
            urlencode(version)
        );
        let resp = self
            .with_auth(self.http.head(url))
            .send()
            .await
            .map_err(MountError::Http)?;
        if !resp.status().is_success() {
            return Err(Self::error_for(resp).await);
        }
        stat_from_headers(resp.headers())
    }

    /// One byte range of a file. `len == 0` is answered locally as empty. Ranges past EOF are
    /// clamped by the server (`206`/`200`); a fully out-of-range read returns empty bytes.
    pub async fn read_range(
        &self,
        version: &str,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<Bytes, MountError> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let url = format!(
            "{}?version={}",
            self.control(&format!("files/{}", urlencode_path(path))),
            urlencode(version)
        );
        let end = offset.saturating_add(len).saturating_sub(1);
        let resp = self
            .get(url)
            .header("Range", format!("bytes={offset}-{end}"))
            .send()
            .await
            .map_err(MountError::Http)?;
        if resp.status().as_u16() == 416 {
            // Requested range entirely past EOF: a read at/after the end of file is empty.
            return Ok(Bytes::new());
        }
        if !resp.status().is_success() {
            return Err(Self::error_for(resp).await);
        }
        // A server that ignores Range answers 200 with the whole file; slice locally so callers
        // always get exactly the requested span.
        let full = resp.status().as_u16() == 200;
        let body = resp.bytes().await.map_err(MountError::Http)?;
        if full {
            let start = (offset as usize).min(body.len());
            let stop = (offset.saturating_add(len) as usize).min(body.len());
            return Ok(body.slice(start..stop));
        }
        Ok(body)
    }
}

fn stat_from_headers(headers: &reqwest::header::HeaderMap) -> Result<FileStat, MountError> {
    let header = |name: &str| -> Result<String, MountError> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| MountError::Protocol(format!("missing {name} header")))
    };
    let oid = header("x-gsvc-object-id")?;
    let mode = u32::from_str_radix(&header("x-gsvc-mode")?, 8)
        .map_err(|_| MountError::Protocol("bad x-gsvc-mode".to_string()))?;
    let size = header("content-length")?
        .parse::<u64>()
        .map_err(|_| MountError::Protocol("bad content-length".to_string()))?;
    Ok(FileStat { oid, mode, size })
}

/// Percent-encode a query value (conservative: everything but unreserved characters).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Percent-encode a path for a URL path segment sequence, preserving `/` separators.
fn urlencode_path(path: &str) -> String {
    path.split('/').map(urlencode).collect::<Vec<_>>().join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlencode_preserves_unreserved_and_escapes_the_rest() {
        assert_eq!(urlencode("refs/heads/main"), "refs%2Fheads%2Fmain");
        assert_eq!(urlencode_path("dir a/b#c.txt"), "dir%20a/b%23c.txt");
        assert_eq!(urlencode("abc-_.~123"), "abc-_.~123");
    }
}
