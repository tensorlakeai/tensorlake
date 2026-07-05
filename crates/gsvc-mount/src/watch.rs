//! Ref-follow poller: one small `ref-status` read per interval, refreshing the mount root when
//! the generation journal says the ref moved. Distinct from any per-read revalidation — content
//! reads never revalidate, because everything below a commit is immutable.

use std::sync::Arc;

use crate::core::{MountCore, RefreshDelta};

/// Spawn the follow-mode poller. Returns immediately for pinned mounts. The task ends when the
/// core is dropped (it holds only a weak reference). `on_refresh` fires after each root swap with
/// the refresh's invalidation delta — the exact rebound and staled inos a binding should push to
/// the kernel (`inval_inode` / `inval_entry`), which is what makes long kernel entry/attr TTLs
/// safe (see the crate docs for the full contract).
pub fn spawn_ref_watcher(
    core: &Arc<MountCore>,
    on_refresh: impl Fn(RefreshDelta) + Send + Sync + 'static,
) -> Option<tokio::task::JoinHandle<()>> {
    if !core.follow() {
        return None;
    }
    let weak = Arc::downgrade(core);
    let interval = core.poll_interval();
    Some(tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let Some(core) = weak.upgrade() else {
                return;
            };
            match core.poll_ref().await {
                Ok(Some(delta)) => on_refresh(delta),
                Ok(None) => {}
                Err(e) => {
                    // Transient poll failures leave the mount serving its current commit; the
                    // next tick retries. This is the sustained-degradation posture a read-only
                    // mount wants: stale beats broken.
                    tracing::warn!(error = %e, "mount: ref poll failed; retaining current root");
                }
            }
        }
    }))
}
