pub mod applications;
pub mod build_images;
pub mod cron;
pub mod deploy;
// Local FUSE/overlay mount stack — depends on the private gsvc-mount core, so it is only
// compiled into official `--features mount` release builds. See crates/cli/Cargo.toml.
#[cfg(feature = "mount")]
pub mod fs;
pub mod git;
pub mod init;
pub mod login;
pub mod new;
pub mod parse;
pub mod sbx;
pub mod secrets;
pub mod ssh_keys;
pub mod whoami;

use std::sync::Arc;

use tensorlake::artifact_storage::ingest::PushEvent;

/// Build a `PushEvent` progress sink that narrates a push/commit onto `bar`.
///
/// Shared by `tl git push` and `tl fs snapshot` so the two commands report identical,
/// in-lockstep progress wording — a new `PushEvent` variant or a phrasing tweak lands in one
/// place. Every arm only sets the spinner message, except the detached-commit job id, which is
/// printed directly so it survives a hidden (piped/non-TTY) bar where the out-of-band id matters.
pub(crate) fn push_progress_spinner(
    bar: &indicatif::ProgressBar,
) -> Arc<dyn Fn(PushEvent) + Send + Sync> {
    let bar = bar.clone();
    Arc::new(move |ev| {
        use indicatif::HumanBytes;
        match ev {
            PushEvent::Chunking {
                files_done,
                files_total,
                bytes_hashed,
            } => bar.set_message(format!(
                "hashing {files_done}/{files_total} files ({})...",
                HumanBytes(bytes_hashed)
            )),
            PushEvent::Hashed {
                files,
                chunks,
                bytes,
            } => bar.set_message(format!(
                "hashed {files} files ({chunks} chunks, {}); asking the server what it already has...",
                HumanBytes(bytes)
            )),
            PushEvent::Negotiated { missing, total } => bar.set_message(format!(
                "uploading {missing} of {total} chunks (rest already stored)..."
            )),
            PushEvent::UploadedBatch { chunks, bytes } => {
                bar.set_message(format!("uploaded {chunks} chunks ({})...", HumanBytes(bytes)))
            }
            PushEvent::Committing { files } => bar.set_message(format!(
                "all bytes on the server; committing {files} files (server builds the tree)..."
            )),
            PushEvent::CommitDetached { job_id } => {
                let line = format!(
                    "commit running as job {job_id} (survives disconnects; check from anywhere \
                     with: tl git commit-status <repo> {job_id})"
                );
                if bar.is_hidden() {
                    println!("{line}");
                } else {
                    bar.println(line);
                }
            }
            PushEvent::CommitProgress { phase, done, total } => {
                if total > 0 {
                    bar.set_message(format!("committing: {phase} {done}/{total} chunks..."))
                } else {
                    bar.set_message(format!("committing: {phase}..."))
                }
            }
            PushEvent::Committed { ref_name, .. } => {
                bar.set_message(format!("committed to {ref_name}"))
            }
        }
    })
}
