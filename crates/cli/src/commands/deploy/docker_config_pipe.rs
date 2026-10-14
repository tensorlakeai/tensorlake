//! Dedicated, anonymous pipe for an optional protocol-v1 Docker config snapshot.

use std::io::{self, PipeReader, PipeWriter};

const PIPE_ENV: &str = "TENSORLAKE_DOCKER_CONFIG_PIPE";

/// Keep the reader alive until spawn completes, then close the parent's copy.
/// Only the descriptor/handle number is exposed in the child's environment.
pub(super) fn prepare(
    command: &mut tokio::process::Command,
) -> io::Result<(PipeReader, PipeWriter)> {
    let (reader, writer) = io::pipe()?;
    inherit_reader(command, &reader)?;
    Ok((reader, writer))
}

#[cfg(unix)]
fn inherit_reader(command: &mut tokio::process::Command, reader: &PipeReader) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let fd = reader.as_raw_fd();
    command.env(PIPE_ENV, fd.to_string());
    // SAFETY: The reader remains open until spawn returns. fcntl is async-signal-safe;
    // changing only the child's descriptor avoids leaking it into unrelated children.
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

#[cfg(windows)]
fn inherit_reader(command: &mut tokio::process::Command, reader: &PipeReader) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation};

    let handle = reader.as_raw_handle();
    // SAFETY: The reader owns a valid handle and remains alive until spawn completes.
    // Windows preserves the handle value when inheriting it into the child.
    if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) } == 0 {
        return Err(io::Error::last_os_error());
    }
    command.env(PIPE_ENV, (handle as usize).to_string());
    Ok(())
}
