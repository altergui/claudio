//! `UpdateClaude`: run `claude update` (or the official installer when claude
//! is missing) on this host, then re-probe `claude --version`.
//!
//! The command runs as a task of its own, never inline in a client loop (the
//! reply is queued like `Attach`'s), under a deadline and with its output
//! bounded to the last few KiB. Running sessions are never touched: they keep
//! the binary they started with, new ones pick up the new one.

use std::ffi::OsStr;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

use super::Daemon;
use crate::claude::SESSION_MARKERS;
use crate::proto::Msg;
use crate::term::strip_escapes;

/// How long the update may run.
const TIMEOUT: Duration = Duration::from_secs(300);

/// After a timeout's SIGTERM, how long the update gets before SIGKILL.
const KILL_GRACE: Duration = Duration::from_secs(2);

/// Output kept, counted from the end.
const TAIL_BYTES: usize = 4096;

/// Anthropic's official installer. Only ever run after an explicit `y`.
const INSTALLER: &str = "{ curl -fsSL https://claude.ai/install.sh | bash; } 2>&1";

/// Run the update (or the install) and answer `ClaudeUpdated`. Only one runs
/// at a time per daemon.
pub async fn run(daemon: &Daemon, install: bool) -> Msg {
    let Ok(_running) = daemon.updating.try_lock() else {
        return Msg::Error {
            message: "a claude update is already running on this host".into(),
        };
    };
    let claude = daemon.config.claude_bin();
    let path = daemon.config.path.as_deref();
    let (ok, tail) = match execute(&claude, install, path, TIMEOUT).await {
        Ok(done) => done,
        Err(e) => (false, e),
    };
    Msg::ClaudeUpdated {
        version: daemon.refresh_host().await.claude.map(|c| c.version),
        ok,
        tail,
    }
}

/// `(succeeded, output tail)`, or a description of why it could not run.
///
/// The command runs in a process group of its own: the installer is a
/// pipeline (`curl | bash`) whose `bash` starts more processes, and killing
/// only `sh` on timeout would leave those running, still installing.
///
/// `path` (the daemon's login-shell `PATH`) is the update's `PATH`: an npm
/// install updates itself with the user's `npm`.
async fn execute(
    claude: &Path,
    install: bool,
    path: Option<&OsStr>,
    timeout: Duration,
) -> Result<(bool, String), String> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c");
    if install {
        cmd.arg(INSTALLER);
    } else {
        // `$0` is the claude path, so it needs no quoting; stderr joins stdout.
        cmd.arg("exec \"$0\" update 2>&1").arg(claude);
    }
    for marker in SESSION_MARKERS {
        cmd.env_remove(marker);
    }
    if let Some(path) = path {
        cmd.env("PATH", path);
    }
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("could not start the update: {e}"))?;
    let mut out = child.stdout.take().ok_or("no output pipe")?;

    let finished = tokio::time::timeout(timeout, async {
        let mut tail = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            match out.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    tail.extend_from_slice(&buf[..n]);
                    let excess = tail.len().saturating_sub(TAIL_BYTES);
                    tail.drain(..excess);
                }
            }
        }
        (child.wait().await.is_ok_and(|s| s.success()), tail)
    })
    .await;
    match finished {
        Ok((ok, tail)) => Ok((ok, printable(&tail))),
        Err(_) => {
            kill_group(&mut child).await;
            Err(format!("timed out after {} s", timeout.as_secs()))
        }
    }
}

/// Stop the update's whole process group: SIGTERM, a grace period for the
/// leader to exit, then SIGKILL for whatever is left. Reaps the leader.
async fn kill_group(child: &mut Child) {
    // The leader's pid is the group id (`process_group(0)`); `None` once
    // it has been reaped, and then its pid may already be reused.
    let Some(pgid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        return;
    };
    // SAFETY: kill(2) has no memory-safety preconditions.
    unsafe { libc::kill(-pgid, libc::SIGTERM) };
    let _ = tokio::time::timeout(KILL_GRACE, child.wait()).await;
    // The leader may have exited while its descendants did not. A group id
    // is not reused while any member is alive, so this reaches only them.
    // SAFETY: as above.
    unsafe { libc::kill(-pgid, libc::SIGKILL) };
    let _ = child.wait().await;
}

/// Output as text: escape sequences and other control characters dropped.
fn printable(bytes: &[u8]) -> String {
    strip_escapes(bytes)
        .chars()
        .filter(|&c| c == '\n' || !c.is_control())
        .collect::<String>()
        .trim()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_is_stripped_of_escapes_and_control_characters() {
        assert_eq!(
            printable(b"\x1b[1mUpdated\x1b[0m\r\nto 2.1.296\x1b]0;title\x07\n"),
            "Updated\nto 2.1.296"
        );
        assert_eq!(printable(b"  \n"), "");
    }

    /// On timeout the whole process group goes, not just `sh`: a descendant
    /// that would write a file later never gets to.
    #[tokio::test]
    async fn timeout_kills_the_updaters_descendants() {
        let dir = std::env::temp_dir().join(format!("claudio-update-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let marker = dir.join("late");
        let claude = dir.join("claude");
        let script = format!(
            "#!/bin/sh\n\
             ( sleep 1; touch '{}' ) &\n\
             echo updating\n\
             sleep 30\n",
            marker.display()
        );
        std::fs::write(&claude, script).unwrap();
        std::fs::set_permissions(&claude, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let started = std::time::Instant::now();
        let result = execute(&claude, false, None, Duration::from_millis(300)).await;
        assert!(matches!(&result, Err(e) if e.contains("timed out")), "{result:?}");
        assert!(started.elapsed() < Duration::from_secs(5), "the leader was reaped promptly");

        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(!marker.exists(), "a descendant outlived the timeout");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
