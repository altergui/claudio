//! `claudio daemon <subcommand>` implementation.
//!
//! Daemon management commands: `status`, `stop`, `restart`.
//!
//! ## Stop strategy
//!
//! 1. Try an authenticated IPC `Shutdown` request over the control socket
//!    (uid-checked by the daemon).
//! 2. Fall back to the PID in the lock file, **only** after verifying that
//!    the process is our daemon (Linux: `/proc/<pid>/exe` == current exe;
//!    macOS: skip the fallback, IPC is always available when the daemon is up).
//!    Never signal an unverified PID.
//! 3. Wait up to 10 s for the socket to disappear.

use std::time::{Duration, Instant};

use crate::{client, paths, proto};

/// Dispatch a `claudio daemon <args>` invocation.
pub fn daemon_cmd(args: &[String]) -> std::process::ExitCode {
    match args.first().map(String::as_str) {
        Some("status") => daemon_status(),
        Some("stop") => daemon_stop(),
        Some("restart") => daemon_restart(),
        _ => {
            eprintln!("usage: claudio daemon <status|stop|restart>");
            std::process::ExitCode::from(2)
        }
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Read the daemon PID from the lock file. Returns `None` when the file is
/// absent, unreadable, or contains no parseable PID.
fn read_daemon_pid() -> Option<u32> {
    let lock = paths::daemon_lock();
    std::fs::read_to_string(&lock)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// Whether the daemon socket is currently connectable (by us).
fn daemon_socket_live() -> bool {
    std::os::unix::net::UnixStream::connect(paths::daemon_socket()).is_ok()
}

/// Wait up to `timeout` for the daemon socket to disappear (daemon has exited).
fn wait_for_daemon_exit(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if !daemon_socket_live() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The claude the daemon runs, as `daemon status` prints it.
fn claude_line(claude: Option<&proto::ClaudeInfo>) -> String {
    match claude {
        Some(c) => format!("{}, {}", c.path, c.version),
        None => "not found".to_owned(),
    }
}

// ── subcommands ───────────────────────────────────────────────────────────────

pub fn daemon_status() -> std::process::ExitCode {
    let pid = read_daemon_pid();
    let live = daemon_socket_live();

    if !live {
        println!("daemon: not running");
        return std::process::ExitCode::from(1);
    }

    let pid_str = pid.map(|p| p.to_string()).unwrap_or_else(|| "?".into());
    println!("daemon: running (pid {pid_str})");

    // Connect and get session count + version.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let rt = match rt {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("claudio daemon status: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    rt.block_on(async {
        match client::connect(&paths::daemon_socket()).await {
            Ok(c) => {
                let welcome = c.welcome();
                println!("version: {}", welcome.claudio_version);
                println!("claude: {}", claude_line(welcome.host.claude.as_ref()));
                match c.request(proto::Msg::ListSessions).await {
                    Ok(proto::Msg::Sessions { sessions }) => {
                        println!("sessions: {}", sessions.len());
                    }
                    _ => println!("sessions: (could not fetch)"),
                }
            }
            Err(e) => eprintln!("claudio daemon status: connect failed: {e}"),
        }
    });
    std::process::ExitCode::SUCCESS
}

pub fn daemon_stop() -> std::process::ExitCode {
    // Strategy 1: authenticated IPC Shutdown.
    if daemon_socket_live() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        let sent = rt.map(|rt| {
            rt.block_on(async {
                match client::connect(&paths::daemon_socket()).await {
                    Ok(c) => match c.request(proto::Msg::Shutdown).await {
                        Ok(proto::Msg::Ok) => true,
                        Ok(other) => {
                            tracing::warn!(?other, "unexpected reply to Shutdown");
                            false
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Shutdown request failed");
                            false
                        }
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, "could not connect for Shutdown");
                        false
                    }
                }
            })
        });

        if sent.unwrap_or(false) {
            if wait_for_daemon_exit(Duration::from_secs(10)) {
                println!(
                    "daemon stopped. Running sessions are dormant; \
                     the next `claudio` resumes them with --resume."
                );
                return std::process::ExitCode::SUCCESS;
            } else {
                eprintln!("claudio daemon stop: daemon did not exit within 10 s after Shutdown");
                // Fall through to PID fallback.
            }
        }
    }

    // Strategy 2: PID fallback with process identity verification.
    let Some(pid) = read_daemon_pid() else {
        println!("daemon: not running");
        return std::process::ExitCode::SUCCESS;
    };

    if !verify_daemon_pid(pid) {
        eprintln!(
            "claudio daemon stop: PID {pid} in lock file does not match our daemon binary; \
             not signaling"
        );
        return std::process::ExitCode::FAILURE;
    }

    // SAFETY: kill(2) is async-signal-safe; the PID was verified above.
    let ret = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if ret != 0 {
        eprintln!(
            "claudio daemon stop: kill failed: {}",
            std::io::Error::last_os_error()
        );
        return std::process::ExitCode::FAILURE;
    }

    if wait_for_daemon_exit(Duration::from_secs(10)) {
        println!(
            "daemon stopped. Running sessions are dormant; \
             the next `claudio` resumes them with --resume."
        );
        std::process::ExitCode::SUCCESS
    } else {
        eprintln!("claudio daemon stop: daemon did not exit within 10 s");
        std::process::ExitCode::FAILURE
    }
}

pub fn daemon_restart() -> std::process::ExitCode {
    // Stop the existing daemon (if any).
    if daemon_socket_live() {
        let code = daemon_stop();
        if code != std::process::ExitCode::SUCCESS {
            return code;
        }
    }
    // Start a new daemon.
    match client::ensure_daemon() {
        Ok(()) => {
            println!("daemon restarted.");
            daemon_status()
        }
        Err(e) => {
            eprintln!("claudio daemon restart: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

// ── process identity verification ────────────────────────────────────────────

/// Verify that PID `pid` is our daemon process (same binary as `current_exe`).
///
/// On Linux, reads `/proc/<pid>/exe`. On macOS, skips verification (returns
/// `false`) because `proc_pidpath` requires additional dependencies; the IPC
/// path (strategy 1) is always available when the daemon is running on macOS.
fn verify_daemon_pid(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        verify_daemon_pid_linux(pid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // On non-Linux platforms we skip the PID fallback to avoid signaling
        // unverified PIDs. The IPC path (strategy 1) covers the normal case.
        let _ = pid;
        tracing::info!(
            pid,
            "PID-based stop not supported on this platform; IPC path required"
        );
        false
    }
}

#[cfg(target_os = "linux")]
fn verify_daemon_pid_linux(pid: u32) -> bool {
    let exe_link = std::path::PathBuf::from(format!("/proc/{pid}/exe"));
    let current = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "could not determine current exe for PID verification");
            return false;
        }
    };
    match std::fs::read_link(&exe_link) {
        Ok(target) => {
            // Canonicalize both paths to handle symlinks in the binary location.
            let target_canon = target.canonicalize().unwrap_or(target);
            let current_canon = current.canonicalize().unwrap_or(current);
            let matches = target_canon == current_canon;
            if !matches {
                tracing::warn!(
                    pid,
                    daemon_exe = %target_canon.display(),
                    our_exe = %current_canon.display(),
                    "PID exe mismatch — not signaling"
                );
            }
            matches
        }
        Err(e) => {
            tracing::warn!(
                pid,
                error = %e,
                "could not read /proc/{pid}/exe for PID verification"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_shows_claude_version_and_path() {
        let info = proto::ClaudeInfo {
            path: "/home/u/.npm-packages/bin/claude".into(),
            version: "2.1.296 (Claude Code)".into(),
        };
        assert_eq!(
            claude_line(Some(&info)),
            "/home/u/.npm-packages/bin/claude, 2.1.296 (Claude Code)"
        );
        assert_eq!(claude_line(None), "not found");
    }

    #[test]
    fn verify_own_pid_matches() {
        let pid = std::process::id();
        // Our own process must verify as our daemon binary.
        // (The binary is claudio under test, which is the same exe.)
        let result = verify_daemon_pid(pid);
        // On Linux this should be true; on other platforms it's always false.
        #[cfg(target_os = "linux")]
        assert!(result, "our own PID should verify as our binary");
        #[cfg(not(target_os = "linux"))]
        assert!(!result, "non-Linux platforms skip PID verification");
    }

    #[test]
    fn verify_nonexistent_pid_fails() {
        // PID 0 is never a real process.
        assert!(!verify_daemon_pid(0));
    }

    #[test]
    fn verify_unrelated_pid_fails() {
        // PID 1 (init/systemd) is never our daemon binary.
        assert!(!verify_daemon_pid(1));
    }
}
