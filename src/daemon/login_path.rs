//! The user's login-shell `PATH`, read once when the daemon starts.
//!
//! A daemon started over ssh (by `systemd-run --user`, or setsid from the
//! bridge) inherits only the system `PATH`: rc files such as `~/.zshrc` never
//! ran for it, so `~/.npm-packages/bin`, `~/.cargo/bin` and the like are
//! missing. Looking claude up in that `PATH` can find a stale system-wide
//! install, and the sessions' tools miss the user's own directories. So the
//! daemon asks the user's shell once, the way a terminal would start it
//! (`<shell> -l -i -c 'printf … "$PATH"'`), and uses that `PATH` to find
//! claude and as the `PATH` of every session it spawns.
//!
//! The shell runs without a terminal (`setsid`, stdin `/dev/null`) under a
//! hard deadline, its whole process group killed when it runs over. rc files
//! may print anything, so the `PATH` is read between markers. On any failure
//! the inherited `PATH` stays.

use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::io::{self, Read};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::claude::SESSION_MARKERS;

/// How long the login shell may take to print its `PATH`.
pub const TIMEOUT: Duration = Duration::from_secs(3);

const BEGIN: &[u8] = b"__CLAUDIO_PATH_BEGIN__";
const END: &[u8] = b"__CLAUDIO_PATH_END__";

/// Output kept while looking for the markers, counted from the end (the
/// markers come last; rc noise before them is dropped).
const MAX_OUTPUT: usize = 256 * 1024;

/// The `PATH` for claude and the sessions: the login shell's, then the
/// `inherited` entries it lacks. `None` (keep the inherited `PATH`) when the
/// shell could not tell.
pub fn resolve(shell: &Path, inherited: Option<&OsStr>, timeout: Duration) -> Option<OsString> {
    let started = Instant::now();
    match capture(shell, timeout) {
        Ok(login) => {
            let path = merge(&login, inherited);
            tracing::info!(
                shell = %shell.display(),
                ms = started.elapsed().as_millis() as u64,
                path = %path.to_string_lossy(),
                "using the login shell's PATH"
            );
            Some(path)
        }
        Err(e) => {
            tracing::warn!(
                shell = %shell.display(),
                error = %e,
                "could not read the login shell's PATH; keeping the inherited one"
            );
            None
        }
    }
}

/// Run `shell` as an interactive login shell and read its `PATH`.
pub fn capture(shell: &Path, timeout: Duration) -> Result<OsString, String> {
    let script = format!(
        "printf '\\n{}%s{}\\n' \"$PATH\"",
        String::from_utf8_lossy(BEGIN),
        String::from_utf8_lossy(END)
    );
    let mut cmd = Command::new(shell);
    cmd.args(["-l", "-i", "-c", &script])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for marker in SESSION_MARKERS {
        cmd.env_remove(marker);
    }
    // A session of its own: no controlling terminal for an interactive shell
    // to take over, and a process group to kill on timeout.
    // SAFETY: setsid is async-signal-safe and touches no parent state.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("cannot start {}: {e}", shell.display()))?;
    let mut stdout = child.stdout.take().ok_or("no output pipe")?;

    // Read on a thread: something the rc files started in the background
    // may hold the pipe open long after the shell has printed and exited.
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match stdout.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let deadline = Instant::now() + timeout;
    let mut out = Vec::new();
    let mut timed_out = false;
    let found = loop {
        if let Some(path) = parse(&out) {
            break Some(path);
        }
        match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(chunk) => {
                out.extend_from_slice(&chunk);
                let excess = out.len().saturating_sub(MAX_OUTPUT);
                out.drain(..excess);
            }
            Err(RecvTimeoutError::Disconnected) => break None,
            Err(RecvTimeoutError::Timeout) => {
                timed_out = true;
                break None;
            }
        }
    };

    // Done with the shell: reap it, or stop its whole group if it lingers.
    let status = match child.try_wait() {
        Ok(Some(status)) => Some(status),
        _ => {
            if let Ok(pgid) = i32::try_from(child.id()) {
                // SAFETY: kill(2) has no memory-safety preconditions; the
                // child is not reaped yet, so its pid (the group id) is ours.
                unsafe { libc::kill(-pgid, libc::SIGKILL) };
            }
            child.wait().ok()
        }
    };
    found.ok_or_else(|| {
        if timed_out {
            format!("no answer within {} s", timeout.as_secs_f32())
        } else {
            match status {
                Some(s) => format!("no PATH in its output ({s})"),
                None => "no PATH in its output".to_owned(),
            }
        }
    })
}

/// The `PATH` between the markers of the last complete marked line, if any.
/// Empty counts as none.
fn parse(out: &[u8]) -> Option<OsString> {
    let start = rfind(out, BEGIN)? + BEGIN.len();
    let len = find(&out[start..], END)?;
    let path = &out[start..start + len];
    (!path.is_empty()).then(|| OsString::from_vec(path.to_vec()))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn rfind(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).rposition(|w| w == needle)
}

/// `login`'s entries in order, then `inherited`'s that it lacks. Duplicates
/// and empty entries (an implicit current directory) are dropped.
pub fn merge(login: &OsStr, inherited: Option<&OsStr>) -> OsString {
    let mut seen = HashSet::new();
    let dirs: Vec<PathBuf> = std::env::split_paths(login)
        .chain(inherited.into_iter().flat_map(std::env::split_paths))
        .filter(|dir| !dir.as_os_str().is_empty())
        .filter(|dir| seen.insert(dir.clone()))
        .collect();
    std::env::join_paths(dirs).unwrap_or_else(|_| login.to_owned())
}

/// The user's login shell: `$SHELL`, else the passwd entry's, else `/bin/sh`.
pub fn login_shell() -> PathBuf {
    std::env::var_os("SHELL")
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(passwd_shell)
        .unwrap_or_else(|| PathBuf::from("/bin/sh"))
}

fn passwd_shell() -> Option<PathBuf> {
    // SAFETY: `getpwuid_r` fills `pw` and `buf` and nothing else; `pw_shell`
    // points into `buf`, which outlives the copy below.
    let shell = unsafe {
        let mut pw: libc::passwd = std::mem::zeroed();
        let mut buf = [0 as libc::c_char; 2048];
        let mut found = std::ptr::null_mut();
        let rc = libc::getpwuid_r(
            libc::getuid(),
            &mut pw,
            buf.as_mut_ptr(),
            buf.len(),
            &mut found,
        );
        if rc != 0 || found.is_null() || pw.pw_shell.is_null() {
            return None;
        }
        std::ffi::CStr::from_ptr(pw.pw_shell)
            .to_str()
            .ok()?
            .to_owned()
    };
    (!shell.is_empty()).then(|| PathBuf::from(shell))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn marked(path: &str) -> Vec<u8> {
        format!("\n__CLAUDIO_PATH_BEGIN__{path}__CLAUDIO_PATH_END__\n").into_bytes()
    }

    #[test]
    fn parse_reads_between_the_markers() {
        assert_eq!(parse(&marked("/a:/b")), Some("/a:/b".into()));
    }

    #[test]
    fn parse_ignores_noise_before_and_after() {
        let mut out = b"Welcome back!\nfortune: today is a good day\n\x1b[1mbold\x1b[0m".to_vec();
        out.extend(marked("/home/u/.npm-packages/bin:/usr/bin"));
        out.extend_from_slice(b"logout noise\n");
        assert_eq!(
            parse(&out),
            Some("/home/u/.npm-packages/bin:/usr/bin".into())
        );
    }

    #[test]
    fn parse_needs_both_markers_and_a_value() {
        assert_eq!(parse(b""), None);
        assert_eq!(parse(b"/usr/bin:/bin\n"), None);
        assert_eq!(parse(b"__CLAUDIO_PATH_BEGIN__/usr/bin"), None, "no end yet");
        assert_eq!(parse(b"/usr/bin__CLAUDIO_PATH_END__"), None, "no begin");
        assert_eq!(parse(&marked("")), None, "empty");
    }

    #[test]
    fn parse_keeps_non_utf8_bytes() {
        let mut out = b"__CLAUDIO_PATH_BEGIN__/opt/".to_vec();
        out.push(0xff);
        out.extend_from_slice(b"__CLAUDIO_PATH_END__");
        let path = parse(&out).unwrap();
        assert_eq!(path.into_vec(), b"/opt/\xff");
    }

    #[test]
    fn merge_puts_the_login_path_first_and_adds_what_it_lacks() {
        let merged = merge(
            OsStr::new("/home/u/.npm-packages/bin:/usr/local/bin:/usr/bin"),
            Some(OsStr::new("/usr/local/bin:/usr/bin:/sbin")),
        );
        assert_eq!(
            merged,
            "/home/u/.npm-packages/bin:/usr/local/bin:/usr/bin:/sbin"
        );
    }

    #[test]
    fn merge_drops_duplicates_and_empty_entries() {
        let merged = merge(OsStr::new("/a::/b:/a:"), Some(OsStr::new(":/c:/b")));
        assert_eq!(merged, "/a:/b:/c");
        assert_eq!(merge(OsStr::new("/a"), None), "/a");
    }

    fn scratch() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("claudio-login-path-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A fake login shell: `script` runs for any arguments.
    fn fake_shell(dir: &Path, script: &str) -> PathBuf {
        let path = dir.join("fake-shell");
        std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[test]
    fn capture_reads_a_noisy_shell() {
        let dir = scratch();
        // Behaves like `sh -l -i -c <script>` after rc files that print.
        let shell = fake_shell(
            dir.as_path(),
            "echo 'motd: hello'; echo 'rc noise' >&2\n\
             PATH=/opt/login/bin:$PATH; export PATH\n\
             shift 3; eval \"$1\"\necho bye",
        );
        let path = capture(&shell, TIMEOUT).unwrap();
        let first = std::env::split_paths(&path).next().unwrap();
        assert_eq!(first, Path::new("/opt/login/bin"));
    }

    #[test]
    fn capture_passes_login_interactive_flags() {
        let dir = scratch();
        let shell = fake_shell(
            dir.as_path(),
            "[ \"$1 $2 $3\" = '-l -i -c' ] || exit 1\nshift 3; eval \"$1\"",
        );
        assert!(capture(&shell, TIMEOUT).is_ok());
    }

    #[test]
    fn capture_fails_without_markers() {
        let dir = scratch();
        let shell = fake_shell(dir.as_path(), "echo \"$PATH\"");
        let err = capture(&shell, TIMEOUT).unwrap_err();
        assert!(err.contains("no PATH"), "{err}");
        assert!(capture(&dir.as_path().join("missing"), TIMEOUT).is_err());
    }

    #[test]
    fn capture_times_out_and_kills_the_shell() {
        let dir = scratch();
        let pidfile = dir.as_path().join("pid");
        // The shell hangs (an rc file waiting for input, say), with a child
        // in its process group that would hold the pipe open.
        let shell = fake_shell(
            dir.as_path(),
            &format!("sleep 30 & echo $! > {}\nsleep 30", pidfile.display()),
        );
        let started = Instant::now();
        let err = capture(&shell, Duration::from_millis(300)).unwrap_err();
        assert!(err.contains("no answer"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(5));
        // The background child went down with the group.
        let pid: i32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        // SAFETY: kill(pid, 0) only checks for existence.
        while unsafe { libc::kill(pid, 0) } == 0 {
            assert!(Instant::now() < deadline, "background child survived");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn resolve_falls_back_to_none() {
        let dir = scratch();
        let shell = fake_shell(dir.as_path(), "exit 1");
        assert_eq!(resolve(&shell, Some(OsStr::new("/usr/bin")), TIMEOUT), None);
        let shell = fake_shell(dir.as_path(), "PATH=/x; shift 3; eval \"$1\"");
        assert_eq!(
            resolve(&shell, Some(OsStr::new("/usr/bin")), TIMEOUT),
            Some("/x:/usr/bin".into())
        );
    }
}
