//! Facts about the host the daemon runs on, and its filesystem RPCs.
//!
//! [`probe`] gathers the [`HostInfo`] sent in `Welcome` (it runs
//! `claude --version`, so it is computed once and cached by the caller).
//! [`list_dir`] backs the new-session directory picker.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::proto::{ClaudeInfo, DirEntry, HostInfo};

/// How long `claude --version` may take before we report claude as missing.
const VERSION_TIMEOUT: Duration = Duration::from_secs(3);

/// Maximum entries returned by [`list_dir`].
const MAX_DIR_ENTRIES: usize = 5000;

/// The current user's home directory.
pub fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

/// Expand a leading `~` (alone or as `~/…`) to the home directory.
pub fn expand_tilde(path: &str) -> PathBuf {
    match path.strip_prefix('~') {
        Some("") => home(),
        Some(rest) if rest.starts_with('/') => home().join(rest.trim_start_matches('/')),
        _ => PathBuf::from(path),
    }
}

/// Gather host facts. Blocking: runs `<claude> --version` (≤ 3 s), with
/// `PATH` set to `path` when given (the daemon's login-shell `PATH`).
pub fn probe(claude: &Path, path: Option<&OsStr>) -> HostInfo {
    HostInfo {
        claude: claude_info(claude, path),
        ..basic()
    }
}

/// Host facts that need no subprocess (`claude` reported as missing).
pub fn basic() -> HostInfo {
    HostInfo {
        hostname: hostname(),
        os: std::env::consts::OS.to_owned(),
        arch: std::env::consts::ARCH.to_owned(),
        home: home().to_string_lossy().into_owned(),
        claude: None,
    }
}

fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: the pointer and length describe a valid, writable buffer.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return String::new();
    }
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

/// `Some` when `claude` resolves to a file and `--version` answers in time.
fn claude_info(claude: &Path, path: Option<&OsStr>) -> Option<ClaudeInfo> {
    let bin = which(claude)?;
    let version = run_version(&bin, path)?;
    Some(ClaudeInfo {
        path: bin.to_string_lossy().into_owned(),
        version,
    })
}

/// Resolve a bare command name through `$PATH`; paths are taken as they are.
fn which(cmd: &Path) -> Option<PathBuf> {
    if cmd.components().count() > 1 {
        return cmd.is_file().then(|| cmd.to_path_buf());
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(cmd))
        .find(|p| p.is_file())
}

/// First line of `<claude> --version`, or `None` on failure or timeout.
fn run_version(claude: &Path, path: Option<&OsStr>) -> Option<String> {
    run_capture(claude, "--version", path)?
        .lines()
        .next()
        .map(|l| l.trim().to_owned())
        .filter(|l| !l.is_empty())
}

/// Lets claude offer bypass-permissions mode (Shift+Tab); it does not start
/// in it.
pub const ALLOW_SKIP_PERMISSIONS: &str = "--allow-dangerously-skip-permissions";

/// Whether `<claude> --help` mentions `flag`. Blocking (≤ 3 s); `false` on
/// failure or timeout, so an old or broken claude never gets the flag.
pub fn claude_supports(claude: &Path, flag: &str, path: Option<&OsStr>) -> bool {
    run_capture(claude, "--help", path).is_some_and(|help| help.contains(flag))
}

/// Stdout of `<claude> <arg>` when it exits 0 within [`VERSION_TIMEOUT`].
/// Stdout is drained on a thread so long output can't fill the pipe and stall
/// the child.
fn run_capture(claude: &Path, arg: &str, path: Option<&OsStr>) -> Option<String> {
    let mut cmd = Command::new(claude);
    if let Some(path) = path {
        cmd.env("PATH", path);
    }
    let mut child = cmd
        .arg(arg)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let reader = std::thread::spawn(move || {
        let mut out = String::new();
        io::Read::read_to_string(&mut stdout, &mut out)
            .ok()
            .map(|_| out)
    });
    let deadline = Instant::now() + VERSION_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => break,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
    reader.join().ok().flatten()
}

/// Detect the git branch for a directory without running `git`.
///
/// Returns `Some(branch)` when the dir contains `.git/HEAD` with a branch ref,
/// `Some("")` for a detached HEAD or an unreadable HEAD, and `None` when the
/// directory is not a git repo. Worktrees (`.git` is a file) are followed one
/// level by parsing the `gitdir:` pointer.
pub fn detect_git_branch(path: &Path) -> Option<String> {
    let git = path.join(".git");
    let head_path = if git.is_dir() {
        git.join("HEAD")
    } else if git.is_file() {
        // Worktree: .git is a file containing "gitdir: <path>"
        let content = std::fs::read_to_string(&git).ok()?;
        let gitdir = content
            .lines()
            .next()
            .and_then(|l| l.strip_prefix("gitdir:"))?
            .trim();
        Path::new(gitdir).join("HEAD")
    } else {
        return None;
    };
    let head = std::fs::read_to_string(&head_path).ok()?;
    let head = head.trim();
    if let Some(branch) = head.strip_prefix("ref: refs/heads/") {
        Some(branch.to_owned())
    } else {
        // Detached HEAD or unknown format.
        Some(String::new())
    }
}

/// List `path` (with `~` expanded): directories first, then by name, capped
/// at 5000 entries. Returns the expanded path alongside the entries.
pub fn list_dir(path: &str) -> io::Result<(String, Vec<DirEntry>)> {
    let dir = expand_tilde(path);
    let mut entries: Vec<DirEntry> = std::fs::read_dir(&dir)?
        .filter_map(Result::ok)
        .take(MAX_DIR_ENTRIES)
        .map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let path = e.path();
            let hidden = name.starts_with('.');
            // Detect symlinks without following them (symlink_metadata vs metadata).
            let symlink = e
                .metadata()
                .map(|m| m.file_type().is_symlink())
                .unwrap_or(false);
            // Follow symlinks so a link to a directory can be descended into.
            let dir_flag = path.is_dir();
            DirEntry {
                name,
                dir: dir_flag,
                git: detect_git_branch(&path),
                claude_at: None, // enriched below for dirs only
                symlink,
                hidden,
            }
        })
        .collect();
    entries.sort_by(|a, b| b.dir.cmp(&a.dir).then_with(|| a.name.cmp(&b.name)));
    Ok((dir.to_string_lossy().into_owned(), entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tilde_expansion() {
        let h = home();
        assert_eq!(expand_tilde("~"), h);
        assert_eq!(expand_tilde("~/src/x"), h.join("src/x"));
        assert_eq!(expand_tilde("/abs/~"), PathBuf::from("/abs/~"));
        assert_eq!(expand_tilde("~other"), PathBuf::from("~other"));
    }

    #[test]
    fn probe_reports_host_and_missing_claude() {
        let info = probe(Path::new("/nonexistent/claude"), None);
        assert_eq!(info.os, std::env::consts::OS);
        assert!(!info.hostname.is_empty());
        assert!(info.claude.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn claude_supports_reads_help() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("claudio-help-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("claude");
        // Long help output: more than a pipe buffer, so draining must not stall.
        std::fs::write(
            &bin,
            "#!/bin/sh\nseq 1 20000\necho '  --allow-dangerously-skip-permissions  Enable bypass'\n",
        )
        .unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(claude_supports(&bin, ALLOW_SKIP_PERMISSIONS, None));
        assert!(!claude_supports(&bin, "--no-such-flag", None));
        assert!(!claude_supports(
            Path::new("/nonexistent/claude"),
            ALLOW_SKIP_PERMISSIONS,
            None
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn probe_runs_claude_with_the_given_path() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("claudio-probe-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("claude");
        // An npm-style claude needs its interpreter from the user's PATH.
        std::fs::write(&bin, "#!/bin/sh\necho \"$PATH\"\n").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        let info = probe(&bin, Some(OsStr::new("/login/bin:/usr/bin:/bin")));
        let claude = info.claude.unwrap();
        assert_eq!(claude.version, "/login/bin:/usr/bin:/bin");
        assert_eq!(claude.path, bin.to_string_lossy());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
