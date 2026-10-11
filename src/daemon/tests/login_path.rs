//! Daemon tests for the login-shell `PATH` ([`crate::daemon::login_path`]):
//! a fake login shell puts a directory with its own claude first in `PATH`,
//! one the inherited `PATH` lacks. The daemon must run that claude, report
//! it in `Welcome`, and give the session that `PATH`.

use super::*;
use crate::daemon::login_path;

/// Writes `script` as an executable `name` in `dir`.
fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\n{body}")).unwrap();
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sessions_run_the_login_shells_claude_with_its_path() {
    let dir = TestDaemon::new_dir();
    let login_bin = dir.join("login-bin");
    // The claude only the login shell's PATH has: it records how it was run.
    let claude = script(
        &login_bin,
        "claude",
        &format!(
            "if [ \"$1\" = \"--version\" ]; then echo '7.7.7 (Claude Code)'; exit 0; fi\n\
             if [ \"$1\" = \"--help\" ]; then exit 0; fi\n\
             printf '%s\\n%s\\n' \"$0\" \"$PATH\" > ran.txt\n\
             echo {BANNER}\n\
             exec cat\n"
        ),
    );
    // Like `zsh -l -i -c <script>` after a ~/.zshrc that prints and exports.
    let shell = script(
        &dir,
        "fake-login-shell",
        &format!(
            "echo 'Last login: yesterday'\n\
             PATH={}:$PATH; export PATH\n\
             shift 3; eval \"$1\"\n",
            login_bin.display()
        ),
    );
    let inherited = std::ffi::OsString::from("/usr/bin:/bin");
    let path = login_path::resolve(&shell, Some(&inherited), login_path::TIMEOUT)
        .expect("the fake shell answers");
    let config = Config {
        claude: PathBuf::from("claude"),
        path: Some(path.clone()),
        ..TestDaemon::config(&dir)
    };
    let d = TestDaemon::start_with_config(dir, config);

    let mut c = Client::raw(&d.config.socket).await;
    match c.call(Msg::Hello(hello(PROTO))).await {
        Msg::Welcome(w) => {
            let info = w.host.claude.expect("claude found");
            assert_eq!(info.path, claude.to_string_lossy());
            assert_eq!(info.version, "7.7.7 (Claude Code)");
        }
        other => panic!("expected welcome, got {other:?}"),
    }

    let id = Uuid::new_v4();
    c.spawn(d.spec(id)).await;
    c.attach_until(id, BANNER).await;
    let ran = std::fs::read_to_string(d.work().join("ran.txt")).unwrap();
    let mut lines = ran.lines();
    assert_eq!(lines.next(), Some(claude.to_str().unwrap()));
    assert_eq!(lines.next(), path.to_str(), "the session's PATH");
    let session_path = std::ffi::OsString::from(path);
    let mut dirs = std::env::split_paths(&session_path);
    assert_eq!(dirs.next().as_deref(), Some(login_bin.as_path()));
    assert!(std::env::split_paths(&session_path).any(|d| d == Path::new("/usr/bin")));
}
