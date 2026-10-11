//! End-to-end daemon tests: a real daemon in a temp dir, a fake `claude`
//! (a shell script that prints a banner, then `exec cat`), and real Unix
//! socket clients speaking the wire protocol. Every read has a timeout.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::net::UnixStream;
use uuid::Uuid;

use super::{server, Config, Daemon};
use crate::proto::{
    self, Envelope, Frame, Hello, Msg, SessionEvent, SessionId, SessionState, SpawnSpec, PROTO,
};

const BANNER: &str = "FAKE-CLAUDE-BANNER";
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// A variant of the fake claude that exits 3 immediately on any real invocation.
/// Used to trigger the commit-before-exit race deterministically.
fn fake_claude_exits_immediately() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("claudio-fake-claude-ei-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("claude");
        let script = "#!/bin/sh\n\
             if [ \"$1\" = \"--version\" ]; then echo '9.9.9 (Claude Code)'; exit 0; fi\n\
             if [ \"$1\" = \"--help\" ]; then exit 0; fi\n\
             exit 3\n";
        std::fs::write(&bin, script).unwrap();
        std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        bin
    })
}

/// A variant of the fake claude that exits 1 immediately when given `--resume`
/// (simulating a deleted conversation), and otherwise behaves like the normal
/// fake claude (prints banner + echoes input).
fn fake_claude_resume_fails() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("claudio-fake-claude-rf-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("claude");
        // Exit 1 immediately if --resume is in the arguments, otherwise behave
        // like the normal fake claude.
        let script = format!(
            "#!/bin/sh\n\
             if [ \"$1\" = \"--version\" ]; then echo '9.9.9 (Claude Code)'; exit 0; fi\n\
             if [ \"$1\" = \"--help\" ]; then exit 0; fi\n\
             # Check all args for --resume\n\
             for arg in \"$@\"; do\n\
               if [ \"$arg\" = \"--resume\" ]; then exit 1; fi\n\
             done\n\
             printf '%s' \"$2\" > settings.json\n\
             echo {BANNER}\n\
             exec cat\n"
        );
        std::fs::write(&bin, script).unwrap();
        std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        bin
    })
}

/// The fake claude, shared by all tests (written once, so no exec races with
/// a file still open for writing). It answers `--version`, saves its
/// `--settings` JSON into its cwd (so tests can read the hook token), prints a
/// banner and echoes input.
fn fake_claude() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("claudio-fake-claude-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("claude");
        let script = format!(
            "#!/bin/sh\n\
             if [ \"$1\" = \"--version\" ]; then echo '9.9.9 (Claude Code)'; exit 0; fi\n\
             if [ \"$1\" = \"--help\" ]; then exit 0; fi\n\
             printf '%s' \"$2\" > settings.json\n\
             echo {BANNER}\n\
             exec cat\n"
        );
        std::fs::write(&bin, script).unwrap();
        std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        bin
    })
}

/// A daemon running in its own temp dir.
struct TestDaemon {
    dir: PathBuf,
    config: Config,
    daemon: Arc<Daemon>,
}

impl TestDaemon {
    fn config(dir: &Path) -> Config {
        Config {
            socket: dir.join("rt/d.sock"),
            lock: dir.join("rt/d.lock"),
            journal: dir.join("state/journal.json"),
            claude: fake_claude().to_path_buf(),
            claudio: PathBuf::from("/bin/true"),
            skip_permissions: true,
            build: None,
            path: None,
        }
    }

    fn new_dir() -> PathBuf {
        let dir = crate::paths::socket_test_dir("cl-d");
        std::fs::create_dir_all(dir.join("work")).unwrap();
        dir
    }

    async fn start_in(dir: PathBuf) -> TestDaemon {
        let config = Self::config(&dir);
        Self::start_with_config(dir, config)
    }

    fn start_with_config(dir: PathBuf, config: Config) -> TestDaemon {
        let listening = server::start(config.clone())
            .unwrap()
            .expect("lock is free");
        let daemon = listening.daemon();
        tokio::spawn(listening.serve());
        TestDaemon {
            dir,
            config,
            daemon,
        }
    }

    async fn start() -> TestDaemon {
        Self::start_in(Self::new_dir()).await
    }

    /// A daemon whose claude is `claude` instead of the shared fake.
    async fn start_with_claude(dir: PathBuf, claude: PathBuf) -> TestDaemon {
        let config = Config {
            claude,
            ..Self::config(&dir)
        };
        Self::start_with_config(dir, config)
    }

    fn work(&self) -> PathBuf {
        self.dir.join("work")
    }

    async fn client(&self) -> Client {
        Client::connect(&self.config.socket).await
    }

    fn spec(&self, id: SessionId) -> SpawnSpec {
        SpawnSpec {
            id,
            cwd: self.work().to_string_lossy().into_owned(),
            name: Some("test".into()),
            args: vec![],
            env: vec![("SECRET_ENV".into(), "hunter2".into())],
            rows: 24,
            cols: 80,
            ephemeral: false,
        }
    }

    /// The hook token the fake claude was started with.
    async fn token(&self) -> String {
        self.token_other_than("").await
    }

    /// The hook token of a process started after the one with `previous`:
    /// after a respawn, `settings.json` holds the old token until the new
    /// process rewrites it.
    async fn token_other_than(&self, previous: &str) -> String {
        let path = self.work().join("settings.json");
        within(async {
            loop {
                let token = std::fs::read(&path)
                    .ok()
                    .and_then(|raw| serde_json::from_slice::<serde_json::Value>(&raw).ok())
                    .and_then(|settings| {
                        let cmd = settings["hooks"]["Stop"][0]["hooks"][0]["command"].as_str()?;
                        Some(cmd.rsplit(' ').next()?.to_owned())
                    });
                match token {
                    Some(token) if token != previous => return token,
                    _ => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
    }

    fn journal(&self) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(&self.config.journal).unwrap()).unwrap()
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(READ_TIMEOUT, f)
        .await
        .expect("timed out")
}

/// A protocol client over a real socket.
struct Client {
    stream: UnixStream,
    next_req: u64,
}

impl Client {
    async fn raw(socket: &Path) -> Client {
        Client {
            stream: UnixStream::connect(socket).await.unwrap(),
            next_req: 0,
        }
    }

    async fn connect(socket: &Path) -> Client {
        let mut c = Self::raw(socket).await;
        match c.call(Msg::Hello(hello(PROTO))).await {
            Msg::Welcome(w) => assert_eq!(w.proto, PROTO),
            other => panic!("expected welcome, got {other:?}"),
        }
        c
    }

    async fn send(&mut self, frame: Frame) {
        proto::write_frame(&mut self.stream, &frame).await.unwrap();
    }

    async fn request(&mut self, msg: Msg) -> u64 {
        self.next_req += 1;
        self.send(Frame::Control(Envelope::request(self.next_req, msg)))
            .await;
        self.next_req
    }

    async fn recv(&mut self) -> Option<Frame> {
        within(proto::read_frame(&mut self.stream)).await.unwrap()
    }

    /// Read until `pick` accepts a frame. On a timeout, the panic lists the
    /// last frames that did arrive, so a failure says what was seen instead.
    async fn until<T>(&mut self, mut pick: impl FnMut(&Frame) -> Option<T>) -> T {
        let mut seen: std::collections::VecDeque<String> = Default::default();
        let wait = async {
            loop {
                let frame = proto::read_frame(&mut self.stream)
                    .await
                    .unwrap()
                    .expect("daemon closed the connection");
                if let Some(v) = pick(&frame) {
                    return v;
                }
                if seen.len() == 12 {
                    seen.pop_front();
                }
                seen.push_back(match &frame {
                    Frame::Control(env) => format!("{:?}", env.msg).chars().take(160).collect(),
                    Frame::Data { bytes, .. } => format!("{} data bytes", bytes.len()),
                });
            }
        };
        match tokio::time::timeout(READ_TIMEOUT, wait).await {
            Ok(v) => v,
            Err(_) => panic!("timed out; last frames seen:\n{}", Vec::from(seen).join("\n")),
        }
    }

    /// Send a request and wait for its reply.
    async fn call(&mut self, msg: Msg) -> Msg {
        let req = self.request(msg).await;
        self.until(|f| match f {
            Frame::Control(env) if env.req == Some(req) => Some(env.msg.clone()),
            _ => None,
        })
        .await
    }

    /// Wait for a session event matching `pick`.
    async fn event<T>(&mut self, id: SessionId, pick: impl Fn(&SessionEvent) -> Option<T>) -> T {
        self.until(|f| match f {
            Frame::Control(Envelope {
                req: None,
                msg: Msg::Event { id: eid, event },
            }) if *eid == id => pick(event),
            _ => None,
        })
        .await
    }

    /// Accumulate `D` bytes for `id` until they contain `needle`.
    async fn output_until(&mut self, id: SessionId, needle: &str) -> String {
        let mut seen = Vec::new();
        self.until(|f| {
            if let Frame::Data { session, bytes } = f {
                if *session == id {
                    seen.extend_from_slice(bytes);
                }
            }
            String::from_utf8_lossy(&seen)
                .contains(needle)
                .then(|| String::from_utf8_lossy(&seen).into_owned())
        })
        .await
    }

    /// Attach and return the snapshot (the `D` frame right after `Attached`).
    async fn attach(&mut self, id: SessionId, rows: u16, cols: u16) -> String {
        let req = self.request(Msg::Attach { id, rows, cols }).await;
        let attached = self
            .until(|f| match f {
                Frame::Control(env) if env.req == Some(req) => Some(env.msg.clone()),
                _ => None,
            })
            .await;
        assert_eq!(attached, Msg::Attached { id, rows, cols });
        self.until(|f| match f {
            Frame::Data { session, bytes } if *session == id => {
                Some(String::from_utf8_lossy(bytes).into_owned())
            }
            _ => None,
        })
        .await
    }

    /// Attach, then wait until the screen (snapshot or later output) shows
    /// `needle`.
    async fn attach_until(&mut self, id: SessionId, needle: &str) {
        if !self.attach(id, 24, 80).await.contains(needle) {
            self.output_until(id, needle).await;
        }
    }

    async fn spawn(&mut self, spec: SpawnSpec) -> Option<u32> {
        match self.call(Msg::Spawn(spec)).await {
            Msg::Spawned { pid, .. } => pid,
            other => panic!("spawn failed: {other:?}"),
        }
    }

    async fn sessions(&mut self) -> Vec<crate::proto::SessionInfo> {
        match self.call(Msg::ListSessions).await {
            Msg::Sessions { sessions } => sessions,
            other => panic!("expected sessions, got {other:?}"),
        }
    }
}

fn hello(proto: u32) -> Hello {
    Hello {
        claudio_version: "test".into(),
        proto,
        colors: None,
    }
}

async fn send_hook(socket: &Path, token: &str, event: &str, payload: serde_json::Value) {
    let mut c = Client::raw(socket).await;
    let hook = Msg::Hook {
        token: token.into(),
        event: event.into(),
        payload,
    };
    c.send(Frame::Control(Envelope::event(hook))).await;
    // Stay connected until the daemon hangs up, as the real relay does.
    let _ = tokio::time::timeout(READ_TIMEOUT, proto::read_frame(&mut c.stream)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_reports_host_and_rejects_wrong_proto() {
    let d = TestDaemon::start().await;

    let mut c = Client::raw(&d.config.socket).await;
    match c.call(Msg::Hello(hello(PROTO))).await {
        Msg::Welcome(w) => {
            assert_eq!(w.claudio_version, env!("CARGO_PKG_VERSION"));
            let claude = w.host.claude.expect("fake claude found");
            assert_eq!(claude.version, "9.9.9 (Claude Code)");
            assert_eq!(w.host.os, std::env::consts::OS);
        }
        other => panic!("expected welcome, got {other:?}"),
    }

    let mut bad = Client::raw(&d.config.socket).await;
    assert!(matches!(
        bad.call(Msg::Hello(hello(PROTO + 1))).await,
        Msg::Error { .. }
    ));
    assert_eq!(bad.recv().await, None, "connection closed after refusal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_daemon_backs_off() {
    let d = TestDaemon::start().await;
    assert!(server::start(d.config.clone()).unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_attach_echo_and_list() {
    let d = TestDaemon::start().await;
    let mut a = d.client().await;
    let mut observer = d.client().await;
    let id = Uuid::new_v4();

    // Created reaches the spawning client before its Spawned reply.
    let req = a.request(Msg::Spawn(d.spec(id))).await;
    let created = a
        .until(|f| match f {
            Frame::Control(env) if env.req == Some(req) => {
                panic!("Spawned before Created: {env:?}")
            }
            Frame::Control(Envelope {
                msg:
                    Msg::Event {
                        event: SessionEvent::Created { info },
                        ..
                    },
                ..
            }) => Some(info.clone()),
            _ => None,
        })
        .await;
    assert_eq!(created.id, id);
    assert_eq!(created.state, SessionState::Starting);
    let pid = a
        .until(|f| match f {
            Frame::Control(Envelope {
                req: Some(r),
                msg: Msg::Spawned { pid, .. },
            }) if *r == req => Some(*pid),
            _ => None,
        })
        .await;
    assert!(pid.is_some());
    assert_eq!(created.pid, pid);
    // ...and every other client.
    observer
        .event(id, |e| {
            matches!(e, SessionEvent::Created { .. }).then_some(())
        })
        .await;

    // Spawn is idempotent for a live id.
    assert_eq!(a.spawn(d.spec(id)).await, pid);

    // Live output, then a later attach sees the banner in its snapshot.
    a.attach_until(id, BANNER).await;
    let snapshot = observer.attach(id, 24, 80).await;
    assert!(snapshot.contains(BANNER), "snapshot: {snapshot:?}");

    // Keyboard input echoes back.
    a.send(Frame::Data {
        session: id,
        bytes: b"hello\r".to_vec(),
    })
    .await;
    a.output_until(id, "hello").await;

    let sessions = a.sessions().await;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].id, id);
    assert_eq!(sessions[0].pid, pid);
    assert_eq!(sessions[0].name.as_deref(), Some("test"));
    assert_eq!(sessions[0].state, SessionState::Starting);

    // The journal never holds the env.
    let raw = std::fs::read_to_string(&d.config.journal).unwrap();
    assert!(!raw.contains("hunter2") && !raw.contains("SECRET_ENV") && !raw.contains("--settings"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ephemeral_session_is_listed_as_such_and_never_journaled() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let (kept, plain) = (Uuid::new_v4(), Uuid::new_v4());
    c.spawn(d.spec(kept)).await;
    let ephemeral = SpawnSpec {
        ephemeral: true,
        ..d.spec(plain)
    };
    assert!(c.spawn(ephemeral).await.is_some());

    let sessions = c.sessions().await;
    let flag = |id| sessions.iter().find(|s| s.id == id).map(|s| s.ephemeral);
    assert_eq!((flag(kept), flag(plain)), (Some(false), Some(true)));
    let journaled: Vec<_> = d.journal()["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(journaled, [kept.to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hooks_update_state_and_journal() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    c.spawn(d.spec(id)).await;
    let token = d.token().await;

    // A forged token is ignored: had this Stop applied, Idle would come first.
    send_hook(&d.config.socket, "0000", "Stop", serde_json::json!({})).await;
    send_hook(
        &d.config.socket,
        &token,
        "UserPromptSubmit",
        serde_json::json!({"prompt": "x"}),
    )
    .await;
    let state = c
        .event(id, |e| match e {
            SessionEvent::State { state } => Some(*state),
            _ => None,
        })
        .await;
    assert_eq!(state, SessionState::Working);

    // A new claude conversation id is journaled immediately.
    let payload = serde_json::json!({"session_id": "conv-42", "source": "clear"});
    send_hook(&d.config.socket, &token, "SessionStart", payload).await;
    c.event(id, |e| match e {
        SessionEvent::ClaudeSession { claude_session_id } => {
            Some(assert_eq!(claude_session_id, "conv-42"))
        }
        _ => None,
    })
    .await;
    assert_eq!(d.journal()["sessions"][0]["claude_session_id"], "conv-42");
    assert_eq!(c.sessions().await[0].state, SessionState::NeedsInput);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_removes_session_everywhere() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    c.spawn(d.spec(id)).await;
    assert_eq!(d.journal()["sessions"].as_array().unwrap().len(), 1);

    // Removed is broadcast before the Ok reply.
    let req = c.request(Msg::Kill { id }).await;
    c.event(id, |e| matches!(e, SessionEvent::Removed).then_some(()))
        .await;
    let ok = c
        .until(|f| match f {
            Frame::Control(env) if env.req == Some(req) => Some(env.msg.clone()),
            _ => None,
        })
        .await;
    assert_eq!(ok, Msg::Ok);
    assert!(d.journal()["sessions"].as_array().unwrap().is_empty());
    assert!(c.sessions().await.is_empty());
    assert!(matches!(c.call(Msg::Kill { id }).await, Msg::Error { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_exit_closes_the_session() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    c.spawn(d.spec(id)).await;
    c.attach_until(id, BANNER).await;

    // ^D ends `cat` with status 0, like Ctrl+D twice or `/exit` in claude.
    c.send(Frame::Data {
        session: id,
        bytes: vec![4],
    })
    .await;
    let code = c
        .event(id, |e| match e {
            SessionEvent::Exited { code } => Some(*code),
            _ => None,
        })
        .await;
    assert_eq!(code, Some(0));
    c.event(id, |e| matches!(e, SessionEvent::Removed).then_some(()))
        .await;

    assert!(c.sessions().await.is_empty());
    assert!(d.journal()["sessions"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_leaves_a_dormant_session() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    c.spawn(d.spec(id)).await;
    c.attach_until(id, BANNER).await;

    // ^C kills `cat` with SIGINT.
    c.send(Frame::Data {
        session: id,
        bytes: vec![3],
    })
    .await;
    let code = c
        .event(id, |e| match e {
            SessionEvent::Exited { code } => Some(*code),
            _ => None,
        })
        .await;
    assert_ne!(code, Some(0));

    let sessions = c.sessions().await;
    assert_eq!(sessions[0].pid, None);
    assert_eq!(sessions[0].state, SessionState::Exited);
    assert!(matches!(
        c.call(Msg::Attach {
            id,
            rows: 24,
            cols: 80
        })
        .await,
        Msg::Error { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_reloads_as_dormant() {
    let dir = TestDaemon::new_dir();
    let id = Uuid::new_v4();
    let journal = serde_json::json!({"sessions": [{
        "id": id, "cwd": dir.join("work"), "name": "old", "args": ["--resume", "c1"],
        "claude_session_id": "c1", "created_at": 1700000000u64,
    }]});
    let path = TestDaemon::config(&dir).journal;
    crate::paths::write_atomic(&path, journal.to_string().as_bytes()).unwrap();

    let d = TestDaemon::start_in(dir).await;
    let mut c = d.client().await;
    let sessions = c.sessions().await;
    assert_eq!(sessions.len(), 1);
    let s = &sessions[0];
    assert_eq!((s.id, s.pid, s.state), (id, None, SessionState::Exited));
    assert_eq!(s.claude_session_id.as_deref(), Some("c1"));
    assert_eq!(s.created_at, 1700000000);

    // Re-spawning the dormant entry starts it and keeps its history.
    let pid = c.spawn(d.spec(id)).await;
    assert!(pid.is_some());
    let s = &c.sessions().await[0];
    assert_eq!((s.pid, s.created_at), (pid, 1700000000));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resize_resyncs_other_subscribers() {
    let d = TestDaemon::start().await;
    let mut a = d.client().await;
    let mut b = d.client().await;
    let id = Uuid::new_v4();
    a.spawn(d.spec(id)).await;
    a.attach(id, 24, 80).await;
    b.attach(id, 24, 80).await;

    assert_eq!(
        a.call(Msg::Resize {
            id,
            rows: 30,
            cols: 100
        })
        .await,
        Msg::Ok
    );
    let size = b
        .until(|f| match f {
            Frame::Control(Envelope {
                req: None,
                msg: Msg::Attached { id: i, rows, cols },
            }) if *i == id => Some((*rows, *cols)),
            _ => None,
        })
        .await;
    assert_eq!(size, (30, 100));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_dir_sorts_dirs_first() {
    let d = TestDaemon::start().await;
    let root = d.dir.join("browse");
    std::fs::create_dir_all(root.join("zdir")).unwrap();
    std::fs::create_dir_all(root.join("bdir")).unwrap();
    std::fs::write(root.join("afile"), b"").unwrap();
    let mut c = d.client().await;

    let path = root.to_string_lossy().into_owned();
    match c.call(Msg::ListDir { path: path.clone() }).await {
        Msg::DirEntries {
            path: p, entries, ..
        } => {
            assert_eq!(p, path);
            let got: Vec<(&str, bool)> = entries.iter().map(|e| (e.name.as_str(), e.dir)).collect();
            assert_eq!(got, [("bdir", true), ("zdir", true), ("afile", false)]);
        }
        other => panic!("expected entries, got {other:?}"),
    }
    let missing = Msg::ListDir {
        path: format!("{path}/nope"),
    };
    assert!(matches!(c.call(missing).await, Msg::Error { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_requests_get_errors_and_keep_the_connection() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;

    // An op this daemon doesn't know (e.g. from a newer client).
    let body = br#"J{"req":77,"op":"teleport"}"#;
    let mut frame = (body.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    tokio::io::AsyncWriteExt::write_all(&mut c.stream, &frame)
        .await
        .unwrap();
    let reply = c
        .until(|f| match f {
            Frame::Control(env) if env.req == Some(77) => Some(env.msg.clone()),
            _ => None,
        })
        .await;
    assert!(matches!(reply, Msg::Error { .. }));

    // A known op the daemon doesn't serve.
    assert!(matches!(c.call(Msg::Pong).await, Msg::Error { .. }));
    // Spawning into a missing directory fails cleanly.
    let mut spec = d.spec(Uuid::new_v4());
    spec.cwd = d.dir.join("missing").to_string_lossy().into_owned();
    assert!(matches!(c.call(Msg::Spawn(spec)).await, Msg::Error { .. }));
    assert_eq!(c.call(Msg::Ping).await, Msg::Pong);
}

/// When a session is spawned with `--resume` and claude exits immediately with
/// a non-zero code before any SessionStart hook fires, the daemon should:
/// 1. Broadcast a `Notice` event (not `Title`) with a descriptive message.
/// 2. Spawn a fresh session (without `--resume`), which becomes live.
/// 3. `ListSessions` shows one live session with a NEW pid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_retry_spawns_fresh_on_quick_exit() {
    // Use a daemon backed by the fake claude that fails on --resume.
    let dir = TestDaemon::new_dir();
    let config = Config {
        socket: dir.join("rt/d.sock"),
        lock: dir.join("rt/d.lock"),
        journal: dir.join("state/journal.json"),
        claude: fake_claude_resume_fails().to_path_buf(),
        claudio: PathBuf::from("/bin/true"),
        skip_permissions: true,
        build: None,
        path: None,
    };
    // Pre-populate a journal entry with a claude_session_id so the spawn
    // request carries --resume.
    let journal_path = &config.journal;
    std::fs::create_dir_all(journal_path.parent().unwrap()).unwrap();
    let id = Uuid::new_v4();
    let journal_entry = serde_json::json!({
        "sessions": [{
            "id": id,
            "cwd": dir.join("work"),
            "name": "resume-test",
            "args": [],
            "claude_session_id": "old-conv-id",
            "created_at": 1700000000u64
        }]
    });
    crate::paths::write_atomic(journal_path, journal_entry.to_string().as_bytes()).unwrap();

    let listening = server::start(config).unwrap().expect("lock is free");
    tokio::spawn(listening.serve());
    let socket = dir.join("rt/d.sock");
    // Wait for daemon to start.
    within(async {
        loop {
            if tokio::net::UnixStream::connect(&socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    // Use TWO clients: `observer` subscribes first and receives every broadcast
    // independently; `spawner` sends the Spawn request and waits for Spawned.
    // This avoids a race where the actor broadcasts `Notice` (and the retry
    // `Created`) concurrently with the daemon sending the `Spawned` reply, so
    // `spawner.spawn`'s internal `until` loop might consume the Notice frame
    // before the test gets to read it.
    let mut observer = Client::connect(&socket).await;
    let mut spawner = Client::connect(&socket).await;

    let spec = SpawnSpec {
        id,
        cwd: dir.join("work").to_string_lossy().into_owned(),
        name: Some("resume-test".into()),
        args: vec!["--resume".into(), "old-conv-id".into()],
        env: vec![],
        rows: 24,
        cols: 80,
        ephemeral: false,
    };

    // spawner handles the Spawn/Spawned handshake; observer watches events.
    let initial_pid = spawner.spawn(spec).await;

    // Wait for the Notice event (resume failed → retry fresh).
    let notice_text = observer
        .event(id, |e| match e {
            SessionEvent::Notice { text } => Some(text.clone()),
            _ => None,
        })
        .await;
    assert!(
        notice_text.contains("conversation not found"),
        "expected conversation-not-found notice, got: {notice_text:?}"
    );

    // Wait for the new fresh session's Created event (different pid).
    // Skip the initial Created (if observer catches it before Spawned arrives
    // on the spawner) by waiting for any Created that appears after the Notice.
    let new_info = observer
        .event(id, |e| match e {
            SessionEvent::Created { info } => Some(info.clone()),
            _ => None,
        })
        .await;
    assert!(
        new_info.pid != initial_pid || new_info.pid.is_none(),
        "fresh spawn should have a different pid; got {:?} vs initial {:?}",
        new_info.pid,
        initial_pid
    );
    assert_eq!(new_info.state, proto::SessionState::Starting);

    // ListSessions must show exactly one live session (the fresh one).
    let sessions = spawner.sessions().await;
    assert_eq!(sessions.len(), 1, "should have exactly one session");
    assert!(
        sessions[0].pid.is_some(),
        "fresh session should be live (pid != None)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A child that crashes immediately (exit 3, no --resume) must leave the session
/// dormant (pid=None) and must not strand a stale live handle for a dead pid.
///
/// This is a direct regression test for the commit-before-exit race: the fake
/// claude exits before `Daemon::spawn` Phase 3 has a chance to commit the
/// handle to `registry.live`. The §Race fix (committed oneshot signal) makes
/// the actor wait for the commit before running on_exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_exit_leaves_session_dormant() {
    let dir = TestDaemon::new_dir();
    let config = Config {
        socket: dir.join("rt/d.sock"),
        lock: dir.join("rt/d.lock"),
        journal: dir.join("state/journal.json"),
        claude: fake_claude_exits_immediately().to_path_buf(),
        claudio: PathBuf::from("/bin/true"),
        skip_permissions: true,
        build: None,
        path: None,
    };

    let listening = server::start(config).unwrap().expect("lock is free");
    tokio::spawn(listening.serve());
    let socket = dir.join("rt/d.sock");
    within(async {
        loop {
            if tokio::net::UnixStream::connect(&socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    let mut c = Client::connect(&socket).await;
    let id = Uuid::new_v4();
    let spec = SpawnSpec {
        id,
        cwd: dir.join("work").to_string_lossy().into_owned(),
        name: Some("immediate-exit".into()),
        args: vec![],
        env: vec![],
        rows: 24,
        cols: 80,
        ephemeral: false,
    };
    c.spawn(spec).await;

    // Session must become dormant (pid == None) — not stranded as "live" with
    // a dead pid. We poll `sessions()` rather than waiting for the `Exited`
    // event, because the event may have been consumed by `c.spawn`'s internal
    // frame-draining loop (it arrives between `Created` and `Spawned`).
    within(async {
        loop {
            let sessions = c.sessions().await;
            let s = sessions
                .iter()
                .find(|s| s.id == id)
                .expect("session in list");
            if s.pid.is_none() {
                return; // dormant — correct
            }
            // Still reports a live pid — wait and retry.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    let _ = std::fs::remove_dir_all(&dir);
}

/// Kill a session while its spawn is still in-flight (i.e. before the daemon
/// has committed the actor to the live registry). The kill must succeed (Ok)
/// rather than returning "no such session", and the session must not appear
/// live afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_in_flight_session_succeeds() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();

    // Start the spawn and immediately issue a Kill — both are sent over c.
    // We accept that the spawn may or may not have committed by the time Kill
    // arrives; what matters is that Kill returns Ok in both cases.
    c.request(Msg::Spawn(d.spec(id))).await;
    let kill_result = c.call(Msg::Kill { id }).await;

    // Kill must succeed (Ok), not "no such session", regardless of timing.
    assert_eq!(
        kill_result,
        Msg::Ok,
        "Kill of in-flight (or just-committed) session must return Ok; got {kill_result:?}"
    );

    // After all events settle, the session must not appear live in ListSessions.
    // We poll until settled or timeout.
    within(async {
        loop {
            let sessions = c.sessions().await;
            let found = sessions.iter().find(|s| s.id == id);
            match found {
                // Session is gone — success.
                None => return,
                // Session is dormant/exited — acceptable (kill happened, actor finished).
                Some(s) if s.pid.is_none() => return,
                // Still live — wait a bit and retry.
                Some(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await;
}

/// A fake claude that reports the version in its `version` file and, on
/// `update`, either bumps it to 2.0.0 or (with a `fail` file) exits 1.
fn fake_claude_updatable(dir: &Path) -> PathBuf {
    let bin = dir.join("claude");
    std::fs::write(dir.join("version"), "1.0.0 (Claude Code)\n").unwrap();
    let script = "#!/bin/sh\n\
         D=$(dirname \"$0\")\n\
         if [ \"$1\" = \"--version\" ]; then cat \"$D/version\"; exit 0; fi\n\
         if [ \"$1\" = \"update\" ]; then\n\
           echo \"Checking for updates\"\n\
           echo \"permission note\" >&2\n\
           if [ -e \"$D/fail\" ]; then exit 1; fi\n\
           echo \"2.0.0 (Claude Code)\" > \"$D/version\"\n\
           echo \"Updated to 2.0.0\"; exit 0\n\
         fi\n\
         exit 3\n";
    std::fs::write(&bin, script).unwrap();
    std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    bin
}

async fn welcome_claude_version(d: &TestDaemon) -> Option<String> {
    let mut c = Client::raw(&d.config.socket).await;
    match c.call(Msg::Hello(hello(PROTO))).await {
        Msg::Welcome(w) => w.host.claude.map(|c| c.version),
        other => panic!("expected welcome, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_claude_runs_claude_update_and_refreshes_the_host() {
    let dir = TestDaemon::new_dir();
    let claude = fake_claude_updatable(&dir);
    let d = TestDaemon::start_with_claude(dir.clone(), claude).await;
    assert_eq!(
        welcome_claude_version(&d).await.as_deref(),
        Some("1.0.0 (Claude Code)")
    );

    let mut c = d.client().await;
    match c.call(Msg::UpdateClaude { install: false }).await {
        Msg::ClaudeUpdated { version, ok, tail } => {
            assert!(ok, "update failed: {tail}");
            assert_eq!(version.as_deref(), Some("2.0.0 (Claude Code)"));
            assert!(tail.contains("Checking for updates"), "stdout in tail: {tail:?}");
            assert!(tail.contains("permission note"), "stderr in tail: {tail:?}");
            assert!(tail.ends_with("Updated to 2.0.0"), "tail is trimmed: {tail:?}");
        }
        other => panic!("expected ClaudeUpdated, got {other:?}"),
    }
    // The next Welcome reports the refreshed version, and the connection that
    // asked kept working meanwhile.
    assert_eq!(
        welcome_claude_version(&d).await.as_deref(),
        Some("2.0.0 (Claude Code)")
    );
    assert_eq!(c.call(Msg::Ping).await, Msg::Pong);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_update_reports_ok_false_beside_other_requests() {
    let dir = TestDaemon::new_dir();
    let claude = fake_claude_updatable(&dir);
    std::fs::write(dir.join("fail"), "").unwrap();
    let d = TestDaemon::start_with_claude(dir.clone(), claude).await;

    let mut c = d.client().await;
    // The reply comes from a task: a Ping sent right behind it is answered
    // whichever finishes first, and both arrive.
    let update = c.request(Msg::UpdateClaude { install: false }).await;
    let ping = c.request(Msg::Ping).await;
    let (mut updated, mut ponged) = (None, false);
    c.until(|f| {
        if let Frame::Control(env) = f {
            if env.req == Some(update) {
                updated = Some(env.msg.clone());
            } else if env.req == Some(ping) {
                ponged = env.msg == Msg::Pong;
            }
        }
        (updated.is_some() && ponged).then_some(())
    })
    .await;
    match updated.unwrap() {
        Msg::ClaudeUpdated { version, ok, tail } => {
            assert!(!ok);
            assert_eq!(version.as_deref(), Some("1.0.0 (Claude Code)"));
            assert!(tail.contains("permission note"));
        }
        other => panic!("expected ClaudeUpdated, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_with_a_missing_claude_reports_failure() {
    let dir = TestDaemon::new_dir();
    let d = TestDaemon::start_with_claude(dir.clone(), dir.join("nope/claude")).await;
    let mut c = d.client().await;
    match c.call(Msg::UpdateClaude { install: false }).await {
        Msg::ClaudeUpdated { version, ok, tail } => {
            assert!(!ok && version.is_none(), "{tail}");
        }
        other => panic!("expected ClaudeUpdated, got {other:?}"),
    }
}

// ── Terminal tabs (SessionKind::Shell) ───────────────────────────────────────

impl TestDaemon {
    /// `SpawnShell` for `id` in the work dir. The shell is pinned to `/bin/sh`
    /// so the tests do not depend on the user's login shell and rc files.
    fn shell_spec(&self, id: SessionId) -> proto::ShellSpec {
        std::env::set_var("SHELL", "/bin/sh");
        proto::ShellSpec {
            id,
            cwd: self.work().to_string_lossy().into_owned(),
            name: None,
            rows: 24,
            cols: 80,
        }
    }
}

impl Client {
    async fn spawn_shell(&mut self, spec: proto::ShellSpec) -> Option<u32> {
        match self.call(Msg::SpawnShell(spec)).await {
            Msg::Spawned { pid, .. } => pid,
            other => panic!("spawn_shell failed: {other:?}"),
        }
    }

    async fn type_line(&mut self, id: SessionId, line: &str) {
        self.send(Frame::Data {
            session: id,
            bytes: format!("{line}\r").into_bytes(),
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shell_spawn_runs_a_shell_and_echoes() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();

    let req = c.request(Msg::SpawnShell(d.shell_spec(id))).await;
    let created = c
        .until(|f| match f {
            Frame::Control(Envelope {
                msg:
                    Msg::Event {
                        event: SessionEvent::Created { info },
                        ..
                    },
                ..
            }) => Some(info.clone()),
            _ => None,
        })
        .await;
    assert_eq!(created.kind, proto::SessionKind::Shell);
    assert_eq!(created.state, SessionState::Idle, "shells never wait on hooks");
    c.until(|f| matches!(f, Frame::Control(env) if env.req == Some(req)).then_some(()))
        .await;

    c.attach(id, 24, 80).await;
    c.type_line(id, "echo hi-$((20 + 22))").await;
    c.output_until(id, "hi-42").await;

    // Not claude: the fake would have written its --settings here.
    assert!(!d.work().join("settings.json").exists());
    let journal = d.journal();
    let entry = &journal["sessions"][0];
    assert_eq!(entry["kind"], "shell");
    assert_eq!(entry["args"], serde_json::json!([]));
    assert_eq!(c.sessions().await[0].kind, proto::SessionKind::Shell);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shell_closes_on_any_exit_status() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    c.spawn_shell(d.shell_spec(id)).await;
    c.attach(id, 24, 80).await;

    // A claude session would stay dormant after a status-1 exit.
    c.type_line(id, "exit 1").await;
    let code = c
        .event(id, |e| match e {
            SessionEvent::Exited { code } => Some(*code),
            _ => None,
        })
        .await;
    assert_eq!(code, Some(1));
    c.event(id, |e| matches!(e, SessionEvent::Removed).then_some(()))
        .await;
    assert!(c.sessions().await.is_empty());
    assert!(d.journal()["sessions"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_for_a_journaled_shell_starts_a_shell() {
    // An old client (or a recovering one) sends a plain `Spawn`, with claude
    // args and a proxy env, for a terminal. The journal says it is a shell.
    let dir = TestDaemon::new_dir();
    let id = Uuid::new_v4();
    let journal = serde_json::json!({"sessions": [{
        "id": id, "cwd": dir.join("work"), "kind": "shell", "created_at": 1700000000u64,
    }]});
    let path = TestDaemon::config(&dir).journal;
    crate::paths::write_atomic(&path, journal.to_string().as_bytes()).unwrap();

    let d = TestDaemon::start_in(dir).await;
    let mut c = d.client().await;
    let dormant = &c.sessions().await[0];
    assert_eq!(dormant.kind, proto::SessionKind::Shell);
    assert_eq!(dormant.pid, None);

    let mut spec = d.spec(id);
    spec.args = vec!["--resume".into(), "c1".into()];
    std::env::set_var("SHELL", "/bin/sh");
    assert!(c.spawn(spec).await.is_some());
    c.attach(id, 24, 80).await;
    c.type_line(id, "echo [${SECRET_ENV}]").await;
    let seen = c.output_until(id, "[]").await;
    assert!(!seen.contains("hunter2"), "a shell gets no proxy env");
    assert!(!d.work().join("settings.json").exists(), "claude was not run");

    let s = &c.sessions().await[0];
    assert_eq!((s.kind, s.created_at), (proto::SessionKind::Shell, 1700000000));
}
mod freshness;
mod git;
mod login_path;

// ── Respawn (reset a session in place) ───────────────────────────────────────

/// A fake claude that appends its arguments, one per line and closed by a
/// `---` line, to `argv.log` in its cwd, then behaves like the normal fake.
fn fake_claude_logging_argv() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("claudio-fake-claude-av-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("claude");
        let script = format!(
            "#!/bin/sh\n\
             if [ \"$1\" = \"--version\" ]; then echo '9.9.9 (Claude Code)'; exit 0; fi\n\
             if [ \"$1\" = \"--help\" ]; then exit 0; fi\n\
             printf '%s' \"$2\" > settings.json\n\
             {{ for a in \"$@\"; do printf '%s\\n' \"$a\"; done; echo ---; }} >> argv.log\n\
             echo {BANNER}\n\
             exec cat\n"
        );
        std::fs::write(&bin, script).unwrap();
        std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        bin
    })
}

impl TestDaemon {
    async fn start_logging_argv() -> TestDaemon {
        Self::start_with_claude(Self::new_dir(), fake_claude_logging_argv().to_path_buf()).await
    }

    /// The arguments of every claude run so far, without the daemon's
    /// `--settings <json>` prefix.
    fn runs(&self) -> Vec<Vec<String>> {
        let log = std::fs::read_to_string(self.work().join("argv.log")).unwrap_or_default();
        log.split("---\n")
            .filter(|run| !run.is_empty())
            .map(|run| run.lines().skip(2).map(str::to_owned).collect())
            .collect()
    }

    /// Wait until the fake claude has been started `n` times.
    async fn runs_reach(&self, n: usize) {
        within(async {
            while self.runs().len() < n {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
    }
}

impl Client {
    /// Like `call`, also returning the session events seen until the reply.
    async fn call_seen(&mut self, msg: Msg) -> (Msg, Vec<SessionEvent>) {
        let mut seen = Vec::new();
        let req = self.request(msg).await;
        let reply = self
            .until(|f| match f {
                Frame::Control(env) if env.req == Some(req) => Some(env.msg.clone()),
                Frame::Control(Envelope {
                    req: None,
                    msg: Msg::Event { event, .. },
                }) => {
                    seen.push(event.clone());
                    None
                }
                _ => None,
            })
            .await;
        (reply, seen)
    }

    async fn respawn(&mut self, id: SessionId, fresh: bool) -> (Option<u32>, Vec<SessionEvent>) {
        let spec = proto::RespawnSpec {
            id,
            fresh,
            env: vec![("SECRET_ENV".into(), "hunter2".into())],
            rows: 24,
            cols: 80,
        };
        match self.call_seen(Msg::Respawn(spec)).await {
            (Msg::Spawned { id: spawned, pid }, seen) => {
                assert_eq!(spawned, id);
                (pid, seen)
            }
            (other, _) => panic!("respawn failed: {other:?}"),
        }
    }
}

/// The process is gone (or was never ours).
fn is_dead(pid: u32) -> bool {
    // SAFETY: signal 0 only checks that the process exists.
    unsafe { libc::kill(pid as i32, 0) != 0 }
}

/// Wait for `pid` to be gone. A SIGKILLed child lingers as a zombie until
/// its waiter thread reaps it, a moment after the daemon reports it stopped.
async fn wait_dead(pid: u32) {
    within(async {
        while !is_dead(pid) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

fn assert_restarted_in_place(seen: &[SessionEvent]) {
    assert!(
        seen.iter().any(|e| matches!(e, SessionEvent::Created { .. })),
        "a respawn is announced like a spawn: {seen:?}"
    );
    assert!(
        !seen
            .iter()
            .any(|e| matches!(e, SessionEvent::Removed | SessionEvent::Exited { .. })),
        "the tab must not close: {seen:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn respawn_resumes_the_journaled_conversation() {
    let d = TestDaemon::start_logging_argv().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    let mut spec = d.spec(id);
    spec.args = vec!["--model".into(), "m".into(), "--resume".into(), "stale".into()];
    let old_pid = c.spawn(spec).await.unwrap();
    c.attach_until(id, BANNER).await;
    let payload = serde_json::json!({"session_id": "conv-1", "source": "startup"});
    send_hook(&d.config.socket, &d.token().await, "SessionStart", payload).await;
    c.event(id, |e| matches!(e, SessionEvent::ClaudeSession { .. }).then_some(()))
        .await;
    let created_at = d.journal()["sessions"][0]["created_at"].clone();

    let (pid, seen) = c.respawn(id, false).await;
    assert_restarted_in_place(&seen);
    assert_ne!(pid, Some(old_pid));
    wait_dead(old_pid).await;

    d.runs_reach(2).await;
    let runs = d.runs();
    assert_eq!(runs[0], ["--model", "m", "--resume", "stale", "-n", "test"]);
    assert_eq!(runs[1], ["--model", "m", "--resume", "conv-1", "-n", "test"]);

    // Same id, same journal entry (with the conversation), now live again.
    let journal = d.journal();
    assert_eq!(journal["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(journal["sessions"][0]["claude_session_id"], "conv-1");
    assert_eq!(journal["sessions"][0]["created_at"], created_at);
    let (listed, seen) = c.call_seen(Msg::ListSessions).await;
    let Msg::Sessions { sessions } = listed else {
        panic!("expected sessions, got {listed:?}");
    };
    assert_eq!((sessions.len(), sessions[0].id, sessions[0].pid), (1, id, pid));
    assert!(!seen.iter().any(|e| matches!(e, SessionEvent::Removed)));

    // The tab works again after a re-attach.
    c.attach_until(id, BANNER).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_respawn_starts_a_new_conversation() {
    let d = TestDaemon::start_logging_argv().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    let mut spec = d.spec(id);
    spec.args = vec!["--model".into(), "m".into()];
    c.spawn(spec).await;
    c.attach_until(id, BANNER).await;
    let first = d.token().await;

    // No conversation recorded yet (no SessionStart): nothing to resume.
    let (_, seen) = c.respawn(id, false).await;
    assert_restarted_in_place(&seen);
    c.attach_until(id, BANNER).await;

    let payload = serde_json::json!({"session_id": "conv-1", "source": "startup"});
    let token = d.token_other_than(&first).await;
    send_hook(&d.config.socket, &token, "SessionStart", payload).await;
    c.event(id, |e| matches!(e, SessionEvent::ClaudeSession { .. }).then_some(()))
        .await;

    let (_, seen) = c.respawn(id, true).await;
    assert_restarted_in_place(&seen);
    d.runs_reach(3).await;
    let runs = d.runs();
    assert_eq!(runs[1], ["--model", "m", "-n", "test"]);
    assert_eq!(runs[2], ["--model", "m", "-n", "test"], "fresh omits --resume");
    assert_eq!(d.journal()["sessions"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn respawn_revives_a_dormant_session_and_rejects_unknown_ones() {
    let d = TestDaemon::start_logging_argv().await;
    let mut c = d.client().await;
    let unknown = c
        .call(Msg::Respawn(proto::RespawnSpec {
            id: Uuid::new_v4(),
            fresh: false,
            env: vec![],
            rows: 24,
            cols: 80,
        }))
        .await;
    assert!(matches!(unknown, Msg::Error { message } if message.contains("no such session")));

    // A crash leaves the session dormant; a respawn brings it back.
    let id = Uuid::new_v4();
    c.spawn(d.spec(id)).await;
    c.attach_until(id, BANNER).await;
    c.send(Frame::Data {
        session: id,
        bytes: vec![3],
    })
    .await;
    c.event(id, |e| matches!(e, SessionEvent::Exited { .. }).then_some(()))
        .await;
    assert_eq!(c.sessions().await[0].pid, None);
    let (pid, _) = c.respawn(id, false).await;
    assert!(pid.is_some());
    assert_eq!(c.sessions().await[0].pid, pid);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn respawn_restarts_a_shell() {
    let d = TestDaemon::start().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    let old_pid = c.spawn_shell(d.shell_spec(id)).await.unwrap();
    c.attach(id, 24, 80).await;
    c.type_line(id, "FLAG=set; echo flag-[$FLAG]").await;
    c.output_until(id, "flag-[set]").await;

    let (pid, seen) = c.respawn(id, true).await;
    assert_restarted_in_place(&seen);
    assert_ne!(pid, Some(old_pid));
    wait_dead(old_pid).await;

    // A new login shell: the variable is gone, and it is still a terminal.
    c.attach(id, 24, 80).await;
    c.type_line(id, "echo flag-[$FLAG]").await;
    c.output_until(id, "flag-[]").await;
    assert!(!d.work().join("settings.json").exists(), "claude was not run");
    let journal = d.journal();
    assert_eq!(journal["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(journal["sessions"][0]["kind"], "shell");
}

// ── Respawn races ────────────────────────────────────────────────────────────

/// A fake claude that ignores SIGHUP, so stopping it takes the actor's whole
/// kill grace (3 s): a wide, deterministic window in the middle of a respawn.
fn fake_claude_slow_to_stop() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("claudio-fake-claude-ss-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join("claude");
        let script = format!(
            "#!/bin/sh\n\
             if [ \"$1\" = \"--version\" ]; then echo '9.9.9 (Claude Code)'; exit 0; fi\n\
             if [ \"$1\" = \"--help\" ]; then exit 0; fi\n\
             trap '' HUP\n\
             printf '%s' \"$2\" > settings.json\n\
             echo {BANNER}\n\
             exec cat\n"
        );
        std::fs::write(&bin, script).unwrap();
        std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        bin
    })
}

fn respawn_spec(id: SessionId, fresh: bool) -> proto::RespawnSpec {
    proto::RespawnSpec {
        id,
        fresh,
        env: vec![],
        rows: 24,
        cols: 80,
    }
}

impl Client {
    /// Wait until `id` no longer runs `old_pid`: a respawn has taken it over
    /// and is stopping the old process (or, where it died at once, already
    /// started the new one).
    async fn until_stopping(&mut self, id: SessionId, old_pid: u32) {
        within(async {
            while self
                .sessions()
                .await
                .iter()
                .any(|s| s.id == id && s.pid == Some(old_pid))
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
    }

    /// The reply to request `req`.
    async fn reply(&mut self, req: u64) -> Msg {
        self.until(|f| match f {
            Frame::Control(env) if env.req == Some(req) => Some(env.msg.clone()),
            _ => None,
        })
        .await
    }
}

/// A Kill from another client while a respawn is stopping the old process
/// wins: it is acknowledged, the respawn gives up, and the session never
/// comes back. A second respawn meanwhile is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_during_a_respawn_is_not_undone() {
    let d = TestDaemon::start_with_claude(
        TestDaemon::new_dir(),
        fake_claude_slow_to_stop().to_path_buf(),
    )
    .await;
    let mut a = d.client().await;
    let mut b = d.client().await;
    let id = Uuid::new_v4();
    let old_pid = a.spawn(d.spec(id)).await.unwrap();
    a.attach_until(id, BANNER).await;

    let respawn = a.request(Msg::Respawn(respawn_spec(id, false))).await;
    b.until_stopping(id, old_pid).await;

    let second = b.call(Msg::Respawn(respawn_spec(id, true))).await;
    assert!(
        matches!(&second, Msg::Error { message } if message.contains("in progress")),
        "{second:?}"
    );
    assert_eq!(b.call(Msg::Kill { id }).await, Msg::Ok);

    let reply = a.reply(respawn).await;
    assert!(matches!(reply, Msg::Error { .. }), "{reply:?}");
    assert!(b.sessions().await.is_empty(), "the killed session came back");
    assert!(d.journal()["sessions"].as_array().unwrap().is_empty());
    wait_dead(old_pid).await;
    assert!(d.daemon.registry().in_flight.is_empty());
}

/// The old process's clean-exit close, landing while a respawn has taken the
/// id over (or after it committed), must leave the new process alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_clean_exit_close_spares_the_respawned_session() {
    let d = TestDaemon::start_with_claude(
        TestDaemon::new_dir(),
        fake_claude_slow_to_stop().to_path_buf(),
    )
    .await;
    let mut a = d.client().await;
    let mut b = d.client().await;
    let id = Uuid::new_v4();
    let old_pid = a.spawn(d.spec(id)).await.unwrap();

    let respawn = a.request(Msg::Respawn(respawn_spec(id, false))).await;
    b.until_stopping(id, old_pid).await;
    // What the old actor does after a clean exit, at the worst moment.
    d.daemon.close_exited(id);
    let pid = match a.reply(respawn).await {
        Msg::Spawned { pid, .. } => pid,
        other => panic!("respawn failed: {other:?}"),
    };
    // ...or once the new process is committed.
    d.daemon.close_exited(id);

    let sessions = b.sessions().await;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].pid, pid);
    assert_eq!(d.journal()["sessions"].as_array().unwrap().len(), 1);
}

/// A clean exit racing a respawn from another client, end to end: whichever
/// wins, the outcome is consistent. A respawn that answers `Spawned` keeps
/// its session; otherwise the session is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clean_exit_racing_a_respawn_stays_consistent() {
    let d = TestDaemon::start().await;
    let mut a = d.client().await;
    let mut b = d.client().await;
    for _ in 0..5 {
        let id = Uuid::new_v4();
        a.spawn(d.spec(id)).await;
        a.attach_until(id, BANNER).await;
        a.send(Frame::Data {
            session: id,
            bytes: vec![4],
        })
        .await;
        let reply = b.call(Msg::Respawn(respawn_spec(id, false))).await;
        // Let a close, if one is coming, land.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let listed = b.sessions().await.into_iter().find(|s| s.id == id);
        match reply {
            Msg::Spawned { pid, .. } => {
                let s = listed.expect("a respawned session was closed");
                assert_eq!(s.pid, pid);
                b.call(Msg::Kill { id }).await;
            }
            other => assert!(listed.is_none(), "{other:?} but still listed"),
        }
    }
}

/// Every client attached to a session keeps receiving it after another
/// client resets it: it gets `Attached` and the new snapshot unasked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_respawn_keeps_other_clients_attached() {
    let d = TestDaemon::start().await;
    let mut a = d.client().await;
    let mut b = d.client().await;
    let id = Uuid::new_v4();
    a.spawn(d.spec(id)).await;
    a.attach_until(id, BANNER).await;
    b.attach_until(id, BANNER).await;

    a.respawn(id, false).await;
    // `Attached` (unrequested) comes before any of the new process's output.
    b.until(|f| match f {
        Frame::Control(Envelope {
            req: None,
            msg: Msg::Attached { id: i, .. },
        }) if *i == id => Some(()),
        _ => None,
    })
    .await;
    b.output_until(id, BANNER).await;
    b.type_line(id, "after-reset").await;
    b.output_until(id, "after-reset").await;
}

/// A fresh reset drops the old conversation everywhere: the journal, the
/// listing, and clients (told by `ClaudeSessionCleared`). So when the new
/// process dies before its first `SessionStart`, bringing it back does not
/// resume the conversation the user discarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fresh_reset_forgets_the_old_conversation() {
    let d = TestDaemon::start_logging_argv().await;
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    c.spawn(d.spec(id)).await;
    c.attach_until(id, BANNER).await;
    let payload = serde_json::json!({"session_id": "conv-1", "source": "startup"});
    send_hook(&d.config.socket, &d.token().await, "SessionStart", payload).await;
    c.event(id, |e| matches!(e, SessionEvent::ClaudeSession { .. }).then_some(()))
        .await;

    let (_, seen) = c.respawn(id, true).await;
    assert!(seen.contains(&SessionEvent::ClaudeSessionCleared), "{seen:?}");
    let journal = d.journal();
    assert_eq!(journal["sessions"][0]["claude_session_id"], serde_json::Value::Null);

    // The new claude crashes before SessionStart.
    c.attach_until(id, BANNER).await;
    c.send(Frame::Data {
        session: id,
        bytes: vec![3],
    })
    .await;
    c.event(id, |e| matches!(e, SessionEvent::Exited { .. }).then_some(()))
        .await;
    let s = &c.sessions().await[0];
    assert_eq!((s.pid, s.claude_session_id.as_deref()), (None, None));

    c.respawn(id, false).await;
    d.runs_reach(3).await;
    let runs = d.runs();
    assert!(
        !runs[2].iter().any(|a| a == "--resume"),
        "resumed the discarded conversation: {:?}",
        runs[2]
    );
}

/// The `--allow-dangerously-skip-permissions` check is redone after a claude
/// update: a claude missing at startup is not stuck without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_permissions_support_is_rechecked_after_an_update() {
    let dir = TestDaemon::new_dir();
    let claude = dir.join("bin/claude");
    let d = TestDaemon::start_with_claude(dir.clone(), claude.clone()).await;
    let check = |daemon: &Arc<Daemon>| {
        let daemon = Arc::clone(daemon);
        tokio::task::spawn_blocking(move || daemon.skip_permissions())
    };
    assert!(!check(&d.daemon).await.unwrap(), "no claude yet");

    std::fs::create_dir_all(claude.parent().unwrap()).unwrap();
    let script = "#!/bin/sh\n\
         if [ \"$1\" = \"--help\" ]; then echo '  --allow-dangerously-skip-permissions'; fi\n\
         exit 0\n";
    std::fs::write(&claude, script).unwrap();
    std::fs::set_permissions(&claude, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .unwrap();
    assert!(!check(&d.daemon).await.unwrap(), "cached until a refresh");
    d.daemon.refresh_host().await;
    assert!(check(&d.daemon).await.unwrap());
}

/// A session started in a repository announces its git status (branch,
/// ahead/behind its upstream, dirty paths) in `Meta` without any hook: a
/// terminal tab gets it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_carries_the_git_status_of_the_session_directory() {
    let d = TestDaemon::start().await;
    let repo = super::git_status::tests::temp_repo();
    let mut c = d.client().await;
    let id = Uuid::new_v4();
    let spec = SpawnSpec {
        cwd: repo.to_string_lossy().into_owned(),
        ..d.spec(id)
    };
    c.spawn(spec).await;
    let (branch, git) = c
        .event(id, |e| match e {
            SessionEvent::Meta {
                branch,
                git: Some(git),
                ..
            } => Some((branch.clone(), git.clone())),
            _ => None,
        })
        .await;
    assert_eq!(branch.as_deref(), Some("main"));
    assert_eq!(git.branch(), Some("main"));
    assert!(git.upstream);
    assert_eq!((git.ahead, git.behind), (1, 1));
    assert_eq!((git.staged, git.unstaged), (0, 1));
    // The repo's own untracked file, plus the fake claude's `settings.json`
    // when it has been written by the time git looked.
    assert!((1..=2).contains(&git.untracked), "{}", git.untracked);
    assert!(git.is_dirty());
    let _ = std::fs::remove_dir_all(&repo);
}
