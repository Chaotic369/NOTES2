//! Firebase Realtime Database sync engine (Phase 2).
//!
//! Design
//! * All state lives in [`Engine`], owned by the worker thread. Every mutation of the
//!   note store and of the persisted queue therefore happens on the worker (serialized
//!   disk writes).
//! * Network work runs on short-lived helper threads (snapshot GET, one upload at a time)
//!   plus one long-lived stream thread. They only read the current tokens from
//!   [`SyncCtx`] and report back through the `notify` callback (the worker's command
//!   channel). Messages carry the session generation, so a stale session is ignored.
//! * Uploads are compare-and-swap per note (Firebase ETag + `if-match`), so an older local
//!   edit can never overwrite a newer remote note, even when another device writes between
//!   our read and our write.

use crate::auth::{self, AuthError, Refreshed};
use crate::config::AppConfig;
use crate::model::Note;
use crate::queue::Queue;
use crate::rtdb::{get_notes, spawn_stream, upload_note};
use crate::sse::{RemoteState, SseEvent};
use crate::store::NoteStore;
use crate::worker::SyncStatus;
use rand::Rng;
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

/// Consecutive failed stream connections before polling takes over.
const FALLBACK_AFTER: u32 = 3;
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// Entries older than this no longer matter for "acked after the snapshot started".
const ACK_MEMORY: Duration = Duration::from_secs(300);

// ---------------------------------------------------------------- messages

/// Helper thread -> worker.
#[derive(Debug)]
pub enum SyncMsg {
    Snapshot { poll: bool, started: Instant, result: Result<Value, AuthError> },
    Upload { id: String, updated_at: i64, result: Result<UploadOk, AuthError> },
    StreamUp,
    StreamEvent(SseEvent),
    StreamDown { error: AuthError, failures: u32 },
    /// A sync thread refreshed the idToken (or tried to); the worker persists it.
    TokenRefreshed(Result<Refreshed, AuthError>),
}

#[derive(Debug)]
pub enum UploadOk {
    /// Written (or the server already had exactly this version).
    Done,
    /// The server has a newer version; it is merged locally instead.
    Superseded(Note),
}

/// What the worker must do after the engine handled a message.
#[derive(Default)]
pub struct Effects {
    pub updated: Vec<Note>,
    pub errors: Vec<String>,
    pub token: Option<Result<Refreshed, AuthError>>,
}

pub type Notify = Arc<dyn Fn(SyncMsg) + Send + Sync>;

// ---------------------------------------------------------------- shared context

struct Tokens {
    id_token: String,
    refresh_token: String,
}

/// State shared with the helper threads: the *current* tokens (so a refresh is picked up
/// without restarting anything) and the stop flag.
pub struct SyncCtx {
    pub(crate) cfg: AppConfig,
    pub(crate) uid: String,
    tokens: Mutex<Tokens>,
    refresh_gate: Mutex<()>,
    stop: AtomicBool,
    stop_cv: (Mutex<()>, Condvar),
    pub(crate) notify: Notify,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl SyncCtx {
    pub(crate) fn new(cfg: AppConfig, uid: &str, id_token: &str, refresh_token: &str, notify: Notify) -> Self {
        SyncCtx {
            cfg,
            uid: uid.to_string(),
            tokens: Mutex::new(Tokens { id_token: id_token.to_string(), refresh_token: refresh_token.to_string() }),
            refresh_gate: Mutex::new(()),
            stop: AtomicBool::new(false),
            stop_cv: (Mutex::new(()), Condvar::new()),
            notify,
        }
    }

    pub(crate) fn token(&self) -> String {
        lock(&self.tokens).id_token.clone()
    }

    pub(crate) fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let _g = lock(&self.stop_cv.0);
        self.stop_cv.1.notify_all();
    }

    /// Sleep that ends early (returning false) when the session is stopped.
    pub(crate) fn sleep(&self, d: Duration) -> bool {
        let deadline = Instant::now() + d;
        let mut g = lock(&self.stop_cv.0);
        loop {
            if self.stopped() {
                return false;
            }
            let now = Instant::now();
            if now >= deadline {
                return true;
            }
            g = self.stop_cv.1.wait_timeout(g, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// Refresh the idToken unless somebody already did (`stale` is the token that failed).
    /// Serialized, so several threads hitting a 401 together cause one refresh.
    pub(crate) fn refresh_if_stale(&self, stale: &str) -> Result<String, AuthError> {
        if self.stopped() {
            return Err(AuthError::Cancelled);
        }
        let _gate = lock(&self.refresh_gate);
        {
            let t = lock(&self.tokens);
            if !t.id_token.is_empty() && t.id_token != stale {
                return Ok(t.id_token.clone());
            }
        }
        let refresh_token = lock(&self.tokens).refresh_token.clone();
        let result = auth::refresh(&self.cfg, &refresh_token);
        if let Ok(r) = &result {
            if r.uid != self.uid {
                return Err(AuthError::Other("The refreshed token belongs to a different account.".into()));
            }
            let mut t = lock(&self.tokens);
            t.id_token = r.id_token.clone();
            t.refresh_token = r.refresh_token.clone();
        }
        (self.notify)(SyncMsg::TokenRefreshed(result.clone()));
        result.map(|r| r.id_token)
    }

    /// Run an authenticated call with the current token; refresh first if there is none,
    /// and on 401 refresh once and retry.
    pub(crate) fn authed<T>(&self, f: impl Fn(&str) -> Result<T, AuthError>) -> Result<T, AuthError> {
        let mut tok = self.token();
        if tok.is_empty() {
            tok = self.refresh_if_stale("")?;
        }
        match f(&tok) {
            Err(AuthError::Unauthorized) => {
                let fresh = self.refresh_if_stale(&tok)?;
                f(&fresh)
            }
            other => other,
        }
    }
}

// ---------------------------------------------------------------- pure helpers

pub fn next_backoff(d: Duration) -> Duration {
    if d.is_zero() {
        Duration::from_secs(1)
    } else {
        (d * 2).min(MAX_BACKOFF)
    }
}

fn poll_interval() -> Duration {
    Duration::from_millis(rand::thread_rng().gen_range(10_000..=15_000))
}

/// A remote note that carries nothing new for us: our copy is at least as new, or has the
/// same version written by the same device (our own upload coming back through the stream).
pub fn is_echo(local: Option<&Note>, remote: &Note) -> bool {
    match local {
        Some(l) => l.updated_at > remote.updated_at || (l.updated_at == remote.updated_at && l.device_id == remote.device_id),
        None => false,
    }
}

/// Merge incoming remote notes into the store (newest updatedAt wins). Incoming notes
/// are never queued, so they cannot echo back to the server. A queued local edit that the
/// server's version has overtaken is dropped.
fn merge_incoming(store: &mut NoteStore, queue: &mut Queue, notes: Vec<Note>, fx: &mut Effects) {
    let mut changed = false;
    for n in notes {
        let (id, ts) = (n.id.clone(), n.updated_at);
        if !is_echo(store.get(&id), &n) && store.merge_remote(n.clone()) {
            changed = true;
            fx.updated.push(n);
        }
        if let Err(e) = queue.remove_up_to(&id, ts) {
            fx.errors.push(format!("Could not update the upload queue: {e}"));
        }
    }
    if changed {
        if let Err(e) = store.save() {
            fx.errors.push(format!("Could not save synced notes: {e}"));
        }
    }
}

fn is_root(path: &str) -> bool {
    path.trim_matches('/').is_empty()
}

// ---------------------------------------------------------------- engine

pub struct Engine {
    gen: u64,
    ctx: Arc<SyncCtx>,
    queue: Queue,
    remote: RemoteState,

    started: bool,
    initial_done: bool,
    initial_inflight: bool,
    initial_retry_at: Option<Instant>,
    initial_backoff: Duration,

    /// Last network outcome was a success (optimistic once started).
    online: bool,
    /// The server rejected our token even after a refresh.
    denied: bool,

    stream_started: bool,
    stream_up: bool,
    stream_failures: u32,
    stream_connected_at: Instant,
    poll_inflight: bool,
    next_poll_at: Option<Instant>,

    upload_inflight: Option<(String, i64)>,
    upload_retry_at: Option<Instant>,
    upload_backoff: Duration,
    /// When each note was last confirmed uploaded (guards against stale snapshots).
    acked: HashMap<String, Instant>,
}

impl Engine {
    /// Loads the persisted queue. Nothing runs until [`Engine::try_start`] finds a token.
    pub fn new(
        gen: u64,
        cfg: AppConfig,
        uid: &str,
        id_token: &str,
        refresh_token: &str,
        queue_path: PathBuf,
        notify: Notify,
    ) -> (Self, Option<String>) {
        let (queue, warning) = Queue::open(queue_path);
        let engine = Engine {
            gen,
            ctx: Arc::new(SyncCtx::new(cfg, uid, id_token, refresh_token, notify)),
            queue,
            remote: RemoteState::new(),
            started: false,
            initial_done: false,
            initial_inflight: false,
            initial_retry_at: None,
            initial_backoff: Duration::ZERO,
            online: false,
            denied: false,
            stream_started: false,
            stream_up: false,
            stream_failures: 0,
            stream_connected_at: Instant::now(),
            poll_inflight: false,
            next_poll_at: None,
            upload_inflight: None,
            upload_retry_at: None,
            upload_backoff: Duration::ZERO,
            acked: HashMap::new(),
        };
        (engine, warning)
    }

    pub fn gen(&self) -> u64 {
        self.gen
    }

    pub fn set_tokens(&self, id_token: &str, refresh_token: &str) {
        let mut t = lock(&self.ctx.tokens);
        t.id_token = id_token.to_string();
        t.refresh_token = refresh_token.to_string();
    }

    /// Stop every sync thread of this session (they exit at their next wake-up) and
    /// forget the in-memory sync state. The queue file stays on disk: it holds unsent edits.
    pub fn stop(&mut self) {
        self.ctx.stop();
        self.started = false;
        self.stream_up = false;
        self.upload_inflight = None;
        self.remote.clear();
        self.acked.clear();
    }

    /// Begin the initial sync as soon as an idToken exists.
    pub fn try_start(&mut self) {
        if self.started || self.ctx.stopped() || self.ctx.token().is_empty() {
            return;
        }
        self.started = true;
        self.online = true;
        self.request_snapshot(false);
    }

    /// Queue a local edit and try to upload it right away.
    pub fn local_change(&mut self, note: Note) -> io::Result<()> {
        self.queue.push(note)?;
        self.pump();
        Ok(())
    }

    pub fn status(&self) -> SyncStatus {
        if !self.started || !self.online || self.denied {
            SyncStatus::Offline
        } else if !self.initial_done || self.upload_inflight.is_some() || !self.queue.is_empty() {
            SyncStatus::Syncing
        } else {
            SyncStatus::Synced
        }
    }

    fn polling_active(&self) -> bool {
        self.initial_done && !self.stream_up && self.stream_failures >= FALLBACK_AFTER
    }

    /// Earliest moment `tick` has something to do (so the worker can sleep precisely).
    pub fn next_deadline(&self) -> Option<Instant> {
        if !self.started {
            return None;
        }
        let mut best: Option<Instant> = None;
        let mut consider = |t: Instant| best = Some(best.map_or(t, |b| b.min(t)));
        if !self.initial_done && !self.initial_inflight {
            consider(self.initial_retry_at.unwrap_or_else(Instant::now));
        }
        if self.initial_done && self.upload_inflight.is_none() && !self.queue.is_empty() {
            consider(self.upload_retry_at.unwrap_or_else(Instant::now));
        }
        if self.polling_active() && !self.poll_inflight {
            consider(self.next_poll_at.unwrap_or_else(Instant::now));
        }
        best
    }

    /// Timers: initial-sync retry, upload retry with backoff, polling fallback.
    pub fn tick(&mut self) {
        if !self.started || self.ctx.stopped() {
            return;
        }
        let now = Instant::now();
        if !self.initial_done && !self.initial_inflight && self.initial_retry_at.map_or(true, |t| now >= t) {
            self.request_snapshot(false);
        }
        if self.polling_active() && !self.poll_inflight && self.next_poll_at.map_or(true, |t| now >= t) {
            self.request_snapshot(true);
        }
        self.pump();
    }

    pub fn handle(&mut self, msg: SyncMsg, store: &mut NoteStore) -> Effects {
        let mut fx = Effects::default();
        match msg {
            SyncMsg::Snapshot { poll, started, result } => self.on_snapshot(poll, started, result, store, &mut fx),
            SyncMsg::Upload { id, updated_at, result } => self.on_upload(id, updated_at, result, store, &mut fx),
            SyncMsg::StreamUp => {
                self.stream_up = true;
                self.stream_failures = 0;
                self.stream_connected_at = Instant::now();
                self.next_poll_at = None;
                self.mark_online();
            }
            SyncMsg::StreamEvent(ev) => {
                self.mark_online();
                let snapshot = matches!(&ev, SseEvent::Put { path, .. } if is_root(path));
                if let Some(notes) = self.remote.apply(&ev) {
                    merge_incoming(store, &mut self.queue, notes, &mut fx);
                    if snapshot {
                        // Full state after (re)connecting: push what the server lacks.
                        let since = self.stream_connected_at;
                        self.reconcile(store, since, &mut fx);
                    }
                }
            }
            SyncMsg::StreamDown { error, failures } => {
                self.stream_up = false;
                self.stream_failures = failures;
                self.note_failure(&error);
                if failures >= FALLBACK_AFTER && self.next_poll_at.is_none() {
                    self.next_poll_at = Some(Instant::now());
                }
            }
            SyncMsg::TokenRefreshed(r) => fx.token = Some(r),
        }
        self.pump();
        fx
    }

    // ------------------------------------------------------------ internals

    fn mark_online(&mut self) {
        if !self.online {
            // Connection is back: flush the queue immediately instead of waiting out a backoff.
            self.upload_backoff = Duration::ZERO;
            self.upload_retry_at = None;
        }
        self.online = true;
        self.denied = false;
    }

    fn note_failure(&mut self, e: &AuthError) {
        match e {
            AuthError::Network(_) => self.online = false,
            AuthError::Unauthorized => self.denied = true,
            _ => {}
        }
    }

    fn request_snapshot(&mut self, poll: bool) {
        if poll {
            self.poll_inflight = true;
        } else {
            self.initial_inflight = true;
        }
        let ctx = self.ctx.clone();
        thread::spawn(move || {
            let started = Instant::now();
            let result = get_notes(&ctx);
            if !ctx.stopped() {
                (ctx.notify)(SyncMsg::Snapshot { poll, started, result });
            }
        });
    }

    fn on_snapshot(
        &mut self,
        poll: bool,
        started: Instant,
        result: Result<Value, AuthError>,
        store: &mut NoteStore,
        fx: &mut Effects,
    ) {
        if poll {
            self.poll_inflight = false;
            self.next_poll_at = Some(Instant::now() + poll_interval());
        } else {
            self.initial_inflight = false;
        }
        match result {
            Ok(value) => {
                self.mark_online();
                let notes = self.remote.reset_from_snapshot(&value);
                merge_incoming(store, &mut self.queue, notes, fx);
                self.reconcile(store, started, fx);
                if !poll {
                    self.initial_done = true;
                    self.initial_backoff = Duration::ZERO;
                    if !self.stream_started {
                        self.stream_started = true;
                        spawn_stream(self.ctx.clone());
                    }
                }
            }
            Err(e) => {
                self.note_failure(&e);
                if !poll {
                    self.initial_backoff = next_backoff(self.initial_backoff);
                    self.initial_retry_at = Some(Instant::now() + self.initial_backoff);
                }
            }
        }
    }

    fn on_upload(
        &mut self,
        id: String,
        updated_at: i64,
        result: Result<UploadOk, AuthError>,
        store: &mut NoteStore,
        fx: &mut Effects,
    ) {
        self.upload_inflight = None;
        match result {
            Ok(ok) => {
                self.mark_online();
                self.upload_backoff = Duration::ZERO;
                self.upload_retry_at = None;
                match ok {
                    UploadOk::Done => {
                        // A newer queued version of the same note stays queued.
                        if let Err(e) = self.queue.remove_up_to(&id, updated_at) {
                            fx.errors.push(format!("Could not update the upload queue: {e}"));
                        }
                        self.acked.insert(id, Instant::now());
                    }
                    UploadOk::Superseded(remote) => {
                        merge_incoming(store, &mut self.queue, vec![remote], fx);
                    }
                }
            }
            Err(AuthError::Cancelled) => {}
            Err(e) => {
                self.note_failure(&e);
                self.upload_backoff = next_backoff(self.upload_backoff);
                self.upload_retry_at = Some(Instant::now() + self.upload_backoff);
                // A note the server keeps rejecting (HTTP 4xx other than auth/rate limits)
                // must not block the notes behind it.
                if let AuthError::Api { status, .. } = &e {
                    if (400..500).contains(status) && !matches!(*status, 408 | 429) {
                        if let Err(err) = self.queue.rotate() {
                            fx.errors.push(format!("Could not update the upload queue: {err}"));
                        }
                        fx.errors.push(format!("A note could not be uploaded: {e}"));
                    }
                }
            }
        }
    }

    /// Start uploading the oldest queued change, one at a time and in order.
    fn pump(&mut self) {
        if !self.started || !self.initial_done || self.ctx.stopped() || self.upload_inflight.is_some() {
            return;
        }
        if self.upload_retry_at.map_or(false, |t| Instant::now() < t) {
            return;
        }
        let Some(note) = self.queue.front().cloned() else { return };
        self.upload_inflight = Some((note.id.clone(), note.updated_at));
        let ctx = self.ctx.clone();
        let (id, updated_at) = (note.id.clone(), note.updated_at);
        thread::spawn(move || {
            let result = upload_note(&ctx, &note);
            if !ctx.stopped() {
                (ctx.notify)(SyncMsg::Upload { id, updated_at, result });
            }
        });
    }

    /// After a full snapshot of the server: queue every local note that is missing there
    /// or newer than the server's copy (tombstones included). `snapshot_started` is when
    /// the snapshot was requested; anything confirmed uploaded after that is not stale news.
    fn reconcile(&mut self, store: &NoteStore, snapshot_started: Instant, fx: &mut Effects) {
        self.acked.retain(|_, t| t.elapsed() < ACK_MEMORY);
        for local in store.all() {
            if self.queue.get(&local.id).map_or(false, |q| q.updated_at >= local.updated_at) {
                continue;
            }
            if self.upload_inflight.as_ref().map_or(false, |(id, ts)| *id == local.id && *ts >= local.updated_at) {
                continue;
            }
            if self.acked.get(&local.id).map_or(false, |t| *t >= snapshot_started) {
                continue;
            }
            let needs = match self.remote.get(&local.id) {
                None => true,
                Some(r) => local.updated_at > r.updated_at,
            };
            if needs {
                if let Err(e) = self.queue.push(local) {
                    fx.errors.push(format!("Could not update the upload queue: {e}"));
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::now_ms;
    use serde_json::json;

    fn note(id: &str, updated: i64, device: &str) -> Note {
        Note { id: id.into(), title: format!("t{updated}"), body: "b".into(), created_at: 1, updated_at: updated, deleted: false, device_id: device.into() }
    }

    fn cfg() -> AppConfig {
        AppConfig {
            api_key: "k".into(),
            database_url: "https://db.example.com".into(),
            project_id: "p".into(),
            google_client_id: String::new(),
            google_client_secret: String::new(),
        }
    }

    struct Fixture {
        engine: Engine,
        store: NoteStore,
        dir: PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// An engine that never touches the network (no token, not started).
    fn fixture(tag: &str) -> Fixture {
        let dir = std::env::temp_dir().join(format!("ntnotes-sync-{tag}-{}-{}", std::process::id(), now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let notify: Notify = Arc::new(|_| {});
        let (engine, w) = Engine::new(1, cfg(), "u1", "", "", dir.join("queue.json"), notify);
        assert!(w.is_none());
        let (store, _) = NoteStore::open(dir.join("notes.json"));
        Fixture { engine, store, dir }
    }


    #[test]
    fn backoff_doubles_to_cap() {
        let mut d = Duration::ZERO;
        let mut seen = Vec::new();
        for _ in 0..9 {
            d = next_backoff(d);
            seen.push(d.as_secs());
        }
        assert_eq!(seen, vec![1, 2, 4, 8, 16, 32, 60, 60, 60]);
    }

    // ---- merge rules and echo suppression

    #[test]
    fn echo_detection() {
        let l = note("a", 10, "me");
        assert!(is_echo(Some(&l), &note("a", 10, "me"))); // our upload coming back
        assert!(is_echo(Some(&l), &note("a", 9, "other"))); // older than ours
        assert!(!is_echo(Some(&l), &note("a", 10, "other"))); // tie from another device: merge rules decide
        assert!(!is_echo(Some(&l), &note("a", 11, "me")));
        assert!(!is_echo(None, &note("a", 1, "me")));
    }

    #[test]
    fn newer_remote_wins_and_drops_obsolete_queued_edit() {
        let mut f = fixture("merge");
        f.store.upsert_local(note("a", 10, "me")).unwrap();
        f.engine.queue.push(note("a", 10, "me")).unwrap();
        let mut fx = Effects::default();
        merge_incoming(&mut f.store, &mut f.engine.queue, vec![note("a", 20, "other")], &mut fx);
        assert_eq!(fx.updated.len(), 1);
        assert_eq!(f.store.get("a").unwrap().updated_at, 20);
        assert!(f.engine.queue.is_empty(), "the stale local edit must not be uploaded over the newer remote note");
        // persisted
        let (s2, _) = NoteStore::open(f.dir.join("notes.json"));
        assert_eq!(s2.get("a").unwrap().updated_at, 20);
    }

    #[test]
    fn older_remote_is_ignored_and_local_edit_stays_queued() {
        let mut f = fixture("older");
        f.store.upsert_local(note("a", 30, "me")).unwrap();
        f.engine.queue.push(note("a", 30, "me")).unwrap();
        let mut fx = Effects::default();
        merge_incoming(&mut f.store, &mut f.engine.queue, vec![note("a", 20, "other")], &mut fx);
        assert!(fx.updated.is_empty());
        assert_eq!(f.store.get("a").unwrap().updated_at, 30);
        assert_eq!(f.engine.queue.len(), 1);
    }

    #[test]
    fn echo_of_our_own_upload_is_not_requeued_or_reported() {
        let mut f = fixture("echo");
        f.store.upsert_local(note("a", 10, "me")).unwrap();
        let mut fx = Effects::default();
        // upload finished (queue empty), then the stream delivers the same version
        merge_incoming(&mut f.store, &mut f.engine.queue, vec![note("a", 10, "me")], &mut fx);
        assert!(fx.updated.is_empty());
        assert!(f.engine.queue.is_empty());
    }

    #[test]
    fn stream_events_update_the_store() {
        let mut f = fixture("events");
        let ev = SseEvent::Put { path: "/".into(), data: json!({"a": {"id":"a","title":"A","body":"","createdAt":1,"updatedAt":5,"deleted":false,"deviceId":"x"}}) };
        let fx = f.engine.handle(SyncMsg::StreamEvent(ev), &mut f.store);
        assert_eq!(fx.updated.len(), 1);
        assert_eq!(f.store.get("a").unwrap().title, "A");
        // deep patch of one field
        let ev = SseEvent::Patch { path: "/a".into(), data: json!({"title": "B", "updatedAt": 9}) };
        let fx = f.engine.handle(SyncMsg::StreamEvent(ev), &mut f.store);
        assert_eq!(fx.updated[0].title, "B");
        assert_eq!(f.store.get("a").unwrap().updated_at, 9);
        // tombstone arrives as a normal update
        let ev = SseEvent::Put { path: "/a/deleted".into(), data: json!(true) };
        let ev2 = SseEvent::Put { path: "/a/updatedAt".into(), data: json!(12) };
        f.engine.handle(SyncMsg::StreamEvent(ev), &mut f.store); // same updatedAt: ignored (tie keeps local)
        let fx = f.engine.handle(SyncMsg::StreamEvent(ev2), &mut f.store);
        assert!(fx.updated[0].deleted);
        // removal on the server does not delete our tombstone/note locally
        let ev = SseEvent::Put { path: "/a".into(), data: Value::Null };
        let fx = f.engine.handle(SyncMsg::StreamEvent(ev), &mut f.store);
        assert!(fx.updated.is_empty());
        assert!(f.store.get("a").is_some());
    }

    // ---- reconcile (initial push of local-newer / missing notes)

    #[test]
    fn reconcile_queues_missing_and_newer_notes_only() {
        let mut f = fixture("reconcile");
        for n in [note("missing", 5, "me"), note("newer", 50, "me"), note("older", 5, "me"), note("equal", 7, "me")] {
            f.store.upsert_local(n).unwrap();
        }
        let mut tomb = note("tomb", 9, "me");
        tomb.deleted = true;
        f.store.upsert_local(tomb).unwrap();
        let remote = json!({
            "newer": {"id":"newer","createdAt":1,"updatedAt":40},
            "older": {"id":"older","createdAt":1,"updatedAt":99},
            "equal": {"id":"equal","createdAt":1,"updatedAt":7},
        });
        f.engine.remote.reset_from_snapshot(&remote);
        let mut fx = Effects::default();
        f.engine.reconcile(&f.store, Instant::now(), &mut fx);
        let mut ids: Vec<String> = Vec::new();
        while let Some(n) = f.engine.queue.front().cloned() {
            ids.push(n.id.clone());
            f.engine.queue.remove_up_to(&n.id, i64::MAX).unwrap();
        }
        ids.sort();
        assert_eq!(ids, vec!["missing", "newer", "tomb"]);
    }

    #[test]
    fn reconcile_skips_queued_and_recently_acked_notes() {
        let mut f = fixture("ack");
        f.store.upsert_local(note("q", 5, "me")).unwrap();
        f.store.upsert_local(note("acked", 5, "me")).unwrap();
        f.engine.queue.push(note("q", 5, "me")).unwrap();
        let snapshot_started = Instant::now();
        f.engine.acked.insert("acked".into(), Instant::now()); // confirmed after the snapshot began
        f.engine.remote.reset_from_snapshot(&Value::Null);
        let mut fx = Effects::default();
        f.engine.reconcile(&f.store, snapshot_started, &mut fx);
        assert_eq!(f.engine.queue.len(), 1, "no duplicate for q, nothing for the freshly acked note");
    }

    // ---- upload results

    #[test]
    fn upload_done_keeps_a_newer_queued_version() {
        let mut f = fixture("done");
        f.store.upsert_local(note("a", 5, "me")).unwrap();
        f.engine.queue.push(note("a", 5, "me")).unwrap();
        f.engine.upload_inflight = Some(("a".into(), 5));
        // the user edited again while the upload was in flight
        f.engine.queue.push(note("a", 8, "me")).unwrap();
        let fx = f.engine.handle(SyncMsg::Upload { id: "a".into(), updated_at: 5, result: Ok(UploadOk::Done) }, &mut f.store);
        assert!(fx.errors.is_empty());
        assert_eq!(f.engine.queue.get("a").unwrap().updated_at, 8);
        assert!(f.engine.acked.contains_key("a"));
    }

    #[test]
    fn upload_done_empties_queue_and_superseded_merges_remote() {
        let mut f = fixture("sup");
        f.store.upsert_local(note("a", 5, "me")).unwrap();
        f.store.upsert_local(note("b", 5, "me")).unwrap();
        f.engine.queue.push(note("a", 5, "me")).unwrap();
        f.engine.queue.push(note("b", 5, "me")).unwrap();
        f.engine.upload_inflight = Some(("a".into(), 5));
        f.engine.handle(SyncMsg::Upload { id: "a".into(), updated_at: 5, result: Ok(UploadOk::Done) }, &mut f.store);
        assert_eq!(f.engine.queue.len(), 1);
        f.engine.upload_inflight = Some(("b".into(), 5));
        let fx = f.engine.handle(
            SyncMsg::Upload { id: "b".into(), updated_at: 5, result: Ok(UploadOk::Superseded(note("b", 50, "other"))) },
            &mut f.store,
        );
        assert_eq!(fx.updated.len(), 1);
        assert_eq!(f.store.get("b").unwrap().updated_at, 50);
        assert!(f.engine.queue.is_empty());
    }

    #[test]
    fn network_failure_keeps_queue_and_backs_off() {
        let mut f = fixture("fail");
        f.engine.started = true;
        f.engine.online = true;
        f.engine.initial_done = true;
        f.engine.queue.push(note("a", 5, "me")).unwrap();
        f.engine.upload_inflight = Some(("a".into(), 5));
        f.engine.handle(
            SyncMsg::Upload { id: "a".into(), updated_at: 5, result: Err(AuthError::Network("down".into())) },
            &mut f.store,
        );
        assert_eq!(f.engine.queue.len(), 1, "edits made offline are never dropped");
        assert_eq!(f.engine.upload_backoff, Duration::from_secs(1));
        assert!(f.engine.upload_retry_at.is_some());
        assert_eq!(f.engine.status(), SyncStatus::Offline);
        // the connection returns (e.g. the stream reconnects): backoff is cleared, flush at once
        f.engine.handle(SyncMsg::StreamUp, &mut f.store);
        assert!(f.engine.upload_retry_at.is_none());
        assert_eq!(f.engine.upload_backoff, Duration::ZERO);
    }

    #[test]
    fn queue_survives_restart() {
        let f = fixture("restart");
        let path = f.dir.join("queue.json");
        let mut f = f;
        f.engine.local_change(note("a", 5, "me")).unwrap();
        f.engine.local_change(note("a", 6, "me")).unwrap();
        f.engine.local_change(note("b", 7, "me")).unwrap();
        let notify: Notify = Arc::new(|_| {});
        let (e2, w) = Engine::new(2, cfg(), "u1", "", "", path, notify);
        assert!(w.is_none());
        assert_eq!(e2.queue.len(), 2);
        assert_eq!(e2.queue.get("a").unwrap().updated_at, 6);
    }

    // ---- status

    #[test]
    fn status_transitions() {
        let mut f = fixture("status");
        assert_eq!(f.engine.status(), SyncStatus::Offline); // no token yet
        f.engine.started = true;
        f.engine.online = true;
        assert_eq!(f.engine.status(), SyncStatus::Syncing); // catching up
        f.engine.initial_done = true;
        assert_eq!(f.engine.status(), SyncStatus::Synced);
        f.engine.queue.push(note("a", 1, "me")).unwrap();
        assert_eq!(f.engine.status(), SyncStatus::Syncing);
        f.engine.queue.remove_up_to("a", 1).unwrap();
        assert_eq!(f.engine.status(), SyncStatus::Synced);
        f.engine.online = false;
        assert_eq!(f.engine.status(), SyncStatus::Offline);
        f.engine.online = true;
        f.engine.denied = true;
        assert_eq!(f.engine.status(), SyncStatus::Offline); // no valid token
    }

    #[test]
    fn stream_failures_activate_polling_and_stream_up_stops_it() {
        let mut f = fixture("poll");
        f.engine.started = true;
        f.engine.initial_done = true;
        f.engine.handle(SyncMsg::StreamDown { error: AuthError::Network("x".into()), failures: 2 }, &mut f.store);
        assert!(!f.engine.polling_active());
        f.engine.handle(SyncMsg::StreamDown { error: AuthError::Network("x".into()), failures: 3 }, &mut f.store);
        assert!(f.engine.polling_active());
        assert!(f.engine.next_deadline().is_some());
        f.engine.handle(SyncMsg::StreamUp, &mut f.store);
        assert!(!f.engine.polling_active());
        assert!(f.engine.next_deadline().is_none());
    }

    #[test]
    fn stopped_engine_forgets_state_but_keeps_queue_file() {
        let mut f = fixture("stop");
        f.engine.local_change(note("a", 1, "me")).unwrap();
        f.engine.stop();
        assert_eq!(f.engine.status(), SyncStatus::Offline);
        assert!(f.engine.ctx.stopped());
        assert!(f.dir.join("queue.json").exists());
    }
}
