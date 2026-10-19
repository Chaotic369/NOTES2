//! Background worker. Owns the session, the local note store and the settings file.
//! The UI talks to it through `Cmd` (UI -> worker) and `Evt` (worker -> UI) channels and
//! never blocks. Network calls run on short-lived helper threads that report back to the
//! worker through the same command channel.
//!
//! Phase 2 (Firebase sync) plugs in through the Phase 1 hooks:
//!  * `Worker::on_local_change`   - queues the note for upload (persisted offline queue).
//!  * `Worker::begin_session`     - creates the per-user `sync::Engine`.
//!  * `Worker::set_status`        - reports Synced / Syncing / Offline.
//!  * `NoteStore::merge_remote`   - "newest updatedAt wins" merge of remote notes.
//! The engine itself (see sync.rs) is owned by the worker; its helper threads talk back
//! through `Cmd::Sync`, tagged with the session generation.

use crate::auth::{self, AuthError, Refreshed, Session};
use crate::config::{self, AppConfig, Settings};
use crate::model::Note;
use crate::secure::{self, StoredSession};
use crate::store::NoteStore;
use crate::sync::{Effects, Engine, Notify, SyncMsg};
use eframe::egui;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const REFRESH_EVERY: Duration = Duration::from_secs(50 * 60);
const RETRY_AFTER: Duration = Duration::from_secs(60);
const NO_TOKEN_RETRY: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SyncStatus {
    Synced,
    Syncing,
    Offline,
}

/// UI -> worker.
pub enum Cmd {
    SignIn { email: String, password: String },
    SignUp { email: String, password: String },
    ResetPassword { email: String },
    GoogleSignIn,
    CancelGoogle,
    Logout,
    SaveNote(Note),
    SetTheme(bool),
    Quit,
    // internal (sent by helper threads)
    AuthDone { result: Result<Session, AuthError>, google: bool },
    RefreshDone(Result<Refreshed, AuthError>),
    /// Sync helper thread -> worker. `gen` identifies the session; stale ones are ignored.
    Sync { gen: u64, msg: SyncMsg },
}

/// Worker -> UI.
pub enum Evt {
    NoSession,
    SignedIn { email: String, notes: Vec<Note> },
    /// Notes that changed because of a remote update (newer than what the UI had),
    /// including deleted tombstones.
    NotesUpdated(Vec<Note>),
    SignedOut { message: Option<String> },
    AuthError(String),
    AuthCancelled,
    Info(String),
    Status(SyncStatus),
    Error(String),
}

#[derive(Clone)]
struct Notifier {
    tx: Sender<Evt>,
    ctx: egui::Context,
}

impl Notifier {
    fn send(&self, e: Evt) {
        let _ = self.tx.send(e);
        self.ctx.request_repaint();
    }
}

struct ActiveUser {
    session: Session,
    store: NoteStore,
    engine: Engine,
    next_refresh: Instant,
}

struct Worker {
    cfg: AppConfig,
    settings: Settings,
    settings_fresh: bool,
    rx: Receiver<Cmd>,
    self_tx: Sender<Cmd>,
    out: Notifier,
    user: Option<ActiveUser>,
    google_cancel: Arc<AtomicBool>,
    google_running: bool,
    refreshing: bool,
    next_gen: u64,
    last_status: Option<SyncStatus>,
}

pub fn spawn(
    cfg: AppConfig,
    settings: Settings,
    settings_fresh: bool,
    ctx: egui::Context,
) -> (Sender<Cmd>, Receiver<Evt>, JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::channel();
    let (evt_tx, evt_rx) = mpsc::channel();
    let worker = Worker {
        cfg,
        settings,
        settings_fresh,
        rx: cmd_rx,
        self_tx: cmd_tx.clone(),
        out: Notifier { tx: evt_tx, ctx },
        user: None,
        google_cancel: Arc::new(AtomicBool::new(false)),
        google_running: false,
        refreshing: false,
        next_gen: 1,
        last_status: None,
    };
    let handle = thread::Builder::new()
        .name("ntnotes-worker".into())
        .spawn(move || worker.run())
        .expect("failed to start worker thread");
    (cmd_tx, evt_rx, handle)
}

impl Worker {
    fn run(mut self) {
        if self.settings_fresh {
            self.save_settings();
        }
        self.restore_session();
        loop {
            match self.rx.recv_timeout(self.wait_time()) {
                Ok(cmd) => {
                    if !self.handle(cmd) {
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            self.tick();
        }
        if let Some(user) = self.user.as_mut() {
            user.engine.stop();
        }
    }

    // ------------------------------------------------------------ commands

    /// Returns false when the worker should stop.
    fn handle(&mut self, cmd: Cmd) -> bool {
        match cmd {
            Cmd::SignIn { email, password } => {
                self.spawn_auth(false, move |cfg| auth::sign_in(cfg, &email, &password))
            }
            Cmd::SignUp { email, password } => {
                self.spawn_auth(false, move |cfg| auth::sign_up(cfg, &email, &password))
            }
            Cmd::ResetPassword { email } => {
                let cfg = self.cfg.clone();
                let out = self.out.clone();
                thread::spawn(move || match auth::send_password_reset(&cfg, &email) {
                    Ok(()) => out.send(Evt::Info(format!(
                        "Password reset email sent to {email}. Check your inbox."
                    ))),
                    Err(e) => out.send(Evt::AuthError(e.to_string())),
                });
            }
            Cmd::GoogleSignIn => {
                if self.google_running {
                    return true;
                }
                self.google_running = true;
                self.google_cancel.store(false, Ordering::SeqCst);
                let cancel = self.google_cancel.clone();
                self.spawn_auth(true, move |cfg| auth::google_sign_in(cfg, &cancel));
            }
            Cmd::CancelGoogle => self.google_cancel.store(true, Ordering::SeqCst),
            Cmd::Logout => self.sign_out(None),
            Cmd::SaveNote(note) => self.save_note(note),
            Cmd::SetTheme(dark) => {
                self.settings.dark_mode = dark;
                self.save_settings();
            }
            Cmd::Quit => return false,
            Cmd::AuthDone { result, google } => self.auth_done(result, google),
            Cmd::RefreshDone(result) => self.refresh_done(result),
            Cmd::Sync { gen, msg } => self.sync_msg(gen, msg),
        }
        true
    }

    /// How long the loop may sleep: the 5 s housekeeping interval, shortened to the next
    /// token refresh or sync timer (backoff retry, polling) so those fire on time.
    fn wait_time(&self) -> Duration {
        let now = Instant::now();
        let mut d = Duration::from_secs(5);
        if let Some(u) = &self.user {
            if !self.refreshing {
                d = d.min(u.next_refresh.saturating_duration_since(now));
            }
            if let Some(t) = u.engine.next_deadline() {
                d = d.min(t.saturating_duration_since(now));
            }
        }
        d.max(Duration::from_millis(50))
    }

    fn spawn_auth(
        &mut self,
        google: bool,
        f: impl FnOnce(&AppConfig) -> Result<Session, AuthError> + Send + 'static,
    ) {
        let cfg = self.cfg.clone();
        let tx = self.self_tx.clone();
        thread::spawn(move || {
            let result = f(&cfg);
            let _ = tx.send(Cmd::AuthDone { result, google });
        });
    }

    fn auth_done(&mut self, result: Result<Session, AuthError>, google: bool) {
        if google {
            self.google_running = false;
        }
        match result {
            Ok(session) => {
                self.remember(&session);
                if self.settings.last_email != session.email {
                    self.settings.last_email = session.email.clone();
                    self.save_settings();
                }
                self.begin_session(session, Instant::now() + REFRESH_EVERY);
            }
            Err(AuthError::Cancelled) => self.out.send(Evt::AuthCancelled),
            Err(e) => self.out.send(Evt::AuthError(e.to_string())),
        }
    }

    fn refresh_done(&mut self, result: Result<Refreshed, AuthError>) {
        self.refreshing = false;
        match result {
            Ok(r) => self.apply_refreshed(r),
            Err(e) if e.is_session_dead() => {
                self.sign_out(Some("Your session has expired. Please sign in again.".into()));
            }
            Err(_) => {
                // Offline or a temporary server problem: keep working locally, retry soon
                // (sooner while there is no token at all, so sync starts quickly).
                if let Some(user) = self.user.as_mut() {
                    let wait = if user.session.id_token.is_empty() { NO_TOKEN_RETRY } else { RETRY_AFTER };
                    user.next_refresh = Instant::now() + wait;
                }
            }
        }
        self.refresh_status();
    }

    /// A fresh idToken (from the worker's own timer or from a sync thread's 401 handling):
    /// store it, hand it to the sync engine and start syncing if this was the first token.
    fn apply_refreshed(&mut self, r: Refreshed) {
        let mut to_store = None;
        if let Some(user) = self.user.as_mut() {
            if user.session.uid == r.uid {
                let lifetime = Duration::from_secs(r.expires_in.saturating_sub(300).max(30));
                user.next_refresh = Instant::now() + lifetime.min(REFRESH_EVERY);
                user.session.apply(r);
                user.engine.set_tokens(&user.session.id_token, &user.session.refresh_token);
                user.engine.try_start();
                to_store = Some(user.session.clone());
            }
        }
        if let Some(s) = to_store {
            self.remember(&s);
        }
    }

    // ------------------------------------------------------------ session

    fn restore_session(&mut self) {
        match secure::load() {
            Some(s) if !s.uid.is_empty() && !s.refresh_token.is_empty() => {
                // Open instantly with local data; the token is refreshed in the background.
                let session = Session {
                    uid: s.uid,
                    email: s.email,
                    id_token: String::new(),
                    refresh_token: s.refresh_token,
                };
                self.begin_session(session, Instant::now());
            }
            _ => self.out.send(Evt::NoSession),
        }
    }

    fn begin_session(&mut self, session: Session, next_refresh: Instant) {
        // Never let a previous user's sync threads outlive their session.
        if let Some(old) = self.user.as_mut() {
            old.engine.stop();
        }
        let notes_path = config::notes_file(&session.uid);
        let (store, warning) = NoteStore::open(notes_path.clone());
        let notes = store.all();
        let email = session.email.clone();

        let gen = self.next_gen;
        self.next_gen += 1;
        let tx = self.self_tx.clone();
        let notify: Notify = Arc::new(move |msg| {
            let _ = tx.send(Cmd::Sync { gen, msg });
        });
        let queue_path = notes_path.with_file_name("queue.json");
        let (mut engine, queue_warning) = Engine::new(
            gen,
            self.cfg.clone(),
            &session.uid,
            &session.id_token,
            &session.refresh_token,
            queue_path,
            notify,
        );
        // With a fresh idToken (interactive sign-in) syncing starts now; after a restart the
        // token is refreshed first and `apply_refreshed` starts it.
        engine.try_start();

        self.user = Some(ActiveUser { session, store, engine, next_refresh });
        self.out.send(Evt::SignedIn { email, notes });
        for w in [warning, queue_warning].into_iter().flatten() {
            self.out.send(Evt::Error(w));
        }
        self.last_status = None;
        self.refresh_status();
    }

    fn sign_out(&mut self, message: Option<String>) {
        // Stops all sync threads and drops the in-memory sync state of this session. The
        // user's queue file stays on disk (it holds unsent edits); it is per user, so a
        // different user signing in can never see or send it.
        if let Some(mut user) = self.user.take() {
            user.engine.stop();
        }
        self.last_status = None;
        secure::clear();
        self.out.send(Evt::SignedOut { message });
    }

    // ------------------------------------------------------------ sync

    fn sync_msg(&mut self, gen: u64, msg: SyncMsg) {
        let Some(user) = self.user.as_mut() else { return };
        if user.engine.gen() != gen {
            return; // message from a previous session
        }
        let fx = user.engine.handle(msg, &mut user.store);
        self.apply_effects(fx);
    }

    fn apply_effects(&mut self, fx: Effects) {
        if !fx.updated.is_empty() {
            self.out.send(Evt::NotesUpdated(fx.updated));
        }
        for e in fx.errors {
            self.out.send(Evt::Error(e));
        }
        match fx.token {
            Some(Ok(r)) => self.apply_refreshed(r),
            Some(Err(e)) if e.is_session_dead() => {
                self.sign_out(Some("Your session has expired. Please sign in again.".into()));
                return;
            }
            _ => {}
        }
        self.refresh_status();
    }

    /// Emit the sync status to the UI when it changed.
    fn refresh_status(&mut self) {
        let status = self.user.as_ref().map(|u| u.engine.status()).unwrap_or(SyncStatus::Offline);
        if self.last_status != Some(status) {
            self.last_status = Some(status);
            self.set_status(status);
        }
    }

    fn remember(&self, s: &Session) {
        let ok = secure::save(&StoredSession {
            uid: s.uid.clone(),
            email: s.email.clone(),
            refresh_token: s.refresh_token.clone(),
        });
        if !ok {
            self.out.send(Evt::Error("Could not store the login on this PC; you will have to sign in again next time.".into()));
        }
    }

    fn tick(&mut self) {
        if let Some(user) = self.user.as_mut() {
            user.engine.tick();
        }
        self.refresh_status();
        if self.refreshing {
            return;
        }
        let Some(user) = &self.user else { return };
        if Instant::now() < user.next_refresh {
            return;
        }
        let refresh_token = user.session.refresh_token.clone();
        self.refreshing = true;
        let cfg = self.cfg.clone();
        let tx = self.self_tx.clone();
        thread::spawn(move || {
            let _ = tx.send(Cmd::RefreshDone(auth::refresh(&cfg, &refresh_token)));
        });
    }

    // ------------------------------------------------------------ notes / settings

    fn save_note(&mut self, note: Note) {
        let Some(user) = self.user.as_mut() else { return };
        if let Some(existing) = user.store.get(&note.id) {
            if existing.updated_at > note.updated_at {
                // A newer version arrived from the server while this edit was on its way
                // here: newest wins, so the edit is dropped and the UI gets the newer note.
                let newer = existing.clone();
                self.out.send(Evt::NotesUpdated(vec![newer]));
                return;
            }
        }
        let result = user.store.upsert_local(note.clone());
        if let Err(e) = result {
            self.out.send(Evt::Error(format!("Could not save note to disk: {e}")));
        }
        self.on_local_change(&note);
    }

    /// Queue `note` (persisted) for upload to users/{uid}/notes/{id}; uploads right away
    /// when online, otherwise as soon as the connection returns.
    fn on_local_change(&mut self, note: &Note) {
        let Some(user) = self.user.as_mut() else { return };
        if let Err(e) = user.engine.local_change(note.clone()) {
            self.out.send(Evt::Error(format!("Could not save the upload queue: {e}")));
        }
        self.refresh_status();
    }

    fn set_status(&self, status: SyncStatus) {
        self.out.send(Evt::Status(status));
    }

    fn save_settings(&self) {
        if let Err(e) = self.settings.save() {
            self.out.send(Evt::Error(format!("Could not save settings: {e}")));
        }
    }
}
