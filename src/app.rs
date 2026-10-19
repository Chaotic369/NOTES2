//! egui user interface. Never blocks: all disk/network work is done by the worker thread.

use crate::config::{AppConfig, Loaded};
use crate::model::{now_ms, Note};
use crate::worker::{self, Cmd, Evt, SyncStatus};
use eframe::egui::{
    self, Align, Color32, FontId, Key, Layout, RichText, Sense, TextStyle, Vec2,
};
use std::sync::mpsc::{Receiver, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const AUTOSAVE_DELAY: Duration = Duration::from_millis(800);
const ROW_HEIGHT: f32 = 54.0;

#[derive(PartialEq, Clone, Copy)]
enum Screen {
    Booting,
    Login,
    Notes,
}

enum LoginAction {
    SignIn,
    SignUp,
    Forgot,
    Google,
    CancelGoogle,
}

pub struct NotesApp {
    cfg: AppConfig,
    cmd: Sender<Cmd>,
    evt: Receiver<Evt>,
    worker: Option<JoinHandle<()>>,
    device_id: String,
    screen: Screen,
    dark: bool,
    notice: Option<String>,

    // login screen
    email: String,
    password: String,
    busy: bool,
    google_waiting: bool,
    google_cancelling: bool,
    auth_error: Option<String>,
    auth_info: Option<String>,

    // notes screen
    user_email: String,
    notes: Vec<Note>,
    visible: Vec<usize>,
    view_dirty: bool,
    search: String,
    last_search: String,
    selected: Option<String>,
    edit_title: String,
    edit_body: String,
    dirty: bool,
    last_edit: Instant,
    draft_id: Option<String>,
    focus_title: bool,
    status: SyncStatus,
    toast: Option<String>,
}

fn apply_theme(ctx: &egui::Context, dark: bool) {
    ctx.set_visuals(if dark { egui::Visuals::dark() } else { egui::Visuals::light() });
}

impl NotesApp {
    pub fn new(cc: &eframe::CreationContext<'_>, loaded: Loaded) -> Self {
        let Loaded { cfg, notice, settings, settings_fresh } = loaded;
        apply_theme(&cc.egui_ctx, settings.dark_mode);
        let (cmd, evt, handle) =
            worker::spawn(cfg.clone(), settings.clone(), settings_fresh, cc.egui_ctx.clone());
        NotesApp {
            cfg,
            cmd,
            evt,
            worker: Some(handle),
            device_id: settings.device_id,
            screen: Screen::Booting,
            dark: settings.dark_mode,
            notice,
            email: settings.last_email,
            password: String::new(),
            busy: false,
            google_waiting: false,
            google_cancelling: false,
            auth_error: None,
            auth_info: None,
            user_email: String::new(),
            notes: Vec::new(),
            visible: Vec::new(),
            view_dirty: true,
            search: String::new(),
            last_search: String::new(),
            selected: None,
            edit_title: String::new(),
            edit_body: String::new(),
            dirty: false,
            last_edit: Instant::now(),
            draft_id: None,
            focus_title: false,
            status: SyncStatus::Offline,
            toast: None,
        }
    }

    // ------------------------------------------------------------ events

    fn poll_events(&mut self) {
        while let Ok(e) = self.evt.try_recv() {
            match e {
                Evt::NoSession => self.screen = Screen::Login,
                Evt::SignedIn { email, notes } => {
                    self.reset_login_state();
                    self.password.clear();
                    self.user_email = email;
                    self.notes = notes;
                    self.search.clear();
                    self.draft_id = None;
                    self.dirty = false;
                    self.view_dirty = true;
                    self.rebuild_view();
                    let first = self.visible.first().map(|&i| self.notes[i].id.clone());
                    self.selected = first;
                    self.load_buffers();
                    self.screen = Screen::Notes;
                }
                Evt::SignedOut { message } => {
                    self.clear_notes_state();
                    self.reset_login_state();
                    self.auth_error = message;
                    self.screen = Screen::Login;
                }
                Evt::AuthError(m) => {
                    self.reset_login_state();
                    self.auth_error = Some(m);
                }
                Evt::AuthCancelled => self.reset_login_state(),
                Evt::Info(m) => {
                    self.reset_login_state();
                    self.auth_info = Some(m);
                }
                Evt::NotesUpdated(changed) => self.apply_remote(changed),
                Evt::Status(s) => self.status = s,
                Evt::Error(m) => self.toast = Some(m),
            }
        }
    }

    /// Merge notes that changed remotely (newest updatedAt wins), keep the selection and
    /// never overwrite what the user is typing right now.
    fn apply_remote(&mut self, changed: Vec<Note>) {
        if self.screen != Screen::Notes {
            return;
        }
        for remote in changed {
            match self.notes.iter_mut().find(|n| n.id == remote.id) {
                Some(local) => {
                    if remote.updated_at <= local.updated_at {
                        continue; // ours is at least as new (e.g. our own edit coming back)
                    }
                    *local = remote.clone();
                }
                None => self.notes.push(remote.clone()),
            }
            if self.selected.as_deref() != Some(remote.id.as_str()) {
                continue;
            }
            if self.dirty {
                // Unsaved typing wins: the next autosave stamps a newer updatedAt (and
                // brings back a note that was deleted remotely in the meantime).
                continue;
            }
            if remote.deleted {
                self.selected = None;
                self.edit_title.clear();
                self.edit_body.clear();
            } else if remote.title != self.edit_title || remote.body != self.edit_body {
                self.edit_title = remote.title;
                self.edit_body = remote.body;
            }
        }
        self.view_dirty = true;
    }

    fn reset_login_state(&mut self) {
        self.busy = false;
        self.google_waiting = false;
        self.google_cancelling = false;
        self.auth_error = None;
        self.auth_info = None;
    }

    fn clear_notes_state(&mut self) {
        self.notes.clear();
        self.visible.clear();
        self.selected = None;
        self.edit_title.clear();
        self.edit_body.clear();
        self.dirty = false;
        self.draft_id = None;
        self.search.clear();
        self.last_search.clear();
    }

    // ------------------------------------------------------------ note logic

    fn rebuild_view(&mut self) {
        let q = self.search.trim().to_lowercase();
        let mut idx: Vec<usize> = self
            .notes
            .iter()
            .enumerate()
            .filter(|(_, n)| {
                !n.deleted
                    && (q.is_empty()
                        || n.title.to_lowercase().contains(&q)
                        || n.body.to_lowercase().contains(&q))
            })
            .map(|(i, _)| i)
            .collect();
        idx.sort_by(|&a, &b| self.notes[b].updated_at.cmp(&self.notes[a].updated_at));
        self.visible = idx;
        self.view_dirty = false;
        self.last_search = self.search.clone();
    }

    fn load_buffers(&mut self) {
        let found = self
            .selected
            .as_ref()
            .and_then(|id| self.notes.iter().find(|n| &n.id == id))
            .map(|n| (n.title.clone(), n.body.clone()));
        let (t, b) = found.unwrap_or_default();
        self.edit_title = t;
        self.edit_body = b;
        self.dirty = false;
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
        self.last_edit = Instant::now();
    }

    /// Write the edit buffers into the selected note and hand it to the worker.
    fn commit_edit(&mut self) {
        self.dirty = false;
        let Some(id) = self.selected.clone() else { return };
        let Some(note) = self.notes.iter_mut().find(|n| n.id == id) else { return };
        if note.title == self.edit_title && note.body == self.edit_body && !note.deleted {
            return;
        }
        note.title = self.edit_title.clone();
        note.body = self.edit_body.clone();
        note.deleted = false; // an open, edited note is alive (even if it was deleted remotely meanwhile)
        note.updated_at = now_ms().max(note.updated_at + 1);
        note.device_id = self.device_id.clone();
        let saved = note.clone();
        if self.draft_id.as_deref() == Some(id.as_str()) {
            self.draft_id = None;
        }
        self.view_dirty = true;
        let _ = self.cmd.send(Cmd::SaveNote(saved));
    }

    /// A brand-new note that was never edited is not stored; drop it when leaving it.
    fn drop_untouched_draft(&mut self) {
        if let Some(d) = self.draft_id.take() {
            self.notes.retain(|n| n.id != d);
            self.view_dirty = true;
        }
    }

    fn select(&mut self, id: Option<String>) {
        if self.selected == id {
            return;
        }
        if self.dirty {
            self.commit_edit();
        }
        self.drop_untouched_draft();
        self.selected = id;
        self.load_buffers();
        self.view_dirty = true;
    }

    fn new_note(&mut self) {
        if self.dirty {
            self.commit_edit();
        }
        self.drop_untouched_draft();
        let note = Note::new(&self.device_id);
        self.draft_id = Some(note.id.clone());
        self.selected = Some(note.id.clone());
        self.notes.push(note);
        self.edit_title.clear();
        self.edit_body.clear();
        self.dirty = false;
        self.search.clear();
        self.focus_title = true;
        self.view_dirty = true;
    }

    fn delete_selected(&mut self) {
        let Some(id) = self.selected.clone() else { return };
        self.dirty = false;
        if self.draft_id.as_deref() == Some(id.as_str()) {
            self.draft_id = None;
            self.notes.retain(|n| n.id != id);
        } else if let Some(note) = self.notes.iter_mut().find(|n| n.id == id) {
            note.title = self.edit_title.clone();
            note.body = self.edit_body.clone();
            note.deleted = true;
            note.updated_at = now_ms().max(note.updated_at + 1);
            note.device_id = self.device_id.clone();
            let _ = self.cmd.send(Cmd::SaveNote(note.clone()));
        }
        self.selected = None;
        self.edit_title.clear();
        self.edit_body.clear();
        self.view_dirty = true;
    }

    fn autosave_tick(&mut self, ctx: &egui::Context) {
        if !self.dirty {
            return;
        }
        let elapsed = self.last_edit.elapsed();
        if elapsed >= AUTOSAVE_DELAY {
            self.commit_edit();
        } else {
            ctx.request_repaint_after(AUTOSAVE_DELAY - elapsed + Duration::from_millis(20));
        }
    }

    fn logout(&mut self) {
        if self.dirty {
            self.commit_edit();
        }
        self.drop_untouched_draft();
        let _ = self.cmd.send(Cmd::Logout);
        self.clear_notes_state();
        self.reset_login_state();
        self.screen = Screen::Login;
    }

    // ------------------------------------------------------------ screens

    fn booting_ui(&self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.centered_and_justified(|ui| {
                ui.spinner();
            });
        });
    }

    fn login_ui(&mut self, ctx: &egui::Context) {
        let mut action: Option<LoginAction> = None;
        egui::CentralPanel::default().show(ctx, |ui| {
            let w = (ui.available_width() - 32.0).clamp(220.0, 340.0);
            let side = ((ui.available_width() - w) / 2.0).max(0.0);
            ui.add_space((ui.available_height() * 0.08).max(8.0));
            ui.horizontal_top(|ui| {
                ui.add_space(side);
                ui.vertical(|ui| {
                    ui.set_width(w);
                    ui.heading("NT Notes");
                    ui.label(RichText::new("Sign in to open your notes").weak());
                    ui.add_space(10.0);

                    if let Some(n) = &self.notice {
                        egui::Frame::group(ui.style()).show(ui, |ui| {
                            ui.set_width(w - 16.0);
                            ui.colored_label(Color32::from_rgb(230, 160, 0), n);
                        });
                        ui.add_space(8.0);
                    }

                    let busy = self.busy;
                    ui.add_enabled_ui(!busy, |ui| {
                        ui.label("Email");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.email)
                                .hint_text("you@example.com")
                                .desired_width(w),
                        );
                        ui.add_space(4.0);
                        ui.label("Password");
                        let pw = ui.add(
                            egui::TextEdit::singleline(&mut self.password)
                                .password(true)
                                .desired_width(w),
                        );
                        if pw.lost_focus() && ui.input(|i| i.key_pressed(Key::Enter)) {
                            action = Some(LoginAction::SignIn);
                        }
                        ui.add_space(8.0);
                        ui.horizontal(|ui| {
                            let bw = (w - ui.spacing().item_spacing.x) / 2.0;
                            if ui
                                .add_sized([bw, 32.0], egui::Button::new(RichText::new("Sign in").strong()))
                                .clicked()
                            {
                                action = Some(LoginAction::SignIn);
                            }
                            if ui.add_sized([bw, 32.0], egui::Button::new("Create account")).clicked() {
                                action = Some(LoginAction::SignUp);
                            }
                        });
                        ui.add_space(4.0);
                        if ui.link("Forgot password?").clicked() {
                            action = Some(LoginAction::Forgot);
                        }
                    });

                    ui.add_space(10.0);
                    ui.separator();
                    ui.add_space(10.0);

                    if self.google_waiting {
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label(if self.google_cancelling {
                                "Cancelling..."
                            } else {
                                "Finish signing in in your browser..."
                            });
                        });
                        ui.add_enabled_ui(!self.google_cancelling, |ui| {
                            if ui.button("Cancel").clicked() {
                                action = Some(LoginAction::CancelGoogle);
                            }
                        });
                    } else {
                        ui.add_enabled_ui(!busy, |ui| {
                            if ui.add_sized([w, 34.0], egui::Button::new("Continue with Google")).clicked() {
                                action = Some(LoginAction::Google);
                            }
                        });
                    }

                    if self.busy && !self.google_waiting {
                        ui.add_space(8.0);
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label("Please wait...");
                        });
                    }
                    if let Some(e) = &self.auth_error {
                        ui.add_space(8.0);
                        ui.colored_label(Color32::from_rgb(230, 80, 80), e);
                    }
                    if let Some(i) = &self.auth_info {
                        ui.add_space(8.0);
                        ui.colored_label(Color32::from_rgb(76, 175, 80), i);
                    }
                });
            });
        });

        if let Some(a) = action {
            self.do_login_action(a);
        }
    }

    fn do_login_action(&mut self, action: LoginAction) {
        let email = self.email.trim().to_string();
        match action {
            LoginAction::SignIn | LoginAction::SignUp => {
                if email.is_empty() || self.password.is_empty() {
                    self.auth_info = None;
                    self.auth_error = Some("Enter your email and password.".into());
                    return;
                }
                self.auth_error = None;
                self.auth_info = None;
                self.busy = true;
                let password = self.password.clone();
                let _ = self.cmd.send(if matches!(action, LoginAction::SignIn) {
                    Cmd::SignIn { email, password }
                } else {
                    Cmd::SignUp { email, password }
                });
            }
            LoginAction::Forgot => {
                if email.is_empty() {
                    self.auth_info = None;
                    self.auth_error = Some("Enter your email above, then click Forgot password.".into());
                    return;
                }
                self.auth_error = None;
                self.auth_info = None;
                self.busy = true;
                let _ = self.cmd.send(Cmd::ResetPassword { email });
            }
            LoginAction::Google => {
                if !self.cfg.google_configured() {
                    self.auth_info = None;
                    self.auth_error = Some(
                        "Google sign-in is not configured. Fill googleClientId and googleClientSecret in config.json and restart."
                            .into(),
                    );
                    return;
                }
                self.auth_error = None;
                self.auth_info = None;
                self.busy = true;
                self.google_waiting = true;
                let _ = self.cmd.send(Cmd::GoogleSignIn);
            }
            LoginAction::CancelGoogle => {
                self.google_cancelling = true;
                let _ = self.cmd.send(Cmd::CancelGoogle);
            }
        }
    }

    fn notes_ui(&mut self, ctx: &egui::Context) {
        self.autosave_tick(ctx);
        if ctx.input_mut(|i| i.consume_key(egui::Modifiers::COMMAND, Key::N)) {
            self.new_note();
        }
        if self.view_dirty || self.search != self.last_search {
            self.rebuild_view();
        }

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.heading("NT Notes");
                ui.add_space(12.0);
                self.status_indicator(ui);
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui.button("Logout").clicked() {
                        self.logout();
                    }
                    ui.label(RichText::new(&self.user_email).weak());
                    let label = if self.dark { "Light theme" } else { "Dark theme" };
                    if ui.button(label).clicked() {
                        self.dark = !self.dark;
                        apply_theme(ctx, self.dark);
                        let _ = self.cmd.send(Cmd::SetTheme(self.dark));
                    }
                });
            });
            ui.add_space(4.0);
        });

        egui::TopBottomPanel::bottom("bottom").show(ctx, |ui| {
            ui.horizontal(|ui| {
                let count = self.notes.iter().filter(|n| !n.deleted).count();
                ui.label(RichText::new(format!("{count} notes")).small().weak());
                if let Some(msg) = self.toast.clone() {
                    ui.colored_label(Color32::from_rgb(230, 80, 80), msg);
                    if ui.small_button("Dismiss").clicked() {
                        self.toast = None;
                    }
                }
            });
        });

        egui::SidePanel::left("list")
            .resizable(true)
            .default_width(280.0)
            .width_range(200.0..=520.0)
            .show(ctx, |ui| self.list_ui(ui));

        egui::CentralPanel::default().show(ctx, |ui| self.editor_ui(ui));
    }

    fn status_indicator(&self, ui: &mut egui::Ui) {
        let (color, text) = match self.status {
            SyncStatus::Synced => (Color32::from_rgb(76, 175, 80), "Synced"),
            SyncStatus::Syncing => (Color32::from_rgb(255, 179, 0), "Syncing"),
            SyncStatus::Offline => (Color32::GRAY, "Offline"),
        };
        let (rect, _) = ui.allocate_exact_size(Vec2::splat(10.0), Sense::hover());
        ui.painter().circle_filled(rect.center(), 5.0, color);
        ui.label(text);
    }

    fn list_ui(&mut self, ui: &mut egui::Ui) {
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            if ui.button("+ New").clicked() {
                self.new_note();
            }
            ui.add(
                egui::TextEdit::singleline(&mut self.search)
                    .hint_text("Search notes")
                    .desired_width(f32::INFINITY),
            );
        });
        ui.add_space(4.0);
        ui.separator();

        let total = self.visible.len();
        if total == 0 {
            ui.add_space(24.0);
            ui.vertical_centered(|ui| {
                let msg = if self.search.trim().is_empty() { "No notes yet" } else { "No matching notes" };
                ui.label(RichText::new(msg).weak());
            });
            return;
        }

        let mut clicked: Option<String> = None;
        egui::ScrollArea::vertical().auto_shrink([false, false]).show_rows(
            ui,
            ROW_HEIGHT,
            total,
            |ui, range| {
                for i in range {
                    let note = &self.notes[self.visible[i]];
                    let selected = self.selected.as_deref() == Some(note.id.as_str());
                    let (title, body) = if selected {
                        (self.edit_title.as_str(), self.edit_body.as_str())
                    } else {
                        (note.title.as_str(), note.body.as_str())
                    };
                    if note_row(ui, title, body, note.updated_at, selected).clicked() {
                        clicked = Some(note.id.clone());
                    }
                }
            },
        );
        if let Some(id) = clicked {
            self.select(Some(id));
        }
    }

    fn editor_ui(&mut self, ui: &mut egui::Ui) {
        let Some(id) = self.selected.clone() else {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("Select a note, or press + New (Ctrl+N)").weak());
            });
            return;
        };

        let mut delete = false;
        ui.horizontal(|ui| {
            let title_w = (ui.available_width() - 76.0).max(80.0);
            let resp = ui.add(
                egui::TextEdit::singleline(&mut self.edit_title)
                    .hint_text("Title")
                    .font(TextStyle::Heading)
                    .desired_width(title_w),
            );
            if self.focus_title {
                resp.request_focus();
                self.focus_title = false;
            }
            if resp.changed() {
                self.mark_dirty();
            }
            if ui.button("Delete").clicked() {
                delete = true;
            }
        });
        if delete {
            self.delete_selected();
            return;
        }

        if let Some(n) = self.notes.iter().find(|n| n.id == id) {
            ui.label(RichText::new(format!("Last edited {}", format_full(n.updated_at))).small().weak());
        }
        ui.separator();

        let font = FontId::proportional(16.0);
        let row_h = ui.fonts(|f| f.row_height(&font));
        let rows = ((ui.available_height() / row_h).floor() as usize).saturating_sub(1).max(4);
        egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
            let resp = ui.add(
                egui::TextEdit::multiline(&mut self.edit_body)
                    .font(font)
                    .frame(false)
                    .desired_width(f32::INFINITY)
                    .desired_rows(rows),
            );
            if resp.changed() {
                self.mark_dirty();
            }
        });
    }
}

impl eframe::App for NotesApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_events();
        match self.screen {
            Screen::Booting => self.booting_ui(ctx),
            Screen::Login => self.login_ui(ctx),
            Screen::Notes => self.notes_ui(ctx),
        }
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        if self.dirty {
            self.commit_edit();
        }
        // Commands are processed in order, so pending saves finish before Quit.
        let _ = self.cmd.send(Cmd::Quit);
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------- helpers

fn note_row(ui: &mut egui::Ui, title: &str, body: &str, updated_at: i64, selected: bool) -> egui::Response {
    let (rect, resp) =
        ui.allocate_exact_size(Vec2::new(ui.available_width(), ROW_HEIGHT), Sense::click());
    if ui.is_rect_visible(rect) {
        let painter = ui.painter_at(rect);
        if selected {
            painter.rect_filled(rect, 4.0, ui.visuals().selection.bg_fill);
        } else if resp.hovered() {
            painter.rect_filled(rect, 4.0, ui.visuals().widgets.hovered.weak_bg_fill);
        }
        let strong = if selected { ui.visuals().selection.stroke.color } else { ui.visuals().text_color() };
        let weak = ui.visuals().weak_text_color();
        let pad = 10.0;
        let left = rect.left() + pad;
        let width = rect.width() - 2.0 * pad;

        let title_line = first_line(title);
        let title_text = if title_line.is_empty() { "Untitled" } else { title_line };
        let date = format_short(updated_at);

        let date_galley = painter.layout_no_wrap(date, FontId::proportional(11.5), weak);
        let date_w = date_galley.size().x;
        painter.galley(egui::pos2(rect.right() - pad - date_w, rect.top() + 9.0), date_galley, weak);

        clipped_text(ui, &painter, egui::pos2(left, rect.top() + 7.0), title_text, FontId::proportional(15.0), strong, (width - date_w - 8.0).max(20.0));
        clipped_text(ui, &painter, egui::pos2(left, rect.top() + 29.0), first_line(body), FontId::proportional(12.5), weak, width);
    }
    resp
}

fn clipped_text(
    ui: &egui::Ui,
    painter: &egui::Painter,
    pos: egui::Pos2,
    text: &str,
    font: FontId,
    color: Color32,
    max_width: f32,
) {
    if text.is_empty() {
        return;
    }
    let mut job = egui::text::LayoutJob::simple_singleline(text.to_owned(), font, color);
    job.wrap = egui::text::TextWrapping {
        max_width,
        max_rows: 1,
        break_anywhere: true,
        overflow_character: Some('…'),
    };
    let galley = ui.fonts(|f| f.layout_job(job));
    painter.galley(pos, galley, color);
}

fn first_line(s: &str) -> &str {
    s.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or("")
}

fn to_local(ms: i64) -> Option<chrono::DateTime<chrono::Local>> {
    chrono::DateTime::from_timestamp_millis(ms).map(|d| d.with_timezone(&chrono::Local))
}

fn format_short(ms: i64) -> String {
    match to_local(ms) {
        Some(d) if d.date_naive() == chrono::Local::now().date_naive() => d.format("%H:%M").to_string(),
        Some(d) => d.format("%d %b").to_string(),
        None => String::new(),
    }
}

fn format_full(ms: i64) -> String {
    to_local(ms).map(|d| d.format("%d %b %Y, %H:%M").to_string()).unwrap_or_default()
}

// ---------------------------------------------------------------- fatal error window

pub struct FatalApp(pub String);

impl eframe::App for FatalApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.add_space(12.0);
            ui.heading("NT Notes cannot start");
            ui.add_space(8.0);
            ui.colored_label(Color32::from_rgb(230, 80, 80), &self.0);
            ui.add_space(12.0);
            if ui.button("Close").clicked() {
                ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            }
        });
    }
}
