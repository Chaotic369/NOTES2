//! config.json (next to the exe) and the per-user local settings (device id, theme, ...).

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const PLACEHOLDER_PREFIX: &str = "PASTE_";

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AppConfig {
    #[serde(default)]
    pub api_key: String,
    #[serde(default, rename = "databaseURL")]
    pub database_url: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub google_client_id: String,
    #[serde(default)]
    pub google_client_secret: String,
}

impl AppConfig {
    fn template() -> Self {
        AppConfig {
            api_key: "AIzaSyDTy-tl_W5bqjyZQGaHsQv0IWIdZo5ysIQ".into(),
            database_url: "https://ntnotes-58411-default-rtdb.firebaseio.com".into(),
            project_id: "ntnotes-58411".into(),
            google_client_id: "PASTE_DESKTOP_CLIENT_ID".into(),
            google_client_secret: "PASTE_DESKTOP_CLIENT_SECRET".into(),
        }
    }

    pub fn google_configured(&self) -> bool {
        let ok = |s: &str| !s.trim().is_empty() && !s.starts_with(PLACEHOLDER_PREFIX);
        ok(&self.google_client_id) && ok(&self.google_client_secret)
    }
}

/// Small per-machine settings kept in the user config folder (not in config.json).
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    #[serde(default)]
    pub device_id: String,
    #[serde(default = "default_dark")]
    pub dark_mode: bool,
    #[serde(default)]
    pub last_email: String,
}

fn default_dark() -> bool {
    true
}

impl Settings {
    /// Returns the settings and whether they must be written to disk (first run).
    pub fn load() -> (Settings, bool) {
        let mut needs_save = false;
        let mut s: Settings = fs::read(settings_path())
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_else(|| {
                needs_save = true;
                Settings { device_id: String::new(), dark_mode: true, last_email: String::new() }
            });
        if s.device_id.is_empty() {
            s.device_id = uuid::Uuid::new_v4().to_string();
            needs_save = true;
        }
        (s, needs_save)
    }

    pub fn save(&self) -> std::io::Result<()> {
        write_atomic(&settings_path(), &serde_json::to_vec_pretty(self)?)
    }
}

pub struct Loaded {
    pub cfg: AppConfig,
    /// A warning to show on the login screen (config created / Google values missing).
    pub notice: Option<String>,
    pub settings: Settings,
    pub settings_fresh: bool,
}

pub fn exe_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn project_dirs() -> Option<directories::ProjectDirs> {
    directories::ProjectDirs::from("app", "NTNotes", "NTNotes")
}

pub fn config_dir() -> PathBuf {
    project_dirs()
        .map(|d| d.config_dir().to_path_buf())
        .unwrap_or_else(|| exe_dir().join("userdata").join("config"))
}

fn data_dir() -> PathBuf {
    project_dirs()
        .map(|d| d.data_local_dir().to_path_buf())
        .unwrap_or_else(|| exe_dir().join("userdata").join("data"))
}

fn settings_path() -> PathBuf {
    config_dir().join("settings.json")
}

/// One notes file per user.
pub fn notes_file(uid: &str) -> PathBuf {
    let safe: String = uid
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    data_dir().join("users").join(safe).join("notes.json")
}

/// Write via temp file + rename so a crash never leaves a half-written file.
pub fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    fs::write(&tmp, data)?;
    fs::rename(&tmp, path)
}

pub fn load_config() -> Result<Loaded, String> {
    let path = exe_dir().join("config.json");
    let (mut cfg, mut notice) = if path.exists() {
        let text = fs::read_to_string(&path)
            .map_err(|e| format!("Could not read {}:\n{e}", path.display()))?;
        let cfg: AppConfig = serde_json::from_str(&text).map_err(|e| {
            format!("config.json is not valid:\n{e}\n\nFile: {}", path.display())
        })?;
        (cfg, None)
    } else {
        let cfg = AppConfig::template();
        let json = serde_json::to_string_pretty(&cfg).map_err(|e| e.to_string())?;
        fs::write(&path, json).map_err(|e| {
            format!("config.json was missing and could not be created at\n{}\n\n{e}", path.display())
        })?;
        (
            cfg,
            Some(format!(
                "config.json was missing, so a new one was created at:\n{}\n\n\
                 Please open it, replace googleClientId and googleClientSecret with your Google \
                 OAuth Desktop client values, and restart the app. \
                 Email/password sign-in already works.",
                path.display()
            )),
        )
    };

    if cfg.api_key.trim().is_empty() || cfg.database_url.trim().is_empty() {
        return Err(format!(
            "config.json must contain apiKey and databaseURL.\n\nFile: {}",
            path.display()
        ));
    }
    cfg.database_url = cfg.database_url.trim().trim_end_matches('/').to_string();

    if notice.is_none() && !cfg.google_configured() {
        notice = Some(format!(
            "Google sign-in is not configured. Fill googleClientId and googleClientSecret in:\n{}",
            path.display()
        ));
    }

    let (settings, settings_fresh) = Settings::load();
    Ok(Loaded { cfg, notice, settings, settings_fresh })
}
