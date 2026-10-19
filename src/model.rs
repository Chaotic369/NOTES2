//! Note data model. The JSON field names (camelCase) are the shared contract with the
//! website and the Android app: users/{uid}/notes/{noteId} in Firebase Realtime Database.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Note {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub body: String,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub device_id: String,
}

impl Note {
    pub fn new(device_id: &str) -> Self {
        let now = now_ms();
        Note {
            id: uuid::Uuid::new_v4().to_string(),
            title: String::new(),
            body: String::new(),
            created_at: now,
            updated_at: now,
            deleted: false,
            device_id: device_id.to_string(),
        }
    }
}

/// Current time in milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
