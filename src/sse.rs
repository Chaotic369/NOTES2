//! Firebase REST streaming (Server-Sent Events) parsing, plus the pure logic that turns
//! `put` / `patch` events on users/{uid}/notes into note changes.
//!
//! No I/O in this module, so it is fully unit-testable.

use crate::model::Note;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum SseEvent {
    Put { path: String, data: Value },
    Patch { path: String, data: Value },
    KeepAlive,
    Cancel,
    AuthRevoked,
    /// Any other (or malformed) event; ignored by the engine.
    Unknown,
}

/// Incremental parser: feed it lines (without the trailing newline) as they arrive.
/// A blank line terminates an event. Partial chunks are handled by the caller's line
/// reader; this struct keeps the multi-line state (`event:` + `data:` pairs).
#[derive(Default)]
pub struct SseParser {
    event: String,
    data: Vec<String>,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed_line(&mut self, line: &str) -> Option<SseEvent> {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            return self.dispatch();
        }
        if line.starts_with(':') {
            return None; // comment
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "event" => self.event = value.to_string(),
            "data" => self.data.push(value.to_string()),
            _ => {}
        }
        None
    }

    fn dispatch(&mut self) -> Option<SseEvent> {
        let event = std::mem::take(&mut self.event);
        let data = std::mem::take(&mut self.data).join("\n");
        if event.is_empty() && data.is_empty() {
            return None;
        }
        Some(parse_event(&event, &data))
    }
}

fn parse_event(event: &str, data: &str) -> SseEvent {
    match event {
        "keep-alive" => SseEvent::KeepAlive,
        "cancel" => SseEvent::Cancel,
        "auth_revoked" => SseEvent::AuthRevoked,
        "put" | "patch" => {
            let Ok(v) = serde_json::from_str::<Value>(data) else { return SseEvent::Unknown };
            let path = v.get("path").and_then(Value::as_str).unwrap_or("").to_string();
            let data = v.get("data").cloned().unwrap_or(Value::Null);
            if event == "put" {
                SseEvent::Put { path, data }
            } else {
                SseEvent::Patch { path, data }
            }
        }
        _ => SseEvent::Unknown,
    }
}

// ---------------------------------------------------------------- applying events

/// Mirror of the remote `users/{uid}/notes` node as raw JSON per note id. Keeping the raw
/// JSON (not parsed `Note`s) lets deep `put`/`patch` events on single fields be applied
/// exactly like Firebase does.
#[derive(Default, Debug)]
pub struct RemoteState {
    nodes: HashMap<String, Value>,
}

impl RemoteState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
    }

    /// Replace everything with a full snapshot (initial `put` at "/" or a polled GET).
    /// Returns every note that parses.
    pub fn reset_from_snapshot(&mut self, root: &Value) -> Vec<Note> {
        self.nodes.clear();
        if let Value::Object(map) = root {
            for (k, v) in map {
                if v.is_object() {
                    self.nodes.insert(k.clone(), v.clone());
                }
            }
        }
        let ids: Vec<String> = self.nodes.keys().cloned().collect();
        ids.iter().filter_map(|id| self.note(id)).collect()
    }

    /// Apply a `put` event. Returns the notes whose remote value changed (and parse).
    pub fn apply_put(&mut self, path: &str, data: &Value) -> Vec<Note> {
        let segs = split_path(path);
        match segs.as_slice() {
            [] => self.reset_from_snapshot(data),
            [id] => {
                if data.is_object() {
                    self.nodes.insert((*id).to_string(), data.clone());
                    self.note(id).into_iter().collect()
                } else {
                    // null (or garbage): the node was removed on the server.
                    self.nodes.remove(*id);
                    Vec::new()
                }
            }
            [id, rest @ ..] => {
                let node = self.nodes.entry((*id).to_string()).or_insert_with(|| Value::Object(Default::default()));
                set_deep(node, rest, data.clone());
                if node.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                    self.nodes.remove(*id);
                }
                self.note(id).into_iter().collect()
            }
        }
    }

    /// Apply a `patch` event: each key of `data` is a (possibly nested) child path that is
    /// put under `path`.
    pub fn apply_patch(&mut self, path: &str, data: &Value) -> Vec<Note> {
        let mut changed: HashMap<String, Note> = HashMap::new();
        let Value::Object(map) = data else { return Vec::new() };
        for (k, v) in map {
            let full = format!("{}/{}", path.trim_end_matches('/'), k);
            for n in self.apply_put(&full, v) {
                changed.insert(n.id.clone(), n);
            }
        }
        // A later key may have invalidated an earlier note; re-read final values.
        changed.into_keys().filter_map(|id| self.note(&id)).collect()
    }

    /// Apply any event. `None` for events that carry no data changes.
    pub fn apply(&mut self, ev: &SseEvent) -> Option<Vec<Note>> {
        match ev {
            SseEvent::Put { path, data } => Some(self.apply_put(path, data)),
            SseEvent::Patch { path, data } => Some(self.apply_patch(path, data)),
            _ => None,
        }
    }

    /// The current remote version of a note, if present and valid.
    pub fn get(&self, id: &str) -> Option<Note> {
        parse_note(id, self.nodes.get(id)?)
    }

    fn note(&self, id: &str) -> Option<Note> {
        self.get(id)
    }
}

/// Parse a remote note. The database key is authoritative for the id (and supplies it
/// when another client left the field out).
pub fn parse_note(key: &str, value: &Value) -> Option<Note> {
    let mut v = value.clone();
    v.as_object_mut()?.entry("id").or_insert_with(|| Value::String(key.to_string()));
    let mut note: Note = serde_json::from_value(v).ok()?;
    note.id = key.to_string();
    Some(note)
}

fn split_path(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

fn set_deep(node: &mut Value, segs: &[&str], data: Value) {
    let Some((first, rest)) = segs.split_first() else {
        *node = data;
        return;
    };
    if !node.is_object() {
        *node = Value::Object(Default::default());
    }
    let map = node.as_object_mut().expect("object");
    if rest.is_empty() {
        if data.is_null() {
            map.remove(*first);
        } else {
            map.insert((*first).to_string(), data);
        }
    } else {
        let child = map.entry((*first).to_string()).or_insert_with(|| Value::Object(Default::default()));
        set_deep(child, rest, data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn note_json(id: &str, updated: i64) -> Value {
        json!({"id": id, "title": "t", "body": "b", "createdAt": 1, "updatedAt": updated, "deleted": false, "deviceId": "d"})
    }

    fn parse_all(chunks: &[&str]) -> Vec<SseEvent> {
        // Simulate a byte stream split at arbitrary points, then line splitting.
        let mut buf = String::new();
        let mut p = SseParser::new();
        let mut out = Vec::new();
        for c in chunks {
            buf.push_str(c);
            while let Some(i) = buf.find('\n') {
                let line: String = buf.drain(..=i).collect();
                if let Some(e) = p.feed_line(&line) {
                    out.push(e);
                }
            }
        }
        out
    }

    #[test]
    fn parses_put_with_data() {
        let evs = parse_all(&["event: put\ndata: {\"path\":\"/\",\"data\":{\"a\":1}}\n\n"]);
        assert_eq!(evs, vec![SseEvent::Put { path: "/".into(), data: json!({"a": 1}) }]);
    }

    #[test]
    fn parses_null_data_and_keepalive_and_cancel() {
        let evs = parse_all(&[
            "event: put\ndata: {\"path\":\"/x\",\"data\":null}\n\n",
            "event: keep-alive\ndata: null\n\n",
            "event: cancel\ndata: \"permission_denied\"\n\n",
            "event: auth_revoked\ndata: \"credential is no longer valid\"\n\n",
        ]);
        assert_eq!(
            evs,
            vec![
                SseEvent::Put { path: "/x".into(), data: Value::Null },
                SseEvent::KeepAlive,
                SseEvent::Cancel,
                SseEvent::AuthRevoked
            ]
        );
    }

    #[test]
    fn handles_partial_chunks_and_crlf() {
        let evs = parse_all(&["event: pa", "tch\r\ndata: {\"path\":\"/a\",", "\"data\":{\"title\":\"x\"}}\r\n", "\r\n"]);
        assert_eq!(evs, vec![SseEvent::Patch { path: "/a".into(), data: json!({"title": "x"}) }]);
    }

    #[test]
    fn ignores_comments_and_malformed() {
        let evs = parse_all(&[": hello\n\nevent: put\ndata: not json\n\nevent: weird\ndata: 1\n\n"]);
        assert_eq!(evs, vec![SseEvent::Unknown, SseEvent::Unknown]);
    }

    #[test]
    fn put_root_then_note_then_delete() {
        let mut st = RemoteState::new();
        let all = st.apply_put("/", &json!({"a": note_json("a", 1), "b": note_json("b", 2)}));
        assert_eq!(all.len(), 2);
        let ch = st.apply_put("/a", &note_json("a", 5));
        assert_eq!(ch.len(), 1);
        assert_eq!(ch[0].updated_at, 5);
        assert!(st.apply_put("/a", &Value::Null).is_empty());
        assert!(st.note("a").is_none());
        assert!(st.note("b").is_some());
    }

    #[test]
    fn put_root_null_clears() {
        let mut st = RemoteState::new();
        st.apply_put("/", &json!({"a": note_json("a", 1)}));
        assert!(st.apply_put("/", &Value::Null).is_empty());
        assert!(st.note("a").is_none());
    }

    #[test]
    fn deep_put_updates_one_field() {
        let mut st = RemoteState::new();
        st.apply_put("/", &json!({"a": note_json("a", 1)}));
        let ch = st.apply_put("/a/title", &json!("new"));
        assert_eq!(ch[0].title, "new");
        assert_eq!(ch[0].updated_at, 1);
        let ch = st.apply_put("/a/updatedAt", &json!(9));
        assert_eq!(ch[0].updated_at, 9);
    }

    #[test]
    fn patch_applies_multiple_fields() {
        let mut st = RemoteState::new();
        st.apply_put("/", &json!({"a": note_json("a", 1)}));
        let ch = st.apply_patch("/a", &json!({"title": "T", "body": "B", "updatedAt": 7}));
        assert_eq!(ch.len(), 1);
        assert_eq!((ch[0].title.as_str(), ch[0].body.as_str(), ch[0].updated_at), ("T", "B", 7));
        // patch at root creates a note
        let ch = st.apply_patch("/", &json!({"c": note_json("c", 3)}));
        assert_eq!(ch.len(), 1);
        assert_eq!(ch[0].id, "c");
    }

    #[test]
    fn id_is_taken_from_key_when_missing() {
        let mut st = RemoteState::new();
        let all = st.apply_put("/", &json!({"k": {"title": "x", "createdAt": 1, "updatedAt": 2}}));
        assert_eq!(all[0].id, "k");
        assert!(parse_note("k", &Value::Null).is_none());
    }

    #[test]
    fn invalid_notes_are_skipped() {
        let mut st = RemoteState::new();
        let all = st.apply_put("/", &json!({"a": {"title": "no timestamps"}, "b": note_json("b", 1)}));
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, "b");
    }
}
