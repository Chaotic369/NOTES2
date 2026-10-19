//! Local per-user notes file. Works fully offline; deleted notes are kept as tombstones.

use crate::config::write_atomic;
use crate::model::{now_ms, Note};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;

#[derive(Deserialize)]
struct FileIn {
    notes: Vec<Note>,
}

#[derive(Serialize)]
struct FileOut<'a> {
    version: u32,
    notes: Vec<&'a Note>,
}

pub struct NoteStore {
    path: PathBuf,
    notes: HashMap<String, Note>,
}

impl NoteStore {
    /// Opens the store. The second value is a warning to show to the user, if any.
    pub fn open(path: PathBuf) -> (Self, Option<String>) {
        let mut notes = HashMap::new();
        let mut warning = None;
        match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<FileIn>(&bytes) {
                Ok(f) => {
                    for n in f.notes {
                        notes.insert(n.id.clone(), n);
                    }
                }
                Err(e) => {
                    let mut backup = path.as_os_str().to_owned();
                    backup.push(format!(".corrupt-{}", now_ms()));
                    let backup = PathBuf::from(backup);
                    let _ = fs::rename(&path, &backup);
                    warning = Some(format!(
                        "The notes file could not be read ({e}). A copy was kept at {}",
                        backup.display()
                    ));
                }
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => warning = Some(format!("Could not read the notes file: {e}")),
        }
        (NoteStore { path, notes }, warning)
    }

    /// All notes including deleted tombstones, newest first.
    pub fn all(&self) -> Vec<Note> {
        let mut v: Vec<Note> = self.notes.values().cloned().collect();
        v.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        v
    }

    /// Insert or replace a locally edited note and persist.
    pub fn upsert_local(&mut self, note: Note) -> io::Result<()> {
        self.notes.insert(note.id.clone(), note);
        self.save()
    }

    pub fn get(&self, id: &str) -> Option<&Note> {
        self.notes.get(id)
    }

    /// Merge a note received from Firebase. Newest updatedAt wins.
    /// Returns true if the local copy changed (caller should then `save()`).
    pub fn merge_remote(&mut self, remote: Note) -> bool {
        match self.notes.get(&remote.id) {
            Some(local) if local.updated_at >= remote.updated_at => false,
            _ => {
                self.notes.insert(remote.id.clone(), remote);
                true
            }
        }
    }

    pub fn save(&self) -> io::Result<()> {
        let mut list: Vec<&Note> = self.notes.values().collect();
        list.sort_by(|a, b| a.created_at.cmp(&b.created_at).then_with(|| a.id.cmp(&b.id)));
        let data = serde_json::to_vec(&FileOut { version: 1, notes: list })?;
        write_atomic(&self.path, &data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(id: &str, updated: i64) -> Note {
        Note { id: id.into(), title: "t".into(), body: "b".into(), created_at: 1, updated_at: updated, deleted: false, device_id: "d".into() }
    }

    #[test]
    fn newest_wins_and_roundtrips() {
        let path = std::env::temp_dir().join(format!("ntnotes-test-{}.json", now_ms()));
        let (mut s, w) = NoteStore::open(path.clone());
        assert!(w.is_none());
        s.upsert_local(note("a", 10)).unwrap();
        assert!(!s.merge_remote(note("a", 5)));
        assert!(s.merge_remote(note("a", 20)));
        s.save().unwrap();
        let (s2, _) = NoteStore::open(path.clone());
        assert_eq!(s2.all()[0].updated_at, 20);
        let json = std::fs::read_to_string(&path).unwrap();
        assert!(json.contains("\"updatedAt\"") && json.contains("\"deviceId\""));
        let _ = std::fs::remove_file(path);
    }
}
