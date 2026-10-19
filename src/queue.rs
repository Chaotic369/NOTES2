//! Persisted offline upload queue (one file per user, next to notes.json).
//!
//! Holds at most one entry per note id (the latest version), oldest change first.
//! Every mutation is written atomically, so unsent edits survive restarts and crashes.
//! Owned by the worker thread; never touched from helper threads.

use crate::config::write_atomic;
use crate::model::{now_ms, Note};
use serde::{Deserialize, Serialize};
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
    notes: &'a [Note],
}

pub struct Queue {
    path: PathBuf,
    items: Vec<Note>,
}

impl Queue {
    /// Opens (or creates empty) the queue. The second value is a warning for the user.
    pub fn open(path: PathBuf) -> (Self, Option<String>) {
        let mut items: Vec<Note> = Vec::new();
        let mut warning = None;
        match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<FileIn>(&bytes) {
                Ok(f) => {
                    for n in f.notes {
                        coalesce(&mut items, n);
                    }
                }
                Err(e) => {
                    let mut backup = path.as_os_str().to_owned();
                    backup.push(format!(".corrupt-{}", now_ms()));
                    let backup = PathBuf::from(backup);
                    let _ = fs::rename(&path, &backup);
                    // Not fatal: the next sync re-pushes every local note that is newer
                    // than (or missing from) the server.
                    warning = Some(format!(
                        "The upload queue could not be read ({e}); it will be rebuilt from your notes. Copy kept at {}",
                        backup.display()
                    ));
                }
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => warning = Some(format!("Could not read the upload queue: {e}")),
        }
        (Queue { path, items }, warning)
    }

    /// Queue a new version of a note, replacing any queued older version of the same id.
    pub fn push(&mut self, note: Note) -> io::Result<()> {
        coalesce(&mut self.items, note);
        self.save()
    }

    /// Oldest pending change.
    pub fn front(&self) -> Option<&Note> {
        self.items.first()
    }

    pub fn get(&self, id: &str) -> Option<&Note> {
        self.items.iter().find(|n| n.id == id)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Remove the entry for `id` if its version is not newer than `updated_at`
    /// (it was uploaded, or the server already has something at least as new).
    /// A newer queued version of the same note is kept.
    pub fn remove_up_to(&mut self, id: &str, updated_at: i64) -> io::Result<bool> {
        let before = self.items.len();
        self.items.retain(|n| !(n.id == id && n.updated_at <= updated_at));
        let changed = self.items.len() != before;
        if changed {
            self.save()?;
        }
        Ok(changed)
    }

    /// Move the head entry to the back so one permanently rejected note cannot block
    /// the notes behind it.
    pub fn rotate(&mut self) -> io::Result<()> {
        if self.items.len() > 1 {
            let first = self.items.remove(0);
            self.items.push(first);
            self.save()?;
        }
        Ok(())
    }

    fn save(&self) -> io::Result<()> {
        if self.items.is_empty() {
            // Keep an (empty) file rather than deleting, so "queue is empty" is explicit.
            return write_atomic(&self.path, &serde_json::to_vec(&FileOut { version: 1, notes: &[] })?);
        }
        write_atomic(&self.path, &serde_json::to_vec(&FileOut { version: 1, notes: &self.items })?)
    }
}

/// Keep only the latest version per id; a newer change moves to the back of the line.
/// An older version never replaces a newer one.
fn coalesce(items: &mut Vec<Note>, note: Note) {
    if let Some(pos) = items.iter().position(|n| n.id == note.id) {
        if items[pos].updated_at > note.updated_at {
            return;
        }
        items.remove(pos);
    }
    items.push(note);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(id: &str, updated: i64) -> Note {
        Note { id: id.into(), title: format!("t{updated}"), body: "b".into(), created_at: 1, updated_at: updated, deleted: false, device_id: "d".into() }
    }

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ntnotes-queue-{}-{}-{}.json", name, std::process::id(), now_ms()))
    }

    #[test]
    fn coalesces_by_id_keeping_latest_and_order() {
        let mut v = Vec::new();
        coalesce(&mut v, note("a", 1));
        coalesce(&mut v, note("b", 2));
        coalesce(&mut v, note("a", 3));
        let ids: Vec<_> = v.iter().map(|n| (n.id.as_str(), n.updated_at)).collect();
        assert_eq!(ids, vec![("b", 2), ("a", 3)]);
        coalesce(&mut v, note("a", 2)); // older never replaces newer
        assert_eq!(v.last().unwrap().updated_at, 3);
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn persistence_round_trip() {
        let p = tmp("rt");
        let (mut q, w) = Queue::open(p.clone());
        assert!(w.is_none() && q.is_empty());
        q.push(note("a", 1)).unwrap();
        q.push(note("b", 2)).unwrap();
        q.push(note("a", 5)).unwrap();
        let (q2, w2) = Queue::open(p.clone());
        assert!(w2.is_none());
        assert_eq!(q2.len(), 2);
        assert_eq!(q2.front().unwrap().id, "b");
        assert_eq!(q2.get("a").unwrap().updated_at, 5);
        let json = std::fs::read_to_string(&p).unwrap();
        assert!(json.contains("\"updatedAt\"") && json.contains("\"deviceId\""));
        let _ = fs::remove_file(p);
    }

    #[test]
    fn remove_up_to_keeps_newer_version() {
        let p = tmp("rm");
        let (mut q, _) = Queue::open(p.clone());
        q.push(note("a", 5)).unwrap();
        // upload of version 3 finished, but 5 is queued: keep it
        assert!(!q.remove_up_to("a", 3).unwrap());
        assert_eq!(q.len(), 1);
        assert!(q.remove_up_to("a", 5).unwrap());
        assert!(q.is_empty());
        let (q2, _) = Queue::open(p.clone());
        assert!(q2.is_empty());
        let _ = fs::remove_file(p);
    }

    #[test]
    fn rotate_moves_head_to_back() {
        let p = tmp("rot");
        let (mut q, _) = Queue::open(p.clone());
        q.push(note("a", 1)).unwrap();
        q.push(note("b", 2)).unwrap();
        q.rotate().unwrap();
        assert_eq!(q.front().unwrap().id, "b");
        let _ = fs::remove_file(p);
    }

    #[test]
    fn corrupt_file_is_set_aside() {
        let p = tmp("bad");
        fs::write(&p, b"{not json").unwrap();
        let (q, w) = Queue::open(p.clone());
        assert!(q.is_empty());
        assert!(w.is_some());
        assert!(!p.exists());
        if let Some(dir) = p.parent() {
            let stem = p.file_name().unwrap().to_string_lossy().to_string();
            for e in fs::read_dir(dir).unwrap().flatten() {
                if e.file_name().to_string_lossy().starts_with(&stem) {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
    }
}
