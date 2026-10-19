# NT Notes (Phase 1 + Phase 2)

Native Windows notes app: Rust + eframe/egui, no webview.
Phase 1 = Firebase auth + local offline notes. Phase 2 = Firebase Realtime Database sync.

## Build
    cargo build --release --locked
Output: `target/release/ntnotes.exe` (also built by `.github/workflows/build-ntnotes.yml` on windows-latest).
Tests: `cargo test` (pure logic plus HTTP/SSE tests against a local mock server; no network needed).

## Configure
Put `config.json` next to the exe. If it is missing, the app creates it and shows a notice.
Replace `googleClientId` / `googleClientSecret` with your Google OAuth **Desktop app** client values
(Google Cloud Console > Credentials). Email/password works without them.
`databaseURL` must be the exact URL shown at the top of the Realtime Database page.

## Firebase console setup (needed for sync)
1. Authentication > Sign-in method: enable Email/Password and Google.
2. Build > Realtime Database > Create database (any region; start in locked mode).
3. Realtime Database > Rules: paste the rules below and press **Publish**.
4. Make sure `databaseURL` in config.json equals the database URL (no trailing path).

```json
{
  "rules": {
    ".read": false,
    ".write": false,
    "users": {
      "$uid": {
        ".read": "auth != null && auth.uid === $uid",
        ".write": "auth != null && auth.uid === $uid",
        "notes": {
          "$noteId": {
            ".validate": "newData.hasChildren(['id','title','body','createdAt','updatedAt','deleted','deviceId'])",
            "id": {
              ".validate": "newData.isString() && newData.val() === $noteId"
            },
            "title": {
              ".validate": "newData.isString() && newData.val().length <= 2000"
            },
            "body": {
              ".validate": "newData.isString() && newData.val().length <= 1000000"
            },
            "createdAt": {
              ".validate": "newData.isNumber() && newData.val() > 0"
            },
            "updatedAt": {
              ".validate": "newData.isNumber() && newData.val() > 0"
            },
            "deleted": {
              ".validate": "newData.isBoolean()"
            },
            "deviceId": {
              ".validate": "newData.isString() && newData.val().length <= 200"
            },
            "$other": {
              ".validate": false
            }
          }
        }
      }
    }
  }
}
```
`users/$uid` is readable/writable only by that signed-in user; every note must have exactly the
fields id, title, body, createdAt, updatedAt, deleted, deviceId with the right types
(`id` must equal the key). The `$other` rule rejects extra fields: delete that block if the
website/Android clients add fields of their own.

## How sync works
- Remote path: `users/{uid}/notes/{noteId}`, the exact Note JSON (camelCase), tombstones included.
- Conflict rule: newest `updatedAt` wins, per note. Uploads are compare-and-swap (Firebase ETag +
  `if-match`), so an older local edit never overwrites a newer remote note.
- Sign-in: GET all notes, merge, then push local notes that are newer than / missing from the server.
- Offline queue: every local edit is queued (one entry per note, latest version) in
  `queue.json` next to `notes.json`, written atomically, retried with backoff (1 s ... 60 s), and
  flushed as soon as the connection returns.
- Live updates: REST streaming (`Accept: text/event-stream`); falls back to polling every 10-15 s
  when streaming keeps failing, and keeps trying to restore the stream.
- Status dot: Synced = connected and queue empty; Syncing = uploading/catching up;
  Offline = no connection or no valid token.

## Where data lives
- Notes: `%LOCALAPPDATA%\NTNotes\NTNotes\data\users\<uid>\notes.json`
- Upload queue: `%LOCALAPPDATA%\NTNotes\NTNotes\data\users\<uid>\queue.json`
- Settings (deviceId, theme): `%APPDATA%\NTNotes\NTNotes\config\settings.json`
- Refresh token: Windows Credential Manager (`NTNotes/firebase-session`), file fallback in the config folder.

## Source layout
`model.rs` note type (shared JSON contract) | `config.rs` config/settings | `auth.rs` Firebase Auth |
`store.rs` local notes | `secure.rs` token storage | `worker.rs` background worker |
`sync.rs` sync engine (state machine) | `rtdb.rs` REST/SSE I/O | `sse.rs` stream parser + event
application | `queue.rs` persisted upload queue | `app.rs` UI.
