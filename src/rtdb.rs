//! Firebase Realtime Database REST I/O: whole-node GET, compare-and-swap note upload and
//! the Server-Sent-Events stream thread. Blocking calls only; everything here runs on
//! helper threads and reports back through `SyncCtx::notify`.

use crate::auth::AuthError;
use crate::config::AppConfig;
use crate::model::Note;
use crate::sse::{parse_note, SseEvent, SseParser};
use crate::sync::{next_backoff, SyncCtx, SyncMsg, UploadOk};
use serde_json::Value;
use std::io::{self, BufRead};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

const CAS_TRIES: usize = 4;


// ---------------------------------------------------------------- HTTP

fn client() -> &'static reqwest::blocking::Client {
    static C: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .user_agent("NTNotes/0.1")
            .build()
            .expect("failed to build HTTP client")
    })
}

/// For the live stream. In reqwest's blocking client `timeout` bounds the wait for the
/// response headers and every individual body read, never the whole body. So 60 s is the
/// keep-alive timeout (Firebase sends a keep-alive about every 30 s) and the stream can
/// otherwise run for hours.
fn stream_client() -> &'static reqwest::blocking::Client {
    static C: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .user_agent("NTNotes/0.1")
            .build()
            .expect("failed to build streaming HTTP client")
    })
}

fn net_err(e: reqwest::Error) -> AuthError {
    // Never let the URL (it carries the auth token) reach a message.
    AuthError::Network(e.without_url().to_string())
}

fn io_err(e: io::Error) -> AuthError {
    AuthError::Network(e.to_string())
}

fn status_err(status: u16) -> AuthError {
    if status == 401 {
        AuthError::Unauthorized
    } else {
        AuthError::Api { code: format!("HTTP_{status}"), status }
    }
}

/// {databaseURL}/users/{uid}/notes.json or .../notes/{id}.json, with ?auth=TOKEN.
/// Path segments are percent-encoded.
fn notes_url(cfg: &AppConfig, uid: &str, id: Option<&str>, token: &str) -> Result<reqwest::Url, AuthError> {
    let mut url = reqwest::Url::parse(&cfg.database_url)
        .map_err(|e| AuthError::Other(format!("databaseURL in config.json is not valid: {e}")))?;
    {
        let mut seg = url
            .path_segments_mut()
            .map_err(|_| AuthError::Other("databaseURL in config.json is not valid.".into()))?;
        seg.pop_if_empty().push("users").push(uid);
        match id {
            Some(id) => seg.push("notes").push(&format!("{id}.json")),
            None => seg.push("notes.json"),
        };
    }
    url.query_pairs_mut().append_pair("auth", token);
    Ok(url)
}

fn read_json(resp: reqwest::blocking::Response) -> Result<Value, AuthError> {
    let text = resp.text().map_err(net_err)?;
    serde_json::from_str(&text).map_err(|e| AuthError::Other(format!("Unexpected server response: {e}")))
}

/// GET the whole users/{uid}/notes node (JSON `null` when empty).
pub(crate) fn get_notes(ctx: &SyncCtx) -> Result<Value, AuthError> {
    ctx.authed(|tok| {
        let url = notes_url(&ctx.cfg, &ctx.uid, None, tok)?;
        let resp = client().get(url).send().map_err(net_err)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(status_err(status.as_u16()));
        }
        read_json(resp)
    })
}

#[derive(Debug, PartialEq)]
enum Decision {
    Put,
    AlreadyThere,
    RemoteNewer(Note),
}

/// Never overwrite a newer remote note with an older local one.
fn decide(local: &Note, remote: &Value) -> Decision {
    match parse_note(&local.id, remote) {
        None => Decision::Put, // absent (null) or unreadable junk
        Some(r) if r.updated_at > local.updated_at => Decision::RemoteNewer(r),
        Some(r) if r.updated_at == local.updated_at => Decision::AlreadyThere,
        Some(_) => Decision::Put,
    }
}

/// Upload one note as a compare-and-swap: read it with its ETag, decide, then PUT with
/// `if-match`. HTTP 412 means somebody wrote in between: look again.
pub(crate) fn upload_note(ctx: &SyncCtx, note: &Note) -> Result<UploadOk, AuthError> {
    let body = serde_json::to_vec(note).map_err(|e| AuthError::Other(e.to_string()))?;
    ctx.authed(|tok| {
        let url = notes_url(&ctx.cfg, &ctx.uid, Some(&note.id), tok)?;
        for _ in 0..CAS_TRIES {
            if ctx.stopped() {
                return Err(AuthError::Cancelled);
            }
            let resp = client().get(url.clone()).header("X-Firebase-ETag", "true").send().map_err(net_err)?;
            let status = resp.status();
            if !status.is_success() {
                return Err(status_err(status.as_u16()));
            }
            let etag = resp.headers().get("etag").and_then(|v| v.to_str().ok()).map(str::to_owned);
            let remote = read_json(resp)?;
            match decide(note, &remote) {
                Decision::AlreadyThere => return Ok(UploadOk::Done),
                Decision::RemoteNewer(r) => return Ok(UploadOk::Superseded(r)),
                Decision::Put => {}
            }
            let mut req = client().put(url.clone()).header("Content-Type", "application/json").body(body.clone());
            if let Some(e) = &etag {
                req = req.header("if-match", e.as_str());
            }
            let resp = req.send().map_err(net_err)?;
            let status = resp.status();
            if status.is_success() {
                return Ok(UploadOk::Done);
            }
            if status.as_u16() == 412 {
                continue;
            }
            return Err(status_err(status.as_u16()));
        }
        Err(AuthError::Other("The note kept changing on the server; will retry.".into()))
    })
}

// ---------------------------------------------------------------- stream thread

pub(crate) fn spawn_stream(ctx: Arc<SyncCtx>) {
    thread::Builder::new()
        .name("ntnotes-stream".into())
        .spawn(move || stream_loop(&ctx))
        .expect("failed to start stream thread");
}

fn stream_loop(ctx: &SyncCtx) {
    let mut failures: u32 = 0;
    let mut backoff = Duration::from_secs(1);
    while !ctx.stopped() {
        let mut healthy = false;
        let error = match stream_session(ctx, &mut healthy) {
            Err(e) => e,
            Ok(()) => return, // stopped
        };
        if ctx.stopped() {
            return;
        }
        if healthy {
            // It worked for a while; a normal disconnect, not a reason to fall back.
            failures = 0;
            backoff = Duration::from_secs(1);
        } else {
            failures += 1;
        }
        (ctx.notify)(SyncMsg::StreamDown { error, failures });
        if !ctx.sleep(backoff) {
            return;
        }
        if !healthy {
            backoff = next_backoff(backoff);
        }
    }
}

/// One connection. Always ends with an error (the caller reconnects), except when stopped.
fn stream_session(ctx: &SyncCtx, healthy: &mut bool) -> Result<(), AuthError> {
    let mut tok = ctx.token();
    if tok.is_empty() {
        tok = ctx.refresh_if_stale("")?;
    }
    let url = notes_url(&ctx.cfg, &ctx.uid, None, &tok)?;
    // reqwest follows Firebase's 307 redirect to the right shard automatically.
    let resp = stream_client().get(url).header("Accept", "text/event-stream").send().map_err(net_err)?;
    let status = resp.status();
    if status.as_u16() == 401 {
        let _ = ctx.refresh_if_stale(&tok);
        return Err(AuthError::Unauthorized);
    }
    if !status.is_success() {
        return Err(status_err(status.as_u16()));
    }
    let is_sse = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("text/event-stream"))
        .unwrap_or(false);
    if !is_sse {
        // e.g. a proxy answering with its own page
        return Err(AuthError::Other("The server did not open an event stream.".into()));
    }
    (ctx.notify)(SyncMsg::StreamUp);

    let mut reader = io::BufReader::new(resp);
    let mut parser = SseParser::new();
    let mut line = String::new();
    loop {
        if ctx.stopped() {
            return Ok(());
        }
        line.clear();
        let n = reader.read_line(&mut line).map_err(io_err)?;
        if n == 0 {
            return Err(AuthError::Network("The live connection was closed.".into()));
        }
        let Some(ev) = parser.feed_line(&line) else { continue };
        match ev {
            SseEvent::KeepAlive => *healthy = true,
            SseEvent::Unknown => {}
            SseEvent::AuthRevoked | SseEvent::Cancel => {
                // Token expired / permission denied: get a fresh token, then reconnect.
                let _ = ctx.refresh_if_stale(&tok);
                return Err(AuthError::Unauthorized);
            }
            ev @ (SseEvent::Put { .. } | SseEvent::Patch { .. }) => {
                *healthy = true;
                (ctx.notify)(SyncMsg::StreamEvent(ev));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::Notify;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Mutex;

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

    #[test]
    fn decide_never_overwrites_newer_remote() {
        let local = note("a", 10, "d1");
        assert_eq!(decide(&local, &Value::Null), Decision::Put);
        assert_eq!(decide(&local, &json!({"junk": true})), Decision::Put);
        let older = json!({"id":"a","title":"x","body":"","createdAt":1,"updatedAt":5,"deleted":false,"deviceId":"d2"});
        assert_eq!(decide(&local, &older), Decision::Put);
        let same = json!({"id":"a","title":"x","body":"","createdAt":1,"updatedAt":10,"deleted":false,"deviceId":"d1"});
        assert_eq!(decide(&local, &same), Decision::AlreadyThere);
        let newer = json!({"id":"a","title":"x","body":"","createdAt":1,"updatedAt":11,"deleted":true,"deviceId":"d2"});
        match decide(&local, &newer) {
            Decision::RemoteNewer(r) => assert!(r.deleted && r.updated_at == 11),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn url_layout_and_encoding() {
        let u = notes_url(&cfg(), "u1", None, "TOK").unwrap();
        assert_eq!(u.as_str(), "https://db.example.com/users/u1/notes.json?auth=TOK");
        let u = notes_url(&cfg(), "u1", Some("n 1"), "TOK").unwrap();
        assert_eq!(u.as_str(), "https://db.example.com/users/u1/notes/n%201.json?auth=TOK");
        let mut c = cfg();
        c.database_url = "https://db.example.com/".into();
        assert_eq!(notes_url(&c, "u1", Some("x"), "T").unwrap().path(), "/users/u1/notes/x.json");
    }

    // ---- real HTTP against a local mock of the Firebase REST API

    #[derive(Default)]
    struct MockState {
        notes: HashMap<String, Value>,
        etags: HashMap<String, String>,
        version: u64,
        log: Vec<String>,
        /// On the next PUT of this id: another device "wins the race" first (-> HTTP 412).
        race_once: Option<(String, Value)>,
        stream_body: String,
    }

    struct Mock {
        url: String,
        state: Arc<Mutex<MockState>>,
    }

    fn header_of(req: &tiny_http::Request, name: &'static str) -> Option<String> {
        req.headers().iter().find(|h| h.field.equiv(name)).map(|h| h.value.as_str().to_string())
    }

    fn start_mock() -> Mock {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        let state = Arc::new(Mutex::new(MockState::default()));
        let st = state.clone();
        thread::spawn(move || {
            for mut req in server.incoming_requests() {
                let url = req.url().to_string();
                let path = url.split('?').next().unwrap_or("").to_string();
                let method = req.method().to_string();
                let if_match = header_of(&req, "if-match");
                let accept = header_of(&req, "accept").unwrap_or_default();
                let mut body = String::new();
                let _ = std::io::Read::read_to_string(req.as_reader(), &mut body);
                let mut s = st.lock().unwrap();
                s.log.push(format!("{method} {path} if-match={}", if_match.clone().unwrap_or_default()));
                let json = |code: u16, text: String, etag: Option<String>| {
                    let mut r = tiny_http::Response::from_string(text).with_status_code(code);
                    r = r.with_header(tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
                    if let Some(e) = etag {
                        r = r.with_header(tiny_http::Header::from_bytes(&b"ETag"[..], e.as_bytes()).unwrap());
                    }
                    r
                };
                if path == "/users/u1/notes.json" && method == "GET" {
                    if accept.contains("text/event-stream") {
                        let r = tiny_http::Response::from_string(s.stream_body.clone())
                            .with_header(tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/event-stream"[..]).unwrap());
                        let _ = req.respond(r);
                    } else {
                        let all = if s.notes.is_empty() { Value::Null } else { json!(s.notes) };
                        let _ = req.respond(json(200, all.to_string(), None));
                    }
                } else if let Some(id) = path.strip_prefix("/users/u1/notes/").and_then(|p| p.strip_suffix(".json")) {
                    let id = id.to_string();
                    let cur_etag = s.etags.get(&id).cloned().unwrap_or_else(|| "null_etag".into());
                    if method == "GET" {
                        let v = s.notes.get(&id).cloned().unwrap_or(Value::Null);
                        let _ = req.respond(json(200, v.to_string(), Some(cur_etag)));
                    } else {
                        if let Some((rid, rv)) = s.race_once.take() {
                            if rid == id {
                                s.version += 1;
                                let e = format!("etag-{}", s.version);
                                s.notes.insert(id.clone(), rv);
                                s.etags.insert(id.clone(), e);
                                let _ = req.respond(json(412, "{\"error\":\"Precondition failed\"}".into(), None));
                                continue;
                            }
                        }
                        if if_match.as_deref().map_or(false, |m| m != cur_etag) {
                            let _ = req.respond(json(412, "{\"error\":\"Precondition failed\"}".into(), None));
                            continue;
                        }
                        s.version += 1;
                        let e = format!("etag-{}", s.version);
                        s.notes.insert(id.clone(), serde_json::from_str(&body).unwrap());
                        s.etags.insert(id, e);
                        let _ = req.respond(json(200, body, None));
                    }
                } else {
                    let _ = req.respond(json(404, "{}".into(), None));
                }
            }
        });
        Mock { url: format!("http://127.0.0.1:{port}"), state }
    }

    fn test_ctx(mock: &Mock) -> (Arc<SyncCtx>, Arc<Mutex<Vec<String>>>) {
        let msgs = Arc::new(Mutex::new(Vec::new()));
        let m2 = msgs.clone();
        let notify: Notify = Arc::new(move |m| m2.lock().unwrap().push(format!("{m:?}")));
        let mut c = cfg();
        c.database_url = mock.url.clone();
        (Arc::new(SyncCtx::new(c, "u1", "TOK", "REFRESH", notify)), msgs)
    }

    #[test]
    fn http_upload_creates_with_null_etag_and_updates_with_current_etag() {
        let mock = start_mock();
        let (ctx, _) = test_ctx(&mock);
        assert!(matches!(upload_note(&ctx, &note("a", 10, "me")), Ok(UploadOk::Done)));
        assert!(matches!(upload_note(&ctx, &note("a", 20, "me")), Ok(UploadOk::Done)));
        let s = mock.state.lock().unwrap();
        assert_eq!(s.notes["a"]["updatedAt"], 20);
        assert_eq!(s.notes["a"]["deviceId"], "me");
        assert!(s.log.contains(&"PUT /users/u1/notes/a.json if-match=null_etag".to_string()), "{:?}", s.log);
        assert!(s.log.iter().any(|l| l.starts_with("PUT") && l.ends_with("if-match=etag-1")), "{:?}", s.log);
    }

    #[test]
    fn http_upload_never_overwrites_a_newer_remote_note() {
        let mock = start_mock();
        mock.state.lock().unwrap().notes.insert("a".into(), json!({"id":"a","title":"remote","body":"","createdAt":1,"updatedAt":50,"deleted":false,"deviceId":"other"}));
        let (ctx, _) = test_ctx(&mock);
        match upload_note(&ctx, &note("a", 10, "me")) {
            Ok(UploadOk::Superseded(r)) => assert_eq!((r.title.as_str(), r.updated_at), ("remote", 50)),
            other => panic!("unexpected {other:?}"),
        }
        let s = mock.state.lock().unwrap();
        assert_eq!(s.notes["a"]["title"], "remote");
        assert!(!s.log.iter().any(|l| l.starts_with("PUT")));
    }

    #[test]
    fn http_upload_survives_a_write_race_via_412() {
        let mock = start_mock();
        {
            let mut s = mock.state.lock().unwrap();
            s.notes.insert("a".into(), json!({"id":"a","title":"v1","body":"","createdAt":1,"updatedAt":5,"deleted":false,"deviceId":"other"}));
            s.etags.insert("a".into(), "etag-0".into());
            // between our GET and our PUT another device writes a NEWER version
            s.race_once = Some(("a".into(), json!({"id":"a","title":"raced","body":"","createdAt":1,"updatedAt":99,"deleted":false,"deviceId":"other"})));
        }
        let (ctx, _) = test_ctx(&mock);
        match upload_note(&ctx, &note("a", 10, "me")) {
            Ok(UploadOk::Superseded(r)) => assert_eq!(r.updated_at, 99),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(mock.state.lock().unwrap().notes["a"]["title"], "raced");
    }

    #[test]
    fn http_upload_retries_412_when_remote_is_still_older() {
        let mock = start_mock();
        {
            let mut s = mock.state.lock().unwrap();
            s.notes.insert("a".into(), json!({"id":"a","title":"v1","body":"","createdAt":1,"updatedAt":5,"deleted":false,"deviceId":"other"}));
            s.etags.insert("a".into(), "etag-0".into());
            s.race_once = Some(("a".into(), json!({"id":"a","title":"v2","body":"","createdAt":1,"updatedAt":6,"deleted":false,"deviceId":"other"})));
        }
        let (ctx, _) = test_ctx(&mock);
        assert!(matches!(upload_note(&ctx, &note("a", 10, "me")), Ok(UploadOk::Done)));
        assert_eq!(mock.state.lock().unwrap().notes["a"]["updatedAt"], 10);
    }

    #[test]
    fn http_get_notes_returns_the_node() {
        let mock = start_mock();
        let (ctx, _) = test_ctx(&mock);
        assert_eq!(get_notes(&ctx).unwrap(), Value::Null);
        assert!(matches!(upload_note(&ctx, &note("a", 10, "me")), Ok(UploadOk::Done)));
        let v = get_notes(&ctx).unwrap();
        assert_eq!(v["a"]["updatedAt"], 10);
        assert_eq!(v["a"]["id"], "a");
    }

    #[test]
    fn http_stream_delivers_events_then_reports_closed_connection() {
        let mock = start_mock();
        mock.state.lock().unwrap().stream_body = concat!(
            "event: put\ndata: {\"path\":\"/\",\"data\":null}\n\n",
            "event: keep-alive\ndata: null\n\n",
            "event: patch\ndata: {\"path\":\"/a\",\"data\":{\"title\":\"x\"}}\n\n",
        )
        .to_string();
        let (ctx, msgs) = test_ctx(&mock);
        let mut healthy = false;
        let res = stream_session(&ctx, &mut healthy);
        assert!(matches!(res, Err(AuthError::Network(_))), "{res:?}");
        assert!(healthy);
        let m = msgs.lock().unwrap();
        assert_eq!(m.len(), 3, "{m:?}");
        assert!(m[0].starts_with("StreamUp"));
        assert!(m[1].contains("Put") && m[1].contains("\"/\""));
        assert!(m[2].contains("Patch") && m[2].contains("\"/a\""));
    }
}
