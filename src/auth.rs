//! Firebase Auth REST API (email/password, password reset, token refresh) and the
//! Google OAuth desktop loopback flow with PKCE. Everything here is blocking and must be
//! called from background threads only.

use crate::config::AppConfig;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const SIGN_UP_URL: &str = "https://identitytoolkit.googleapis.com/v1/accounts:signUp";
const SIGN_IN_URL: &str = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword";
const OOB_URL: &str = "https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode";
const IDP_URL: &str = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithIdp";
const REFRESH_URL: &str = "https://securetoken.googleapis.com/v1/token";
const GOOGLE_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const GOOGLE_TIMEOUT: Duration = Duration::from_secs(180);

// ---------------------------------------------------------------- types

#[derive(Debug, Clone)]
pub struct Session {
    pub uid: String,
    pub email: String,
    /// Empty means "not refreshed yet" (e.g. restored from disk while offline).
    pub id_token: String,
    pub refresh_token: String,
}

impl Session {
    pub fn apply(&mut self, r: Refreshed) {
        self.id_token = r.id_token;
        self.refresh_token = r.refresh_token;
    }
}

#[derive(Debug, Clone)]
pub struct Refreshed {
    pub uid: String,
    pub id_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
}

#[derive(Debug, Clone)]
pub enum AuthError {
    Network(String),
    Api { code: String, status: u16 },
    /// HTTP 401 - the idToken is no longer valid; refresh and retry.
    Unauthorized,
    Cancelled,
    Other(String),
}

impl AuthError {
    /// True when the refresh token itself is no longer usable (user must sign in again).
    pub fn is_session_dead(&self) -> bool {
        match self {
            AuthError::Api { code, .. } => matches!(
                code.as_str(),
                "TOKEN_EXPIRED"
                    | "USER_DISABLED"
                    | "USER_NOT_FOUND"
                    | "INVALID_REFRESH_TOKEN"
                    | "MISSING_REFRESH_TOKEN"
                    | "INVALID_GRANT_TYPE"
                    | "CREDENTIAL_TOO_OLD_LOGIN_AGAIN"
            ),
            _ => false,
        }
    }
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::Network(e) => write!(f, "Network error: {e}. Check your internet connection."),
            AuthError::Api { code, status } => match friendly(code) {
                Some(msg) => write!(f, "{msg} ({code})"),
                None => write!(f, "Firebase error: {code} (HTTP {status})"),
            },
            AuthError::Unauthorized => write!(f, "Not authorized. Please sign in again."),
            AuthError::Cancelled => write!(f, "Cancelled."),
            AuthError::Other(m) => write!(f, "{m}"),
        }
    }
}

fn friendly(code: &str) -> Option<&'static str> {
    Some(match code {
        "EMAIL_EXISTS" => "This email is already registered. Try signing in instead.",
        "INVALID_PASSWORD" => "Wrong password.",
        "EMAIL_NOT_FOUND" => "No account exists for this email.",
        "INVALID_LOGIN_CREDENTIALS" => "Incorrect email or password.",
        "USER_DISABLED" => "This account has been disabled.",
        "WEAK_PASSWORD" => "Password is too weak. Use at least 6 characters.",
        "INVALID_EMAIL" => "That email address is not valid.",
        "MISSING_EMAIL" => "Please enter your email.",
        "MISSING_PASSWORD" => "Please enter your password.",
        "TOO_MANY_ATTEMPTS_TRY_LATER" => "Too many attempts. Please wait a while and try again.",
        "OPERATION_NOT_ALLOWED" => "This sign-in method is not enabled in the Firebase console.",
        "INVALID_IDP_RESPONSE" => "Google sign-in was rejected by Firebase.",
        "TOKEN_EXPIRED" | "INVALID_REFRESH_TOKEN" | "CREDENTIAL_TOO_OLD_LOGIN_AGAIN" => {
            "Your session has expired. Please sign in again."
        }
        "USER_NOT_FOUND" => "This account no longer exists.",
        _ => return None,
    })
}

// ---------------------------------------------------------------- http helpers

fn client() -> &'static reqwest::blocking::Client {
    static CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .user_agent("NTNotes/0.1")
            .build()
            .expect("failed to build HTTP client")
    })
}

fn net_err(e: reqwest::Error) -> AuthError {
    // Strip the URL so the api key query parameter never ends up in the UI.
    AuthError::Network(e.without_url().to_string())
}

fn send(req: reqwest::blocking::RequestBuilder) -> Result<Value, AuthError> {
    let resp = req.send().map_err(net_err)?;
    let status = resp.status();
    let text = resp.text().map_err(net_err)?;
    if status.is_success() {
        serde_json::from_str(&text)
            .map_err(|e| AuthError::Other(format!("Unexpected server response: {e}")))
    } else if status.as_u16() == 401 {
        Err(AuthError::Unauthorized)
    } else {
        Err(parse_api_error(&text, status.as_u16()))
    }
}

fn parse_api_error(text: &str, status: u16) -> AuthError {
    let v: Value = serde_json::from_str(text).unwrap_or(Value::Null);
    let raw = v["error"]["message"].as_str().unwrap_or("");
    // Messages look like "WEAK_PASSWORD : Password should be at least 6 characters".
    let code = raw.split([' ', ':']).next().unwrap_or("").to_string();
    let code = if code.is_empty() { format!("HTTP_{status}") } else { code };
    AuthError::Api { code, status }
}

// ---------------------------------------------------------------- Firebase email/password

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignInResp {
    id_token: String,
    refresh_token: String,
    local_id: String,
    #[serde(default)]
    email: Option<String>,
}

fn session_from(v: Value, fallback_email: &str) -> Result<Session, AuthError> {
    let r: SignInResp = serde_json::from_value(v)
        .map_err(|e| AuthError::Other(format!("Unexpected sign-in response: {e}")))?;
    Ok(Session {
        uid: r.local_id,
        email: r.email.filter(|e| !e.is_empty()).unwrap_or_else(|| fallback_email.to_string()),
        id_token: r.id_token,
        refresh_token: r.refresh_token,
    })
}

fn password_auth(cfg: &AppConfig, url: &str, email: &str, password: &str) -> Result<Session, AuthError> {
    let v = send(
        client()
            .post(url)
            .query(&[("key", cfg.api_key.as_str())])
            .json(&json!({ "email": email, "password": password, "returnSecureToken": true })),
    )?;
    session_from(v, email)
}

pub fn sign_up(cfg: &AppConfig, email: &str, password: &str) -> Result<Session, AuthError> {
    password_auth(cfg, SIGN_UP_URL, email, password)
}

pub fn sign_in(cfg: &AppConfig, email: &str, password: &str) -> Result<Session, AuthError> {
    password_auth(cfg, SIGN_IN_URL, email, password)
}

pub fn send_password_reset(cfg: &AppConfig, email: &str) -> Result<(), AuthError> {
    send(
        client()
            .post(OOB_URL)
            .query(&[("key", cfg.api_key.as_str())])
            .json(&json!({ "requestType": "PASSWORD_RESET", "email": email })),
    )
    .map(|_| ())
}

// ---------------------------------------------------------------- token refresh

#[derive(Deserialize)]
struct RefreshResp {
    id_token: String,
    refresh_token: String,
    user_id: String,
    #[serde(default)]
    expires_in: Option<String>,
}

pub fn refresh(cfg: &AppConfig, refresh_token: &str) -> Result<Refreshed, AuthError> {
    let v = send(
        client()
            .post(REFRESH_URL)
            .query(&[("key", cfg.api_key.as_str())])
            .form(&[("grant_type", "refresh_token"), ("refresh_token", refresh_token)]),
    )?;
    let r: RefreshResp = serde_json::from_value(v)
        .map_err(|e| AuthError::Other(format!("Unexpected refresh response: {e}")))?;
    Ok(Refreshed {
        uid: r.user_id,
        id_token: r.id_token,
        refresh_token: r.refresh_token,
        expires_in: r.expires_in.and_then(|s| s.parse().ok()).unwrap_or(3600),
    })
}

/// Phase 2 helper: run an authenticated call; on 401 (or a missing token) refresh the
/// idToken once and retry. The closure receives the current idToken.
#[allow(dead_code)]
pub fn call_with_refresh<T>(
    cfg: &AppConfig,
    session: &mut Session,
    f: impl Fn(&str) -> Result<T, AuthError>,
) -> Result<T, AuthError> {
    if session.id_token.is_empty() {
        let r = refresh(cfg, &session.refresh_token)?;
        session.apply(r);
    }
    match f(&session.id_token) {
        Err(AuthError::Unauthorized) => {
            let r = refresh(cfg, &session.refresh_token)?;
            session.apply(r);
            f(&session.id_token)
        }
        other => other,
    }
}

// ---------------------------------------------------------------- Google OAuth (loopback + PKCE)

fn random_token(bytes: usize) -> String {
    let mut b = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut b);
    URL_SAFE_NO_PAD.encode(b)
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn page(status: i32, title: &str, message: &str) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let html = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>NT Notes</title></head>\
         <body style=\"font-family:Segoe UI,Arial,sans-serif;text-align:center;margin-top:18vh;color:#222\">\
         <h2>{}</h2><p>{}</p></body></html>",
        html_escape(title),
        html_escape(message)
    );
    let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..])
        .expect("valid header");
    tiny_http::Response::from_string(html).with_status_code(status).with_header(header)
}

fn wait_for_code(server: &tiny_http::Server, state: &str, cancel: &AtomicBool) -> Result<String, AuthError> {
    let deadline = Instant::now() + GOOGLE_TIMEOUT;
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Err(AuthError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(AuthError::Other(
                "Google sign-in timed out after 3 minutes. Please try again.".into(),
            ));
        }
        let request = match server.recv_timeout(Duration::from_millis(400)) {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(e) => return Err(AuthError::Other(format!("Local sign-in listener failed: {e}"))),
        };

        let parsed = reqwest::Url::parse(&format!("http://127.0.0.1{}", request.url()));
        let url = match parsed {
            Ok(u) if u.path() == "/" => u,
            _ => {
                let _ = request.respond(page(404, "Not found", ""));
                continue;
            }
        };

        let (mut code, mut got_state, mut error) = (None, None, None);
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "code" => code = Some(v.into_owned()),
                "state" => got_state = Some(v.into_owned()),
                "error" => error = Some(v.into_owned()),
                _ => {}
            }
        }
        if code.is_none() && error.is_none() {
            let _ = request.respond(page(404, "Not found", ""));
            continue;
        }
        // Ignore anything that does not carry our state (stray or forged requests).
        if got_state.as_deref() != Some(state) {
            let _ = request.respond(page(400, "Invalid request", "State mismatch. Please try signing in again."));
            continue;
        }
        if let Some(e) = error {
            let _ = request.respond(page(400, "Sign-in failed", &e));
            return Err(AuthError::Other(format!("Google sign-in failed: {e}")));
        }
        let _ = request.respond(page(200, "Signed in", "You can close this tab and return to NT Notes."));
        return Ok(code.unwrap_or_default());
    }
}

fn exchange_code(cfg: &AppConfig, code: &str, verifier: &str, redirect_uri: &str) -> Result<String, AuthError> {
    let resp = client()
        .post(GOOGLE_TOKEN_URL)
        .form(&[
            ("code", code),
            ("client_id", cfg.google_client_id.as_str()),
            ("client_secret", cfg.google_client_secret.as_str()),
            ("redirect_uri", redirect_uri),
            ("grant_type", "authorization_code"),
            ("code_verifier", verifier),
        ])
        .send()
        .map_err(net_err)?;
    let status = resp.status();
    let text = resp.text().map_err(net_err)?;
    let v: Value = serde_json::from_str(&text)
        .map_err(|e| AuthError::Other(format!("Unexpected response from Google: {e}")))?;
    if !status.is_success() {
        let e = v["error"].as_str().unwrap_or("unknown_error");
        let d = v["error_description"].as_str().unwrap_or("");
        return Err(AuthError::Other(format!("Google rejected the sign-in ({e}) {d}")));
    }
    v["id_token"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| AuthError::Other("Google did not return an id_token.".into()))
}

pub fn google_sign_in(cfg: &AppConfig, cancel: &AtomicBool) -> Result<Session, AuthError> {
    if !cfg.google_configured() {
        return Err(AuthError::Other(
            "Google sign-in is not configured. Fill googleClientId and googleClientSecret in config.json and restart."
                .into(),
        ));
    }
    let server = tiny_http::Server::http("127.0.0.1:0")
        .map_err(|e| AuthError::Other(format!("Could not start the local sign-in listener: {e}")))?;
    let port = server
        .server_addr()
        .to_ip()
        .map(|a| a.port())
        .ok_or_else(|| AuthError::Other("Could not determine the local listener port.".into()))?;
    let redirect_uri = format!("http://127.0.0.1:{port}");

    let verifier = random_token(32);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let state = random_token(16);

    let auth_url = reqwest::Url::parse_with_params(
        GOOGLE_AUTH_URL,
        &[
            ("client_id", cfg.google_client_id.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
            ("response_type", "code"),
            ("scope", "openid email profile"),
            ("state", state.as_str()),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
        ],
    )
    .map_err(|e| AuthError::Other(format!("Could not build the Google sign-in URL: {e}")))?;

    open::that(auth_url.as_str())
        .map_err(|e| AuthError::Other(format!("Could not open the browser: {e}")))?;

    let code = wait_for_code(&server, &state, cancel)?;
    let google_id_token = exchange_code(cfg, &code, &verifier, &redirect_uri)?;

    let v = send(
        client()
            .post(IDP_URL)
            .query(&[("key", cfg.api_key.as_str())])
            .json(&json!({
                "postBody": format!("id_token={google_id_token}&providerId=google.com"),
                "requestUri": "http://localhost",
                "returnSecureToken": true,
                "returnIdpCredential": true
            })),
    )?;
    if v["needConfirmation"].as_bool().unwrap_or(false) {
        return Err(AuthError::Other(
            "An account with this email already exists with a different sign-in method. \
             Sign in with your password instead."
                .into(),
        ));
    }
    session_from(v, "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_firebase_error_codes() {
        let e = parse_api_error(r#"{"error":{"code":400,"message":"EMAIL_EXISTS"}}"#, 400);
        assert!(e.to_string().contains("EMAIL_EXISTS"));
        let e = parse_api_error(
            r#"{"error":{"code":400,"message":"WEAK_PASSWORD : Password should be at least 6 characters"}}"#,
            400,
        );
        assert!(matches!(e, AuthError::Api { ref code, .. } if code == "WEAK_PASSWORD"));
        let e = parse_api_error("garbage", 502);
        assert!(matches!(e, AuthError::Api { ref code, .. } if code == "HTTP_502"));
    }

    #[test]
    fn dead_session_detection() {
        let dead = AuthError::Api { code: "TOKEN_EXPIRED".into(), status: 400 };
        assert!(dead.is_session_dead());
        assert!(!AuthError::Network("x".into()).is_session_dead());
    }

    #[test]
    fn pkce_challenge_matches_rfc7636_example() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        assert_eq!(challenge, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }
}
