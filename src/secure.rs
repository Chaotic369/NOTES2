//! Remembers the Firebase refresh token. Uses Windows Credential Manager when possible
//! (raw advapi32 FFI, no extra crates) and falls back to a file in the user config folder.

use crate::config::{config_dir, write_atomic};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StoredSession {
    pub uid: String,
    pub email: String,
    pub refresh_token: String,
}

#[cfg(windows)]
const TARGET: &str = "NTNotes/firebase-session";

fn fallback_path() -> PathBuf {
    config_dir().join("session.json")
}

/// Returns false if the session could not be stored anywhere.
pub fn save(s: &StoredSession) -> bool {
    let Ok(json) = serde_json::to_vec(s) else { return false };
    #[cfg(windows)]
    {
        if wincred::write(TARGET, &s.email, &json) {
            let _ = fs::remove_file(fallback_path());
            return true;
        }
    }
    write_atomic(&fallback_path(), &json).is_ok()
}

pub fn load() -> Option<StoredSession> {
    #[cfg(windows)]
    {
        if let Some(bytes) = wincred::read(TARGET) {
            if let Ok(s) = serde_json::from_slice(&bytes) {
                return Some(s);
            }
        }
    }
    fs::read(fallback_path()).ok().and_then(|b| serde_json::from_slice(&b).ok())
}

pub fn clear() {
    #[cfg(windows)]
    wincred::delete(TARGET);
    let _ = fs::remove_file(fallback_path());
}

#[cfg(windows)]
#[allow(non_snake_case)]
mod wincred {
    use std::ffi::c_void;
    use std::ptr::null_mut;

    const CRED_TYPE_GENERIC: u32 = 1;
    const CRED_PERSIST_LOCAL_MACHINE: u32 = 2;

    #[repr(C)]
    struct FILETIME {
        dwLowDateTime: u32,
        dwHighDateTime: u32,
    }

    #[repr(C)]
    struct CREDENTIALW {
        Flags: u32,
        Type: u32,
        TargetName: *mut u16,
        Comment: *mut u16,
        LastWritten: FILETIME,
        CredentialBlobSize: u32,
        CredentialBlob: *mut u8,
        Persist: u32,
        AttributeCount: u32,
        Attributes: *mut c_void,
        TargetAlias: *mut u16,
        UserName: *mut u16,
    }

    #[link(name = "advapi32")]
    extern "system" {
        fn CredWriteW(credential: *const CREDENTIALW, flags: u32) -> i32;
        fn CredReadW(target: *const u16, kind: u32, flags: u32, out: *mut *mut CREDENTIALW) -> i32;
        fn CredDeleteW(target: *const u16, kind: u32, flags: u32) -> i32;
        fn CredFree(buffer: *mut c_void);
    }

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    pub fn write(target: &str, user: &str, data: &[u8]) -> bool {
        if data.len() > 2560 {
            return false; // CRED_MAX_CREDENTIAL_BLOB_SIZE
        }
        let mut target_w = wide(target);
        let mut user_w = wide(user);
        let mut blob = data.to_vec();
        let cred = CREDENTIALW {
            Flags: 0,
            Type: CRED_TYPE_GENERIC,
            TargetName: target_w.as_mut_ptr(),
            Comment: null_mut(),
            LastWritten: FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 },
            CredentialBlobSize: blob.len() as u32,
            CredentialBlob: blob.as_mut_ptr(),
            Persist: CRED_PERSIST_LOCAL_MACHINE,
            AttributeCount: 0,
            Attributes: null_mut(),
            TargetAlias: null_mut(),
            UserName: user_w.as_mut_ptr(),
        };
        unsafe { CredWriteW(&cred, 0) != 0 }
    }

    pub fn read(target: &str) -> Option<Vec<u8>> {
        let target_w = wide(target);
        let mut out: *mut CREDENTIALW = null_mut();
        unsafe {
            if CredReadW(target_w.as_ptr(), CRED_TYPE_GENERIC, 0, &mut out) == 0 || out.is_null() {
                return None;
            }
            let c = &*out;
            let bytes = if c.CredentialBlob.is_null() || c.CredentialBlobSize == 0 {
                None
            } else {
                Some(std::slice::from_raw_parts(c.CredentialBlob, c.CredentialBlobSize as usize).to_vec())
            };
            CredFree(out as *mut c_void);
            bytes
        }
    }

    pub fn delete(target: &str) {
        let target_w = wide(target);
        unsafe {
            CredDeleteW(target_w.as_ptr(), CRED_TYPE_GENERIC, 0);
        }
    }
}
