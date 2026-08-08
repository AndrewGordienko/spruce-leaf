//! Browser-based Google OAuth for per-brand Gmail login.
//!
//! Flow:
//!   1. `/login gnk` (etc.) opens Chrome/Safari to Google's consent screen
//!   2. A one-shot localhost callback captures the auth code
//!   3. Tokens are stored under `.spruce/google/<brand>.json`
//!
//! Requires only a Google Cloud **Desktop** OAuth client (`GOOGLE_CLIENT_ID` +
//! `GOOGLE_CLIENT_SECRET`). No Gmail App Passwords, no IMAP toggles in the
//! Google account UI — the user just signs into the right brand mailbox.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Gmail scopes: read mail + send later via API without SMTP app passwords.
const GMAIL_SCOPES: &str = "https://www.googleapis.com/auth/gmail.readonly \
https://www.googleapis.com/auth/gmail.send \
https://www.googleapis.com/auth/userinfo.email \
openid";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleTokenSet {
    pub brand: String,
    pub email: String,
    pub refresh_token: String,
    #[serde(default)]
    pub access_token: String,
    /// Unix seconds when `access_token` expires.
    #[serde(default)]
    pub expires_at: u64,
    #[serde(default)]
    pub scopes: String,
    #[serde(default)]
    pub updated_at: String,
}

impl GoogleTokenSet {
    pub fn token_path(brand: &str) -> PathBuf {
        PathBuf::from(".spruce")
            .join("google")
            .join(format!("{}.json", brand.trim().to_ascii_lowercase()))
    }

    pub fn load(brand: &str) -> Result<Option<Self>> {
        let path = Self::token_path(brand);
        if !path.is_file() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("reading OAuth tokens at {}", path.display()))?;
        let set: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parsing OAuth tokens at {}", path.display()))?;
        Ok(Some(set))
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::token_path(&self.brand);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        let raw = serde_json::to_string_pretty(self).context("serializing OAuth tokens")?;
        fs::write(&path, raw).with_context(|| format!("writing {}", path.display()))?;
        // Best-effort: owner-only on Unix so refresh tokens aren't world-readable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    pub fn delete(brand: &str) -> Result<bool> {
        let path = Self::token_path(brand);
        if path.is_file() {
            fs::remove_file(&path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn is_logged_in(brand: &str) -> bool {
        Self::load(brand)
            .ok()
            .flatten()
            .is_some_and(|t| !t.refresh_token.trim().is_empty() && !t.email.trim().is_empty())
    }

    /// Status lines for every brand key we care about.
    pub fn status_report(brands: &[&str]) -> String {
        brands
            .iter()
            .map(|brand| match Self::load(brand).ok().flatten() {
                Some(t) if !t.refresh_token.is_empty() => {
                    format!("  · {brand}: logged in as {}", t.email)
                }
                _ => format!("  · {brand}: not logged in  (/login {brand})"),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Clone)]
pub struct GoogleOAuthConfig {
    pub client_id: String,
    pub client_secret: String,
}

impl GoogleOAuthConfig {
    pub fn from_env() -> Result<Self> {
        let client_id = std::env::var("GOOGLE_CLIENT_ID")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "GOOGLE_CLIENT_ID is not set. Create a Desktop OAuth client in Google Cloud \
                     (Gmail API enabled) and put the client id/secret in .env — no Gmail account \
                     settings changes required."
                )
            })?;
        let client_secret = std::env::var("GOOGLE_CLIENT_SECRET")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                anyhow!("GOOGLE_CLIENT_SECRET is not set (pair it with GOOGLE_CLIENT_ID)")
            })?;
        Ok(Self {
            client_id,
            client_secret,
        })
    }
}

/// Run the interactive browser login for one brand and persist tokens.
pub async fn login_brand(brand: &str, open_browser: impl FnOnce(&str)) -> Result<GoogleTokenSet> {
    let brand = brand.trim().to_ascii_lowercase();
    if brand.is_empty() {
        bail!("brand is required, e.g. /login gnk");
    }
    let cfg = GoogleOAuthConfig::from_env()?;
    let listener = TcpListener::bind("127.0.0.1:0").context("binding OAuth callback listener")?;
    let port = listener
        .local_addr()
        .context("reading OAuth callback address")?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{port}/oauth2callback");
    let state = uuid::Uuid::new_v4().to_string();

    let auth_url = format!(
        "https://accounts.google.com/o/oauth2/v2/auth?\
         client_id={client_id}\
         &redirect_uri={redirect}\
         &response_type=code\
         &scope={scopes}\
         &access_type=offline\
         &prompt=consent select_account\
         &include_granted_scopes=true\
         &state={state}",
        client_id = urlencoding(&cfg.client_id),
        redirect = urlencoding(&redirect_uri),
        scopes = urlencoding(GMAIL_SCOPES),
        state = urlencoding(&state),
    );

    let (tx, rx) = mpsc::channel::<Result<(String, String)>>();
    let expected_state = state.clone();
    std::thread::spawn(move || {
        let _ = tx.send(wait_for_callback(listener, &expected_state));
    });

    open_browser(&auth_url);

    let (code, returned_state) = rx
        .recv_timeout(Duration::from_secs(300))
        .map_err(|_| anyhow!("timed out waiting for Google login (5 minutes)"))?
        .context("OAuth callback failed")?;
    if returned_state != state {
        bail!("OAuth state mismatch — refusing to continue");
    }

    let tokens = exchange_code(&cfg, &code, &redirect_uri).await?;
    let email = fetch_email(&tokens.access_token).await?;
    let set = GoogleTokenSet {
        brand: brand.clone(),
        email,
        refresh_token: tokens
            .refresh_token
            .filter(|t| !t.is_empty())
            .or_else(|| {
                // Google only returns refresh_token on first consent; keep prior.
                GoogleTokenSet::load(&brand)
                    .ok()
                    .flatten()
                    .map(|t| t.refresh_token)
            })
            .ok_or_else(|| {
                anyhow!(
                    "Google did not return a refresh token. Revoke app access at \
                     https://myaccount.google.com/permissions and try /login again."
                )
            })?,
        access_token: tokens.access_token,
        expires_at: now_unix().saturating_add(tokens.expires_in.unwrap_or(3600)),
        scopes: GMAIL_SCOPES.replace('\n', " ").replace("  ", " "),
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    if set.refresh_token.trim().is_empty() {
        bail!("empty refresh token after login");
    }
    set.save()?;
    Ok(set)
}

/// Fresh access token for a brand, refreshing from disk as needed.
pub async fn access_token_for(brand: &str) -> Result<(String, GoogleTokenSet)> {
    let cfg = GoogleOAuthConfig::from_env()?;
    let mut set = GoogleTokenSet::load(brand)?
        .ok_or_else(|| anyhow!("not logged in for '{brand}' — run /login {brand}"))?;
    let skew = 60u64;
    let now = now_unix();
    if !set.access_token.is_empty() && set.expires_at > now.saturating_add(skew) {
        return Ok((set.access_token.clone(), set));
    }
    let refreshed = refresh_access_token(&cfg, &set.refresh_token).await?;
    set.access_token = refreshed.access_token;
    set.expires_at = now.saturating_add(refreshed.expires_in.unwrap_or(3600));
    if let Ok(email) = fetch_email(&set.access_token).await {
        set.email = email;
    }
    set.updated_at = chrono::Utc::now().to_rfc3339();
    set.save()?;
    Ok((set.access_token.clone(), set))
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

async fn exchange_code(
    cfg: &GoogleOAuthConfig,
    code: &str,
    redirect_uri: &str,
) -> Result<TokenResponse> {
    let response = Client::new()
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("code", code),
            ("client_id", cfg.client_id.as_str()),
            ("client_secret", cfg.client_secret.as_str()),
            ("redirect_uri", redirect_uri),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await
        .context("exchanging OAuth code")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("Google token exchange failed ({status}): {body}");
    }
    serde_json::from_str(&body).context("decoding token exchange response")
}

async fn refresh_access_token(
    cfg: &GoogleOAuthConfig,
    refresh_token: &str,
) -> Result<TokenResponse> {
    let response = Client::new()
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("client_id", cfg.client_id.as_str()),
            ("client_secret", cfg.client_secret.as_str()),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token"),
        ])
        .send()
        .await
        .context("refreshing Google access token")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("Google token refresh failed ({status}): {body}");
    }
    serde_json::from_str(&body).context("decoding token refresh response")
}

async fn fetch_email(access_token: &str) -> Result<String> {
    let response = Client::new()
        .get("https://www.googleapis.com/oauth2/v2/userinfo")
        .bearer_auth(access_token)
        .send()
        .await
        .context("fetching Google userinfo")?;
    let status = response.status();
    let body: Value = response.json().await.context("decoding userinfo")?;
    if !status.is_success() {
        bail!("Google userinfo failed ({status}): {body}");
    }
    body.get("email")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("Google userinfo had no email"))
}

/// Block until Google hits our loopback callback with ?code=&state=.
fn wait_for_callback(listener: TcpListener, expected_state: &str) -> Result<(String, String)> {
    listener
        .set_nonblocking(false)
        .context("configuring OAuth listener")?;
    let (mut stream, _addr) = listener
        .accept()
        .context("accepting OAuth callback connection")?;
    let mut buf = [0u8; 8192];
    let n = stream
        .read(&mut buf)
        .context("reading OAuth callback request")?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");
    // GET /oauth2callback?code=...&state=... HTTP/1.1
    let path = first_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("malformed OAuth callback request"))?;
    let query = path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let params = parse_query(query);
    if let Some(err) = params.get("error") {
        let desc = params.get("error_description").cloned().unwrap_or_default();
        let _ = write_html(
            &mut stream,
            400,
            &format!("<h1>Login failed</h1><p>{err}: {desc}</p><p>You can close this tab.</p>"),
        );
        bail!("Google returned error: {err} {desc}");
    }
    let code = params
        .get("code")
        .cloned()
        .ok_or_else(|| anyhow!("OAuth callback missing code"))?;
    let state = params
        .get("state")
        .cloned()
        .ok_or_else(|| anyhow!("OAuth callback missing state"))?;
    if state != expected_state {
        let _ = write_html(
            &mut stream,
            400,
            "<h1>Login failed</h1><p>State mismatch.</p>",
        );
        bail!("OAuth state mismatch");
    }
    let _ = write_html(
        &mut stream,
        200,
        "<h1>Spruce Leaf connected</h1>\
         <p>Gmail is linked. You can close this tab and return to the terminal.</p>",
    );
    Ok((code, state))
}

fn write_html(stream: &mut impl Write, status: u16, body: &str) -> Result<()> {
    let reason = if status == 200 { "OK" } else { "Bad Request" };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: text/html; charset=utf-8\r\n\
         Connection: close\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(urldecode(k), urldecode(v));
    }
    out
}

fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = &s[i + 1..i + 3];
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[allow(dead_code)]
pub fn tokens_dir() -> PathBuf {
    PathBuf::from(".spruce/google")
}

#[allow(dead_code)]
pub fn ensure_tokens_dir() -> Result<()> {
    fs::create_dir_all(tokens_dir())?;
    Ok(())
}

/// True when path looks like our token store (for tests / diagnostics).
#[allow(dead_code)]
pub fn token_file_exists(brand: &str) -> bool {
    Path::new(&GoogleTokenSet::token_path(brand)).is_file()
}

#[cfg(test)]
mod tests {
    use super::{parse_query, urldecode, urlencoding};

    #[test]
    fn roundtrips_url_encoding() {
        let raw = "a b/c?x=1&y=2";
        assert_eq!(urldecode(&urlencoding(raw)), raw);
    }

    #[test]
    fn parses_callback_query() {
        let q = parse_query("code=abc%2Fdef&state=xyz&scope=a%20b");
        assert_eq!(q.get("code").map(String::as_str), Some("abc/def"));
        assert_eq!(q.get("state").map(String::as_str), Some("xyz"));
    }
}
