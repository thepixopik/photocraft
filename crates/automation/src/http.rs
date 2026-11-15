//! Streamable HTTP transport (`/mcp` and `/health`) for [`PhotocraftMcp`].
//!
//! Implements the MCP Streamable HTTP specification over `tokio::net::TcpListener` without adding
//! extra HTTP crate dependencies:
//!
//! * `POST /mcp` (or `POST /`) — JSON-RPC 2.0 request, notification, or batch. All MCP pipes are
//!   spawned from clones of the shared [`PhotocraftMcp`] instance so cloud AI orchestrators that
//!   re-initialize or omit `Mcp-Session-Id` between turns keep full document, layer, selection and
//!   undo state.
//! * `DELETE /mcp` — closes the caller's `Mcp-Session-Id` transport pipe while keeping shared
//!   studio documents intact (`204 No Content`).
//! * `GET /mcp` — returns `405 Method Not Allowed` per the Streamable HTTP specification when
//!   standalone SSE push streams are not offered.
//! * `OPTIONS /mcp` — CORS preflight (`204 No Content`).
//! * `GET /health` / `GET /healthz` — liveness probe (`200 OK` JSON).

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::{AutomationError, PhotocraftMcp};

const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_HEADER_LINES: usize = 128;
const MAX_BODY_BYTES: usize = crate::security::MAX_REQUEST_BYTES;
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(30 * 60);
const DEFAULT_MAX_SESSIONS: usize = 64;
const READ_IO_TIMEOUT: Duration = Duration::from_secs(30);
const CALL_TIMEOUT: Duration = Duration::from_secs(300);
const STATELESS_KEY: &str = "pc-shared-studio";

/// Configuration for the Streamable HTTP MCP listener.
#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// Optional Bearer token required on `/mcp` requests.
    pub auth_token: Option<String>,
    /// Idle timeout before an inactive `Mcp-Session-Id` pipe is evicted.
    pub session_ttl: Duration,
    /// Maximum concurrent `Mcp-Session-Id` transport pipes kept in memory.
    pub max_sessions: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self { auth_token: None, session_ttl: DEFAULT_SESSION_TTL, max_sessions: DEFAULT_MAX_SESSIONS }
    }
}

impl HttpConfig {
    /// Default configuration with an optional Bearer token (empty strings are treated as `None`).
    pub fn with_token(token: Option<String>) -> Self {
        Self { auth_token: token.filter(|t| !t.is_empty()), session_ttl: DEFAULT_SESSION_TTL, max_sessions: DEFAULT_MAX_SESSIONS }
    }
}

/// Bind `addr` and serve [`PhotocraftMcp`] over Streamable HTTP until an accept error occurs.
pub async fn serve(addr: impl ToSocketAddrs, config: HttpConfig, server: PhotocraftMcp) -> Result<(), AutomationError> {
    let bind_addr = addr
        .to_socket_addrs()
        .map_err(|e| AutomationError::Io(format!("invalid HTTP bind address: {e}")))?
        .next()
        .ok_or_else(|| AutomationError::BadRequest("HTTP bind address resolved to no socket addresses".to_string()))?;
    let listener = TcpListener::bind(bind_addr).await.map_err(|e| AutomationError::Io(format!("bind {bind_addr}: {e}")))?;
    serve_listener(listener, config, server).await
}

/// Serve [`PhotocraftMcp`] on an already-bound [`TcpListener`].
pub async fn serve_listener(listener: TcpListener, config: HttpConfig, server: PhotocraftMcp) -> Result<(), AutomationError> {
    let state = Arc::new(ServerState::new(config, server));
    loop {
        let (stream, _) = listener.accept().await.map_err(|e| AutomationError::Io(format!("accept failed: {e}")))?;
        let _ = stream.set_nodelay(true);
        let st = Arc::clone(&state);
        tokio::spawn(async move {
            handle_connection(stream, st).await;
        });
    }
}

struct ServerState {
    config: HttpConfig,
    base_server: PhotocraftMcp,
    pipes: Mutex<HashMap<String, Arc<Mutex<McpPipe>>>>,
}

impl ServerState {
    fn new(config: HttpConfig, base_server: PhotocraftMcp) -> Self {
        Self { config, base_server, pipes: Mutex::new(HashMap::new()) }
    }

    async fn get_or_spawn_pipe(&self, session_id: &str, force_fresh: bool) -> Arc<Mutex<McpPipe>> {
        let mut map = self.pipes.lock().await;
        let ttl = self.config.session_ttl;
        map.retain(|_, entry| entry.try_lock().map(|p| p.last_used.elapsed() < ttl).unwrap_or(true));
        if !force_fresh && let Some(existing) = map.get(session_id) {
            return Arc::clone(existing);
        }
        let cap = self.config.max_sessions.max(1);
        while map.len() >= cap && !map.contains_key(session_id) {
            let oldest = map
                .iter()
                .filter_map(|(k, v)| v.try_lock().ok().map(|p| (k.clone(), p.last_used)))
                .min_by_key(|(_, t)| *t)
                .map(|(k, _)| k)
                .or_else(|| map.keys().next().cloned());
            let Some(victim) = oldest else { break };
            map.remove(&victim);
        }
        let pipe = Arc::new(Mutex::new(McpPipe::spawn(self.base_server.clone())));
        map.insert(session_id.to_string(), Arc::clone(&pipe));
        pipe
    }

    async fn remove_pipe(&self, session_id: &str) -> bool {
        let mut map = self.pipes.lock().await;
        map.remove(session_id).is_some()
    }
}

struct McpPipe {
    writer: DuplexStream,
    reader: BufReader<DuplexStream>,
    task: JoinHandle<Result<(), AutomationError>>,
    initialized: bool,
    last_used: Instant,
}

impl Drop for McpPipe {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl McpPipe {
    fn spawn(server: PhotocraftMcp) -> Self {
        let (writer, server_in) = tokio::io::duplex(2 << 20);
        let (server_out, reader_raw) = tokio::io::duplex(2 << 20);
        let task = tokio::spawn(server.serve_io(server_in, server_out));
        Self { writer, reader: BufReader::new(reader_raw), task, initialized: false, last_used: Instant::now() }
    }

    async fn send_line(&mut self, line: &str) -> Result<(), String> {
        self.writer.write_all(line.as_bytes()).await.map_err(|e| format!("MCP pipe write failed: {e}"))?;
        self.writer.write_all(b"\n").await.map_err(|e| format!("MCP pipe newline write failed: {e}"))?;
        self.writer.flush().await.map_err(|e| format!("MCP pipe flush failed: {e}"))
    }

    async fn read_matching_reply(&mut self, expected_id: &Value) -> Result<String, String> {
        let fut = async {
            loop {
                let mut line = String::new();
                let n = self.reader.read_line(&mut line).await.map_err(|e| format!("MCP pipe read failed: {e}"))?;
                if n == 0 {
                    return Err("MCP server closed pipe before replying".to_string());
                }
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if serde_json::from_str::<Value>(trimmed).ok().is_some_and(|val| val.get("id") == Some(expected_id)) {
                    return Ok(trimmed.to_string());
                }
            }
        };
        timeout(CALL_TIMEOUT, fut).await.map_err(|_| "MCP tool call timed out".to_string())?
    }

    async fn bootstrap(&mut self) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }
        let boot_id = Value::String("__pc_http_boot__".to_string());
        let init_line = r#"{"jsonrpc":"2.0","id":"__pc_http_boot__","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"photocraft-http","version":"1.0"}}}"#;
        self.send_line(init_line).await?;
        let _ = self.read_matching_reply(&boot_id).await?;
        self.send_line(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).await?;
        self.initialized = true;
        Ok(())
    }

    async fn dispatch_one(&mut self, msg: &Value) -> Result<Option<String>, String> {
        self.last_used = Instant::now();
        let Some(obj) = msg.as_object() else {
            return Ok(Some(jsonrpc_error(&Value::Null, -32600, "invalid JSON-RPC message")));
        };
        let method = obj.get("method").and_then(Value::as_str);
        if !obj.contains_key("id") {
            if method == Some("notifications/initialized") && self.initialized {
                return Ok(None);
            }
            if !self.initialized {
                self.bootstrap().await?;
            }
            let compact = serde_json::to_string(msg).map_err(|e| format!("serialize notification: {e}"))?;
            self.send_line(&compact).await?;
            if method == Some("notifications/initialized") {
                self.initialized = true;
            }
            return Ok(None);
        }
        let req_id = obj.get("id").cloned().unwrap_or(Value::Null);
        let compact = serde_json::to_string(msg).map_err(|e| format!("serialize request: {e}"))?;
        if method == Some("initialize") {
            self.send_line(&compact).await?;
            let reply = self.read_matching_reply(&req_id).await?;
            self.send_line(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).await?;
            self.initialized = true;
            return Ok(Some(reply));
        }
        if !self.initialized {
            self.bootstrap().await?;
        }
        self.send_line(&compact).await?;
        let reply = self.read_matching_reply(&req_id).await?;
        Ok(Some(reply))
    }
}

async fn handle_connection(mut stream: TcpStream, state: Arc<ServerState>) {
    loop {
        let req = match timeout(READ_IO_TIMEOUT, read_http_request(&mut stream)).await {
            Ok(Ok(Some(r))) => r,
            Ok(Ok(None)) | Err(_) => return,
            Ok(Err(HttpReadError::PayloadTooLarge)) => {
                let _ = write_http_response(&mut stream, &HttpResponse::json(413, "Payload Too Large", None, r#"{"error":"request too large"}"#.into()), true).await;
                return;
            }
            Ok(Err(HttpReadError::BadRequest(msg))) => {
                let body = serde_json::json!({ "error": msg }).to_string();
                let _ = write_http_response(&mut stream, &HttpResponse::json(400, "Bad Request", None, body), true).await;
                return;
            }
        };
        let close = req.wants_close();
        let resp = route_request(&req, &state).await;
        if write_http_response(&mut stream, &resp, close).await.is_err() || close {
            return;
        }
    }
}

async fn route_request(req: &HttpRequest, state: &ServerState) -> HttpResponse {
    let path = req.path.as_str();
    if req.method == "OPTIONS" {
        return HttpResponse::empty(204, "No Content", None, Vec::new());
    }
    if req.method == "GET" && (path == "/health" || path == "/healthz") {
        return HttpResponse::json(200, "OK", None, r#"{"ok":true,"service":"photocraft-mcp"}"#.to_string());
    }
    if path != "/mcp" && path != "/" {
        return HttpResponse::json(404, "Not Found", None, r#"{"error":"not found; use POST /mcp or GET /health"}"#.to_string());
    }
    if let Some(expected) = state.config.auth_token.as_deref() {
        let authorized = req.header("authorization").is_some_and(|h| bearer_matches(h, expected));
        if !authorized {
            let hdrs = vec![("WWW-Authenticate", "Bearer realm=\"photocraft-mcp\"")];
            return HttpResponse::with_headers(401, "Unauthorized", "application/json", None, hdrs, r#"{"error":"missing or invalid Bearer token"}"#.into());
        }
    }
    match req.method.as_str() {
        "POST" => handle_post(req, state).await,
        "DELETE" => handle_delete(req, state).await,
        "GET" => {
            let hdrs = vec![("Allow", "POST, DELETE, OPTIONS")];
            let body = r#"{"error":"SSE stream not offered; send JSON-RPC requests via POST /mcp"}"#.to_string();
            HttpResponse::with_headers(405, "Method Not Allowed", "application/json", None, hdrs, body)
        }
        _ => {
            let hdrs = vec![("Allow", "POST, DELETE, OPTIONS")];
            HttpResponse::with_headers(405, "Method Not Allowed", "application/json", None, hdrs, r#"{"error":"method not allowed"}"#.to_string())
        }
    }
}

async fn handle_delete(req: &HttpRequest, state: &ServerState) -> HttpResponse {
    let Some(sid) = req.header("mcp-session-id").map(str::trim).filter(|s| !s.is_empty()) else {
        return HttpResponse::json(400, "Bad Request", None, r#"{"error":"missing Mcp-Session-Id header"}"#.to_string());
    };
    let _ = state.remove_pipe(sid).await;
    HttpResponse::empty(204, "No Content", None, Vec::new())
}

async fn handle_post(req: &HttpRequest, state: &ServerState) -> HttpResponse {
    let parsed: Value = match serde_json::from_str(&req.body) {
        Ok(v) => v,
        Err(e) => {
            let err_body = jsonrpc_error(&Value::Null, -32700, &format!("parse error: {e}"));
            return HttpResponse::json(200, "OK", None, err_body);
        }
    };
    let is_init = is_initialize_payload(&parsed);
    let client_sid = req.header("mcp-session-id").map(str::trim).filter(|s| !s.is_empty()).map(str::to_string);
    let session_id = match (&client_sid, is_init) {
        (Some(sid), _) => sid.clone(),
        (None, true) => generate_session_id(),
        (None, false) => STATELESS_KEY.to_string(),
    };
    let response_sid = if session_id == STATELESS_KEY && client_sid.is_none() { None } else { Some(session_id.clone()) };
    let pipe = state.get_or_spawn_pipe(&session_id, is_init).await;
    let mut guard = pipe.lock().await;
    match &parsed {
        Value::Array(items) => {
            if items.is_empty() {
                return HttpResponse::json(200, "OK", response_sid, jsonrpc_error(&Value::Null, -32600, "empty JSON-RPC batch"));
            }
            let mut replies = Vec::new();
            for item in items {
                match guard.dispatch_one(item).await {
                    Ok(Some(r)) => replies.push(r),
                    Ok(None) => {}
                    Err(e) => return HttpResponse::json(500, "Internal Server Error", response_sid, jsonrpc_error(&Value::Null, -32603, &e)),
                }
            }
            if replies.is_empty() {
                HttpResponse::empty(202, "Accepted", response_sid, Vec::new())
            } else {
                HttpResponse::json(200, "OK", response_sid, format!("[{}]", replies.join(",")))
            }
        }
        _ => match guard.dispatch_one(&parsed).await {
            Ok(Some(reply)) => HttpResponse::json(200, "OK", response_sid, reply),
            Ok(None) => HttpResponse::empty(202, "Accepted", response_sid, Vec::new()),
            Err(e) => {
                let req_id = parsed.get("id").cloned().unwrap_or(Value::Null);
                HttpResponse::json(500, "Internal Server Error", response_sid, jsonrpc_error(&req_id, -32603, &e))
            }
        },
    }
}

fn is_initialize_payload(val: &Value) -> bool {
    match val {
        Value::Object(obj) => obj.get("method").and_then(Value::as_str) == Some("initialize"),
        Value::Array(items) => items.iter().any(|item| item.get("method").and_then(Value::as_str) == Some("initialize")),
        _ => false,
    }
}

fn jsonrpc_error(id: &Value, code: i32, message: &str) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
    .to_string()
}

fn generate_session_id() -> String {
    let mut raw = [0u8; 16];
    if getrandom::fill(&mut raw).is_err() {
        return "pc-fallback-session".to_string();
    }
    let mut out = String::with_capacity(35);
    out.push_str("pc-");
    for b in raw {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn bearer_matches(header_val: &str, expected: &str) -> bool {
    let trimmed = header_val.trim();
    let Some((scheme, rest)) = trimmed.split_once(' ') else { return false };
    if !scheme.eq_ignore_ascii_case("bearer") {
        return false;
    }
    let provided = rest.trim().as_bytes();
    let want = expected.as_bytes();
    let max_len = provided.len().max(want.len());
    let mut diff = provided.len() ^ want.len();
    for i in 0..max_len {
        let a = provided.get(i).copied().unwrap_or(0);
        let b = want.get(i).copied().unwrap_or(0);
        diff |= usize::from(a ^ b);
    }
    diff == 0
}

struct HttpRequest {
    method: String,
    path: String,
    version: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl HttpRequest {
    fn header(&self, lower_name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == lower_name).map(|(_, v)| v.as_str())
    }

    fn wants_close(&self) -> bool {
        if let Some(conn) = self.header("connection") {
            if conn.split(',').any(|t| t.trim().eq_ignore_ascii_case("close")) {
                return true;
            }
            if conn.split(',').any(|t| t.trim().eq_ignore_ascii_case("keep-alive")) {
                return false;
            }
        }
        self.version == "HTTP/1.0"
    }
}

struct HttpResponse {
    status: u16,
    status_text: &'static str,
    content_type: &'static str,
    session_id: Option<String>,
    extra_headers: Vec<(&'static str, &'static str)>,
    body: String,
}

impl HttpResponse {
    fn json(status: u16, status_text: &'static str, session_id: Option<String>, body: String) -> Self {
        Self { status, status_text, content_type: "application/json", session_id, extra_headers: Vec::new(), body }
    }

    fn empty(status: u16, status_text: &'static str, session_id: Option<String>, extra_headers: Vec<(&'static str, &'static str)>) -> Self {
        Self { status, status_text, content_type: "text/plain; charset=utf-8", session_id, extra_headers, body: String::new() }
    }

    fn with_headers(
        status: u16,
        status_text: &'static str,
        content_type: &'static str,
        session_id: Option<String>,
        extra_headers: Vec<(&'static str, &'static str)>,
        body: String,
    ) -> Self {
        Self { status, status_text, content_type, session_id, extra_headers, body }
    }
}

enum HttpReadError {
    BadRequest(String),
    PayloadTooLarge,
}

async fn read_bounded_line<R: AsyncBufReadExt + Unpin>(reader: &mut R, budget: &mut usize) -> Result<Option<String>, HttpReadError> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(|e| HttpReadError::BadRequest(format!("read error: {e}")))?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            return Err(HttpReadError::BadRequest("unexpected EOF in HTTP headers".to_string()));
        }
        if let Some(nl) = available.iter().position(|&b| b == b'\n') {
            let take = nl.saturating_add(1);
            *budget = budget.saturating_add(take);
            if *budget > MAX_HEADER_BYTES {
                return Err(HttpReadError::PayloadTooLarge);
            }
            if let Some(prefix) = available.get(..take) {
                bytes.extend_from_slice(prefix);
            }
            reader.consume(take);
            break;
        }
        let len = available.len();
        *budget = budget.saturating_add(len);
        if *budget > MAX_HEADER_BYTES {
            return Err(HttpReadError::PayloadTooLarge);
        }
        bytes.extend_from_slice(available);
        reader.consume(len);
    }
    String::from_utf8(bytes).map(Some).map_err(|_| HttpReadError::BadRequest("HTTP headers must be valid UTF-8".to_string()))
}

async fn read_http_request<R: AsyncRead + Unpin>(stream: &mut R) -> Result<Option<HttpRequest>, HttpReadError> {
    let mut reader = BufReader::new(stream);
    let mut header_budget = 0usize;
    let Some(req_line_raw) = read_bounded_line(&mut reader, &mut header_budget).await? else {
        return Ok(None);
    };
    let req_line = req_line_raw.trim_end_matches(['\r', '\n']);
    let mut parts = req_line.split_whitespace();
    let method = parts.next().ok_or_else(|| HttpReadError::BadRequest("missing HTTP method".into()))?.to_uppercase();
    let raw_target = parts.next().ok_or_else(|| HttpReadError::BadRequest("missing request target".into()))?;
    let version = parts.next().unwrap_or("HTTP/1.1").to_uppercase();
    let path = raw_target.split('?').next().unwrap_or(raw_target).to_string();

    let mut headers = Vec::new();
    loop {
        let Some(line_raw) = read_bounded_line(&mut reader, &mut header_budget).await? else {
            return Err(HttpReadError::BadRequest("incomplete HTTP headers".to_string()));
        };
        let line = line_raw.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if headers.len() >= MAX_HEADER_LINES {
            return Err(HttpReadError::PayloadTooLarge);
        }
        let Some((name, val)) = line.split_once(':') else {
            return Err(HttpReadError::BadRequest("malformed HTTP header".to_string()));
        };
        headers.push((name.trim().to_ascii_lowercase(), val.trim().to_string()));
    }

    let is_chunked = headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v.to_ascii_lowercase().contains("chunked"));
    let body_bytes = if is_chunked {
        read_chunked_body(&mut reader).await?
    } else {
        let content_len = parse_content_length(&headers)?;
        if content_len > MAX_BODY_BYTES {
            return Err(HttpReadError::PayloadTooLarge);
        }
        let mut buf = vec![0u8; content_len];
        reader.read_exact(&mut buf).await.map_err(|e| HttpReadError::BadRequest(format!("incomplete request body: {e}")))?;
        buf
    };
    let body = String::from_utf8(body_bytes).map_err(|_| HttpReadError::BadRequest("request body must be valid UTF-8".to_string()))?;
    Ok(Some(HttpRequest { method, path, version, headers, body }))
}

fn parse_content_length(headers: &[(String, String)]) -> Result<usize, HttpReadError> {
    let Some((_, raw)) = headers.iter().find(|(k, _)| k == "content-length") else {
        return Ok(0);
    };
    raw.parse::<usize>().map_err(|_| HttpReadError::BadRequest("invalid Content-Length".to_string()))
}

async fn read_chunked_body<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> Result<Vec<u8>, HttpReadError> {
    let mut out = Vec::new();
    let mut meta_budget = 0usize;
    loop {
        let Some(size_line) = read_bounded_line(reader, &mut meta_budget).await? else {
            return Err(HttpReadError::BadRequest("unexpected EOF in chunked body".to_string()));
        };
        let hex = size_line.trim_end_matches(['\r', '\n']).split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(hex, 16).map_err(|_| HttpReadError::BadRequest("invalid chunk size".to_string()))?;
        if chunk_len == 0 {
            let _ = read_bounded_line(reader, &mut meta_budget).await?;
            break;
        }
        if out.len().saturating_add(chunk_len) > MAX_BODY_BYTES {
            return Err(HttpReadError::PayloadTooLarge);
        }
        let mut chunk = vec![0u8; chunk_len];
        reader.read_exact(&mut chunk).await.map_err(|e| HttpReadError::BadRequest(format!("incomplete chunk: {e}")))?;
        out.extend_from_slice(&chunk);
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await.map_err(|e| HttpReadError::BadRequest(format!("missing chunk CRLF: {e}")))?;
    }
    Ok(out)
}

async fn write_http_response<W: AsyncWriteExt + Unpin>(stream: &mut W, resp: &HttpResponse, close: bool) -> std::io::Result<()> {
    let conn = if close { "close" } else { "keep-alive" };
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: {conn}\r\nCache-Control: no-store\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: POST, GET, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Accept, Authorization, Mcp-Session-Id, MCP-Protocol-Version\r\nAccess-Control-Expose-Headers: Mcp-Session-Id\r\n",
        resp.status,
        resp.status_text,
        resp.body.len(),
    );
    if !resp.body.is_empty() {
        head.push_str("Content-Type: ");
        head.push_str(resp.content_type);
        head.push_str("\r\n");
    }
    if let Some(sid) = &resp.session_id {
        head.push_str("Mcp-Session-Id: ");
        head.push_str(sid);
        head.push_str("\r\n");
    }
    for (k, v) in &resp.extra_headers {
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    if !resp.body.is_empty() {
        stream.write_all(resp.body.as_bytes()).await?;
    }
    stream.flush().await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::Headless;

    async fn send_raw(addr: std::net::SocketAddr, req: &str) -> String {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[tokio::test]
    async fn streamable_http_preserves_shared_studio_and_enforces_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = HttpConfig::with_token(Some("top-secret".to_string()));
        let mcp = PhotocraftMcp::with_headless(Headless::new());
        let srv = tokio::spawn(async move {
            let _ = serve_listener(listener, cfg, mcp).await;
        });

        // 1. Health endpoint needs no token.
        let health = send_raw(addr, "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await;
        assert!(health.starts_with("HTTP/1.1 200 OK"), "{health}");
        assert!(health.contains("\"photocraft-mcp\""), "{health}");

        // 2. Missing Bearer token is rejected with 401.
        let init_body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;
        let unauth = send_raw(
            addr,
            &format!("POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{init_body}", init_body.len()),
        )
        .await;
        assert!(unauth.starts_with("HTTP/1.1 401 Unauthorized"), "{unauth}");

        // 3. Authorized initialize returns Mcp-Session-Id.
        let init_resp = send_raw(
            addr,
            &format!(
                "POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer top-secret\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{init_body}",
                init_body.len()
            ),
        )
        .await;
        assert!(init_resp.starts_with("HTTP/1.1 200 OK"), "{init_resp}");
        let sid = init_resp
            .lines()
            .find_map(|l| l.strip_prefix("Mcp-Session-Id: "))
            .expect("session header")
            .trim()
            .to_string();

        // 4. Create a 320x240 document in session 1.
        let new_doc = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"new_document","arguments":{"width":320,"height":240}}}"#;
        let create_resp = send_raw(
            addr,
            &format!(
                "POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer top-secret\r\nMcp-Session-Id: {sid}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{new_doc}",
                new_doc.len()
            ),
        )
        .await;
        assert!(create_resp.starts_with("HTTP/1.1 200 OK"), "{create_resp}");

        // 5. Delete session 1, then send a stateless inspect call (no Mcp-Session-Id) and confirm
        //    the 320x240 document is still open in the shared studio.
        let del_resp = send_raw(
            addr,
            &format!("DELETE /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer top-secret\r\nMcp-Session-Id: {sid}\r\nConnection: close\r\n\r\n"),
        )
        .await;
        assert!(del_resp.starts_with("HTTP/1.1 204 No Content"), "{del_resp}");

        let inspect_body = r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"inspect_document","arguments":{}}}"#;
        let inspect_resp = send_raw(
            addr,
            &format!(
                "POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer top-secret\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{inspect_body}",
                inspect_body.len()
            ),
        )
        .await;
        assert!(inspect_resp.starts_with("HTTP/1.1 200 OK"), "{inspect_resp}");
        assert!(inspect_resp.contains("320"), "{inspect_resp}");
        assert!(inspect_resp.contains("240"), "{inspect_resp}");

        srv.abort();
    }
}
