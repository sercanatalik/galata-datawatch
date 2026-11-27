//! A venue on 127.0.0.1 for this adapter's transport tests: one status and
//! body for every request, and every request kept as it arrived.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The binary installs this once at boot; a test builds a client with no boot
/// behind it. Installing twice is refused and ignored.
pub(super) fn provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Answer every request with one status and body, and keep what was asked:
/// the request line, the headers and the body, as one string each.
pub(super) async fn serve(status: u16, body: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
    serve_routes(vec![("", status, body.to_string())]).await
}

/// Answer each request by its method: the first route whose path, without its
/// leading `/`, the request line names, `""` matching any; 404 where none does.
pub(super) async fn serve_routes(
    routes: Vec<(&'static str, u16, String)>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let (url, asked, _) = serve_live(routes).await;
    (url, asked)
}

/// The routes a live server answers by, which a test may change between
/// requests.
pub(super) type Routes = Arc<Mutex<Vec<(&'static str, u16, String)>>>;

/// As [`serve_routes`], with the routes handed back so a test can change an
/// answer while the server runs.
pub(super) async fn serve_live(
    routes: Vec<(&'static str, u16, String)>,
) -> (String, Arc<Mutex<Vec<String>>>, Routes) {
    provider();
    let routes: Routes = Arc::new(Mutex::new(routes));
    let handed = routes.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let asked = Arc::new(Mutex::new(Vec::new()));
    let record = asked.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut raw = Vec::new();
            let mut buf = [0u8; 4096];
            // The headers, then as much body as `content-length` states.
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => raw.extend_from_slice(&buf[..n]),
                }
                let text = String::from_utf8_lossy(&raw);
                if let Some(end) = text.find("\r\n\r\n") {
                    let length: usize = text
                        .to_lowercase()
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: ").map(str::to_string))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    if raw.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let request = String::from_utf8_lossy(&raw).to_string();
            let line = request.lines().next().unwrap_or_default().to_string();
            record.lock().unwrap().push(request);
            let (status, body) = routes
                .lock()
                .unwrap()
                .iter()
                .find(|(path, _, _)| path.is_empty() || line.contains(&format!(" /{path} ")))
                .map_or((404, String::new()), |(_, s, b)| (*s, b.clone()));
            let reason = if status == 200 { "OK" } else { "Refused" };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (url, asked, handed)
}

/// A header's value in a kept request, by name in any case.
pub(super) fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    let head = request.split("\r\n\r\n").next()?;
    head.lines().skip(1).find_map(|line| {
        let (n, v) = line.split_once(':')?;
        n.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// A kept request's body.
pub(super) fn body(request: &str) -> &str {
    request.split_once("\r\n\r\n").map_or("", |(_, b)| b)
}
