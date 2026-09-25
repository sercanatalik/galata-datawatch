//! Pages saved outside the record, verified before any is taken into it.
//!
//! A rescue is the venue's own response bytes, saved to a directory with a
//! `manifest.json` naming each page's request and its sha256, because the
//! venue was about to stop serving them and nothing in the record was asking
//! yet. This module answers one question: **are these exactly the bytes that
//! were saved?** It reads files and hashes them, and takes nothing: the pages
//! enter the record through the one path, in a capture boot, and only if every
//! one of them verifies. A partial import would be a record holding some of a
//! rescue and no statement of which.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// One verified page: the venue's bytes and what they answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedPage {
    /// The file it came from, for the log.
    pub file: String,
    /// The venue's own symbol the request named, e.g. `BTC` or `xyz:CL`.
    pub symbol: String,
    /// The venue's own label for the bar width, e.g. `1h`.
    pub interval: String,
    /// The response, byte for byte.
    pub bytes: Vec<u8>,
}

/// Why a rescue will not be taken. Every variant names what failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ImportError {
    /// The manifest is missing or unreadable.
    #[error("{path}: the rescue's manifest would not read: {reason}")]
    Manifest {
        /// Where it was looked for.
        path: PathBuf,
        /// Why.
        reason: String,
    },
    /// The rescue is another venue's.
    #[error("the rescue is {found}'s, and this process captures {expected}")]
    Venue {
        /// The manifest's venue.
        found: String,
        /// This process's.
        expected: String,
    },
    /// The manifest names no pages.
    #[error("the rescue's manifest names no pages")]
    Empty,
    /// A page the adapter cannot rebuild into a payload.
    #[error("{file}: a {kind} page is not one an import can take; only candleSnapshot is")]
    Unsupported {
        /// The page.
        file: String,
        /// Its request type.
        kind: String,
    },
    /// A page's file is missing or unreadable.
    #[error("{file}: the page would not read: {reason}")]
    Missing {
        /// The page.
        file: String,
        /// Why.
        reason: String,
    },
    /// A page's bytes are not the bytes that were saved.
    #[error("{file}: its sha256 is {found}, and the manifest recorded {recorded}")]
    Mismatch {
        /// The page.
        file: String,
        /// What the manifest says.
        recorded: String,
        /// What the file hashes to.
        found: String,
    },
}

#[derive(serde::Deserialize)]
struct Manifest {
    venue: String,
    pages: Vec<Page>,
}

#[derive(serde::Deserialize)]
struct Page {
    file: String,
    sha256: String,
    request: Request,
}

#[derive(serde::Deserialize)]
struct Request {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    req: Option<CandleRequest>,
}

#[derive(serde::Deserialize)]
struct CandleRequest {
    coin: String,
    interval: String,
}

/// Every page of a rescue, verified — or a refusal naming the first that is
/// not, with **nothing** returned.
///
/// A file name in the manifest is taken as a name within the directory, never
/// a path: one holding a separator is refused as missing rather than read from
/// wherever it points.
pub fn verified_pages(dir: &Path, venue: &str) -> Result<Vec<ImportedPage>, ImportError> {
    let path = dir.join("manifest.json");
    let text = std::fs::read_to_string(&path).map_err(|e| ImportError::Manifest {
        path: path.clone(),
        reason: e.to_string(),
    })?;
    let manifest: Manifest = serde_json::from_str(&text).map_err(|e| ImportError::Manifest {
        path: path.clone(),
        reason: e.to_string(),
    })?;
    if manifest.venue != venue {
        return Err(ImportError::Venue {
            found: manifest.venue,
            expected: venue.to_string(),
        });
    }
    if manifest.pages.is_empty() {
        return Err(ImportError::Empty);
    }

    let mut out = Vec::with_capacity(manifest.pages.len());
    for page in manifest.pages {
        let candle = match (&page.request.kind[..], page.request.req) {
            ("candleSnapshot", Some(req)) => req,
            (kind, _) => {
                return Err(ImportError::Unsupported {
                    file: page.file,
                    kind: kind.to_string(),
                });
            }
        };
        if page.file.contains('/') || page.file.contains('\\') || page.file.starts_with('.') {
            return Err(ImportError::Missing {
                file: page.file,
                reason: "a page is named within the rescue's directory, never by a path".into(),
            });
        }
        let bytes = std::fs::read(dir.join(&page.file)).map_err(|e| ImportError::Missing {
            file: page.file.clone(),
            reason: e.to_string(),
        })?;
        let found = hex(&Sha256::digest(&bytes));
        if !found.eq_ignore_ascii_case(&page.sha256) {
            return Err(ImportError::Mismatch {
                file: page.file,
                recorded: page.sha256,
                found,
            });
        }
        out.push(ImportedPage {
            file: page.file,
            symbol: candle.coin,
            interval: candle.interval,
            bytes,
        });
    }
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A rescue of two candle pages, as the one-off rescue wrote them.
    fn rescue(pages: &[(&str, &str, &[u8])]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let mut entries = Vec::new();
        for (file, coin, bytes) in pages {
            std::fs::write(dir.path().join(file), bytes).unwrap();
            entries.push(serde_json::json!({
                "file": file,
                "coin": coin,
                "interval": "1h",
                "request": {"type": "candleSnapshot",
                            "req": {"coin": coin, "interval": "1h", "startTime": 0, "endTime": 1}},
                "sha256": hex(&Sha256::digest(bytes)),
            }));
        }
        std::fs::write(
            dir.path().join("manifest.json"),
            serde_json::json!({"venue": "hyperliquid", "pages": entries}).to_string(),
        )
        .unwrap();
        dir
    }

    #[test]
    fn a_rescue_whose_pages_hash_as_recorded_is_returned_whole() {
        let dir = rescue(&[
            ("BTC-1h.json", "BTC", b"[1]"),
            ("xyz_CL-1h.json", "xyz:CL", b"[2]"),
        ]);
        let pages = verified_pages(dir.path(), "hyperliquid").unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[1].symbol, "xyz:CL");
        assert_eq!(pages[1].interval, "1h");
        assert_eq!(pages[1].bytes, b"[2]");
    }

    #[test]
    fn one_bad_page_refuses_the_whole_rescue() {
        let dir = rescue(&[
            ("BTC-1h.json", "BTC", b"[1]"),
            ("ETH-1h.json", "ETH", b"[2]"),
        ]);
        std::fs::write(dir.path().join("ETH-1h.json"), b"[tampered]").unwrap();
        match verified_pages(dir.path(), "hyperliquid") {
            Err(ImportError::Mismatch { file, .. }) => assert_eq!(file, "ETH-1h.json"),
            other => panic!("expected a mismatch naming the page, got {other:?}"),
        }
    }

    #[test]
    fn a_missing_page_is_refused_by_name() {
        let dir = rescue(&[("BTC-1h.json", "BTC", b"[1]")]);
        std::fs::remove_file(dir.path().join("BTC-1h.json")).unwrap();
        match verified_pages(dir.path(), "hyperliquid") {
            Err(ImportError::Missing { file, .. }) => assert_eq!(file, "BTC-1h.json"),
            other => panic!("expected the missing page by name, got {other:?}"),
        }
    }

    #[test]
    fn another_venues_rescue_is_refused() {
        let dir = rescue(&[("BTC-1h.json", "BTC", b"[1]")]);
        assert!(matches!(
            verified_pages(dir.path(), "kraken"),
            Err(ImportError::Venue { .. })
        ));
    }

    #[test]
    fn a_page_named_by_a_path_is_not_read() {
        let dir = rescue(&[("BTC-1h.json", "BTC", b"[1]")]);
        let text = std::fs::read_to_string(dir.path().join("manifest.json"))
            .unwrap()
            .replace("\"BTC-1h.json\"", "\"../../etc/passwd\"");
        std::fs::write(dir.path().join("manifest.json"), text).unwrap();
        assert!(matches!(
            verified_pages(dir.path(), "hyperliquid"),
            Err(ImportError::Missing { .. })
        ));
    }
}
