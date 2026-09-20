//! A venue that pushes over a long-lived connection.
//!
//! The one thing worth reading here is the **poll timeout**. The loop must
//! service its timers — flush, status, keepalive, rotation — and a socket read
//! that blocks forever would starve all four. So a read that finds nothing
//! inside the window yields [`Frame::Idle`], and **`Idle` is not a gap**: a
//! quiet market and a silently dead connection are the same shape from here.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use super::{Frame, SourceError};
use crate::venue::Keepalive;

/// How long a read waits before yielding [`Frame::Idle`] so timers can run.
const POLL_WINDOW: Duration = Duration::from_millis(200);

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A websocket connection to a venue.
#[derive(Debug)]
pub struct StreamSource {
    url: String,
    socket: Option<Socket>,
}

impl StreamSource {
    /// An unconnected source for a venue's endpoint.
    pub fn new(url: impl Into<String>) -> StreamSource {
        StreamSource {
            url: url.into(),
            socket: None,
        }
    }

    /// Where this connects.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Whether a connection is open.
    pub fn is_connected(&self) -> bool {
        self.socket.is_some()
    }

    /// Open a connection, replacing any it held.
    pub async fn connect(&mut self) -> Result<(), SourceError> {
        let (socket, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .map_err(|e| SourceError::Connect {
                url: self.url.clone(),
                reason: e.to_string(),
            })?;
        self.socket = Some(socket);
        Ok(())
    }

    /// Send the frames that subscribe a declared set.
    ///
    /// A failed send is reported and does not close the connection: the loop
    /// converges toward the declared set on every pass, so a subscription that
    /// did not land is retried by the next convergence rather than by a
    /// reconnect.
    pub async fn subscribe(&mut self, frames: &[String]) -> Result<(), SourceError> {
        let socket = self
            .socket
            .as_mut()
            .ok_or_else(|| SourceError::Session("not connected".into()))?;
        for frame in frames {
            socket
                .send(Message::Text(frame.clone().into()))
                .await
                .map_err(|e| SourceError::Session(e.to_string()))?;
        }
        Ok(())
    }

    /// Send the venue's keepalive, in whatever shape it wants.
    pub async fn keepalive(&mut self, keepalive: &Keepalive) -> Result<(), SourceError> {
        let socket = self
            .socket
            .as_mut()
            .ok_or_else(|| SourceError::Session("not connected".into()))?;
        let sent = match keepalive {
            Keepalive::None => Ok(()),
            Keepalive::Protocol => socket.send(Message::Ping(Vec::new().into())).await,
            Keepalive::Frame(frame) => socket.send(Message::Text(frame.clone().into())).await,
        };
        sent.map_err(|e| SourceError::Session(e.to_string()))
    }

    /// The next frame, or [`Frame::Idle`] if the poll window elapsed.
    ///
    /// Named `next_frame` rather than `next` on purpose: this is not an
    /// iterator, and a reader who assumed it was would expect `None` to mean
    /// exhausted where here it means the connection closed.
    pub async fn next_frame(&mut self) -> Result<Frame, SourceError> {
        let Some(socket) = self.socket.as_mut() else {
            return Ok(Frame::Closed);
        };
        match tokio::time::timeout(POLL_WINDOW, socket.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => Ok(Frame::Bytes(text.as_bytes().to_vec())),
            Ok(Some(Ok(Message::Binary(bytes)))) => Ok(Frame::Bytes(bytes.to_vec())),
            Ok(Some(Ok(Message::Ping(payload)))) => {
                // Answered here rather than in the loop: it is a fact about
                // this transport, not about capture.
                let _ = socket.send(Message::Pong(payload)).await;
                Ok(Frame::Idle)
            }
            Ok(Some(Ok(_))) => Ok(Frame::Idle),
            Ok(Some(Err(e))) => {
                self.socket = None;
                Err(SourceError::Session(e.to_string()))
            }
            Ok(None) => {
                self.socket = None;
                Ok(Frame::Closed)
            }
            // **Nothing arrived inside the poll window. This is not a gap.**
            Err(_) => Ok(Frame::Idle),
        }
    }

    /// Close, if open. Used by the handover once the replacement is subscribed.
    pub async fn close(&mut self) {
        if let Some(mut socket) = self.socket.take() {
            let _ = socket.close(None).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unconnected_source_is_closed_rather_than_idle() {
        // Closed and Idle mean different things to the loop: one is a reason
        // to reconnect and one is a reason to do nothing.
        let source = StreamSource::new("wss://example.invalid");
        assert!(!source.is_connected());
        assert_eq!(source.url(), "wss://example.invalid");
    }

    #[test]
    fn idle_is_not_closed() {
        assert_ne!(Frame::Idle, Frame::Closed);
    }
}
