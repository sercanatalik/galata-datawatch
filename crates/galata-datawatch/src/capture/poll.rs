//! The poll loop: capture by **asking on a timer**.
//!
//! ```text
//!   ask        sign, request, take the answer
//!   ingest     THE ONE PATH — archive, normalise, emit
//!   bound      a failure is a gap exactly one cadence wide
//!   pace       the declared interval, or a backoff if we were throttled
//! ```
//!
//! **This is where silence is a gap.** A stream cannot tell a quiet market from
//! a dead socket, because nothing happened either way. A poll can: we asked at
//! a known moment, so its failure is an event *we witnessed*, and the interval
//! is exactly the cadence — see [`crate::venue::poll`].

use std::time::Duration;

use galata_wire::{Envelope, Event, Gap, GapCause, Ticker};

use crate::capture::run::{Capture, CaptureError};
use crate::source::Backoff;
use crate::venue::{Cadence, Polled, Transport};

/// Why a poll did not answer.
///
/// **Two, and they are different facts with different remedies.** Being
/// throttled is ours to fix by asking less often; being unreachable is not ours
/// at all and does not improve by waiting longer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refusal {
    /// The venue said we are asking too often.
    Throttled,
    /// The venue did not answer.
    Unreachable,
}

impl Refusal {
    /// The cause a gap carries.
    pub fn cause(&self) -> GapCause {
        match self {
            Refusal::Throttled => GapCause::Throttled,
            Refusal::Unreachable => GapCause::PollFailed,
        }
    }
}

/// What a run of polls did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Polls {
    /// Polls that answered, **all of which were archived** — including the ones
    /// that returned the same prices as the poll before.
    pub answered: u32,
    /// Polls that did not.
    pub missed: u32,
    /// Gaps published.
    pub gaps: u32,
}

impl Polls {
    /// A line an operator can act on.
    pub fn report(&self) -> String {
        format!(
            "{} answered, {} missed, {} gaps",
            self.answered, self.missed, self.gaps
        )
    }
}

impl Capture {
    /// Poll until the token is cancelled.
    ///
    /// The fetch is a closure, like the walk's — which is not a concession to
    /// testing but what lets a timing rule be asserted at all, without waiting
    /// for real time to pass.
    ///
    /// `about` is the instrument a gap is attributed to; a poll covering many
    /// symbols still has to say *what* was missed, and the venue's answer
    /// covers all of them at once.
    pub async fn run_poll<F, Fut>(
        &mut self,
        shutdown: tokio_util::sync::CancellationToken,
        about: &[Ticker],
        fetch: F,
    ) -> Result<Polls, CaptureError>
    where
        F: Fn(i64) -> Fut,
        Fut: std::future::Future<Output = Result<crate::record::Payload, Refusal>>,
    {
        let interval = match self.venue_transport() {
            Transport::Poll {
                interval_micros, ..
            } => interval_micros,
            other => {
                return Err(CaptureError::NotAPoll {
                    endpoint: other.endpoint().to_string(),
                });
            }
        };

        let venue = self.venue().clone();
        let mut cadence = Cadence::new(interval);
        let mut backoff = Backoff::default();
        let mut polls = Polls::default();

        while !shutdown.is_cancelled() {
            let now = self.now();
            let mut wait = Duration::from_micros(interval.max(1) as u64);

            match fetch(now).await {
                Ok(payload) => {
                    // **Archived whether or not it changed.** The record
                    // records arrivals, and *we asked and the venue answered*
                    // is an arrival. Collapsing identical states is a
                    // projection's job, where it is reversible — doing it here
                    // would destroy the difference between *the price did not
                    // move* and *we did not ask*.
                    self.take(payload)?;
                    cadence.answered(now);
                    polls.answered += 1;
                    backoff.reset();
                }
                Err(refusal) => {
                    polls.missed += 1;
                    if let Polled::Missed {
                        from_micros,
                        to_micros,
                        cause,
                    } = cadence.missed(now, refusal.cause())
                    {
                        for ticker in about {
                            self.publish_poll_gap(&venue, ticker, from_micros, to_micros, cause);
                            polls.gaps += 1;
                        }
                    }
                    // **Throttled backs off; unreachable does not.** Asking too
                    // often is ours to fix and gets worse if we keep the rate;
                    // a venue that is down does not improve by waiting longer.
                    if refusal == Refusal::Throttled {
                        wait = backoff.next_wait();
                    }
                }
            }

            let now = self.now();
            self.flush_if_due(now)?;
            self.publish_status_if_due(now);

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(wait) => {}
            }
        }

        self.shutdown()?;
        Ok(polls)
    }

    /// Record the gap, then emit it.
    fn publish_poll_gap(
        &mut self,
        venue: &galata_wire::Venue,
        ticker: &Ticker,
        from_micros: i64,
        to_micros: i64,
        cause: GapCause,
    ) {
        let envelope = Envelope::new(
            venue.clone(),
            ticker.clone(),
            Some(from_micros),
            to_micros,
            Event::Gap(Gap {
                series: galata_wire::Series::Quotes,
                from_micros,
                to_micros,
                cause,
                // This venue trades continuously; a venue with a calendar
                // declares one, and an unknown overstates the loss rather than
                // erasing it.
                clipped: galata_wire::Clipped::Continuous,
            }),
        );
        if let Err(error) = self.record_generated_event(venue.as_str(), envelope) {
            tracing::warn!(%error, "a poll gap could not be recorded; it is still a fact");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::rh_crypto;
    use crate::capture::{TestClock, Wiring};
    use crate::normalise::{Normalise, NormaliseError};
    use crate::record::{Payload, PayloadAddress};
    use crate::sink::testing::RecordingSink;
    use crate::venue::{Adapter, Budget, ConnectionPolicy, Declaration, Paging};
    use galata_wire::{Origin, Series, Venue};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    const SECOND: i64 = 1_000_000;
    const AT: i64 = 1_789_941_180_000_000;

    /// A polled venue, reduced to what the loop needs.
    struct Polled {
        venue: Venue,
        declaration: Declaration,
        tickers: BTreeMap<String, Ticker>,
    }

    impl Polled {
        fn new() -> Polled {
            Polled {
                venue: Venue::new(rh_crypto::VENUE).unwrap(),
                declaration: Declaration {
                    streams: Vec::new(),
                    historical: Vec::new(),
                    paging: BTreeMap::from([(Series::Quotes, Paging::forward_from_start(1))]),
                    budget: Budget {
                        requests_per_minute: 12.0,
                        min_historical_interval_ms: 5_000,
                    },
                    connection: ConnectionPolicy::KeepAliveOnly { keepalive_secs: 0 },
                    ws_url: "",
                    rest_url: rh_crypto::REST_URL,
                },
                tickers: BTreeMap::from([("BTC-USD".into(), Ticker::new("BTC").unwrap())]),
            }
        }
    }

    impl Normalise for Polled {
        fn venue(&self) -> &Venue {
            &self.venue
        }
        fn normalise(&self, payload: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
            let parsed = rh_crypto::wire::response(&payload.payload)?;
            Ok(rh_crypto::wire::read(
                &self.venue,
                &parsed,
                &self.tickers,
                payload.recv_micros,
            ))
        }
    }

    impl Adapter for Polled {
        fn declaration(&self) -> &Declaration {
            &self.declaration
        }
        fn transport(&self) -> Transport {
            Transport::Poll {
                rest_url: rh_crypto::REST_URL,
                path: rh_crypto::BEST_BID_ASK_PATH,
                interval_micros: 5 * SECOND,
            }
        }
        fn series_of_channel(&self, channel: &str) -> Option<Series> {
            (channel == "best_bid_ask").then_some(Series::Quotes)
        }
        fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload {
            Payload {
                seq: 0,
                recv_micros,
                address: PayloadAddress::Venue(rh_crypto::VENUE.into()),
                channel: "best_bid_ask".into(),
                kind: Series::Quotes.as_str().to_string(),
                symbol: None,
                origin: Origin::Fetched,
                payload: bytes.to_vec(),
            }
        }
        fn venue_symbol(&self, _ticker: &Ticker) -> Option<String> {
            Some("BTC-USD".into())
        }
        fn interval_label(&self, _micros: i64) -> Option<String> {
            None
        }
        fn venue_ticker(&self, _channel: &str, symbol: &str) -> Option<Ticker> {
            self.tickers.get(symbol).cloned()
        }
    }

    struct Fixture {
        capture: Capture,
        clock: Arc<TestClock>,
        sink: Arc<RecordingSink>,
        _root: tempfile::TempDir,
    }

    fn fixture() -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let clock = Arc::new(TestClock::at(AT));
        let sink = Arc::new(RecordingSink::default());
        let capture = Capture::new(Wiring {
            adapter: Box::new(Polled::new()),
            sink: sink.clone(),
            clock: clock.clone(),
            archive_root: root.path().join("archive"),
            status_dir: root.path().join("status"),
            flush_secs: 1,
            status_secs: 1,
            declared: Vec::new(),
            clipped: galata_wire::Clipped::Continuous,
            config_hash: "test".into(),
        });
        Fixture {
            capture,
            clock,
            sink,
            _root: root,
        }
    }

    /// The same two prices, every time.
    fn unchanged() -> Vec<u8> {
        br#"{"results":[{"symbol":"BTC-USD",
             "bid_inclusive_of_sell_spread":"81190.50",
             "ask_inclusive_of_buy_spread":"81235.50"}]}"#
            .to_vec()
    }

    /// Stop after `n` polls, so a loop under a test clock terminates.
    fn after(
        n: usize,
    ) -> (
        tokio_util::sync::CancellationToken,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        (
            tokio_util::sync::CancellationToken::new(),
            Arc::new(std::sync::atomic::AtomicUsize::new(n)),
        )
    }

    // `start_paused` auto-advances tokio's timer, so a five-second cadence
    // costs no wall-clock time. Without it these tests took fifteen seconds
    // between them, which is a test suite that people start skipping.
    #[tokio::test(start_paused = true)]
    async fn an_unchanged_answer_is_still_archived() {
        // The record records ARRIVALS. Collapsing identical states here would
        // destroy the difference between "the price did not move" and "we did
        // not ask" — which is the difference the bounded gap exists to keep.
        let mut f = fixture();
        let (shutdown, left) = after(3);
        let stop = shutdown.clone();
        let adapter = Polled::new();

        let polls = f
            .capture
            .run_poll(shutdown, &[Ticker::new("BTC").unwrap()], |at| {
                let payload = adapter.classify(&unchanged(), at);
                if left.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) <= 1 {
                    stop.cancel();
                }
                async move { Ok(payload) }
            })
            .await
            .unwrap();

        assert_eq!(polls.answered, 3, "an identical answer was skipped");
        assert_eq!(polls.gaps, 0);
        // All three reached the sink as quotes.
        assert_eq!(f.sink.emitted().len(), 3);
    }

    // `start_paused` auto-advances tokio's timer, so a five-second cadence
    // costs no wall-clock time. Without it these tests took fifteen seconds
    // between them, which is a test suite that people start skipping.
    #[tokio::test(start_paused = true)]
    async fn consecutive_failures_are_one_widening_gap() {
        // Not one gap per failure. Three claims where there is one fact would
        // make a consumer count an outage three times.
        let mut f = fixture();
        let clock = f.clock.clone();
        let (shutdown, left) = after(4);
        let stop = shutdown.clone();
        let adapter = Polled::new();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        f.capture
            .run_poll(shutdown, &[Ticker::new("BTC").unwrap()], |at| {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let payload = adapter.classify(&unchanged(), at);
                clock.advance(5 * SECOND);
                if left.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) <= 1 {
                    stop.cancel();
                }
                async move {
                    if n == 0 {
                        Ok(payload)
                    } else {
                        Err(Refusal::Unreachable)
                    }
                }
            })
            .await
            .unwrap();

        let gaps: Vec<&Envelope> = f
            .sink
            .emitted()
            .iter()
            .filter(|e| matches!(e.event, Event::Gap(_)))
            .cloned()
            .collect::<Vec<_>>()
            .leak()
            .iter()
            .collect();
        assert!(!gaps.is_empty(), "no gap was published");

        // Each successive gap starts at the same place and ends later — one
        // fact widening, rather than three separate ones.
        let spans: Vec<(i64, i64)> = gaps
            .iter()
            .filter_map(|e| match &e.event {
                Event::Gap(g) => Some((g.from_micros, g.to_micros)),
                _ => None,
            })
            .collect();
        assert!(
            spans
                .windows(2)
                .all(|w| w[0].0 == w[1].0 && w[1].1 > w[0].1),
            "gaps did not widen from one origin: {spans:?}"
        );
    }

    // `start_paused` auto-advances tokio's timer, so a five-second cadence
    // costs no wall-clock time. Without it these tests took fifteen seconds
    // between them, which is a test suite that people start skipping.
    #[tokio::test(start_paused = true)]
    async fn a_gap_is_durable_before_it_is_emitted() {
        // A gap that reached the sink and not the disk is exactly the evidence
        // an outage erases.
        let mut f = fixture();
        let clock = f.clock.clone();
        let (shutdown, left) = after(2);
        let stop = shutdown.clone();
        let adapter = Polled::new();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        f.capture
            .run_poll(shutdown, &[Ticker::new("BTC").unwrap()], |at| {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let payload = adapter.classify(&unchanged(), at);
                clock.advance(5 * SECOND);
                if left.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) <= 1 {
                    stop.cancel();
                }
                async move {
                    if n == 0 {
                        Ok(payload)
                    } else {
                        Err(Refusal::Unreachable)
                    }
                }
            })
            .await
            .unwrap();

        // The gap is in the record, not only on the wire.
        let gaps = f._root.path().join("archive/venue=rh-crypto/kind=gaps");
        assert!(gaps.is_dir(), "the gap never reached the record");
    }

    #[test]
    fn throttling_and_unreachability_carry_different_causes() {
        // One is ours to fix by asking less often; the other is not ours at
        // all. A consumer must be able to tell them apart.
        assert_eq!(Refusal::Throttled.cause(), GapCause::Throttled);
        assert_eq!(Refusal::Unreachable.cause(), GapCause::PollFailed);
        assert_ne!(Refusal::Throttled.cause(), Refusal::Unreachable.cause());
    }

    // `start_paused` auto-advances tokio's timer, so a five-second cadence
    // costs no wall-clock time. Without it these tests took fifteen seconds
    // between them, which is a test suite that people start skipping.
    #[tokio::test(start_paused = true)]
    async fn a_streaming_venue_is_refused_by_name() {
        let root = tempfile::tempdir().unwrap();
        let mut capture = Capture::new(Wiring {
            adapter: crate::adapters::build(crate::adapters::AdapterConfig::Hyperliquid(
                crate::adapters::hyperliquid::Config {
                    market: crate::adapters::hyperliquid::Market::Mainnet,
                    instruments: vec![crate::adapters::hyperliquid::Instrument::main("BTC")],
                    candle_interval: "1m".into(),
                },
            ))
            .unwrap(),
            sink: Arc::new(RecordingSink::default()),
            clock: Arc::new(TestClock::at(AT)),
            archive_root: root.path().join("archive"),
            status_dir: root.path().join("status"),
            flush_secs: 1,
            status_secs: 1,
            declared: Vec::new(),
            clipped: galata_wire::Clipped::Continuous,
            config_hash: "test".into(),
        });
        let error = capture
            .run_poll(tokio_util::sync::CancellationToken::new(), &[], |_| async {
                Err(Refusal::Unreachable)
            })
            .await
            .unwrap_err();
        assert!(matches!(error, CaptureError::NotAPoll { .. }), "{error}");
    }
}
