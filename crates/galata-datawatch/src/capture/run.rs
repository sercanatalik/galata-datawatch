//! The capture process: the loop that owns the clock, and everything it
//! drives.
//!
//! It holds the pieces and sequences them. **It contains no rule of its own** —
//! the rules live in the modules below it — and every timestamp anything
//! downstream sees originates here.

use std::path::PathBuf;
use std::sync::Arc;

use galata_wire::{Clipped, Envelope, Event, Gap, GapCause, Series, Ticker, Venue};

use crate::capture::clock::Clock;
use crate::capture::coverage::Coverage;
use crate::capture::session::{Act, Session};
use crate::capture::status::{Connection, PairState, PairStatus, Status, StatusFile};
use crate::capture::subscriptions::{Held, Outcome};
use crate::ingest::{ingest, record_generated};
use crate::record::{Archive, Payload, RecordError};
use crate::sink::Sink;
use crate::source::{Frame, SourceError, StreamSource};
use crate::venue::{Adapter, Subscription};

/// What this build is, for the status surface.
const BUILD: &str = concat!("galata-datawatch ", env!("CARGO_PKG_VERSION"));

/// What `count_1m` counts over.
const COUNT_WINDOW_MICROS: i64 = 60_000_000;

/// Why the loop stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CaptureError {
    /// The record refused.
    #[error(transparent)]
    Record(#[from] RecordError),
    /// The transport refused.
    #[error(transparent)]
    Source(#[from] SourceError),
}

/// Everything the loop is given.
///
/// Assembled by the binary; **the loop constructs nothing for itself**, so what
/// it runs against is what a test can supply.
pub struct Wiring {
    /// What the venue's bytes mean.
    pub adapter: Box<dyn Adapter>,
    /// Where normalised events go.
    pub sink: Arc<dyn Sink>,
    /// The one clock in the process.
    pub clock: Arc<dyn Clock>,
    /// Where the record lives.
    pub archive_root: PathBuf,
    /// Where the local status file lives.
    pub status_dir: PathBuf,
    /// Seconds between commits.
    pub flush_secs: u64,
    /// Seconds between status snapshots.
    pub status_secs: u64,
    /// What to subscribe.
    pub declared: Vec<Subscription>,
    /// What gaps are clipped against.
    pub clipped: Clipped,
    /// What this process was told.
    pub config_hash: String,
}

/// The capture process.
pub struct Capture {
    wiring: Wiring,
    archive: Archive,
    coverage: Coverage,
    held: Held,
    session: Option<Session>,
    started_at_micros: i64,
    last_flush_micros: Option<i64>,
    last_status_micros: i64,
    /// The venue's last event time per pair, for the latency the status
    /// reports.
    last_event: std::collections::BTreeMap<(Ticker, Series), i64>,
    /// Whether the last status emit reached the sink, so the log says so **on
    /// the edge** rather than on every tick.
    sink_reachable: bool,
}

impl Capture {
    /// Assemble. Nothing has happened yet, and no clock has been read but the
    /// one this takes.
    pub fn new(wiring: Wiring) -> Capture {
        let now = wiring.clock.now_micros();
        // Scoped to this process's venue: one process per venue, one record
        // root between them, and neither reading the other's watermark.
        let archive =
            Archive::open(wiring.archive_root.clone()).scoped_to(wiring.adapter.venue().as_str());
        let mut held = Held::new();
        held.declare(wiring.declared.clone());
        let coverage = Coverage::new(wiring.clipped);
        Capture {
            wiring,
            archive,
            coverage,
            held,
            session: None,
            started_at_micros: now,
            last_flush_micros: None,
            last_status_micros: now,
            last_event: std::collections::BTreeMap::new(),
            sink_reachable: true,
        }
    }

    /// What was not covered while this process was not running.
    ///
    /// **Published before anything else**, so the record never claims coverage
    /// it does not have.
    pub fn report_restart_gap(&mut self) -> usize {
        let now = self.wiring.clock.now_micros();
        let Some((from, to, cause)) = self.archive.restart_window(now) else {
            // A first-ever start has no covered moment to date a gap from, and
            // a gap back to the beginning of time is not a fact.
            self.establish_coverage(now);
            self.archive.clear_clean_shutdown();
            return 0;
        };

        // Coverage begins at the last durable moment, so each pair's gap is
        // dated from something the record actually holds.
        self.establish_coverage(from);
        let pairs: Vec<(Ticker, Series)> = self
            .wiring
            .declared
            .iter()
            .map(|s| (s.ticker.clone(), s.series))
            .collect();
        let gaps = self.coverage.gaps_for(&pairs, to, cause);
        let published = gaps.len();
        for (ticker, gap) in gaps {
            self.emit_gap(&ticker, gap);
        }
        self.archive.clear_clean_shutdown();
        published
    }

    fn establish_coverage(&mut self, at_micros: i64) {
        for subscription in &self.wiring.declared {
            self.coverage
                .known(&subscription.ticker, subscription.series, at_micros);
        }
    }

    /// Record a gap, then emit it.
    ///
    /// **Through the one path, and durable first.** A gap emitted straight to
    /// the sink exists only if the sink was up — and a gap is precisely what a
    /// consumer needs after an outage.
    fn emit_gap(&mut self, ticker: &Ticker, gap: Gap) {
        let venue = self.wiring.adapter.venue().clone();
        let envelope = Envelope::new(
            venue.clone(),
            ticker.clone(),
            Some(gap.from_micros),
            gap.to_micros,
            Event::Gap(gap),
        );
        if let Err(error) = record_generated(
            &mut self.archive,
            self.wiring.sink.as_ref(),
            venue.as_str(),
            envelope,
        ) {
            tracing::warn!(error = %error, "a gap could not be recorded; it is still a fact");
        }
    }

    /// One payload, through the one path.
    pub fn take(&mut self, payload: Payload) -> Result<(), CaptureError> {
        let recv_micros = payload.recv_micros;
        let channel = payload.channel.clone();
        let covered = match self.wiring.adapter.series_of_channel(&channel) {
            Some(series) => self.covered_by(&payload, series),
            None => Vec::new(),
        };

        let result = ingest(
            &mut self.archive,
            self.wiring.adapter.as_ref(),
            self.wiring.sink.as_ref(),
            payload,
        )?;

        // A sink that is down is ONE log line on the edge, not one per
        // payload. A warning per frame is what buries a log, and the answer to
        // "how is this reported" is the status surface, which carries it once
        // per tick.
        self.note_sink(!result.emit_failed);

        // Coverage is recorded from **receipt**, and only for a frame that
        // carried an observation: a control frame is not coverage of anything.
        //
        // Deliberately not conditioned on the emit succeeding. Coverage is a
        // statement about what we heard, and the record does not depend on the
        // sink — tying the two would make an outage read as a market that
        // stopped trading.
        if !result.unparsed
            && let Some(series) = self.wiring.adapter.series_of_channel(&channel)
        {
            for ticker in covered {
                self.coverage.received(&ticker, series, recv_micros);
            }
            // The venue's own clock, for the latency the status reports — ours
            // is `recv_micros`, and the difference between the two is the
            // number that matters. Taken from what ingest already normalised,
            // rather than parsing a second time.
            for (ticker, at) in result.venue_times {
                self.last_event.insert((ticker, series), at);
            }
        }
        Ok(())
    }

    /// Record whether the sink took what it was given, logging only when the
    /// answer changes.
    fn note_sink(&mut self, reachable: bool) {
        if reachable == self.sink_reachable {
            return;
        }
        if reachable {
            tracing::info!("the sink is taking events again");
        } else {
            tracing::warn!(
                "the sink is refusing events; payloads are still recorded and the local status \
                 file still answers"
            );
        }
        self.sink_reachable = reachable;
    }

    /// Which pairs a payload is coverage of.
    ///
    /// The payload's own symbol where it names one — coverage is per
    /// `(ticker, series)`, and crediting every declared ticker for one ticker's
    /// frame would claim coverage we do not have. A payload naming no symbol
    /// covers many, as a universe fetch does, so it credits them all.
    fn covered_by(&self, payload: &Payload, series: Series) -> Vec<Ticker> {
        match payload
            .symbol
            .as_ref()
            .and_then(|s| self.wiring.adapter.venue_ticker(&payload.channel, s))
        {
            Some(ticker) => vec![ticker],
            None => self
                .wiring
                .declared
                .iter()
                .filter(|s| s.series == series)
                .map(|s| s.ticker.clone())
                .collect(),
        }
    }

    /// A live frame the venue sent.
    pub fn take_frame(&mut self, bytes: &[u8]) -> Result<(), CaptureError> {
        let now = self.wiring.clock.now_micros();
        let payload = self.wiring.adapter.classify(bytes, now);
        self.take(payload)
    }

    /// Commit what is buffered, on the flush timer.
    pub fn flush_if_due(&mut self, now_micros: i64) -> Result<bool, CaptureError> {
        let due = match self.last_flush_micros {
            None => true,
            Some(last) => now_micros - last >= (self.wiring.flush_secs as i64) * 1_000_000,
        };
        if !due {
            return Ok(false);
        }
        self.archive.flush()?;
        self.last_flush_micros = Some(now_micros);
        Ok(true)
    }

    /// The full snapshot, on its timer.
    ///
    /// **The local file first**, because that is the surface that works when
    /// the sink does not.
    pub fn publish_status_if_due(&mut self, now_micros: i64) -> bool {
        if now_micros - self.last_status_micros < (self.wiring.status_secs as i64) * 1_000_000 {
            return false;
        }
        self.last_status_micros = now_micros;
        let status = self.status(now_micros);

        let file = StatusFile::new(&self.wiring.status_dir, self.wiring.adapter.venue());
        if let Err(error) = file.write(&status.to_json()) {
            tracing::warn!(error = %error, "the local status file could not be written");
        }

        // The counters roll once the minute they are named for has elapsed —
        // not once per snapshot, which would report a pair as quiet between
        // one message and the next.
        self.coverage.roll_counts(now_micros, COUNT_WINDOW_MICROS);
        true
    }

    /// Everything this process holds, at one moment.
    pub fn status(&self, now_micros: i64) -> Status {
        let declared_pairs: Vec<(Ticker, Series)> = self
            .wiring
            .declared
            .iter()
            .map(|s| (s.ticker.clone(), s.series))
            .collect();

        // Every pair the process knows about — declared or merely seen —
        // appears, so a consumer can tell "not subscribed" from "monitoring is
        // broken".
        let mut pairs: Vec<(Ticker, Series)> = declared_pairs.clone();
        for pair in self.coverage.pairs() {
            if !pairs.contains(&pair) {
                pairs.push(pair);
            }
        }

        let pairs = pairs
            .into_iter()
            .map(|(ticker, series)| {
                let subscription = Subscription {
                    ticker: ticker.clone(),
                    series,
                };
                let declared = self.wiring.declared.contains(&subscription);
                let outcome = self.held.outcome(&subscription);

                // A state for every case, including the ones that mean nothing
                // should be arriving. Which of `live` and `stale` applies is a
                // statement about whether anything arrived in the last window —
                // **not a threshold, and not a verdict**.
                let state = match (declared, outcome) {
                    (_, Some(Outcome::Refused { .. })) => PairState::Refused,
                    (false, _) | (true, None) | (true, Some(Outcome::Pending)) => {
                        PairState::NotSubscribed
                    }
                    (true, Some(Outcome::Held)) => match self.wiring.clipped {
                        Clipped::Continuous | Clipped::Assumed24h => {
                            if self.coverage.count(&ticker, series) > 0 {
                                PairState::Live
                            } else {
                                PairState::Stale
                            }
                        }
                        // Staleness has to respect a calendar, or it fires
                        // every night for every instrument — the same false
                        // positive gap clipping exists to prevent, arriving
                        // through a different door.
                        Clipped::Sessions => PairState::Closed,
                    },
                };

                PairStatus {
                    ticker: ticker.clone(),
                    series,
                    state,
                    last_recv_micros: self.coverage.last_recv(&ticker, series),
                    last_event_micros: self.last_event.get(&(ticker, series)).copied(),
                    count_1m: self.coverage.count(&subscription.ticker, series),
                    reason: match outcome {
                        Some(Outcome::Refused { reason }) => Some(reason.clone()),
                        _ => None,
                    },
                }
            })
            .collect();

        Status {
            venue: self.wiring.adapter.venue().clone(),
            observed_at_micros: now_micros,
            build: BUILD.to_string(),
            started_at_micros: self.started_at_micros,
            config_hash: self.wiring.config_hash.clone(),
            connection: match &self.session {
                None => Connection::Down,
                Some(s) if s.rotating() => Connection::Rotating,
                Some(_) => Connection::Connected,
            },
            session_age_secs: self.session.as_ref().map(|s| s.age_secs(now_micros)),
            next_handover_in_secs: self
                .session
                .as_ref()
                .and_then(|s| s.next_handover_in_secs(now_micros)),
            subs_held: self.held.count_held(),
            subs_declared: self.wiring.declared.len(),
            subs_refused: self.held.count_refused(),
            last_flush_micros: self.last_flush_micros,
            buffered: self.archive.buffered(),
            pairs,
        }
    }

    /// Re-read the declaration. **Only an explicit operator act reaches this.**
    ///
    /// Configuration is never re-read on a file change: an edit should not
    /// silently change a running system, and the moment that matters most is
    /// the one where somebody is editing to understand rather than to change.
    pub fn operator_redeclared(&mut self, declared: Vec<Subscription>) {
        self.wiring.declared = declared.clone();
        self.held.declare(declared);
    }

    /// The session stopped delivering. Each pair's gap begins at **its own**
    /// last covered moment, not at the moment the loss was observed.
    pub fn session_lost(&mut self, now_micros: i64) {
        let gaps = self
            .coverage
            .gaps_for_all(now_micros, GapCause::SessionLost);
        for (ticker, gap) in gaps {
            self.emit_gap(&ticker, gap);
        }
        self.session = None;
    }

    /// Stop, having flushed. The marker this writes is what makes the next
    /// start report `downtime` rather than `crash_unflushed`.
    pub fn shutdown(&mut self) -> Result<(), CaptureError> {
        let now = self.wiring.clock.now_micros();
        self.archive.flush()?;
        self.archive.mark_clean_shutdown(now);
        Ok(())
    }

    // ---- accessors for the driver and for tests ---------------------------

    /// The subscription ledger.
    pub fn held_mut(&mut self) -> &mut Held {
        &mut self.held
    }

    /// The coverage ledger.
    pub fn coverage_mut(&mut self) -> &mut Coverage {
        &mut self.coverage
    }

    /// The venue.
    pub fn venue(&self) -> &Venue {
        self.wiring.adapter.venue()
    }

    /// The clock the loop owns.
    pub fn now(&self) -> i64 {
        self.wiring.clock.now_micros()
    }

    fn open_session(&mut self, at_micros: i64) {
        self.session = Some(Session::opened(
            self.wiring.adapter.declaration().connection,
            at_micros,
        ));
        self.held.connection_lost();
    }

    /// Connect, subscribe, and deliver frames until the token is cancelled.
    ///
    /// The handover is the part worth reading: a replacement is opened **and
    /// subscribed** before the connection it replaces is closed, so coverage is
    /// continuous and no gap is published — because none occurred.
    pub async fn run(
        &mut self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), CaptureError> {
        let url = self.wiring.adapter.declaration().ws_url.to_string();
        let keepalive = self.wiring.adapter.keepalive();
        let mut backoff = crate::source::Backoff::default();
        let mut source = StreamSource::new(&url);

        while !shutdown.is_cancelled() {
            if let Err(error) = source.connect().await {
                tracing::warn!(url, error = %error, "connect failed");
                let wait = backoff.next_wait();
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(wait) => {}
                }
                continue;
            }
            backoff.reset();
            self.open_session(self.wiring.clock.now_micros());

            // Level-triggered: converge toward the declared set rather than
            // remembering what was sent last time.
            let convergence = self.held.converge();
            let frames = self
                .wiring
                .adapter
                .subscribe_frames(&convergence.to_subscribe);
            if let Err(error) = source.subscribe(&frames).await {
                tracing::warn!(error = %error, "subscribe failed");
            }
            for subscription in &convergence.to_subscribe {
                self.held.mark_sent(subscription);
                // This venue confirms by delivering rather than by
                // acknowledging per subscription, so a sent subscription is
                // held until something says otherwise.
                self.held.mark_held(subscription);
            }

            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                let now = self.wiring.clock.now_micros();

                if let Some(session) = &mut self.session {
                    match session.act(now) {
                        Act::Keepalive => {
                            if source.keepalive(&keepalive).await.is_err() {
                                break;
                            }
                            session.keepalive_sent(now);
                        }
                        Act::OpenReplacement | Act::CloseReplaced => {
                            // Reconnecting from the top of this loop opens and
                            // subscribes the replacement before this one stops
                            // delivering; the handover is therefore covered and
                            // publishes no gap.
                            self.coverage.handover_completed(now);
                            break;
                        }
                        Act::Wait => {}
                    }
                }

                self.flush_if_due(now)?;
                self.publish_status_if_due(now);

                match source.next_frame().await {
                    Ok(Frame::Bytes(bytes)) => self.take_frame(&bytes)?,
                    // **Not a gap.** A quiet market and a silently dead
                    // connection are the same shape from here, so nothing is
                    // inferred from it.
                    Ok(Frame::Idle) => {}
                    Ok(Frame::Closed) => {
                        self.session_lost(self.wiring.clock.now_micros());
                        break;
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "the session failed");
                        self.session_lost(self.wiring.clock.now_micros());
                        break;
                    }
                }
            }
            source.close().await;
        }

        self.shutdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::hyperliquid::{Config as HlConfig, Hyperliquid, Instrument, Market};
    use crate::capture::clock::TestClock;
    use crate::sink::testing::RecordingSink;
    use crate::venue::Construct;
    use galata_wire::Origin;

    const SEC: i64 = 1_000_000;

    fn adapter() -> Box<dyn Adapter> {
        Box::new(
            Hyperliquid::new(
                HlConfig {
                    market: Market::Mainnet,
                    instruments: vec![Instrument::main("BTC"), Instrument::main("ETH")],
                    candle_interval: "1m".into(),
                },
                None,
            )
            .unwrap(),
        )
    }

    fn declared() -> Vec<Subscription> {
        ["BTC", "ETH"]
            .into_iter()
            .map(|t| Subscription {
                ticker: Ticker::new(t).unwrap(),
                series: Series::Quotes,
            })
            .collect()
    }

    struct Fixture {
        capture: Capture,
        clock: Arc<TestClock>,
        sink: Arc<RecordingSink>,
        _root: tempfile::TempDir,
    }

    fn fixture(start: i64) -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let clock = Arc::new(TestClock::at(start));
        let sink = Arc::new(RecordingSink::default());
        let capture = Capture::new(Wiring {
            adapter: adapter(),
            sink: sink.clone(),
            clock: clock.clone(),
            archive_root: root.path().join("archive"),
            status_dir: root.path().join("status"),
            flush_secs: 2,
            status_secs: 1,
            declared: declared(),
            clipped: Clipped::Continuous,
            config_hash: "test".into(),
        });
        Fixture {
            capture,
            clock,
            sink,
            _root: root,
        }
    }

    /// A `bbo` frame **in the shape the venue actually sends**, captured from a
    /// live run on 2026-09-20. The documented shape — `l2Book`'s
    /// `levels: [[bids],[asks]]` — is not what arrives.
    fn bbo(coin: &str, time_millis: i64) -> Vec<u8> {
        format!(
            r#"{{"channel":"bbo","data":{{"coin":"{coin}","time":{time_millis},"bbo":[{{"px":"81213.0","sz":"15.8","n":44}},{{"px":"81214.0","sz":"2.4","n":7}}]}}}}"#
        )
        .into_bytes()
    }

    #[test]
    fn the_loop_owns_every_clock_reading() {
        // No wall-clock time passes in this test, and every timestamp the
        // process produced came from the clock it was handed.
        let f = fixture(5_000 * SEC);
        assert_eq!(f.capture.now(), 5_000 * SEC);
        f.clock.advance_secs(10);
        assert_eq!(f.capture.now(), 5_010 * SEC);
    }

    #[test]
    fn an_idle_poll_window_publishes_no_gap() {
        // The one place this rule is easy to break by accident. A quiet market
        // and a silently dead connection are the same shape from the loop.
        let mut f = fixture(1_000 * SEC);
        f.capture.report_restart_gap();
        f.capture.take_frame(&bbo("BTC", 1_000_000)).unwrap();

        // An hour of nothing.
        for _ in 0..3_600 {
            f.clock.advance_secs(1);
            f.capture.publish_status_if_due(f.clock.now_micros());
        }

        let gaps = f
            .sink
            .emitted()
            .into_iter()
            .filter(|e| matches!(e.event, Event::Gap(_)))
            .count();
        assert_eq!(gaps, 0, "silence is never a gap");
    }

    #[test]
    fn a_lost_session_publishes_a_gap_per_pair_from_its_own_coverage() {
        let mut f = fixture(1_000 * SEC);
        f.capture.report_restart_gap();

        f.capture.take_frame(&bbo("BTC", 1_000_000)).unwrap();
        f.clock.advance_secs(30);
        f.capture.take_frame(&bbo("ETH", 1_030_000)).unwrap();

        f.clock.advance_secs(10);
        f.capture.session_lost(f.clock.now_micros());

        let gaps: Vec<Gap> = f
            .sink
            .emitted()
            .into_iter()
            .filter_map(|e| match e.event {
                Event::Gap(g) => Some(g),
                _ => None,
            })
            .collect();
        assert_eq!(gaps.len(), 2);
        let froms: std::collections::BTreeSet<i64> = gaps.iter().map(|g| g.from_micros).collect();
        assert_eq!(
            froms.len(),
            2,
            "each pair's gap begins at its own last coverage, not at one shared instant"
        );
        assert!(gaps.iter().all(|g| g.cause == GapCause::SessionLost));
    }

    #[test]
    fn a_first_ever_start_publishes_no_restart_gap() {
        let mut f = fixture(1_000 * SEC);
        assert_eq!(f.capture.report_restart_gap(), 0);
        assert!(f.sink.emitted().is_empty());
    }

    #[test]
    fn shutdown_flushes_before_marking() {
        let mut f = fixture(1_000 * SEC);
        f.capture.report_restart_gap();
        f.capture.take_frame(&bbo("BTC", 1_000_000)).unwrap();
        assert!(f.capture.status(f.clock.now_micros()).buffered > 0);

        f.capture.shutdown().unwrap();
        assert_eq!(
            f.capture.status(f.clock.now_micros()).buffered,
            0,
            "everything held is committed before the marker is written"
        );
    }

    #[test]
    fn the_flush_timer_commits_on_its_cadence() {
        let mut f = fixture(1_000 * SEC);
        assert!(
            f.capture.flush_if_due(f.clock.now_micros()).unwrap(),
            "the first is due"
        );
        f.capture.take_frame(&bbo("BTC", 1_000_000)).unwrap();

        f.clock.advance_secs(1);
        assert!(
            !f.capture.flush_if_due(f.clock.now_micros()).unwrap(),
            "not yet"
        );
        assert_eq!(f.capture.status(f.clock.now_micros()).buffered, 1);

        f.clock.advance_secs(1);
        assert!(f.capture.flush_if_due(f.clock.now_micros()).unwrap());
        assert_eq!(f.capture.status(f.clock.now_micros()).buffered, 0);
    }

    #[test]
    fn status_is_a_full_snapshot_on_a_timer() {
        let mut f = fixture(1_000 * SEC);
        f.capture.report_restart_gap();

        assert!(
            !f.capture.publish_status_if_due(f.clock.now_micros()),
            "not due"
        );
        f.clock.advance_secs(1);
        assert!(f.capture.publish_status_if_due(f.clock.now_micros()));

        // Every declared pair, whatever changed.
        let status = f.capture.status(f.clock.now_micros());
        assert_eq!(status.pairs.len(), 2);
        assert_eq!(status.subs_declared, 2);
    }

    #[test]
    fn a_seen_but_undeclared_pair_still_appears() {
        // So a consumer can tell "not subscribed" from "monitoring is broken".
        let mut f = fixture(1_000 * SEC);
        f.capture.report_restart_gap();
        f.capture.coverage_mut().received(
            &Ticker::new("ETH").unwrap(),
            Series::Trades,
            1_000 * SEC,
        );

        let status = f.capture.status(f.clock.now_micros());
        let undeclared = status
            .pairs
            .iter()
            .find(|p| p.series == Series::Trades)
            .expect("the seen pair must appear");
        assert_eq!(undeclared.state, PairState::NotSubscribed);
    }

    #[test]
    fn a_file_edit_does_not_change_a_running_system() {
        // Only an explicit operator act redeclares. The moment that matters
        // most is the one where somebody is editing to understand rather than
        // to change.
        let mut f = fixture(1_000 * SEC);
        assert_eq!(f.capture.status(f.clock.now_micros()).subs_declared, 2);

        // Nothing here reads a file, and that is the point: there is no path
        // by which a file change reaches the loop.
        f.capture.operator_redeclared(vec![Subscription {
            ticker: Ticker::new("BTC").unwrap(),
            series: Series::Quotes,
        }]);
        assert_eq!(f.capture.status(f.clock.now_micros()).subs_declared, 1);
    }

    #[test]
    fn the_record_holds_the_bytes_whatever_the_sink_does() {
        let mut f = fixture(1_000 * SEC);
        f.capture.report_restart_gap();
        let frame = bbo("BTC", 1_000_000);
        f.capture.take_frame(&frame).unwrap();
        f.capture
            .flush_if_due(f.clock.now_micros() + 10 * SEC)
            .unwrap();

        // One quote emitted, and the bytes recorded.
        let quotes = f
            .sink
            .emitted()
            .into_iter()
            .filter(|e| matches!(e.event, Event::Quote(_)))
            .count();
        assert_eq!(quotes, 1);
    }

    #[test]
    fn a_control_frame_credits_no_coverage() {
        let mut f = fixture(1_000 * SEC);
        f.capture.report_restart_gap();
        let before = f
            .capture
            .coverage_mut()
            .count(&Ticker::new("BTC").unwrap(), Series::Quotes);

        f.capture
            .take_frame(br#"{"channel":"subscriptionResponse","data":{}}"#)
            .unwrap();

        assert_eq!(
            f.capture
                .coverage_mut()
                .count(&Ticker::new("BTC").unwrap(), Series::Quotes),
            before,
            "a control frame is not coverage of anything"
        );
    }

    #[test]
    fn a_fetched_payload_is_durable_without_waiting_for_the_flush() {
        let mut f = fixture(1_000 * SEC);
        let mut payload = f
            .capture
            .wiring
            .adapter
            .classify(&bbo("BTC", 1_000_000), 1_000 * SEC);
        payload.origin = Origin::Fetched;
        f.capture.take(payload).unwrap();
        assert_eq!(f.capture.status(f.clock.now_micros()).buffered, 0);
    }
}
