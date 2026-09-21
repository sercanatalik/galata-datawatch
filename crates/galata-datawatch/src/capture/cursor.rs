//! The cursor loop: capture by **asking**, at a position.
//!
//! ```text
//!   ask the head          eth_blockNumber
//!   plan                  from the last block captured, to the head
//!   fetch                 eth_getLogs over each step
//!   ingest                THE ONE PATH — archive, normalise, emit
//!   advance the trail     parent linkage; a disagreement is a reorganisation
//!   pace                  the venue's declared budget
//! ```
//!
//! **No connect, no subscribe, no keepalive, no rotation.** There is no session
//! to lose, so there is no session-lost gap — the absence a chain can have is a
//! reorganisation, and that one is *proved* rather than inferred.
//!
//! What it shares with the streaming loop is everything that is about this
//! system rather than about the venue: the one path, the flush timer, the
//! status surface.

use std::time::Duration;

use galata_wire::{Envelope, Event, Reorg as WireReorg, Ticker, Venue};

use crate::adapters::rh_chain::client::{ChainClient, Header};
use crate::adapters::rh_chain::trail::{Advance, BlockTrail};
use crate::capture::run::{Capture, CaptureError};
use crate::source::Backoff;
use crate::venue::{BlockPaging, Frontier, Transport};

/// What one pass over the chain did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pass {
    /// Blocks the cursor advanced over.
    pub blocks: u64,
    /// Requests made.
    pub requests: u32,
    /// Ranges the provider refused.
    ///
    /// **Not fatal.** A provider refusing one range is not a reason to abandon
    /// the rest, and the cursor simply does not advance past it.
    pub failed: u32,
    /// Reorganisations published.
    pub reorgs: u32,
}

impl Pass {
    /// A line an operator can act on.
    pub fn report(&self) -> String {
        format!(
            "{} blocks in {} requests ({} failed, {} reorgs)",
            self.blocks, self.requests, self.failed, self.reorgs
        )
    }
}

impl Capture {
    /// Capture a chain until the token is cancelled.
    ///
    /// `backfill` is how far behind the head a **cold start** begins. The trail
    /// lives in memory, so a restart re-reads a little; segment naming absorbs
    /// it, because a segment named for the same blocks is the same segment.
    pub async fn run_cursor(
        &mut self,
        shutdown: tokio_util::sync::CancellationToken,
        backfill: u64,
    ) -> Result<(), CaptureError> {
        let (rpc_url, paging, finality_lag) = match self.venue_transport() {
            Transport::Cursor {
                rpc_url,
                paging,
                finality_lag,
                ..
            } => (rpc_url.to_string(), paging, finality_lag),
            other => {
                return Err(CaptureError::NotACursor {
                    endpoint: other.endpoint().to_string(),
                });
            }
        };

        let client = ChainClient::new(&rpc_url);
        // **Before anything is captured.** A provider serving another chain
        // answers every request correctly, and its blocks are real and not
        // ours.
        if let Err(error) = client.check_chain_id().await {
            return Err(CaptureError::Provider(error.to_string()));
        }

        let venue = self.venue().clone();
        let mut trail = BlockTrail::new(finality_lag);
        let mut cursor: Option<u64> = None;
        // **A refused pass waits longer before the next one.**
        //
        // Measured against the public node: without this, a `429 Too Many
        // Requests` is retried at the declared pace — 500 ms — which is
        // precisely what provoked it, so the loop never recovers and adds load
        // to a node already saying stop. Fifteen refusals in forty seconds.
        //
        // The same backoff the reconnect path uses, for the same reason: the
        // remedy for being told to slow down is to slow down.
        let mut backoff = Backoff::default();
        let pace = Duration::from_millis(self.budget().walk_interval_ms(1.0).max(1));

        while !shutdown.is_cancelled() {
            let mut last_pass_failed = false;
            match self
                .one_pass(
                    &client,
                    &venue,
                    &paging,
                    &mut trail,
                    &mut cursor,
                    backfill,
                    pace,
                )
                .await
            {
                Ok(pass) => {
                    if pass.blocks > 0 || pass.reorgs > 0 {
                        tracing::info!(venue = venue.as_str(), "{}", pass.report());
                    }
                    // A pass that got anywhere clears the penalty; one that was
                    // refused throughout keeps it.
                    if pass.failed == 0 {
                        backoff.reset();
                    } else {
                        last_pass_failed = true;
                    }
                }
                Err(error) => {
                    // The chain is not going anywhere. A pass that failed
                    // wholly is retried, and the cursor did not move.
                    tracing::warn!(venue = venue.as_str(), %error, "a pass failed; the cursor did not advance");
                    last_pass_failed = true;
                }
            }

            let now = self.now();
            self.flush_if_due(now)?;
            self.publish_status_if_due(now);

            // The pace when things are working; the backoff when they are not.
            let wait = if last_pass_failed {
                let wait = backoff.next_wait();
                tracing::debug!(?wait, "backing off");
                wait
            } else {
                pace
            };
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(wait) => {}
            }
        }

        self.shutdown()
    }

    #[allow(clippy::too_many_arguments)]
    async fn one_pass(
        &mut self,
        client: &ChainClient,
        venue: &Venue,
        paging: &BlockPaging,
        trail: &mut BlockTrail,
        cursor: &mut Option<u64>,
        backfill: u64,
        pace: Duration,
    ) -> Result<Pass, CaptureError> {
        let head = client
            .frontier(Frontier::Head)
            .await
            .map_err(|e| CaptureError::Provider(e.to_string()))?;

        // A cold start begins a declared distance behind the head rather than
        // at genesis: the public node keeps no archive, and asking for the
        // beginning of the chain returns emptiness that looks like a quiet
        // range.
        let from = cursor
            .map(|c| c + 1)
            .unwrap_or_else(|| head.saturating_sub(backfill));
        if from > head {
            return Ok(Pass::default());
        }

        let mut pass = Pass::default();
        for step in paging
            .plan(from, head)
            .map_err(|e| CaptureError::Provider(e.to_string()))?
        {
            let now = self.now();
            let payload = match client.logs(step.from, step.to, now).await {
                Ok(payload) => payload,
                Err(error) => {
                    // **The cursor does not advance.** Advancing past a failed
                    // range turns a retryable hole into a permanent one,
                    // silently — so this pass stops here and the next one
                    // starts from the same place.
                    tracing::warn!(from = step.from, to = step.to, %error, "a range failed");
                    pass.failed += 1;
                    break;
                }
            };
            pass.requests += 1;
            self.take(payload)?;

            // The tip's header, for the trail. One per step rather than one per
            // block: a reorganisation deeper than a step is still caught, by
            // the next step's parent not linking.
            match client.header_at(step.to).await {
                Ok(header) => {
                    pass.requests += 1;
                    if let Some(reorg) = self.advance_trail(trail, &header, venue) {
                        pass.reorgs += reorg;
                    }
                }
                Err(error) => {
                    tracing::warn!(block = step.to, %error, "a header failed; the trail did not advance");
                }
            }

            *cursor = Some(step.to);
            pass.blocks += step.blocks();

            let now = self.now();
            self.flush_if_due(now)?;
            self.publish_status_if_due(now);
            tokio::time::sleep(pace).await;
        }
        Ok(pass)
    }

    /// Advance the trail, publishing a reorganisation if the chain disagrees.
    fn advance_trail(
        &mut self,
        trail: &mut BlockTrail,
        header: &Header,
        venue: &Venue,
    ) -> Option<u32> {
        match trail.advance(&header.seen()) {
            Advance::Extended | Advance::NotLinked => None,
            Advance::Reorganised(reorg) => {
                tracing::warn!(
                    from = reorg.from_block,
                    to = reorg.to_block,
                    depth = reorg.depth(),
                    "the chain replaced blocks we captured"
                );
                // **Through the one path**, durable before it is emitted — a
                // reorganisation that reached the sink and not the disk is
                // exactly the evidence an outage would erase.
                let envelope = Envelope::new(
                    venue.clone(),
                    // The reorganisation is about the chain, not one
                    // instrument. Addressed to the venue's own name so it lands
                    // somewhere findable rather than being dropped.
                    Ticker::new("CHAIN").ok()?,
                    Some(header.at_micros()),
                    self.now(),
                    Event::Reorg(WireReorg {
                        from_block: reorg.from_block,
                        to_block: reorg.to_block,
                        old_hash: reorg.old_hash,
                        new_hash: reorg.new_hash,
                    }),
                );
                if let Err(error) = self.record_generated_event(venue.as_str(), envelope) {
                    tracing::warn!(%error, "a reorganisation could not be recorded; it is still a fact");
                }
                Some(1)
            }
        }
    }
}
