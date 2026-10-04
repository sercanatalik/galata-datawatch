//! The cursor loop: capture by **asking**, at a position.
//!
//! ```text
//!   ask the head          eth_blockNumber
//!   plan                  from the last block captured, to the head
//!   fetch                 eth_getLogs over each step
//!   ingest                THE ONE PATH — archive, normalise, emit
//!   ask about a contract  on a cadence; a chain announces no corporate action
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
use crate::adapters::rh_chain::trail::{Advance, BlockTrail, Reorg};
use crate::capture::run::{Capture, CaptureError};
use crate::source::Backoff;
use crate::venue::{BlockPaging, Frontier, Reference, Transport};

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
    /// The span the next pass should use, where the provider refused this
    /// one's for its size. `None` means it was not refused that way.
    pub narrow_to: Option<u64>,
    /// The span this pass used, where it was narrowed below the declared one.
    ///
    /// **A node caps `eth_getLogs` by rows as well as by blocks**, and no fixed
    /// span satisfies a row cap — a busy stretch produces more logs per block.
    /// Reported so an operator can see the loop adapting rather than guess why
    /// a pass covered less.
    pub narrowed_to: Option<u64>,
    /// Where the cursor was moved back to, where a reorganisation moved it.
    ///
    /// **The re-read is bounded by the trail's depth**, which is bounded by
    /// finality — 11,678 blocks on this chain, or about twelve thousand-block
    /// ranges at worst. Reported rather than capped: a cap would silently
    /// leave part of a replaced range unread, which is the failure this whole
    /// rewind exists to prevent.
    pub rewound: Option<u64>,
}

impl Pass {
    /// A line an operator can act on.
    pub fn report(&self) -> String {
        let rewound = match self.rewound {
            Some(block) => format!(", rewound to {block}"),
            None => String::new(),
        };
        let narrowed = match self.narrowed_to.or(self.narrow_to) {
            Some(span) => format!(", span narrowed to {span}"),
            None => String::new(),
        };
        format!(
            "{} blocks in {} requests ({} failed, {} reorgs{rewound}{narrowed})",
            self.blocks, self.requests, self.failed, self.reorgs
        )
    }
}

/// Where the cursor goes when a reorganisation replaced blocks from
/// `from_block` onwards.
///
/// The block **before** the divergence, because the cursor names the last block
/// read and the next pass plans from `cursor + 1`.
///
/// **A cursor only ever moves backward here.** The trail is bounded by
/// finality, so a reorganisation ahead of the cursor cannot happen — and this
/// does not depend on that being true.
fn rewind_to(cursor: Option<u64>, from_block: u64) -> u64 {
    let target = from_block.saturating_sub(1);
    cursor.map_or(target, |c| c.min(target))
}

/// The span to try after the provider refused this one for its size.
///
/// Halved, with a floor of one block. **Never the same span again** — the
/// blocks hold what they hold, so a retry unchanged is the livelock a soak
/// found: eighteen identical refusals of one range in twenty-five minutes.
fn narrowed(span: u64) -> u64 {
    (span / 2).max(1)
}

/// The span to try after a clean pass, moving back towards the declared one.
///
/// **Doubling rather than restoring**, because a busy stretch of chain is a
/// stretch and not the whole chain: going straight back to the declared span
/// would refuse again on the very next pass over the same neighbourhood.
fn widened(span: u64, declared: u64) -> u64 {
    (span.saturating_mul(2)).min(declared)
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
            } => (rpc_url, paging, finality_lag),
            other => {
                return Err(CaptureError::NotACursor {
                    endpoint: other.endpoint().to_string(),
                });
            }
        };

        let client = ChainClient::new(rpc_url);
        // **Before anything is captured.** A provider serving another chain
        // answers every request correctly, and its blocks are real and not
        // ours.
        if let Err(error) = client.check_chain_id().await {
            return Err(CaptureError::provider(&error));
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

        // **Reference data is read before the first block, not after.** A
        // multiplier learned an hour into capture dates from an hour into
        // capture, and every amount recorded before it has nothing to join to.
        let reference = self.venue_reference();
        let mut refreshed_at: Option<i64> = None;

        // **The span adapts to what the provider will actually serve.**
        //
        // The declared `max_span` is a block cap. A node also caps by ROWS, and
        // no fixed span satisfies that — a busy stretch produces more logs per
        // block. So the row cap is discovered from the refusal and answered by
        // halving, then given back after a clean pass.
        let mut span = paging.max_span.max(1);

        while !shutdown.is_cancelled() {
            let mut last_pass_failed = false;
            if let Some(reference) = reference.as_ref() {
                self.refresh_reference(&client, reference, &mut refreshed_at)
                    .await?;
            }
            let stepping = BlockPaging {
                max_span: span,
                earliest: paging.earliest,
            };
            match self
                .one_pass(
                    &client,
                    &venue,
                    &stepping,
                    &mut trail,
                    &mut cursor,
                    backfill,
                    pace,
                )
                .await
            {
                Ok(pass) => {
                    if let Some(narrower) = pass.narrow_to {
                        // **Never retried unchanged.** The refusal says the
                        // range held more than the node will return, and it
                        // will hold just as much next time.
                        if span <= 1 {
                            return Err(CaptureError::Provider(format!(
                                "this provider will not serve a single block near {}, so \
                                 narrowing has nowhere left to go. Its row cap is below what \
                                 one block of this chain holds — a different provider, or a \
                                 raised cap, is the only way past it",
                                cursor.map(|c| c + 1).unwrap_or_default()
                            )));
                        }
                        span = narrower;
                        tracing::warn!(
                            venue = venue.as_str(),
                            span,
                            "the provider refused the range for its size; narrowing"
                        );
                    } else if pass.failed == 0 && span < paging.max_span {
                        // Given back gradually. A busy stretch is a stretch,
                        // not the whole chain.
                        span = widened(span, paging.max_span);
                        tracing::debug!(venue = venue.as_str(), span, "widening again");
                    }
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
            .map_err(|e| CaptureError::provider(&e))?;

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
            .map_err(|e| CaptureError::provider(&e))?
        {
            let now = self.now();
            let payload = match client.logs(step.from, step.to, now).await {
                Ok(payload) => payload,
                // **The range was too big, not too soon.** Retrying it
                // unchanged cannot work: the blocks hold what they hold. The
                // pass stops here, the cursor does not advance, and the next
                // one plans the same ground in halves.
                Err(error) if error.is_narrowable() => {
                    tracing::warn!(
                        from = step.from,
                        to = step.to,
                        %error,
                        "the range was refused for its size"
                    );
                    pass.failed += 1;
                    pass.narrow_to = Some(narrowed(step.blocks()));
                    break;
                }
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

            // **Two headers per step: its first block and its last.** The
            // trail links a block only to the one at the height below it, and
            // a step's first block is exactly one above the last step's last —
            // so that is where the chain is checked. Taking only the last
            // block, as this did, meant no header ever had its predecessor
            // held once a step spanned more than one block: every advance was
            // `NotLinked` and no reorganisation was ever seen.
            //
            // A fork anywhere at or below the held tip changes the tip's hash
            // (each hash commits to its parent's), so checking the boundary
            // catches it however deep it runs; `fork_point` then finds how
            // deep.
            let mut heights = vec![step.from];
            if step.to != step.from {
                heights.push(step.to);
            }
            let mut forked = None;
            for height in heights {
                match client.header_at(height).await {
                    Ok(header) => {
                        pass.requests += 1;
                        if let Some(reorg) = self.advance_trail(trail, &header) {
                            forked = Some((reorg, header));
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(block = height, %error, "a header failed; the trail did not advance");
                    }
                }
            }
            if let Some((mut reorg, header)) = forked {
                pass.reorgs += 1;
                let (from_block, requests) = fork_point(client, trail, reorg.from_block).await;
                pass.requests += requests;
                reorg.from_block = from_block;
                self.record_reorg(&reorg, &header, venue);
                // **Rewind to the divergence and stop this pass.**
                //
                // Publishing the reorganisation and advancing past it leaves
                // the record holding the old chain's rows for those blocks
                // with nothing that replaces them. The replacement rows enter
                // with a HIGHER stream sequence, which is what `crate::reorg`
                // uses to tell them apart.
                //
                // `min` because a cursor only ever moves backward here: the
                // trail is bounded by finality so a reorganisation ahead of
                // the cursor cannot happen, and this does not depend on that
                // being true.
                let rewound = rewind_to(*cursor, from_block);
                *cursor = Some(rewound);
                // The break matters: falling through would set the cursor to
                // `step.to` below and undo the rewind.
                pass.rewound = Some(rewound);
                break;
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

    /// Ask each contract about itself, **through the one path**, when the
    /// cadence says it is time.
    ///
    /// A read that fails is not a reason to stop capturing blocks: the
    /// multiplier stays as last recorded, which is stale rather than wrong,
    /// and `observed_at` on the last row says exactly how stale.
    async fn refresh_reference(
        &mut self,
        client: &ChainClient,
        reference: &Reference,
        refreshed_at: &mut Option<i64>,
    ) -> Result<(), CaptureError> {
        let now = self.now();
        if refreshed_at.is_some_and(|last| now - last < reference.interval_micros) {
            return Ok(());
        }
        for symbol in &reference.symbols {
            match client.metadata(symbol, self.now()).await {
                Ok(payload) => self.take(payload)?,
                Err(error) => {
                    tracing::warn!(contract = symbol.as_str(), %error, "a contract's metadata did not read; its last answer stands");
                }
            }
        }
        *refreshed_at = Some(now);
        Ok(())
    }

    /// Advance the trail, returning a reorganisation if the chain disagrees.
    fn advance_trail(&mut self, trail: &mut BlockTrail, header: &Header) -> Option<Reorg> {
        match trail.advance(&header.seen()) {
            Advance::Reorganised(reorg) => Some(reorg),
            _ => None,
        }
    }

    /// Publish a reorganisation, **through the one path**, durable before it
    /// is emitted — a reorganisation that reached the sink and not the disk is
    /// exactly the evidence an outage would erase.
    fn record_reorg(&mut self, reorg: &Reorg, header: &Header, venue: &Venue) {
        tracing::warn!(
            from = reorg.from_block,
            to = reorg.to_block,
            depth = reorg.depth(),
            "the chain replaced blocks we captured"
        );
        // The reorganisation is about the chain, not one instrument. Addressed
        // to the venue's own name so it lands somewhere findable rather than
        // being dropped.
        let Ok(chain) = Ticker::new("CHAIN") else {
            return;
        };
        let envelope = Envelope::new(
            venue.clone(),
            chain,
            Some(header.at_micros()),
            self.now(),
            Event::Reorg(WireReorg {
                from_block: reorg.from_block,
                to_block: reorg.to_block,
                old_hash: reorg.old_hash.clone(),
                new_hash: reorg.new_hash.clone(),
            }),
        );
        if let Err(error) = self.record_generated_event(venue.as_str(), envelope) {
            tracing::warn!(%error, "a reorganisation could not be recorded; it is still a fact");
        }
    }
}

/// Where a fork began: the lowest height that may have changed, found by
/// walking back through the hashes the trail still holds and asking the chain
/// for each, until one agrees. Returns that height and the requests it made.
///
/// **The trail is sparse** — two hashes a step — so the answer is *one above
/// the newest held height the chain still agrees on*: every block above it may
/// have been replaced, and the rewind re-reads them all. Claiming less would
/// leave old-chain rows unreplaced; the rows of a block that did not in fact
/// change are re-read identical, and cost a read.
///
/// A header that will not read stops the search at what was proven so far.
async fn fork_point(client: &ChainClient, trail: &mut BlockTrail, changed: u64) -> (u64, u32) {
    let mut lowest = changed;
    let mut requests = 0;
    for (height, held) in trail.held_below(changed) {
        let Ok(header) = client.header_at(height).await else {
            break;
        };
        requests += 1;
        if header.hash.eq_ignore_ascii_case(&held) {
            // The chain agrees here: the fork began above it.
            lowest = height + 1;
            break;
        }
        lowest = height;
    }
    trail.forget_from(lowest);
    (lowest, requests)
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_size_refusal_never_retries_the_same_span() {
        // The livelock, in one assertion: the next span is always smaller.
        let mut span = 1_000u64;
        for _ in 0..20 {
            let next = narrowed(span);
            assert!(next < span || span == 1, "{span} -> {next}");
            span = next;
        }
        // And it stops at a block rather than at zero, which would plan nothing.
        assert_eq!(span, 1);
        assert_eq!(narrowed(1), 1);
    }

    #[test]
    fn a_clean_pass_widens_gradually_and_never_past_the_declaration() {
        assert_eq!(widened(250, 1_000), 500);
        assert_eq!(widened(500, 1_000), 1_000);
        // **Not past what the venue declared.** The adaptation is downward
        // from a declared cap, never an argument for exceeding it.
        assert_eq!(widened(1_000, 1_000), 1_000);
        assert_eq!(widened(u64::MAX, 1_000), 1_000);
    }

    #[test]
    fn a_size_refusal_halves_the_span_and_a_rate_limit_does_not() {
        use crate::adapters::rh_chain::client::ChainError;
        let rpc = |detail: &str| ChainError::Rpc {
            venue: "rh-chain",
            method: "eth_getLogs",
            detail: detail.to_string(),
        };
        // **Measured in a soak**, eighteen times on one range.
        assert!(
            rpc(r#"{"code":-32000,"message":"logs matched by query exceeds limit of 50000"}"#)
                .is_narrowable()
        );
        // Less to gather is less to time out on.
        assert!(rpc(r#"{"code":-32000,"message":"log query timed out"}"#).is_narrowable());
        // **Not this one.** The range was fine and we were too quick, which
        // the backoff answers — and narrowing would make MORE requests at
        // exactly the wrong moment.
        assert!(!rpc(r#"{"code":429,"message":"Too Many Requests"}"#).is_narrowable());
        assert!(!rpc(r#"{"code":-32000,"message":"header not found"}"#).is_narrowable());
    }

    #[test]
    fn a_pass_that_narrowed_says_so() {
        let pass = Pass {
            blocks: 0,
            requests: 1,
            failed: 1,
            reorgs: 0,
            rewound: None,
            narrow_to: Some(500),
            narrowed_to: None,
        };
        assert!(
            pass.report().contains("span narrowed to 500"),
            "{}",
            pass.report()
        );
        assert!(!Pass::default().report().contains("narrowed"));
    }
    use super::*;

    /// A JSON-RPC stand-in answering `eth_getBlockByNumber` from `hashes`.
    async fn chain(hashes: std::collections::BTreeMap<u64, &'static str>) -> ChainClient {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let _ = rustls::crypto::ring::default_provider().install_default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: &'static str =
            Box::leak(format!("http://{}", listener.local_addr().unwrap()).into_boxed_str());
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                // Headers, then a body of Content-Length bytes.
                let body = loop {
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break None;
                    }
                    raw.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&raw).to_string();
                    if let Some(split) = text.find("\r\n\r\n") {
                        let len = text[..split]
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if raw.len() >= split + 4 + len {
                            break Some(raw[split + 4..split + 4 + len].to_vec());
                        }
                    }
                };
                let Some(body) = body else { continue };
                let asked: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let height = u64::from_str_radix(
                    asked["params"][0]
                        .as_str()
                        .unwrap()
                        .trim_start_matches("0x"),
                    16,
                )
                .unwrap();
                let answer = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {
                    "number": format!("0x{height:x}"),
                    "hash": hashes.get(&height).copied().unwrap_or("0xunknown"),
                    "parentHash": "0xparent",
                    "timestamp": "0x1",
                }})
                .to_string();
                let reply = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}",
                    answer.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            }
        });
        ChainClient::new(crate::venue::Endpoint::held(
            "GALATA_RHCHAIN_RPC_URL",
            crate::config::Secret::new(url),
        ))
    }

    /// A trail as the cursor leaves it: 1,000 and 1,001 held below the block
    /// at 2,000 that was found replaced.
    fn held() -> BlockTrail {
        let mut trail = BlockTrail::new(10_000);
        for (n, hash, parent) in [
            (1_000u64, "0xa1000", "0xa999"),
            (1_001, "0xa1001", "0xa1000"),
            (2_000, "0xa2000", "0xa1999"),
        ] {
            trail.advance(&crate::adapters::rh_chain::trail::Seen {
                number: n,
                hash: hash.into(),
                parent_hash: parent.into(),
            });
        }
        assert_eq!(trail.len(), 3);
        trail.forget_from(2_000);
        trail
    }

    #[tokio::test]
    async fn the_fork_point_is_one_above_the_newest_height_the_chain_still_agrees_on() {
        // 1,001 unchanged: everything above it may have been replaced.
        let client = chain([(1_000, "0xa1000"), (1_001, "0xa1001")].into()).await;
        let mut trail = held();
        let (from, asked) = fork_point(&client, &mut trail, 2_000).await;
        assert_eq!((from, asked), (1_002, 1));
        assert_eq!(rewind_to(Some(2_000), from), 1_001, "re-read from 1,002");

        // 1,001 replaced too, 1,000 not: the fork began above 1,000.
        let client = chain([(1_000, "0xa1000"), (1_001, "0xb1001")].into()).await;
        eprintln!("DEBUG {:?}", client.header_at(1_001).await);
        eprintln!("DEBUG {:?}", client.header_at(1_000).await);
        let mut trail = held();
        let (from, asked) = fork_point(&client, &mut trail, 2_000).await;
        assert_eq!((from, asked), (1_001, 2));
        assert_eq!(
            trail.tip(),
            Some(1_000),
            "the replaced heights are forgotten"
        );
    }

    #[test]
    fn a_rewind_lands_before_the_divergence() {
        // The cursor names the last block READ, and the next pass plans from
        // `cursor + 1` — so landing on `from_block` itself would skip it.
        assert_eq!(rewind_to(Some(5_000), 4_100), 4_099);
    }

    #[test]
    fn a_rewind_never_moves_the_cursor_forward() {
        // A reorganisation ahead of the cursor cannot happen, because the trail
        // is bounded by finality. This does not depend on that being true.
        assert_eq!(rewind_to(Some(100), 4_100), 100);
    }

    #[test]
    fn a_reorganisation_at_genesis_does_not_underflow() {
        assert_eq!(rewind_to(Some(10), 0), 0);
        assert_eq!(rewind_to(None, 0), 0);
    }

    #[test]
    fn a_pass_that_rewound_says_so() {
        // An operator reading one line needs to know the cursor went backwards;
        // a block count that fell is otherwise indistinguishable from a quiet
        // chain.
        let pass = Pass {
            blocks: 40,
            requests: 3,
            failed: 0,
            reorgs: 1,
            rewound: Some(4_099),
            narrow_to: None,
            narrowed_to: None,
        };
        assert!(pass.report().contains("1 reorgs"), "{}", pass.report());
        assert!(
            pass.report().contains("rewound to 4099"),
            "{}",
            pass.report()
        );
        assert!(!Pass::default().report().contains("rewound"));
    }
}
