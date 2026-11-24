# add-derive-options-venue

**Built 2026-11-24. Not deployed.** The change is
`openspec/changes/add-derive-options-venue` in the workspace this repository
sits in; this file is the argument for it.

## What was wanted

Options from Derive (derive.xyz) in the same record and on the same bus as the
perps, so the client the programme is building can show an option's quote,
implied volatility and greeks beside the perps it follows. Scoped to the
options the owner names, at most 50, counted apart from Hyperliquid's set.

## Why it is not just another venue

Every venue here until now lists an instrument until somebody delists it. An
option stops existing on a date in its own name. Three things follow, and they
are most of the change:

1. **The set cannot be a list that is checked once.** `check_universe` refused
   the boot for any declared instrument the venue did not list. Applied to
   options, capture of 49 live ones would stop each time one expired before
   the owner replaced it. So an unknown name still refuses the boot, and an
   expired one leaves the set.
2. **An instrument has to be able to leave a running set.** Nothing changed
   the set while capture ran. Now the loop retires an instrument when the
   venue's reference data says it is inactive, or when the loop's own clock
   passes the expiry that data stated. It reads the vocabulary and names no
   venue, so the ticker-set change the programme plans can reuse it.
3. **The hour is not in the name.** `BTC-20261125-56000-C` and
   `NVDA-20261125-210-C` have the same shape and expire thirteen hours apart.
   The expiry acted on is the venue's.

## What the venue's own behaviour decided

Measured before the adapter was written (`design/measured.md`, 2026-11-24):

- **The venue acknowledges a subscription to an expired, unknown or malformed
  name with `ok`, and then sends nothing.** Hyperliquid closes the connection
  for an unlisted coin; this does the opposite, and it is the quieter failure.
  The reference check at boot is the only place a mistyped name is caught.
- **One payload carries an option's quote, its marks and the venue's own
  pricing.** So one declared series, `quotes`, and three datasets from it.
- **The ticker is sent every second regardless.** A full set is 2.26 GiB a
  day raw, four times Hyperliquid's `bbo` for six instruments. This was
  expected to be small and is not.

## What was decided, and by whom

Confirmed by the owner in exploration on 2026-11-24:

- Derive is a venue of its own, from public data, with no order method.
- A set of named options, at most 50, separate from Hyperliquid's.
- A slot holds one named option and nothing rolls it.
- The ticker at one second, and the underlying's index beside it, outside the
  cap. No book depth.
- **No public trades**, following programme decision P15 the same day.
  Quotes are kept on both venues.

Taken in the design and not confirmed:

- The token `derive-xyz`. `derive` already means statistics derived from the
  tape here: a module, a binary and the watch configuration.
- An expired option leaves quietly; a set where every option has expired
  exits non-zero.
- The set changes by editing the configuration and restarting, until the
  client has a door for it.

## What changed from the design while building

- **The index rides the `quotes` series, not a new `marks` series.**
  `galata-wire` asserts that `marks` is never separately declarable, because
  marks arrive as a consequence of something else. The index is exactly that,
  so it is subscribed under `quotes` for a ticker named for the index
  (`BTC-USD`) and lands in `marks`.
- **The version is not bumped here.** The changelog guard holds that the
  newest release entry is the version that will publish, so a bump is a
  release act. The breaking changes are under `[Unreleased]`, which says the
  next release is 0.2.0.
- **Retirement applies to subscribing venues only.** rh-chain's reference data
  reads `active = false` when a contract did not answer, which is a finding
  about one read and not an instrument that ceased to exist.

## Captured, 2026-11-24

Five live options and one that had expired that morning, no broker. See the
verification in the change's tasks, group 8.

## Not built

- The client's door for adding and removing options, and a cap on
  Hyperliquid's set. Both belong to the programme's "Fetcher's ticker set".
- A slot defined by a rule that rolls.
- Derive perpetuals, book depth, public trades, and any history walk.
- The container, launchd agent, broker password and vault token for the
  capture service.
