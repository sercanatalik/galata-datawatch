# galata-margin

Advice on where to place an option on Derive and its perpetual hedge: all on
Derive, all on Hyperliquid, or split. Each placement's cost is computed
against the owner's real book (`add-cross-venue-margin-optimiser`). It
advises and does nothing else. It names no request that places, amends or
cancels an order, or that moves funds, on either venue.

**Not published** (design D10). It holds the request that simulates the
owner's Derive subaccount, and the four published crates link none of it, as
`scripts/check-margin-reach.sh` proves.

## The estimate

For each venue and account, an estimate states the margin before and after
the placement's positions are added. Each figure carries three things:

| | |
|---|---|
| **Origin** | `venue`: the venue stated it. `computed`: galata computed it. |
| **Estimate label** | Present on a computed figure while galata's portfolio margin ratio for the account disagrees with the venue's beyond the declared tolerance, or has not been compared yet. It carries the disagreement measured and when. |
| **Time** | The time of the account snapshot, or the venue's simulation, it rests on. |

**Derive's figure is Derive's** (D3). `private/get_margin` simulates the
owner's subaccount with the placement's positions added.
- **Net figures:** its four figures are net, mark-to-market value less the
  requirement, and positive while healthy. They're carried digit for digit
  (`derive`).
- **One key:** it signs with the session key the ledger reads with. At start,
  the key is checked by the ledger's own rule (D11): trading scopes,
  `account_info` and `block_trade` are accepted, and anything that can move
  funds is refused by name.

**Hyperliquid's figure is galata's** (D4), computed from the venue's
published formulas over the ledger's projection (`hyperliquid`, `state`):
- **Initial margin** is `size × mark / leverage` in every mode.
- **Maintenance under portfolio margin** is the portfolio margin
  requirement, and the ratio over the liquidation value is the venue's
  liquidation measure.
- **Outside portfolio margin**, maintenance is half the initial margin at the
  market's maximum leverage.

On every recorded snapshot, galata's ratio is compared with the one the venue
states (`agreement`). Measured over 310 snapshots on 2026-11-27, the largest
disagreement was 0.000136 (`design/measured.md`).

**A venue with no figure says why** and does not stop the other: a snapshot
older than the declared bound, a venue that did not answer, a mode with no
formula (`estimate`).

## The `[margin]` block

Every setting is the owner's, required and bounded, and an unknown key is
refused (`config`). galata-datawatch holds the block as written, without
reading it.

```toml
[margin]
session_key_var = "GALATA_LEDGER_DERIVE_SESSION_KEY"
agreement_tolerance = "0.005"   # of the ratio, a decimal string
snapshot_age_secs = 900
split_pct = [25, 50, 75]
funding_hours = 168
venue_cap_pct = 100

[margin.accounts]
derive-xyz = ["owner_s3"]
hyperliquid = ["main"]
```
