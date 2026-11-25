# galata-broker

**The live market-data bus for Galata: subjects, identities, grants, and a
NATS publisher and subscriber.**

This is where [galata-datawatch]'s normalised events leave the capture
process, and where every downstream Galata process (signals, risk, execution)
picks them up. It depends on [`galata-wire`] and nothing else in the
workspace, so a process that only reads the stream links no columnar format.

```toml
galata-wire   = "0.1"
galata-broker = "0.1"
```

## Subjects

```text
  markets.<venue>.<ticker>.<kind>     one instrument, one dataset
  status.<venue>                      what one capture process says about itself
  accounts.<venue>.<alias>.<kind>     what a venue said about one account
```

- **Subjects are built from validated types, never from strings.** A `.`
  inside a token would split one subject level into two. The identity types
  in `galata-wire` already reject it, so a malformed subject cannot be
  constructed.
- **Two roots, granted separately.** A dashboard subscribes to `status.>`
  and holds no market grant at all. Credentials that can read every venue's
  firehose are not what an operator's laptop should carry.
- **An account is named by its alias, never its address.** The alias token
  cannot refuse an address (`0x…` is alphanumeric), so `Subject::account`
  does: an alias shaped like an EVM address forms no subject. No login is
  granted `accounts.>` yet; the ledger's change adds the first.

## What the broker keeps

```text
  MARKETS   markets.>    two hours, within a byte cap    lossy; every hole declared
  ACCOUNTS  accounts.>   seven days                      complete; acknowledged
  STATUS    status.>     the last message per subject    each venue's latest
```

- **The market window bridges the tape and now.** The tape is projected
  hourly, so it trails by up to about 65 minutes; a consumer replays the
  window from where the tape ends, then follows live. It drops what it
  already has by `seq`, address and kind together.
- **Its holes are declared, not prevented.** Capture sends a gap with the
  cause `unpublished` for every span it archived and could not hand to the
  broker. The record holds that span: a consumer fetches it from history.
  Every other gap cause says the data exists nowhere.
- **Account data is complete.** The broker acknowledges each message only once
  stored, and its publisher republishes what was not acknowledged.
- **The streams are a table** (`streams::table`), beside the grants, and
  `streams::apply` creates or updates them. The broker's own start applies
  it (`galata-bus`), as the one login that may create, update and read
  streams, `bus-admin`: it may not delete one or touch data. Every other login
  is refused the stream API.
- **The server's file-store cap must exceed the streams' byte caps
  together**: JetStream reserves a capped stream's bytes up front.

## What is in the crate

| Item | Purpose |
|---|---|
| `Subject` | `Subject::market(venue, ticker, kind)`, `Subject::status(venue)` and `Subject::account(venue, alias, kind)` |
| `Publisher`, `NatsPublisher`, `NullPublisher` | publishing, with a no-op implementation for runs without a bus |
| `NatsSubscriber` | consuming, over a separate connection from publishing |
| `BrokerIdentity`, `Grant`, `Grants` | who may publish or subscribe to which root |
| `to_nats_config` | renders the grant table as a NATS authorization file, so the server's policy is generated rather than hand-written |
| `streams::table`, `streams::apply`, `streams::connect_admin`, `streams::held` | what the broker keeps, applying it as `bus-admin`, and what a stream holds |
| `encode`, `decode` | the wire encoding of an `Envelope` |

**Publishing and consuming are separate authorities.** Datawatch publishes
and never consumes. A component that only reads should not hold a handle
that can write.

## Two ways to fail at boot

```text
  Unreachable   the broker did not answer. An OUTAGE. The record survives it:
                capture archives before it publishes, and has run for nine
                hours with no broker at all.

  Rejected      the broker answered and refused this identity. A
                MISCONFIGURATION. It will never fix itself.
```

These used to be one error, so "archived everything and published nothing
for a day" looked exactly like "a day-long broker outage". `ConnectRefusal`
tells them apart, once, at connect.

## Part of Galata

galata-broker is one of four crates in [galata-datawatch], the data layer of
Galata, a low-latency algorithmic trading framework in Rust. See the
[project README][galata-datawatch] for the architecture and roadmap.

Licensed under MIT.

[galata-datawatch]: https://github.com/sercanatalik/galata-datawatch
[`galata-wire`]: https://crates.io/crates/galata-wire
