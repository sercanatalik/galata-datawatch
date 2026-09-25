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
```

- **Subjects are built from validated types, never from strings.** A `.`
  inside a token would split one subject level into two. The identity types
  in `galata-wire` already reject it, so a malformed subject cannot be
  constructed.
- **Two roots, granted separately.** A dashboard subscribes to `status.>`
  and holds no market grant at all. Credentials that can read every venue's
  firehose are not what an operator's laptop should carry.

## What is in the crate

| Item | Purpose |
|---|---|
| `Subject` | `Subject::market(venue, ticker, kind)` and `Subject::status(venue)` |
| `Publisher`, `NatsPublisher`, `NullPublisher` | publishing, with a no-op implementation for runs without a bus |
| `NatsSubscriber` | consuming, over a separate connection from publishing |
| `BrokerIdentity`, `Grant`, `Grants` | who may publish or subscribe to which root |
| `to_nats_config` | renders the grant table as a NATS authorization file, so the server's policy is generated rather than hand-written |
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
