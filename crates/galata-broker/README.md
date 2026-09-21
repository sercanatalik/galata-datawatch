# galata-broker

Where [`galata-datawatch`](https://github.com/sercanatalik/galata-datawatch)'s
normalised events leave the process.

Subjects, identities, and a NATS publisher and subscriber. It depends on
`galata-wire` and nothing else of the workspace, so a component that reads the
stream links no columnar format to do it.

**The boot asymmetry is the part worth knowing.** A broker that does not answer
is an outage the record survives; a broker that refuses your identity is a
misconfiguration that will never fix itself. They arrive as different errors,
on purpose.

MIT licensed.
