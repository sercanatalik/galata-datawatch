# galata-datawatch

Market data capture: the record every payload lands in verbatim, and the one
path it crosses to get there.

Archive, then normalise, then emit — implemented once, so the ordering holds by
construction rather than by convention at six call sites. A payload is durable
*before* anything tries to parse it, so an adapter meeting a shape it was not
written for costs a parse and never the bytes.

Part of [galata-datawatch](https://github.com/sercanatalik/galata-datawatch).
Licensed under MIT.
