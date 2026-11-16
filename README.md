# Hermes TUI

Composable read-only ledger operations presentation over Hermes's public client.
The `ledger.observe.v1` surface combines status, usage, content-free operational
signals, optional health for one explicitly configured stream, and optional
content-free position/backlog for one explicitly configured consumer. It
neither discovers nor exposes a stream/consumer catalogue nor echoes either
opaque selector. Cursor observation uses `ReadOnlyClient`; the presentation
does not acquire the acknowledgement-capable consumer facade. It owns no
append, acknowledgement, cursor mutation, compaction, retention,
authorization, terminal, daemon, or process lifecycle semantics.

Event payloads are available only through the separate
`ledger.events.disclose.v1` command. It uses Hermes's read-only
`DisclosureClient`, requires an exact stream/start/limit confirmation, returns
at most 16 events, and remains bounded by the negotiated 2 MiB frame ceiling.
Routine observation never calls this client and remains content-free.

Acknowledgement is likewise a separate `ledger.acknowledge.v1` command. It
observes the exact current cursor, compares it with the operator's expected
position, and attempts one exact-confirmed acknowledgement on the same
connection. A post-send transport failure is outcome-unknown and is never
retried. The reusable component still does not acquire `ConsumerClient`.

Standalone selection is explicit:

```text
hermes-tui headless /path/to/hermes.sock \
  --stream 32_HEXADECIMAL_DIGITS \
  --consumer 32_HEXADECIMAL_DIGITS

hermes-tui disclose-events /path/to/hermes.sock STREAM_HEX 0 4 \
  'DISCLOSE HERMES STREAM STREAM_HEX FROM 0 LIMIT 4'

hermes-tui acknowledge /path/to/hermes.sock CONSUMER_HEX STREAM_HEX 1 1 \
  'ACKNOWLEDGE HERMES CONSUMER CONSUMER_HEX STREAM STREAM_HEX FROM 1 THROUGH 1'
```
