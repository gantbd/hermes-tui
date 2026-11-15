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

Standalone selection is explicit:

```text
hermes-tui headless /path/to/hermes.sock \
  --stream 32_HEXADECIMAL_DIGITS \
  --consumer 32_HEXADECIMAL_DIGITS
```
