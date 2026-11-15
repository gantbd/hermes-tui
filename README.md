# Hermes TUI

Composable read-only ledger operations presentation over Hermes's public client.
The `ledger.observe.v1` surface combines status, usage, content-free operational
signals, and optional health for one explicitly configured stream. It neither
discovers nor exposes a stream catalogue or echoes the opaque stream selector.
It owns no append, acknowledgement, cursor mutation, compaction, retention,
authorization, terminal, daemon, or process lifecycle semantics.

Standalone selection is explicit:

```text
hermes-tui headless /path/to/hermes.sock --stream 32_HEXADECIMAL_DIGITS
```
