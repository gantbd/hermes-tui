# Hermes TUI

Status: published through the qualified GitHub source channel at the exact
revision and CI run recorded in
[`release/github-source.json`](release/github-source.json).

Composable ledger operations presentation over Hermes's public clients. The
reusable `ledger.observe.v1` component combines status, usage, content-free operational
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
Disclosure, acknowledgement, and compaction guards are owned by Hermes. The
standalone shell invokes all three; isolated composable disclosure and
acknowledgement sheets also reuse owner guards without making presentation
their semantic owner.

The composable disclosure sheet proposes a bounded 1–16 event batch from the
selected stream's retained edge or selected consumer cursor, requires an
explicit sensitive-content arm/confirm step, and performs one owner-guarded
read. Results use list/detail navigation and immediately discard full payloads
after retaining metadata and at most a 256-byte preview per event. Payload
content never enters routine observation or telemetry, and
retention/publication gaps remain explicit.

The reusable Unix transport now belongs to the engine-owned
`hermes-local-client` crate. This repository's `hermes-tui-local-client` crate
is a compatibility re-export only; new shells, Workbench surfaces, and adapters
should depend on the owner client directly rather than importing transport from
a presentation package.

Acknowledgement is available as both a separate `ledger.acknowledge.v1` command
and an isolated composable two-step sheet. The sheet selects an exact range
from the current cursor through any observed published sequence, with
fine/coarse and bounds navigation. On confirmation it observes the exact
current cursor, compares it with the operator's expected position, and attempts
one exact-confirmed acknowledgement on the same connection. A post-send
transport failure is outcome-unknown and is never retried. The reusable
component still does not acquire `ConsumerClient`.

Compaction is isolated in `ledger.compact.v1`. Its composable sheet selects any
exact boundary in the observed retained range, shows the removal count, and
does not infer consumer safety from aggregate backlog. Its guard observes the
stream's retained frontier and published edge, compares both with the
operator's exact expectation, and then attempts one confirmed compaction on
the same connection. It never retries an uncertain result, and
`OperatorClient` remains outside the reusable observation component.

Standalone selection is explicit:

```text
hermes-tui headless /path/to/hermes.sock \
  --stream 32_HEXADECIMAL_DIGITS \
  --consumer 32_HEXADECIMAL_DIGITS

hermes-tui disclose-events /path/to/hermes.sock STREAM_HEX 0 4 \
  'DISCLOSE HERMES STREAM STREAM_HEX FROM 0 LIMIT 4'

hermes-tui acknowledge /path/to/hermes.sock CONSUMER_HEX STREAM_HEX 1 1 \
  'ACKNOWLEDGE HERMES CONSUMER CONSUMER_HEX STREAM STREAM_HEX FROM 1 THROUGH 1'

hermes-tui compact /path/to/hermes.sock STREAM_HEX 0 1 0 \
  'COMPACT HERMES STREAM STREAM_HEX RETAINED FROM 0 PUBLISHED EDGE 1 THROUGH 0'
```
