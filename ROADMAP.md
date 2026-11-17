# Hermes terminal roadmap

1. Qualify the reusable status/usage/signals component and standalone CLI.
2. Qualify both modes against the real durable Hermes daemon.
3. Compose interactive and headless attachment into Workbench.
4. Configured content-free stream health is qualified in the standalone CLI
   without inventing a catalogue or exposing the selected stream identifier.
   Workbench carries the same selector and observation. Hermes now exposes
   cursor observation through its read-only facade, and standalone/Workbench
   profiles carry an exact opaque consumer selector with current position,
   backlog, and behind-retention evidence. The TUI does not acquire
   `ConsumerClient`, which also owns acknowledgement mutation.
5. Exact-confirmed bounded event disclosure is qualified against the real
   daemon through a separate `DisclosureClient` and command. It caps batches at
   16 events plus the negotiated frame ceiling, reports retention/publication
   gaps explicitly, and never adds payloads to routine observation. The
   composable sheet exposes an operator-selected 1–16 event batch per explicit
   confirmation, list/detail result navigation, and at most a 256-byte payload
   preview per event.
6. A separate consumer command and isolated composable sheet now observe the
   exact current cursor and perform one exact-confirmed acknowledgement across
   an operator-selected range ending at or before the observed published edge.
   They never retry a post-send failure, and the reusable observation component
   remains read-only.
7. A separate operator command and composable range sheet now observe the exact
   retained frontier and published edge before performing one exact-confirmed
   compaction on the same connection. Fine/coarse/bounds movement covers the
   complete observed range; stale state prevents mutation and uncertain
   delivery is not retried. Add append only with an explicit domain envelope
   and equally strong authorization and uncertainty rules.
8. Disclosure, consumer, and operator guards live under Hermes ownership.
   `hermes-tui` retains presentation, isolated disclosure and acknowledgement
   workflows, its compatibility local client, and the standalone application
   shell; semantic mutation remains in the owner contracts.
