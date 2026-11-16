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
   gaps explicitly, and never adds payloads to routine observation.
6. A separate consumer command now observes the exact current cursor and
   performs one exact-confirmed acknowledgement on the same connection. It
   never retries a post-send failure, and the reusable TUI remains read-only.
   Add append and compact only with exact confirmation, authorization,
   retention constraints, and truthful uncertainty recovery.
