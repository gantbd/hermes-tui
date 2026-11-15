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
   `ConsumerClient`, which also owns acknowledgement mutation. Event gap views
   remain open pending an explicitly bounded content-disclosure contract rather
   than importing event reads into the presentation.
5. Add append, acknowledge, and compact only with exact confirmation,
   authorization, retention constraints, and truthful uncertainty recovery.
