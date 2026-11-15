# Hermes terminal roadmap

1. Qualify the reusable status/usage/signals component and standalone CLI.
2. Qualify both modes against the real durable Hermes daemon.
3. Compose interactive and headless attachment into Workbench.
4. Configured content-free stream health is qualified in the standalone CLI
   without inventing a catalogue or exposing the selected stream identifier.
   Workbench selector parity and consumer cursor/gap views remain open until
   their explicit configured selectors are composed.
5. Add append, acknowledge, and compact only with exact confirmation,
   authorization, retention constraints, and truthful uncertainty recovery.
