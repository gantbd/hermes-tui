//! Compatibility facade for the engine-owned Hermes local client.
//!
//! New non-presentation consumers should depend on `hermes-local-client`
//! directly. This crate remains so existing Hermes TUI and downstream callers
//! retain source compatibility while transport ownership moves out of the
//! presentation repository.

#![forbid(unsafe_code)]

pub use hermes_local_client::*;
