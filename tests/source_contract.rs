#[test]
fn reusable_component_is_read_only_and_application_free() {
    let source = include_str!("../src/lib.rs");
    assert!(!source.contains("AppendClient"));
    assert!(!source.contains("ConsumerClient"));
    assert!(!source.contains("OperatorClient"));
    assert!(source.contains("StreamObservationV1"));
    assert!(source.contains("configured_stream"));
    assert!(source.contains("configured_cursor"));
    assert!(source.contains(".cursor("));
    assert!(!source.contains("ratatui::run"));
    assert!(!source.contains("std::env::args"));
}

#[test]
fn acknowledgement_guard_stays_outside_the_reusable_read_only_component() {
    let command = include_str!("../../hermes/crates/hermes-consumer-command/src/lib.rs");
    let cli = include_str!("../cli/src/main.rs");
    let gate = include_str!("live/real_daemon.sh");
    for required in [
        "ledger.acknowledge.v1",
        "ConsumerClient",
        "CursorChanged",
        "MutationTransport",
        "ACKNOWLEDGE HERMES CONSUMER",
    ] {
        assert!(
            command.contains(required),
            "consumer guard omits {required}"
        );
    }
    assert!(cli.contains("hermes_consumer_command::acknowledge"));
    assert!(gate.contains("consumer_daemon_fixture"));
    assert!(gate.contains("previous_next_sequence"));
    assert!(gate.contains("next_sequence"));
}

#[test]
fn compaction_guard_stays_outside_the_reusable_read_only_component() {
    let command = include_str!("../../hermes/crates/hermes-operator-command/src/lib.rs");
    let cli = include_str!("../cli/src/main.rs");
    let gate = include_str!("live/real_daemon.sh");
    for required in [
        "ledger.compact.v1",
        "ReadOnlyClient",
        "OperatorClient",
        "StreamChanged",
        "MutationTransport",
        "COMPACT HERMES STREAM",
    ] {
        assert!(
            command.contains(required),
            "operator guard omits {required}"
        );
    }
    assert!(cli.contains("hermes_operator_command::compact"));
    assert!(gate.contains("compact-refused"));
    assert!(gate.contains("retained_from"));
}

#[test]
fn command_guards_are_owned_by_hermes_not_presentation() {
    let workspace = include_str!("../Cargo.toml");
    let cli = include_str!("../cli/Cargo.toml");
    for command in ["consumer-command", "disclosure-command", "operator-command"] {
        assert!(!workspace.contains(command));
        assert!(
            !std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(command)
                .exists()
        );
    }
    for command in [
        "../../hermes/crates/hermes-consumer-command",
        "../../hermes/crates/hermes-disclosure-command",
        "../../hermes/crates/hermes-operator-command",
    ] {
        assert!(cli.contains(command));
    }
}

#[test]
fn local_transport_is_owned_by_hermes_not_presentation() {
    let manifest = include_str!("../local-client/Cargo.toml");
    let facade = include_str!("../local-client/src/lib.rs");
    assert!(manifest.contains("hermes-local-client"));
    for leaked in ["hermes-client =", "hermes-protocol =", "hermes-wire ="] {
        assert!(!manifest.contains(leaked), "facade retains {leaked}");
    }
    assert!(facade.contains("pub use hermes_local_client::*;"));
    assert!(!facade.contains("UnixStream"));
    assert!(!facade.contains("encode_request"));
}
