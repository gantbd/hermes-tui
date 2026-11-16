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
    let command = include_str!("../consumer-command/src/lib.rs");
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
    let command = include_str!("../operator-command/src/lib.rs");
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
