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
