#![forbid(unsafe_code)]

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let Some(mode) = args.next() else {
        return Err(usage().into());
    };
    if mode == "--help" || mode == "-h" {
        println!("{}", usage());
        return Ok(());
    }
    if mode == "capabilities" {
        if args.next().is_some() {
            return Err(usage().into());
        }
        let value = hermes_tui::CAPABILITIES;
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"schema": value.schema, "peer": value.peer, "interactive": value.interactive, "headless": value.headless})
            )?
        );
        return Ok(());
    }
    if mode == "disclose-events" {
        return disclose_events(args);
    }
    if mode == "acknowledge" {
        return acknowledge(args);
    }
    if mode == "compact" {
        return compact(args);
    }
    if mode != "interactive" && mode != "headless" {
        return Err(usage().into());
    }
    let socket = match args.next() {
        Some(value) => std::path::PathBuf::from(value),
        _ if mode == "headless" => return failure("configuration_refused", usage()),
        _ => return Err(usage().into()),
    };
    let mut configured_stream = None;
    let mut configured_consumer = None;
    while let Some(flag) = args.next() {
        let Some(value) = args.next() else {
            return configuration_failure(&mode, usage());
        };
        if flag == "--stream" && configured_stream.is_none() {
            configured_stream = hermes_tui::parse_stream_id(&value.to_string_lossy());
            if configured_stream.is_none() {
                return configuration_failure(&mode, "stream must be 32 hexadecimal digits");
            }
        } else if flag == "--consumer" && configured_consumer.is_none() {
            configured_consumer = hermes_tui::parse_consumer_id(&value.to_string_lossy());
            if configured_consumer.is_none() {
                return configuration_failure(&mode, "consumer must be 32 hexadecimal digits");
            }
        } else {
            return configuration_failure(&mode, usage());
        }
    }
    if configured_consumer.is_some() && configured_stream.is_none() {
        return configuration_failure(&mode, "consumer requires stream");
    }
    let port = match hermes_tui_local_client::LocalPort::connect(
        socket,
        hermes_tui_local_client::ClientLimits::default(),
    ) {
        Ok(port) => port,
        Err(error) if mode == "headless" => {
            return failure("connect_refused", &format!("{error:?}"));
        }
        Err(error) => return Err(std::io::Error::other(format!("{error:?}")).into()),
    };
    if mode == "interactive" {
        interactive(hermes_tui::Component::new_with_selection(
            hermes_tui_local_client::CONNECTION_SESSION,
            port,
            configured_stream,
            configured_consumer,
        ))
    } else {
        headless(port, configured_stream, configured_consumer)
    }
}

#[cfg(unix)]
fn compact(
    mut args: impl Iterator<Item = std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = args
        .next()
        .map(std::path::PathBuf::from)
        .ok_or_else(usage)?;
    let stream = args
        .next()
        .and_then(|value| hermes_tui::parse_stream_id(&value.to_string_lossy()))
        .ok_or("stream must be 32 hexadecimal digits")?;
    let expected_retained_from = parse_sequence(args.next())?;
    let expected_published_edge = parse_sequence(args.next())?;
    let through = parse_sequence(args.next())?;
    let confirmation = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(usage)?;
    if args.next().is_some() {
        return Err(usage().into());
    }
    let port = match hermes_tui_local_client::LocalPort::connect(
        socket,
        hermes_tui_local_client::ClientLimits::default(),
    ) {
        Ok(port) => port,
        Err(error) => return compact_failure("not_sent", &format!("{error:?}"), None, None),
    };
    let request = hermes_operator_command::CompactRequest {
        stream,
        expected_retained_from: hermes_protocol::EventSequenceV1(expected_retained_from),
        expected_published_edge: hermes_protocol::EventSequenceV1(expected_published_edge),
        through: hermes_protocol::EventSequenceV1(through),
    };
    match hermes_operator_command::compact(
        hermes_tui_local_client::CONNECTION_SESSION,
        port,
        request,
        &confirmation,
    ) {
        Ok(outcome) => {
            let (status, removed, retained_from) = match outcome {
                hermes_protocol::CompactionOutcomeV1::Compacted {
                    removed,
                    retained_from,
                } => ("compacted", removed, retained_from.0),
                hermes_protocol::CompactionOutcomeV1::Identity { retained_from } => {
                    ("identity", 0, retained_from.0)
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "version": 1,
                    "peer": "hermes",
                    "capability": hermes_operator_command::CAPABILITY,
                    "complete": true,
                    "status": status,
                    "removed": removed,
                    "retained_from": retained_from,
                }))?
            );
            Ok(())
        }
        Err(hermes_operator_command::CompactError::ConfirmationMismatch { required }) => {
            compact_failure(
                "confirmation_refused",
                "exact compaction confirmation required",
                Some(&required),
                None,
            )
        }
        Err(hermes_operator_command::CompactError::InvalidRange) => compact_failure(
            "range_refused",
            "compaction boundary must be inside the observed retained range",
            None,
            None,
        ),
        Err(hermes_operator_command::CompactError::StreamMissing) => {
            compact_failure("stream_absent", "stream is absent", None, None)
        }
        Err(hermes_operator_command::CompactError::StreamChanged { actual }) => compact_failure(
            "stale_stream",
            "stream retention or publication edge changed",
            None,
            Some((
                actual.retained_from.0,
                actual.published_edge.map(|edge| edge.0),
            )),
        ),
        Err(hermes_operator_command::CompactError::MutationTransport(
            hermes_tui_local_client::ClientError::OutcomeUnknown(error),
        )) => compact_failure("outcome_unknown", &format!("{error:?}"), None, None),
        Err(hermes_operator_command::CompactError::MutationRefused(refusal)) => {
            compact_failure("refused", &format!("{refusal:?}"), None, None)
        }
        Err(error) => compact_failure("unavailable", &format!("{error:?}"), None, None),
    }
}

#[cfg(unix)]
fn parse_sequence(value: Option<std::ffi::OsString>) -> Result<u64, Box<dyn std::error::Error>> {
    value
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| usage().into())
}

#[cfg(unix)]
fn compact_failure(
    status: &str,
    message: &str,
    required_confirmation: Option<&str>,
    actual_stream: Option<(u64, Option<u64>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "version": 1,
            "peer": "hermes",
            "capability": hermes_operator_command::CAPABILITY,
            "complete": false,
            "status": status,
            "required_confirmation": required_confirmation,
            "actual_retained_from": actual_stream.map(|value| value.0),
            "actual_published_edge": actual_stream.and_then(|value| value.1),
        }))?
    );
    Err(std::io::Error::other(message).into())
}

#[cfg(unix)]
fn acknowledge(
    mut args: impl Iterator<Item = std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = args
        .next()
        .map(std::path::PathBuf::from)
        .ok_or_else(usage)?;
    let consumer = args
        .next()
        .and_then(|value| hermes_tui::parse_consumer_id(&value.to_string_lossy()))
        .ok_or("consumer must be 32 hexadecimal digits")?;
    let stream = args
        .next()
        .and_then(|value| hermes_tui::parse_stream_id(&value.to_string_lossy()))
        .ok_or("stream must be 32 hexadecimal digits")?;
    let expected_next = args
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(usage)?;
    let through = args
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(usage)?;
    let confirmation = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(usage)?;
    if args.next().is_some() {
        return Err(usage().into());
    }
    let port = match hermes_tui_local_client::LocalPort::connect(
        socket,
        hermes_tui_local_client::ClientLimits::default(),
    ) {
        Ok(port) => port,
        Err(error) => return acknowledge_failure("not_sent", &format!("{error:?}"), None, None),
    };
    let request = hermes_consumer_command::AcknowledgeRequest {
        consumer,
        stream,
        expected_next: hermes_protocol::EventSequenceV1(expected_next),
        through: hermes_protocol::EventSequenceV1(through),
    };
    match hermes_consumer_command::acknowledge(
        hermes_tui_local_client::CONNECTION_SESSION,
        port,
        request,
        &confirmation,
    ) {
        Ok(cursor) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "version": 1,
                    "peer": "hermes",
                    "capability": hermes_consumer_command::CAPABILITY,
                    "complete": true,
                    "status": "acknowledged",
                    "previous_next_sequence": expected_next,
                    "next_sequence": cursor.next_sequence.0,
                }))?
            );
            Ok(())
        }
        Err(hermes_consumer_command::AcknowledgeError::ConfirmationMismatch { required }) => {
            acknowledge_failure(
                "confirmation_refused",
                "exact acknowledgement confirmation required",
                Some(&required),
                None,
            )
        }
        Err(hermes_consumer_command::AcknowledgeError::InvalidRange) => acknowledge_failure(
            "range_refused",
            "through must not precede the observed next sequence",
            None,
            None,
        ),
        Err(hermes_consumer_command::AcknowledgeError::CursorMissing) => {
            acknowledge_failure("cursor_absent", "consumer cursor is absent", None, None)
        }
        Err(hermes_consumer_command::AcknowledgeError::CursorChanged { actual }) => {
            acknowledge_failure(
                "stale_cursor",
                "consumer cursor changed",
                None,
                Some(actual.0),
            )
        }
        Err(hermes_consumer_command::AcknowledgeError::MutationTransport(
            hermes_tui_local_client::ClientError::OutcomeUnknown(error),
        )) => acknowledge_failure("outcome_unknown", &format!("{error:?}"), None, None),
        Err(hermes_consumer_command::AcknowledgeError::MutationRefused(refusal)) => {
            acknowledge_failure("refused", &format!("{refusal:?}"), None, None)
        }
        Err(error) => acknowledge_failure("unavailable", &format!("{error:?}"), None, None),
    }
}

#[cfg(unix)]
fn acknowledge_failure(
    status: &str,
    message: &str,
    required_confirmation: Option<&str>,
    actual_next_sequence: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "version": 1,
            "peer": "hermes",
            "capability": hermes_consumer_command::CAPABILITY,
            "complete": false,
            "status": status,
            "required_confirmation": required_confirmation,
            "actual_next_sequence": actual_next_sequence,
        }))?
    );
    Err(std::io::Error::other(message).into())
}

#[cfg(unix)]
fn disclose_events(
    mut args: impl Iterator<Item = std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = args
        .next()
        .map(std::path::PathBuf::from)
        .ok_or_else(usage)?;
    let stream = args
        .next()
        .and_then(|value| hermes_tui::parse_stream_id(&value.to_string_lossy()))
        .ok_or("stream must be 32 hexadecimal digits")?;
    let start = args
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(usage)?;
    let limit = args
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(usage)?;
    let confirmation = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(usage)?;
    if args.next().is_some() {
        return Err(usage().into());
    }
    let port = match hermes_tui_local_client::LocalPort::connect(
        socket,
        hermes_tui_local_client::ClientLimits::default(),
    ) {
        Ok(port) => port,
        Err(error) => return disclosure_failure("not_sent", &format!("{error:?}"), None),
    };
    let request = hermes_disclosure_command::DisclosureRequest {
        stream,
        start: hermes_protocol::EventSequenceV1(start),
        limit,
    };
    match hermes_disclosure_command::disclose(
        hermes_tui_local_client::CONNECTION_SESSION,
        port,
        request,
        &confirmation,
    ) {
        Ok(batch) => {
            let events = batch
                .events
                .into_iter()
                .map(|event| {
                    serde_json::json!({
                        "sequence": event.sequence.0,
                        "producer": hermes_disclosure_command::hex(&event.envelope.producer.0),
                        "schema": hermes_disclosure_command::hex(&event.envelope.schema.0),
                        "idempotency_key": hermes_disclosure_command::hex(&event.envelope.idempotency_key.0),
                        "correlation": hermes_disclosure_command::hex(&event.envelope.correlation.0),
                        "causation": event.envelope.causation.map(|value| hermes_disclosure_command::hex(&value.0)),
                        "payload_hex": hermes_disclosure_command::hex(&event.envelope.payload),
                    })
                })
                .collect::<Vec<_>>();
            let gap = batch.gap.map(|gap| match gap {
                hermes_protocol::ReadGapV1::BeforeRetention {
                    requested,
                    retained_from,
                } => {
                    serde_json::json!({
                        "kind": "before_retention",
                        "requested": requested.0,
                        "retained_from": retained_from.0,
                    })
                }
                hermes_protocol::ReadGapV1::AheadOfPublication {
                    requested,
                    next_sequence,
                } => {
                    serde_json::json!({
                        "kind": "ahead_of_publication",
                        "requested": requested.0,
                        "next_sequence": next_sequence.0,
                    })
                }
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "version": 1,
                    "peer": "hermes",
                    "capability": hermes_disclosure_command::CAPABILITY,
                    "complete": true,
                    "status": "current",
                    "start": start,
                    "limit": limit,
                    "retained_from": batch.retained_from.0,
                    "published_edge": batch.published_edge.map(|value| value.0),
                    "gap": gap,
                    "events": events,
                }))?
            );
            Ok(())
        }
        Err(hermes_disclosure_command::DisclosureError::ConfirmationMismatch { required }) => {
            disclosure_failure(
                "confirmation_refused",
                "exact disclosure confirmation required",
                Some(&required),
            )
        }
        Err(hermes_disclosure_command::DisclosureError::InvalidLimit) => disclosure_failure(
            "limit_refused",
            "event limit must be between 1 and 16",
            None,
        ),
        Err(hermes_disclosure_command::DisclosureError::Transport(
            hermes_tui_local_client::ClientError::OutcomeUnknown(error),
        )) => disclosure_failure("outcome_unknown", &format!("{error:?}"), None),
        Err(hermes_disclosure_command::DisclosureError::Refused(refusal)) => {
            disclosure_failure("refused", &format!("{refusal:?}"), None)
        }
        Err(error) => disclosure_failure("unavailable", &format!("{error:?}"), None),
    }
}

#[cfg(unix)]
fn disclosure_failure(
    status: &str,
    message: &str,
    required_confirmation: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "version": 1,
            "peer": "hermes",
            "capability": hermes_disclosure_command::CAPABILITY,
            "complete": false,
            "status": status,
            "required_confirmation": required_confirmation,
        }))?
    );
    Err(std::io::Error::other(message).into())
}

#[cfg(unix)]
fn interactive(mut component: hermes_tui::Component) -> Result<(), Box<dyn std::error::Error>> {
    use crossterm::event::{self, Event};
    use std::time::Duration;
    ratatui::run(|terminal| -> Result<(), Box<dyn std::error::Error>> {
        loop {
            component.poll();
            terminal.draw(|frame| component.draw_in(frame, frame.area(), true))?;
            if !event::poll(Duration::from_millis(50))? {
                continue;
            }
            if let Event::Key(key) = event::read()?
                && let Some(input) = hermes_tui::translate_key(key)
                && component.update(input, true) == hermes_tui::Lifecycle::CloseRequested
            {
                break;
            }
        }
        Ok(())
    })
}

#[cfg(unix)]
fn headless(
    port: hermes_tui_local_client::LocalPort,
    configured_stream: Option<hermes_protocol::StreamIdV1>,
    configured_consumer: Option<hermes_protocol::ConsumerIdV1>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client =
        hermes_client::ReadOnlyClient::new(hermes_tui_local_client::CONNECTION_SESSION, port);
    match hermes_tui::observe_selected(&mut client, configured_stream, configured_consumer) {
        Ok(value) => {
            let manifest = hermes_tui::CAPABILITIES;
            let stream = value.configured_stream.map(|selection| match selection {
                hermes_tui::ConfiguredStreamObservation::Missing => {
                    serde_json::json!({"status": "absent"})
                }
                hermes_tui::ConfiguredStreamObservation::Current(stream) => serde_json::json!({
                    "status": "current", "retained_from": stream.retained_from.0,
                    "published_edge": stream.published_edge.map(|edge| edge.0),
                    "retained_events": stream.retained_events,
                    "consumer_count": stream.consumer_count,
                    "stalled_consumers": stream.stalled_consumers,
                    "max_consumer_backlog": stream.max_consumer_backlog,
                }),
            });
            let cursor = value.configured_cursor.map(|selection| match selection {
                hermes_tui::ConfiguredCursorObservation::Missing => {
                    serde_json::json!({"status": "absent"})
                }
                hermes_tui::ConfiguredCursorObservation::Current(cursor) => {
                    let stream = match value.configured_stream {
                        Some(hermes_tui::ConfiguredStreamObservation::Current(stream)) => stream,
                        _ => unreachable!("current cursor requires current stream"),
                    };
                    serde_json::json!({
                        "status": "current",
                        "next_sequence": cursor.next_sequence.0,
                        "backlog": hermes_tui::cursor_backlog(stream, cursor),
                        "behind_retention": cursor.next_sequence < stream.retained_from,
                    })
                }
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "version": 1, "mode": "headless", "complete": true,
                    "peer": manifest.peer, "capability": manifest.headless[0], "status": "current",
                    "storage_revision": value.status.storage_revision, "fenced": value.status.fenced,
                    "streams": value.usage.streams, "events": value.usage.events,
                    "cursors": value.usage.cursors, "commits": value.signals.commits,
                    "conflicts": value.signals.conflicts, "saturated": value.signals.saturated,
                    "configured_stream": stream,
                    "configured_cursor": cursor,
                }))?
            );
            Ok(())
        }
        Err(error) => failure("unavailable", &error),
    }
}

#[cfg(unix)]
fn configuration_failure(
    mode: &std::ffi::OsStr,
    message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if mode == "headless" {
        failure("configuration_refused", message)
    } else {
        Err(message.into())
    }
}

#[cfg(unix)]
fn failure(status: &str, message: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = hermes_tui::CAPABILITIES;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "version": 1, "mode": "headless", "complete": false,
            "peer": manifest.peer, "capability": manifest.headless[0], "status": status,
        }))?
    );
    Err(std::io::Error::other(message).into())
}

fn usage() -> &'static str {
    "usage: hermes-tui capabilities | hermes-tui <interactive|headless> SOCKET [--stream STREAM_HEX] [--consumer CONSUMER_HEX] | hermes-tui disclose-events SOCKET STREAM_HEX START LIMIT 'DISCLOSE HERMES STREAM STREAM_HEX FROM START LIMIT LIMIT' | hermes-tui acknowledge SOCKET CONSUMER_HEX STREAM_HEX EXPECTED_NEXT THROUGH 'ACKNOWLEDGE HERMES CONSUMER CONSUMER_HEX STREAM STREAM_HEX FROM EXPECTED_NEXT THROUGH THROUGH' | hermes-tui compact SOCKET STREAM_HEX EXPECTED_RETAINED_FROM EXPECTED_PUBLISHED_EDGE THROUGH 'COMPACT HERMES STREAM STREAM_HEX RETAINED FROM EXPECTED_RETAINED_FROM PUBLISHED EDGE EXPECTED_PUBLISHED_EDGE THROUGH THROUGH'\nheadless emits one bounded versioned ledger.observe.v1 observation without constructing TUI state; consumer requires stream; disclosure, acknowledgement and compaction require exact confirmation; mutations observe their current state and attempt once on the same connection"
}

#[cfg(not(unix))]
fn main() {
    eprintln!("hermes-tui requires Unix");
    std::process::exit(1);
}
