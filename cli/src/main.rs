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
    if mode != "interactive" && mode != "headless" {
        return Err(usage().into());
    }
    let socket = match args.next() {
        Some(value) => std::path::PathBuf::from(value),
        _ if mode == "headless" => return failure("configuration_refused", usage()),
        _ => return Err(usage().into()),
    };
    let configured_stream = match (args.next(), args.next(), args.next()) {
        (None, None, None) => None,
        (Some(flag), Some(value), None) if flag == "--stream" => {
            match hermes_tui::parse_stream_id(&value.to_string_lossy()) {
                Some(stream) => Some(stream),
                None if mode == "headless" => {
                    return failure(
                        "configuration_refused",
                        "stream must be 32 hexadecimal digits",
                    );
                }
                None => return Err("stream must be 32 hexadecimal digits".into()),
            }
        }
        _ if mode == "headless" => return failure("configuration_refused", usage()),
        _ => return Err(usage().into()),
    };
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
        interactive(hermes_tui::Component::new_with_stream(
            hermes_tui_local_client::CONNECTION_SESSION,
            port,
            configured_stream,
        ))
    } else {
        headless(port, configured_stream)
    }
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
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client =
        hermes_client::ReadOnlyClient::new(hermes_tui_local_client::CONNECTION_SESSION, port);
    match hermes_tui::observe_configured(&mut client, configured_stream) {
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
                }))?
            );
            Ok(())
        }
        Err(error) => failure("unavailable", &error),
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
    "usage: hermes-tui capabilities | hermes-tui <interactive|headless> SOCKET [--stream STREAM_HEX]\nheadless emits one bounded versioned ledger.observe.v1 observation without constructing TUI state"
}

#[cfg(not(unix))]
fn main() {
    eprintln!("hermes-tui requires Unix");
    std::process::exit(1);
}
