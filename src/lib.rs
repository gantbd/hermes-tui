//! Composable, read-only Ratatui presentation over Hermes's public client.
#![forbid(unsafe_code)]

use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use hermes_client::{ReadOnlyClient, ServicePort};
use hermes_protocol::{
    OperationalSignalsV1, OutcomeV1, ReplyV1, SessionHandleV1, StatusV1, StreamIdV1,
    StreamObservationV1, UsageV1,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    text::Line,
    widgets::{Block, Borders, Paragraph},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityManifest {
    pub schema: u8,
    pub peer: &'static str,
    pub interactive: &'static [&'static str],
    pub headless: &'static [&'static str],
}

pub const CAPABILITIES: CapabilityManifest = CapabilityManifest {
    schema: 1,
    peer: "hermes",
    interactive: &["ledger.observe.v1"],
    headless: &["ledger.observe.v1"],
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Input {
    Refresh,
    Close,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    Running,
    CloseRequested,
}

pub fn translate_key(key: KeyEvent) -> Option<Input> {
    if key.kind != KeyEventKind::Press || key.modifiers != KeyModifiers::NONE {
        return None;
    }
    match key.code {
        KeyCode::Char('r') => Some(Input::Refresh),
        KeyCode::Char('q') => Some(Input::Close),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Observation {
    pub status: StatusV1,
    pub usage: UsageV1,
    pub signals: OperationalSignalsV1,
    pub configured_stream: Option<ConfiguredStreamObservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredStreamObservation {
    Missing,
    Current(StreamObservationV1),
}

struct WorkerReply(Result<Observation, String>);

pub struct Component {
    refresh: SyncSender<()>,
    replies: Receiver<WorkerReply>,
    pending: bool,
    observation: Option<Observation>,
    notice: String,
}

impl Component {
    pub fn new<P>(session: SessionHandleV1, port: P) -> Self
    where
        P: ServicePort + Send + 'static,
        P::Error: Send + 'static,
    {
        Self::new_with_stream(session, port, None)
    }

    pub fn new_with_stream<P>(
        session: SessionHandleV1,
        port: P,
        configured_stream: Option<StreamIdV1>,
    ) -> Self
    where
        P: ServicePort + Send + 'static,
        P::Error: Send + 'static,
    {
        let (tx, rx) = sync_channel(1);
        let (reply_tx, reply_rx) = sync_channel(1);
        std::thread::spawn(move || {
            let mut client = ReadOnlyClient::new(session, port);
            while rx.recv().is_ok() {
                let result = observe_configured(&mut client, configured_stream);
                if reply_tx.send(WorkerReply(result)).is_err() {
                    break;
                }
            }
        });
        let mut component = Self {
            refresh: tx,
            replies: reply_rx,
            pending: false,
            observation: None,
            notice: "Hermes ready; refresh queued".into(),
        };
        component.offer();
        component
    }

    pub fn poll(&mut self) {
        loop {
            match self.replies.try_recv() {
                Ok(WorkerReply(Ok(value))) => {
                    self.pending = false;
                    self.observation = Some(value);
                    self.notice = "Current ledger observation".into();
                }
                Ok(WorkerReply(Err(status))) => {
                    self.pending = false;
                    self.notice = status;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.pending = false;
                    self.notice = "Hermes worker unavailable".into();
                    break;
                }
            }
        }
    }

    pub fn update(&mut self, input: Input, focused: bool) -> Lifecycle {
        if !focused {
            return Lifecycle::Running;
        }
        match input {
            Input::Close => Lifecycle::CloseRequested,
            Input::Refresh => {
                self.offer();
                Lifecycle::Running
            }
        }
    }

    pub fn draw_in(&self, frame: &mut Frame<'_>, area: Rect, focused: bool) {
        let [body, footer] =
            Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(area);
        let title = if focused {
            " Hermes • focused "
        } else {
            " Hermes "
        };
        let lines = self.observation.map_or_else(
            || vec![Line::from("No current ledger observation")],
            |value| {
                let mut lines = vec![
                    Line::from(format!(
                        "storage revision: {:?}",
                        value.status.storage_revision
                    )),
                    Line::from(format!("fenced: {}", value.status.fenced)),
                    Line::from(format!("streams: {}", value.usage.streams)),
                    Line::from(format!("events: {}", value.usage.events)),
                    Line::from(format!("cursors: {}", value.usage.cursors)),
                    Line::from(format!("commits: {}", value.signals.commits)),
                    Line::from(format!("conflicts: {}", value.signals.conflicts)),
                ];
                match value.configured_stream {
                    None => lines.push(Line::from("configured stream: none")),
                    Some(ConfiguredStreamObservation::Missing) => {
                        lines.push(Line::from("configured stream: absent"));
                    }
                    Some(ConfiguredStreamObservation::Current(stream)) => {
                        lines.extend([
                            Line::from("configured stream: current"),
                            Line::from(format!(
                                "retained/published: {}/{}",
                                stream.retained_from.0,
                                stream
                                    .published_edge
                                    .map_or_else(|| "none".into(), |edge| edge.0.to_string())
                            )),
                            Line::from(format!("retained events: {}", stream.retained_events)),
                            Line::from(format!(
                                "consumers/stalled: {}/{}",
                                stream.consumer_count, stream.stalled_consumers
                            )),
                            Line::from(format!(
                                "max consumer backlog: {}",
                                stream.max_consumer_backlog
                            )),
                        ]);
                    }
                }
                lines
            },
        );
        frame.render_widget(
            Paragraph::new(lines).block(Block::default().title(title).borders(Borders::ALL)),
            body,
        );
        frame.render_widget(Paragraph::new(self.notice.as_str()), footer);
    }

    fn offer(&mut self) {
        if self.pending {
            self.notice = "Hermes refresh already pending".into();
        } else if self.refresh.try_send(()).is_ok() {
            self.pending = true;
            self.notice = "Hermes refresh pending".into();
        } else {
            self.notice = "Hermes worker unavailable".into();
        }
    }
}

pub fn observe<P: ServicePort>(client: &mut ReadOnlyClient<P>) -> Result<Observation, String> {
    observe_configured(client, None)
}

pub fn observe_configured<P: ServicePort>(
    client: &mut ReadOnlyClient<P>,
    configured_stream: Option<StreamIdV1>,
) -> Result<Observation, String> {
    let status = match client.status().map_err(|_| "Hermes status unavailable")? {
        ReplyV1::Accepted(OutcomeV1::Status(value)) => value,
        ReplyV1::Refused(value) => return Err(format!("Hermes refused: {value:?}")),
        _ => return Err("Invalid Hermes status reply".into()),
    };
    let usage = match client.usage().map_err(|_| "Hermes usage unavailable")? {
        ReplyV1::Accepted(OutcomeV1::Usage(value)) => value,
        ReplyV1::Refused(value) => return Err(format!("Hermes refused: {value:?}")),
        _ => return Err("Invalid Hermes usage reply".into()),
    };
    let signals = match client.signals().map_err(|_| "Hermes signals unavailable")? {
        ReplyV1::Accepted(OutcomeV1::Signals(value)) => value,
        ReplyV1::Refused(value) => return Err(format!("Hermes refused: {value:?}")),
        _ => return Err("Invalid Hermes signals reply".into()),
    };
    let configured_stream = match configured_stream {
        None => None,
        Some(stream) => match client
            .stream_observation(stream)
            .map_err(|_| "Hermes stream observation unavailable")?
        {
            ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(value))) => {
                Some(ConfiguredStreamObservation::Current(value))
            }
            ReplyV1::Accepted(OutcomeV1::StreamObservation(None)) => {
                Some(ConfiguredStreamObservation::Missing)
            }
            ReplyV1::Refused(value) => return Err(format!("Hermes refused: {value:?}")),
            _ => return Err("Invalid Hermes stream observation reply".into()),
        },
    };
    Ok(Observation {
        status,
        usage,
        signals,
        configured_stream,
    })
}

/// Parses one exact stream selector without assigning identity or exposing a
/// catalogue. The selected identifier remains configuration input.
pub fn parse_stream_id(value: &str) -> Option<StreamIdV1> {
    if value.len() != 32 {
        return None;
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(StreamIdV1(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_selector_is_exact_and_remains_opaque() {
        let stream = parse_stream_id("05050505050505050505050505050505").unwrap();
        assert_eq!(stream, StreamIdV1([5; 16]));
        assert!(parse_stream_id("05").is_none());
        assert!(parse_stream_id("gggggggggggggggggggggggggggggggg").is_none());
        assert_eq!(format!("{stream:?}"), "StreamIdV1(<opaque>)");
    }
}
