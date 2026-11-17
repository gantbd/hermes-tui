//! Composable Ratatui observation and isolated guarded control over Hermes public clients.
#![forbid(unsafe_code)]

mod acknowledge_control;
mod compaction_control;
mod disclosure_control;

pub use acknowledge_control::{
    AcknowledgeComponent, AcknowledgeDisposition, AcknowledgeInput, AcknowledgeSelection,
    translate_acknowledge_key,
};
pub use compaction_control::{
    CompactionComponent, CompactionDisposition, CompactionInput, CompactionSelection,
    translate_compaction_key,
};
pub use disclosure_control::{
    DISCLOSURE_BATCH_LIMIT, DisclosureComponent, DisclosureDisposition, DisclosureEventView,
    DisclosureGapView, DisclosureInput, DisclosureSelection, DisclosureView,
    translate_disclosure_key,
};

use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use hermes_client::{ReadOnlyClient, ServicePort};
use hermes_protocol::{
    ConsumerCursorV1, ConsumerIdV1, OperationalSignalsV1, OutcomeV1, ReplyV1, SessionHandleV1,
    StatusV1, StreamIdV1, StreamObservationV1, UsageV1,
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
    interactive: &[
        "ledger.observe.v1",
        "ledger.cursor.observe.v1",
        "ledger.events.disclose.v1",
        "ledger.acknowledge.v1",
        "ledger.compact.v1",
    ],
    headless: &["ledger.observe.v1", "ledger.cursor.observe.v1"],
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
    pub configured_cursor: Option<ConfiguredCursorObservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredStreamObservation {
    Missing,
    Current(StreamObservationV1),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredCursorObservation {
    Missing,
    Current(ConsumerCursorV1),
}

struct WorkerReply(Result<Observation, String>);

pub struct Component {
    refresh: SyncSender<()>,
    replies: Receiver<WorkerReply>,
    pending: bool,
    observation: Option<Observation>,
    notice: String,
    configured_stream: Option<StreamIdV1>,
    configured_consumer: Option<ConsumerIdV1>,
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
        Self::new_with_selection(session, port, configured_stream, None)
    }

    pub fn new_with_selection<P>(
        session: SessionHandleV1,
        port: P,
        configured_stream: Option<StreamIdV1>,
        configured_consumer: Option<ConsumerIdV1>,
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
                let result = observe_selected(&mut client, configured_stream, configured_consumer);
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
            configured_stream,
            configured_consumer,
        };
        component.offer();
        component
    }

    pub fn poll(&mut self) {
        let _ = self.poll_settlement();
    }

    pub fn poll_settlement(&mut self) -> Option<Result<(), ()>> {
        let mut settlement = None;
        loop {
            match self.replies.try_recv() {
                Ok(WorkerReply(Ok(value))) => {
                    self.pending = false;
                    self.observation = Some(value);
                    self.notice = "Current ledger observation".into();
                    settlement = Some(Ok(()));
                }
                Ok(WorkerReply(Err(status))) => {
                    self.pending = false;
                    self.notice = status;
                    settlement = Some(Err(()));
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if self.pending {
                        settlement = Some(Err(()));
                    }
                    self.pending = false;
                    self.notice = "Hermes worker unavailable".into();
                    break;
                }
            }
        }
        settlement
    }

    pub const fn request_pending(&self) -> bool {
        self.pending
    }

    /// Proposes one exact, minimal cursor advance from the current projection.
    /// The owner guard re-observes this cursor on the mutation connection.
    pub fn next_acknowledgement(&self) -> Option<AcknowledgeSelection> {
        next_acknowledgement_from(
            self.observation?,
            self.configured_stream?,
            self.configured_consumer?,
        )
    }

    /// Proposes a bounded event batch from the selected stream. If a current
    /// consumer is selected, its cursor is the natural start; otherwise
    /// disclosure begins at the retained edge reported by the owner.
    pub fn next_disclosure(&self) -> Option<DisclosureSelection> {
        next_disclosure_from(self.observation?, self.configured_stream?)
    }

    /// Proposes an explicit exact-state administrative compaction range.
    /// The presentation never infers a safe watermark from aggregate consumer
    /// backlog; the operator must select the destructive boundary.
    pub fn next_compaction(&self) -> Option<CompactionSelection> {
        next_compaction_from(self.observation?, self.configured_stream?)
    }
}

fn next_compaction_from(
    observation: Observation,
    stream: StreamIdV1,
) -> Option<CompactionSelection> {
    let ConfiguredStreamObservation::Current(stream_observation) = observation.configured_stream?
    else {
        return None;
    };
    let published_edge = stream_observation.published_edge?;
    (stream_observation.retained_from <= published_edge).then_some(CompactionSelection {
        stream,
        retained_from: stream_observation.retained_from,
        published_edge,
    })
}

fn next_disclosure_from(
    observation: Observation,
    stream: StreamIdV1,
) -> Option<DisclosureSelection> {
    let ConfiguredStreamObservation::Current(stream_observation) = observation.configured_stream?
    else {
        return None;
    };
    let start = match observation.configured_cursor {
        Some(ConfiguredCursorObservation::Current(cursor)) => cursor.next_sequence,
        _ => stream_observation.retained_from,
    };
    let published_edge = stream_observation.published_edge?;
    (start <= published_edge).then_some(DisclosureSelection {
        stream,
        start,
        max_events: published_edge
            .0
            .saturating_sub(start.0)
            .saturating_add(1)
            .min(DISCLOSURE_BATCH_LIMIT),
    })
}

fn next_acknowledgement_from(
    observation: Observation,
    stream: StreamIdV1,
    consumer: ConsumerIdV1,
) -> Option<AcknowledgeSelection> {
    let ConfiguredCursorObservation::Current(cursor) = observation.configured_cursor? else {
        return None;
    };
    let ConfiguredStreamObservation::Current(stream_observation) = observation.configured_stream?
    else {
        return None;
    };
    let published_edge = stream_observation.published_edge?;
    (cursor.next_sequence <= published_edge).then_some(AcknowledgeSelection {
        consumer,
        stream,
        expected_next: cursor.next_sequence,
        published_edge,
    })
}

impl Component {
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
                match value.configured_cursor {
                    None => lines.push(Line::from("configured consumer: none")),
                    Some(ConfiguredCursorObservation::Missing) => {
                        lines.push(Line::from("configured consumer cursor: absent"));
                    }
                    Some(ConfiguredCursorObservation::Current(cursor)) => {
                        lines.push(Line::from("configured consumer cursor: current"));
                        lines.push(Line::from(format!(
                            "next sequence: {}",
                            cursor.next_sequence.0
                        )));
                        if let Some(ConfiguredStreamObservation::Current(stream)) =
                            value.configured_stream
                        {
                            lines.push(Line::from(format!(
                                "cursor backlog: {}{}",
                                cursor_backlog(stream, cursor),
                                if cursor.next_sequence < stream.retained_from {
                                    " (behind retention)"
                                } else {
                                    ""
                                }
                            )));
                        }
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
    observe_selected(client, configured_stream, None)
}

pub fn observe_selected<P: ServicePort>(
    client: &mut ReadOnlyClient<P>,
    configured_stream: Option<StreamIdV1>,
    configured_consumer: Option<ConsumerIdV1>,
) -> Result<Observation, String> {
    if configured_consumer.is_some() && configured_stream.is_none() {
        return Err("Hermes consumer selector requires a stream selector".into());
    }
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
    let selected_stream = configured_stream;
    let configured_stream = match selected_stream {
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
    let configured_cursor = match (configured_consumer, configured_stream) {
        (None, _) => None,
        (Some(consumer), Some(ConfiguredStreamObservation::Current(_))) => match client
            .cursor(consumer, selected_stream.expect("selected stream"))
            .map_err(|_| "Hermes cursor observation unavailable")?
        {
            ReplyV1::Accepted(OutcomeV1::Cursor(Some(value))) => {
                Some(ConfiguredCursorObservation::Current(value))
            }
            ReplyV1::Accepted(OutcomeV1::Cursor(None)) => {
                Some(ConfiguredCursorObservation::Missing)
            }
            ReplyV1::Refused(value) => return Err(format!("Hermes refused: {value:?}")),
            _ => return Err("Invalid Hermes cursor observation reply".into()),
        },
        (Some(_), Some(ConfiguredStreamObservation::Missing)) => {
            Some(ConfiguredCursorObservation::Missing)
        }
        (Some(_), None) => unreachable!("consumer without stream refused above"),
    };
    Ok(Observation {
        status,
        usage,
        signals,
        configured_stream,
        configured_cursor,
    })
}

pub fn cursor_backlog(stream: StreamObservationV1, cursor: ConsumerCursorV1) -> u64 {
    stream
        .published_edge
        .and_then(|edge| edge.0.checked_add(1))
        .unwrap_or(stream.retained_from.0)
        .saturating_sub(cursor.next_sequence.0)
}

/// Parses one exact stream selector without assigning identity or exposing a
/// catalogue. The selected identifier remains configuration input.
pub fn parse_stream_id(value: &str) -> Option<StreamIdV1> {
    parse_opaque_id(value).map(StreamIdV1)
}

pub fn parse_consumer_id(value: &str) -> Option<ConsumerIdV1> {
    parse_opaque_id(value).map(ConsumerIdV1)
}

fn parse_opaque_id(value: &str) -> Option<[u8; 16]> {
    if value.len() != 32 {
        return None;
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(bytes)
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
        let consumer = parse_consumer_id("06060606060606060606060606060606").unwrap();
        assert_eq!(consumer, ConsumerIdV1([6; 16]));
        assert_eq!(format!("{consumer:?}"), "ConsumerIdV1(<opaque>)");
    }

    #[test]
    fn range_proposals_use_only_current_available_events() {
        let stream = StreamIdV1([5; 16]);
        let consumer = ConsumerIdV1([6; 16]);
        let usage = UsageV1 {
            streams: 1,
            events: 3,
            idempotency_records: 0,
            compacted_records: 0,
            cursors: 1,
            saturated: false,
        };
        let observation = Observation {
            status: StatusV1 {
                storage_revision: Some(1),
                fenced: false,
            },
            usage,
            signals: OperationalSignalsV1 {
                offers: [0; 5],
                residence_nanos: [0; 5],
                domain_refusals: 0,
                admission_refusals: 0,
                replays: 0,
                conflicts: 0,
                commits: 0,
                commit_failures: 0,
                commit_time_nanos: 0,
                confirmed_usage: usage,
                last_checkpoint_bytes: 0,
                fenced: false,
                saturated: false,
            },
            configured_stream: Some(ConfiguredStreamObservation::Current(StreamObservationV1 {
                retained_from: hermes_protocol::EventSequenceV1(0),
                published_edge: Some(hermes_protocol::EventSequenceV1(2)),
                retained_events: 3,
                consumer_count: 1,
                stalled_consumers: 0,
                max_consumer_backlog: 2,
            })),
            configured_cursor: Some(ConfiguredCursorObservation::Current(ConsumerCursorV1 {
                consumer,
                stream,
                next_sequence: hermes_protocol::EventSequenceV1(1),
            })),
        };
        assert_eq!(
            next_acknowledgement_from(observation, stream, consumer),
            Some(AcknowledgeSelection {
                consumer,
                stream,
                expected_next: hermes_protocol::EventSequenceV1(1),
                published_edge: hermes_protocol::EventSequenceV1(2),
            })
        );
        let drained = Observation {
            configured_cursor: Some(ConfiguredCursorObservation::Current(ConsumerCursorV1 {
                consumer,
                stream,
                next_sequence: hermes_protocol::EventSequenceV1(3),
            })),
            ..observation
        };
        assert_eq!(next_acknowledgement_from(drained, stream, consumer), None);
        assert_eq!(
            next_disclosure_from(observation, stream),
            Some(DisclosureSelection {
                stream,
                start: hermes_protocol::EventSequenceV1(1),
                max_events: 2,
            })
        );
        assert_eq!(next_disclosure_from(drained, stream), None);
        let without_consumer = Observation {
            configured_cursor: None,
            ..observation
        };
        assert_eq!(
            next_disclosure_from(without_consumer, stream),
            Some(DisclosureSelection {
                stream,
                start: hermes_protocol::EventSequenceV1(0),
                max_events: 3,
            })
        );
        assert_eq!(
            next_compaction_from(observation, stream),
            Some(CompactionSelection {
                stream,
                retained_from: hermes_protocol::EventSequenceV1(0),
                published_edge: hermes_protocol::EventSequenceV1(2),
            })
        );
    }
}
