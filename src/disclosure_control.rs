//! Explicit, bounded Hermes event disclosure workflow.

use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use hermes_client::ServicePort;
use hermes_disclosure_command::{DisclosureError, DisclosureRequest, disclose, hex};
use hermes_protocol::{ReadBatchV1, ReadGapV1, SessionHandleV1};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::Lifecycle;

const PREVIEW_BYTES: usize = 256;
pub const DISCLOSURE_BATCH_LIMIT: u64 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DisclosureSelection {
    pub stream: hermes_protocol::StreamIdV1,
    pub start: hermes_protocol::EventSequenceV1,
    pub max_events: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DisclosureInput {
    Fewer,
    More,
    Minimum,
    Maximum,
    PreviousEvent,
    NextEvent,
    Arm,
    Confirm,
    Close,
}

pub fn translate_disclosure_key(key: KeyEvent) -> Option<DisclosureInput> {
    if key.kind != KeyEventKind::Press || key.modifiers != KeyModifiers::NONE {
        return None;
    }
    match key.code {
        KeyCode::Left => Some(DisclosureInput::Fewer),
        KeyCode::Right => Some(DisclosureInput::More),
        KeyCode::Home => Some(DisclosureInput::Minimum),
        KeyCode::End => Some(DisclosureInput::Maximum),
        KeyCode::Up => Some(DisclosureInput::PreviousEvent),
        KeyCode::Down => Some(DisclosureInput::NextEvent),
        KeyCode::Char('c') => Some(DisclosureInput::Arm),
        KeyCode::Enter => Some(DisclosureInput::Confirm),
        KeyCode::Esc => Some(DisclosureInput::Close),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DisclosureDisposition {
    Proposed,
    Armed,
    Pending,
    Disclosed,
    Refused,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DisclosureGapView {
    BeforeRetention { requested: u64, retained_from: u64 },
    AheadOfPublication { requested: u64, next_sequence: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisclosureEventView {
    pub sequence: u64,
    pub producer: String,
    pub schema: String,
    pub correlation: String,
    pub causation: Option<String>,
    pub payload_bytes: usize,
    pub payload_preview_hex: String,
    pub payload_preview_truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisclosureView {
    pub retained_from: u64,
    pub published_edge: Option<u64>,
    pub gap: Option<DisclosureGapView>,
    pub events: Vec<DisclosureEventView>,
}

struct WorkerReply {
    disposition: DisclosureDisposition,
    notice: String,
    view: Option<DisclosureView>,
}

pub struct DisclosureComponent {
    submit: SyncSender<DisclosureRequest>,
    replies: Receiver<WorkerReply>,
    selection: DisclosureSelection,
    limit: u64,
    selected_event: usize,
    disposition: DisclosureDisposition,
    notice: String,
    view: Option<DisclosureView>,
}

impl DisclosureComponent {
    pub fn new_with_connector<P, F, C>(connector: F, selection: DisclosureSelection) -> Self
    where
        P: ServicePort + Send + 'static,
        P::Error: std::fmt::Debug + Send + 'static,
        F: FnOnce() -> Result<(SessionHandleV1, P), C> + Send + 'static,
        C: std::fmt::Debug + Send + 'static,
    {
        let (submit_tx, submit_rx) = sync_channel::<DisclosureRequest>(1);
        let (reply_tx, reply_rx) = sync_channel(1);
        std::thread::spawn(move || {
            let Ok(request) = submit_rx.recv() else {
                return;
            };
            let reply = match connector() {
                Ok((session, port)) => {
                    let confirmation = request.confirmation();
                    classify(disclose(session, port, request, &confirmation))
                }
                Err(error) => WorkerReply {
                    disposition: DisclosureDisposition::Failed,
                    notice: format!("Hermes disclosure connection refused: {error:?}"),
                    view: None,
                },
            };
            let _ = reply_tx.send(reply);
        });
        Self {
            submit: submit_tx,
            replies: reply_rx,
            selection,
            limit: 1,
            selected_event: 0,
            disposition: DisclosureDisposition::Proposed,
            notice: "Select a bounded batch; c arms, Enter discloses once".into(),
            view: None,
        }
    }

    fn request(&self) -> DisclosureRequest {
        DisclosureRequest {
            stream: self.selection.stream,
            start: self.selection.start,
            limit: self.limit,
        }
    }

    pub fn poll(&mut self) {
        match self.replies.try_recv() {
            Ok(reply) => {
                self.disposition = reply.disposition;
                self.notice = reply.notice;
                self.view = reply.view;
                self.selected_event = 0;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected)
                if self.disposition == DisclosureDisposition::Pending =>
            {
                self.disposition = DisclosureDisposition::Failed;
                self.notice = "Hermes disclosure worker disconnected; no content retained".into();
            }
            Err(TryRecvError::Disconnected) => {}
        }
    }

    pub fn update(&mut self, input: DisclosureInput, focused: bool) -> Lifecycle {
        if !focused {
            return Lifecycle::Running;
        }
        if input == DisclosureInput::Close {
            if self.disposition == DisclosureDisposition::Pending {
                self.notice = "Disclosure request is pending; wait for the bounded read".into();
                return Lifecycle::Running;
            }
            return Lifecycle::CloseRequested;
        }
        match (self.disposition, input) {
            (DisclosureDisposition::Proposed, DisclosureInput::Fewer) => {
                self.limit = self.limit.saturating_sub(1).max(1);
                self.limit_notice();
            }
            (DisclosureDisposition::Proposed, DisclosureInput::More) => {
                self.limit = self.limit.saturating_add(1).min(self.selection.max_events);
                self.limit_notice();
            }
            (DisclosureDisposition::Proposed, DisclosureInput::Minimum) => {
                self.limit = 1;
                self.limit_notice();
            }
            (DisclosureDisposition::Proposed, DisclosureInput::Maximum) => {
                self.limit = self.selection.max_events;
                self.limit_notice();
            }
            (DisclosureDisposition::Proposed, DisclosureInput::Arm) => {
                self.disposition = DisclosureDisposition::Armed;
                self.notice = format!(
                    "ARMED: Enter reveals up to {} events with bounded previews",
                    self.limit
                );
            }
            (DisclosureDisposition::Armed, DisclosureInput::Confirm) => {
                if self.submit.try_send(self.request()).is_ok() {
                    self.disposition = DisclosureDisposition::Pending;
                    self.notice = format!(
                        "Hermes disclosure pending; {} event(s) requested",
                        self.limit
                    );
                } else {
                    self.disposition = DisclosureDisposition::Failed;
                    self.notice = "Hermes disclosure worker unavailable before dispatch".into();
                }
            }
            (DisclosureDisposition::Armed, _) => {
                self.disposition = DisclosureDisposition::Proposed;
                self.notice = "Hermes disclosure disarmed".into();
            }
            (DisclosureDisposition::Disclosed, DisclosureInput::PreviousEvent) => {
                self.selected_event = self.selected_event.saturating_sub(1);
            }
            (DisclosureDisposition::Disclosed, DisclosureInput::NextEvent) => {
                if let Some(view) = &self.view {
                    self.selected_event = self
                        .selected_event
                        .saturating_add(1)
                        .min(view.events.len().saturating_sub(1));
                }
            }
            _ => {}
        }
        Lifecycle::Running
    }

    fn limit_notice(&mut self) {
        self.notice = format!(
            "Batch limit {} of {}; payload previews remain 256 bytes each",
            self.limit, self.selection.max_events
        );
    }

    pub fn draw_in(&self, frame: &mut Frame<'_>, area: Rect, focused: bool) {
        let [body, footer] =
            Layout::vertical([Constraint::Min(1), Constraint::Length(2)]).areas(area);
        let title = if focused {
            " Hermes event disclosure • focused "
        } else {
            " Hermes event disclosure "
        };
        let warning = Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD);
        let mut lines = vec![
            Line::styled("SENSITIVE LEDGER CONTENT", warning),
            Line::from(format!(
                "start/limit: {}/{} (available maximum {})",
                self.selection.start.0, self.limit, self.selection.max_events
            )),
            Line::from(format!(
                "exact confirmation: {}",
                self.request().confirmation()
            )),
            Line::from("Payload content never enters activity receipts or telemetry."),
            Line::from(format!("state: {:?}", self.disposition)),
        ];
        if let Some(view) = &self.view {
            lines.push(Line::from(format!(
                "retained/published: {}/{}",
                view.retained_from,
                view.published_edge
                    .map_or_else(|| "none".into(), |v| v.to_string())
            )));
            if let Some(gap) = &view.gap {
                lines.push(Line::from(format!("gap: {gap:?}")));
            }
            if let Some(event) = view.events.get(self.selected_event) {
                lines.extend([
                    Line::from(format!(
                        "selected event: {} of {} • sequences: {}",
                        self.selected_event.saturating_add(1),
                        view.events.len(),
                        view.events
                            .iter()
                            .map(|event| event.sequence.to_string())
                            .collect::<Vec<_>>()
                            .join(" · ")
                    )),
                    Line::from(format!("event sequence: {}", event.sequence)),
                    Line::from(format!(
                        "producer/schema: {}/{}",
                        event.producer, event.schema
                    )),
                    Line::from(format!("correlation: {}", event.correlation)),
                    Line::from(format!("causation: {:?}", event.causation)),
                    Line::from(format!(
                        "payload: {} bytes{}",
                        event.payload_bytes,
                        if event.payload_preview_truncated {
                            " (preview truncated)"
                        } else {
                            ""
                        }
                    )),
                    Line::from(format!("preview hex: {}", event.payload_preview_hex)),
                ]);
            } else {
                lines.push(Line::from("No event disclosed at the requested sequence"));
            }
        }
        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::ALL).title(title)),
            body,
        );
        frame.render_widget(
            Paragraph::new(format!(
                "{} • ←/→ batch • Home/End min/max • ↑/↓ disclosed event • c arm • Enter confirm • Esc close",
                self.notice
            )),
            footer,
        );
    }

    pub const fn disposition(&self) -> DisclosureDisposition {
        self.disposition
    }

    pub fn notice(&self) -> &str {
        &self.notice
    }

    pub const fn view(&self) -> Option<&DisclosureView> {
        self.view.as_ref()
    }

    pub const fn limit(&self) -> u64 {
        self.limit
    }
}

fn classify<E: std::fmt::Debug>(result: Result<ReadBatchV1, DisclosureError<E>>) -> WorkerReply {
    match result {
        Ok(batch) => WorkerReply {
            disposition: DisclosureDisposition::Disclosed,
            notice: format!("Hermes disclosed {} bounded event(s)", batch.events.len()),
            view: Some(reduce(batch)),
        },
        Err(DisclosureError::Refused(error)) => WorkerReply {
            disposition: DisclosureDisposition::Refused,
            notice: format!("Hermes disclosure refused: {error:?}"),
            view: None,
        },
        Err(error) => WorkerReply {
            disposition: DisclosureDisposition::Failed,
            notice: format!("Hermes disclosure unavailable: {error:?}"),
            view: None,
        },
    }
}

fn reduce(batch: ReadBatchV1) -> DisclosureView {
    let events = batch
        .events
        .into_iter()
        .map(|event| {
            let payload_bytes = event.envelope.payload.len();
            let preview_bytes = payload_bytes.min(PREVIEW_BYTES);
            DisclosureEventView {
                sequence: event.sequence.0,
                producer: hex(&event.envelope.producer.0),
                schema: hex(&event.envelope.schema.0),
                correlation: hex(&event.envelope.correlation.0),
                causation: event.envelope.causation.map(|value| hex(&value.0)),
                payload_bytes,
                payload_preview_hex: hex(&event.envelope.payload[..preview_bytes]),
                payload_preview_truncated: preview_bytes < payload_bytes,
            }
        })
        .collect();
    let gap = batch.gap.map(|gap| match gap {
        ReadGapV1::BeforeRetention {
            requested,
            retained_from,
        } => DisclosureGapView::BeforeRetention {
            requested: requested.0,
            retained_from: retained_from.0,
        },
        ReadGapV1::AheadOfPublication {
            requested,
            next_sequence,
        } => DisclosureGapView::AheadOfPublication {
            requested: requested.0,
            next_sequence: next_sequence.0,
        },
    });
    DisclosureView {
        retained_from: batch.retained_from.0,
        published_edge: batch.published_edge.map(|value| value.0),
        gap,
        events,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use hermes_protocol::{
        CausationIdV1, CommandV1, CorrelationIdV1, EventEnvelopeV1, EventSequenceV1,
        IdempotencyKeyV1, OutcomeV1, ProducerIdV1, ReplyV1, SchemaIdV1, StoredEventV1, StreamIdV1,
    };

    use super::*;

    struct Port {
        commands: Arc<Mutex<Vec<CommandV1>>>,
        reply: Result<ReplyV1, &'static str>,
    }

    impl ServicePort for Port {
        type Error = &'static str;
        fn execute(
            &mut self,
            _: SessionHandleV1,
            command: CommandV1,
        ) -> Result<ReplyV1, Self::Error> {
            self.commands.lock().unwrap().push(command);
            self.reply.clone()
        }
    }

    fn selection() -> DisclosureSelection {
        DisclosureSelection {
            stream: StreamIdV1([5; 16]),
            start: EventSequenceV1(3),
            max_events: 6,
        }
    }

    fn batch(payload: Vec<u8>) -> ReadBatchV1 {
        ReadBatchV1 {
            events: (0..3)
                .map(|offset| StoredEventV1 {
                    sequence: EventSequenceV1(5 + offset),
                    envelope: EventEnvelopeV1 {
                        producer: ProducerIdV1([1; 16]),
                        schema: SchemaIdV1([2; 16]),
                        idempotency_key: IdempotencyKeyV1([3; 16]),
                        correlation: CorrelationIdV1([4; 16]),
                        causation: Some(CausationIdV1([6; 16])),
                        payload: payload.clone(),
                    },
                })
                .collect(),
            retained_from: EventSequenceV1(5),
            published_edge: Some(EventSequenceV1(8)),
            gap: Some(ReadGapV1::BeforeRetention {
                requested: EventSequenceV1(3),
                retained_from: EventSequenceV1(5),
            }),
        }
    }

    fn settle(component: &mut DisclosureComponent) {
        for _ in 0..1_000 {
            component.poll();
            if component.disposition() != DisclosureDisposition::Pending {
                return;
            }
            std::thread::yield_now();
        }
        panic!("disclosure worker did not settle");
    }

    #[test]
    fn exact_two_step_disclosure_reads_selected_bounded_batch_once() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let worker_commands = Arc::clone(&commands);
        let mut component = DisclosureComponent::new_with_connector(
            move || {
                Ok::<_, &'static str>((
                    SessionHandleV1(0),
                    Port {
                        commands: worker_commands,
                        reply: Ok(ReplyV1::Accepted(OutcomeV1::Read(batch(vec![7; 1024])))),
                    },
                ))
            },
            selection(),
        );
        component.update(DisclosureInput::Confirm, true);
        assert!(commands.lock().unwrap().is_empty());
        component.update(DisclosureInput::More, true);
        component.update(DisclosureInput::More, true);
        assert_eq!(component.limit(), 3);
        component.update(DisclosureInput::Arm, true);
        component.update(DisclosureInput::Confirm, true);
        settle(&mut component);
        assert_eq!(component.disposition(), DisclosureDisposition::Disclosed);
        let commands = commands.lock().unwrap();
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], CommandV1::Read { limit: 3, .. }));
        let view = component.view().unwrap();
        assert_eq!(view.events.len(), 3);
        assert!(view.events.iter().all(|event| event.payload_bytes == 1024));
        assert!(
            view.events
                .iter()
                .all(|event| event.payload_preview_hex.len() == 512)
        );
        assert!(
            view.events
                .iter()
                .all(|event| event.payload_preview_truncated)
        );
        assert!(matches!(
            view.gap,
            Some(DisclosureGapView::BeforeRetention { .. })
        ));
    }

    #[test]
    fn read_transport_failure_is_terminal_and_never_retried() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let worker_commands = Arc::clone(&commands);
        let mut component = DisclosureComponent::new_with_connector(
            move || {
                Ok::<_, &'static str>((
                    SessionHandleV1(0),
                    Port {
                        commands: worker_commands,
                        reply: Err("lost"),
                    },
                ))
            },
            selection(),
        );
        component.update(DisclosureInput::Arm, true);
        component.update(DisclosureInput::Confirm, true);
        settle(&mut component);
        assert_eq!(component.disposition(), DisclosureDisposition::Failed);
        component.update(DisclosureInput::Confirm, true);
        assert_eq!(commands.lock().unwrap().len(), 1);
    }
}
