//! Exact-range Hermes consumer acknowledgement workflow.

use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use hermes_client::ServicePort;
use hermes_consumer_command::{AcknowledgeError, AcknowledgeRequest, acknowledge};
use hermes_protocol::SessionHandleV1;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::Lifecycle;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcknowledgeSelection {
    pub consumer: hermes_protocol::ConsumerIdV1,
    pub stream: hermes_protocol::StreamIdV1,
    pub expected_next: hermes_protocol::EventSequenceV1,
    pub published_edge: hermes_protocol::EventSequenceV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcknowledgeInput {
    Previous,
    Next,
    PreviousPage,
    NextPage,
    First,
    Last,
    Arm,
    Confirm,
    Close,
}

pub fn translate_acknowledge_key(key: KeyEvent) -> Option<AcknowledgeInput> {
    if key.kind != KeyEventKind::Press || key.modifiers != KeyModifiers::NONE {
        return None;
    }
    match key.code {
        KeyCode::Left => Some(AcknowledgeInput::Previous),
        KeyCode::Right => Some(AcknowledgeInput::Next),
        KeyCode::PageUp => Some(AcknowledgeInput::PreviousPage),
        KeyCode::PageDown => Some(AcknowledgeInput::NextPage),
        KeyCode::Home => Some(AcknowledgeInput::First),
        KeyCode::End => Some(AcknowledgeInput::Last),
        KeyCode::Char('c') => Some(AcknowledgeInput::Arm),
        KeyCode::Enter => Some(AcknowledgeInput::Confirm),
        KeyCode::Esc => Some(AcknowledgeInput::Close),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcknowledgeDisposition {
    Proposed,
    Armed,
    Pending,
    Applied { next_sequence: u64 },
    Refused,
    Failed,
    OutcomeUnknown,
}

struct WorkerReply {
    disposition: AcknowledgeDisposition,
    notice: String,
}

pub struct AcknowledgeComponent {
    submit: SyncSender<AcknowledgeRequest>,
    replies: Receiver<WorkerReply>,
    selection: AcknowledgeSelection,
    through: hermes_protocol::EventSequenceV1,
    disposition: AcknowledgeDisposition,
    notice: String,
}

impl AcknowledgeComponent {
    pub fn new_with_connector<P, F, C>(connector: F, selection: AcknowledgeSelection) -> Self
    where
        P: ServicePort + Send + 'static,
        P::Error: std::fmt::Debug + Send + 'static,
        F: FnOnce() -> Result<(SessionHandleV1, P), C> + Send + 'static,
        C: std::fmt::Debug + Send + 'static,
    {
        let (submit_tx, submit_rx) = sync_channel::<AcknowledgeRequest>(1);
        let (reply_tx, reply_rx) = sync_channel(1);
        std::thread::spawn(move || {
            let Ok(request) = submit_rx.recv() else {
                return;
            };
            let reply = match connector() {
                Ok((session, port)) => {
                    let confirmation = request.confirmation();
                    classify(acknowledge(session, port, request, &confirmation))
                }
                Err(error) => WorkerReply {
                    disposition: AcknowledgeDisposition::Failed,
                    notice: format!("Hermes control connection refused: {error:?}"),
                },
            };
            let _ = reply_tx.send(reply);
        });
        Self {
            submit: submit_tx,
            replies: reply_rx,
            selection,
            through: selection.expected_next,
            disposition: AcknowledgeDisposition::Proposed,
            notice: "Select an exact cursor range; c arms, Enter submits once".into(),
        }
    }

    fn request(&self) -> AcknowledgeRequest {
        AcknowledgeRequest {
            consumer: self.selection.consumer,
            stream: self.selection.stream,
            expected_next: self.selection.expected_next,
            through: self.through,
        }
    }

    pub fn poll(&mut self) {
        match self.replies.try_recv() {
            Ok(reply) => {
                self.disposition = reply.disposition;
                self.notice = reply.notice;
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected)
                if self.disposition == AcknowledgeDisposition::Pending =>
            {
                self.disposition = AcknowledgeDisposition::OutcomeUnknown;
                self.notice =
                    "Hermes acknowledgement worker disconnected after dispatch; outcome unknown"
                        .into();
            }
            Err(TryRecvError::Disconnected) => {}
        }
    }

    pub fn update(&mut self, input: AcknowledgeInput, focused: bool) -> Lifecycle {
        if !focused {
            return Lifecycle::Running;
        }
        if input == AcknowledgeInput::Close {
            if matches!(
                self.disposition,
                AcknowledgeDisposition::Pending | AcknowledgeDisposition::OutcomeUnknown
            ) {
                self.notice =
                    "Acknowledgement unresolved; refresh the exact cursor before exit".into();
                return Lifecycle::Running;
            }
            return Lifecycle::CloseRequested;
        }
        match (self.disposition, input) {
            (AcknowledgeDisposition::Proposed, AcknowledgeInput::Previous) => self.shift(-1),
            (AcknowledgeDisposition::Proposed, AcknowledgeInput::Next) => self.shift(1),
            (AcknowledgeDisposition::Proposed, AcknowledgeInput::PreviousPage) => self.shift(-100),
            (AcknowledgeDisposition::Proposed, AcknowledgeInput::NextPage) => self.shift(100),
            (AcknowledgeDisposition::Proposed, AcknowledgeInput::First) => {
                self.through = self.selection.expected_next;
                self.boundary_notice();
            }
            (AcknowledgeDisposition::Proposed, AcknowledgeInput::Last) => {
                self.through = self.selection.published_edge;
                self.boundary_notice();
            }
            (AcknowledgeDisposition::Proposed, AcknowledgeInput::Arm) => {
                self.disposition = AcknowledgeDisposition::Armed;
                self.notice = format!(
                    "ARMED: Enter advances the cursor across {} event(s)",
                    self.event_count()
                );
            }
            (AcknowledgeDisposition::Armed, AcknowledgeInput::Confirm) => {
                if self.submit.try_send(self.request()).is_ok() {
                    self.disposition = AcknowledgeDisposition::Pending;
                    self.notice = "Hermes acknowledgement pending; no retry is permitted".into();
                } else {
                    self.disposition = AcknowledgeDisposition::Failed;
                    self.notice =
                        "Hermes acknowledgement worker unavailable before dispatch".into();
                }
            }
            (AcknowledgeDisposition::Armed, _) => {
                self.disposition = AcknowledgeDisposition::Proposed;
                self.notice = "Hermes acknowledgement disarmed".into();
            }
            _ => {}
        }
        Lifecycle::Running
    }

    fn shift(&mut self, delta: i64) {
        let candidate = i128::from(self.through.0).saturating_add(i128::from(delta));
        let lower = i128::from(self.selection.expected_next.0);
        let upper = i128::from(self.selection.published_edge.0);
        self.through =
            hermes_protocol::EventSequenceV1(u64::try_from(candidate.clamp(lower, upper)).unwrap());
        self.boundary_notice();
    }

    fn boundary_notice(&mut self) {
        self.notice = format!(
            "Through {} selected; {} event(s) will be acknowledged",
            self.through.0,
            self.event_count()
        );
    }

    fn event_count(&self) -> u64 {
        self.through
            .0
            .saturating_sub(self.selection.expected_next.0)
            .saturating_add(1)
    }

    pub fn draw_in(&self, frame: &mut Frame<'_>, area: Rect, focused: bool) {
        let [body, footer] =
            Layout::vertical([Constraint::Min(1), Constraint::Length(2)]).areas(area);
        let title = if focused {
            " Hermes acknowledgement • focused "
        } else {
            " Hermes acknowledgement "
        };
        let warning = Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD);
        let lines = vec![
            Line::styled("EXACT CONSUMER CURSOR ADVANCE", warning),
            Line::from(format!(
                "current next sequence: {}",
                self.selection.expected_next.0
            )),
            Line::from(format!(
                "published edge: {}",
                self.selection.published_edge.0
            )),
            Line::from(format!("acknowledge through: {}", self.through.0)),
            Line::from(format!("selected events: {}", self.event_count())),
            Line::from(format!(
                "exact confirmation: {}",
                self.request().confirmation()
            )),
            Line::from("Hermes re-observes the cursor on the mutation connection."),
            Line::from("No retry after uncertain delivery; refresh to reconcile."),
            Line::from(format!("state: {:?}", self.disposition)),
        ];
        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::ALL).title(title)),
            body,
        );
        frame.render_widget(
            Paragraph::new(format!(
                "{} • ←/→ ±1 • PgUp/PgDn ±100 • Home/End • c arm • Enter confirm • Esc close",
                self.notice
            )),
            footer,
        );
    }

    pub const fn disposition(&self) -> AcknowledgeDisposition {
        self.disposition
    }

    pub fn notice(&self) -> &str {
        &self.notice
    }

    pub const fn through(&self) -> hermes_protocol::EventSequenceV1 {
        self.through
    }
}

fn classify<E: std::fmt::Debug>(
    result: Result<hermes_protocol::ConsumerCursorV1, AcknowledgeError<E>>,
) -> WorkerReply {
    match result {
        Ok(cursor) => WorkerReply {
            disposition: AcknowledgeDisposition::Applied {
                next_sequence: cursor.next_sequence.0,
            },
            notice: format!(
                "Hermes acknowledgement applied; next sequence is {}",
                cursor.next_sequence.0
            ),
        },
        Err(AcknowledgeError::MutationTransport(error)) => WorkerReply {
            disposition: AcknowledgeDisposition::OutcomeUnknown,
            notice: format!("Hermes acknowledgement outcome unknown: {error:?}"),
        },
        Err(error @ AcknowledgeError::CursorChanged { .. })
        | Err(error @ AcknowledgeError::CursorMissing)
        | Err(error @ AcknowledgeError::ObservationRefused(_))
        | Err(error @ AcknowledgeError::MutationRefused(_)) => WorkerReply {
            disposition: AcknowledgeDisposition::Refused,
            notice: format!("Hermes acknowledgement refused: {error:?}"),
        },
        Err(error) => WorkerReply {
            disposition: AcknowledgeDisposition::Failed,
            notice: format!("Hermes acknowledgement failed before a receipt: {error:?}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use hermes_protocol::{
        CommandV1, ConsumerCursorV1, ConsumerIdV1, EventSequenceV1, OutcomeV1, ReplyV1, StreamIdV1,
    };

    use super::*;

    struct Port {
        commands: Arc<Mutex<Vec<CommandV1>>>,
        replies: Vec<Result<ReplyV1, &'static str>>,
    }

    impl ServicePort for Port {
        type Error = &'static str;

        fn execute(
            &mut self,
            _: SessionHandleV1,
            command: CommandV1,
        ) -> Result<ReplyV1, Self::Error> {
            self.commands.lock().unwrap().push(command);
            self.replies.remove(0)
        }
    }

    fn selection() -> AcknowledgeSelection {
        AcknowledgeSelection {
            consumer: ConsumerIdV1([6; 16]),
            stream: StreamIdV1([5; 16]),
            expected_next: EventSequenceV1(3),
            published_edge: EventSequenceV1(203),
        }
    }

    fn cursor(next: u64) -> ConsumerCursorV1 {
        ConsumerCursorV1 {
            consumer: ConsumerIdV1([6; 16]),
            stream: StreamIdV1([5; 16]),
            next_sequence: EventSequenceV1(next),
        }
    }

    fn settle(component: &mut AcknowledgeComponent) {
        for _ in 0..1_000 {
            component.poll();
            if component.disposition() != AcknowledgeDisposition::Pending {
                return;
            }
            std::thread::yield_now();
        }
        panic!("acknowledgement worker did not settle");
    }

    #[test]
    fn exact_range_acknowledgement_uses_owner_guard_once() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let worker_commands = Arc::clone(&commands);
        let mut component = AcknowledgeComponent::new_with_connector(
            move || {
                Ok::<_, &'static str>((
                    SessionHandleV1(0),
                    Port {
                        commands: worker_commands,
                        replies: vec![
                            Ok(ReplyV1::Accepted(OutcomeV1::Cursor(Some(cursor(3))))),
                            Ok(ReplyV1::Accepted(OutcomeV1::Acknowledged(cursor(104)))),
                        ],
                    },
                ))
            },
            selection(),
        );
        component.update(AcknowledgeInput::Confirm, true);
        assert!(commands.lock().unwrap().is_empty());
        component.update(AcknowledgeInput::NextPage, true);
        assert_eq!(component.through(), EventSequenceV1(103));
        component.update(AcknowledgeInput::Arm, true);
        component.update(AcknowledgeInput::Confirm, true);
        settle(&mut component);
        assert_eq!(
            component.disposition(),
            AcknowledgeDisposition::Applied { next_sequence: 104 }
        );
        let commands = commands.lock().unwrap();
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[0], CommandV1::Cursor { .. }));
        assert!(matches!(
            commands[1],
            CommandV1::Acknowledge {
                through: EventSequenceV1(103),
                ..
            }
        ));
    }

    #[test]
    fn mutation_transport_loss_is_unknown_and_never_retried() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let worker_commands = Arc::clone(&commands);
        let mut component = AcknowledgeComponent::new_with_connector(
            move || {
                Ok::<_, &'static str>((
                    SessionHandleV1(0),
                    Port {
                        commands: worker_commands,
                        replies: vec![
                            Ok(ReplyV1::Accepted(OutcomeV1::Cursor(Some(cursor(3))))),
                            Err("lost"),
                        ],
                    },
                ))
            },
            selection(),
        );
        component.update(AcknowledgeInput::Arm, true);
        component.update(AcknowledgeInput::Confirm, true);
        settle(&mut component);
        assert_eq!(
            component.disposition(),
            AcknowledgeDisposition::OutcomeUnknown
        );
        assert_eq!(commands.lock().unwrap().len(), 2);
        component.update(AcknowledgeInput::Confirm, true);
        assert_eq!(commands.lock().unwrap().len(), 2);
    }
}
