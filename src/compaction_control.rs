//! Exact-state, operator-selected Hermes stream compaction workflow.

use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use hermes_client::ServicePort;
use hermes_operator_command::{CompactError, CompactRequest, compact};
use hermes_protocol::{CompactionOutcomeV1, EventSequenceV1, SessionHandleV1};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::Lifecycle;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionSelection {
    pub stream: hermes_protocol::StreamIdV1,
    pub retained_from: EventSequenceV1,
    pub published_edge: EventSequenceV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionInput {
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

pub fn translate_compaction_key(key: KeyEvent) -> Option<CompactionInput> {
    if key.kind != KeyEventKind::Press || key.modifiers != KeyModifiers::NONE {
        return None;
    }
    match key.code {
        KeyCode::Left => Some(CompactionInput::Previous),
        KeyCode::Right => Some(CompactionInput::Next),
        KeyCode::PageUp => Some(CompactionInput::PreviousPage),
        KeyCode::PageDown => Some(CompactionInput::NextPage),
        KeyCode::Home => Some(CompactionInput::First),
        KeyCode::End => Some(CompactionInput::Last),
        KeyCode::Char('c') => Some(CompactionInput::Arm),
        KeyCode::Enter => Some(CompactionInput::Confirm),
        KeyCode::Esc => Some(CompactionInput::Close),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionDisposition {
    Proposed,
    Armed,
    Pending,
    Applied { removed: u64, retained_from: u64 },
    Refused,
    Failed,
    OutcomeUnknown,
}

struct WorkerReply {
    disposition: CompactionDisposition,
    notice: String,
}

pub struct CompactionComponent {
    submit: SyncSender<CompactRequest>,
    replies: Receiver<WorkerReply>,
    selection: CompactionSelection,
    through: EventSequenceV1,
    disposition: CompactionDisposition,
    notice: String,
}

impl CompactionComponent {
    pub fn new_with_connector<P, F, C>(connector: F, selection: CompactionSelection) -> Self
    where
        P: ServicePort + Send + 'static,
        P::Error: std::fmt::Debug + Send + 'static,
        F: FnOnce() -> Result<(SessionHandleV1, P), C> + Send + 'static,
        C: std::fmt::Debug + Send + 'static,
    {
        let (submit_tx, submit_rx) = sync_channel::<CompactRequest>(1);
        let (reply_tx, reply_rx) = sync_channel(1);
        std::thread::spawn(move || {
            let Ok(request) = submit_rx.recv() else {
                return;
            };
            let reply = match connector() {
                Ok((session, port)) => {
                    let confirmation = request.confirmation();
                    classify(compact(session, port, request, &confirmation))
                }
                Err(error) => WorkerReply {
                    disposition: CompactionDisposition::Failed,
                    notice: format!("Hermes operator connection refused: {error:?}"),
                },
            };
            let _ = reply_tx.send(reply);
        });
        Self {
            submit: submit_tx,
            replies: reply_rx,
            selection,
            through: selection.retained_from,
            disposition: CompactionDisposition::Proposed,
            notice: "Select an exact boundary; c arms, Enter compacts once".into(),
        }
    }

    fn request(&self) -> CompactRequest {
        CompactRequest {
            stream: self.selection.stream,
            expected_retained_from: self.selection.retained_from,
            expected_published_edge: self.selection.published_edge,
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
                if self.disposition == CompactionDisposition::Pending =>
            {
                self.disposition = CompactionDisposition::OutcomeUnknown;
                self.notice =
                    "Hermes compaction worker disconnected after dispatch; outcome unknown".into();
            }
            Err(TryRecvError::Disconnected) => {}
        }
    }

    pub fn update(&mut self, input: CompactionInput, focused: bool) -> Lifecycle {
        if !focused {
            return Lifecycle::Running;
        }
        if input == CompactionInput::Close {
            if matches!(
                self.disposition,
                CompactionDisposition::Pending | CompactionDisposition::OutcomeUnknown
            ) {
                self.notice =
                    "Compaction unresolved; refresh exact stream state before exit".into();
                return Lifecycle::Running;
            }
            return Lifecycle::CloseRequested;
        }
        match (self.disposition, input) {
            (CompactionDisposition::Proposed, CompactionInput::Previous) => self.shift(-1),
            (CompactionDisposition::Proposed, CompactionInput::Next) => self.shift(1),
            (CompactionDisposition::Proposed, CompactionInput::PreviousPage) => self.shift(-100),
            (CompactionDisposition::Proposed, CompactionInput::NextPage) => self.shift(100),
            (CompactionDisposition::Proposed, CompactionInput::First) => {
                self.through = self.selection.retained_from;
                self.boundary_notice();
            }
            (CompactionDisposition::Proposed, CompactionInput::Last) => {
                self.through = self.selection.published_edge;
                self.boundary_notice();
            }
            (CompactionDisposition::Proposed, CompactionInput::Arm) => {
                self.disposition = CompactionDisposition::Armed;
                self.notice = format!(
                    "ARMED: Enter permanently removes {} retained event(s)",
                    self.removal_count()
                );
            }
            (CompactionDisposition::Armed, CompactionInput::Confirm) => {
                if self.submit.try_send(self.request()).is_ok() {
                    self.disposition = CompactionDisposition::Pending;
                    self.notice = "Hermes compaction pending; no retry is permitted".into();
                } else {
                    self.disposition = CompactionDisposition::Failed;
                    self.notice = "Hermes compaction worker unavailable before dispatch".into();
                }
            }
            (CompactionDisposition::Armed, _) => {
                self.disposition = CompactionDisposition::Proposed;
                self.notice = "Hermes compaction disarmed; boundary remains selected".into();
            }
            _ => {}
        }
        Lifecycle::Running
    }

    fn shift(&mut self, delta: i64) {
        let current = i128::from(self.through.0);
        let candidate = current.saturating_add(i128::from(delta));
        let lower = i128::from(self.selection.retained_from.0);
        let upper = i128::from(self.selection.published_edge.0);
        self.through = EventSequenceV1(u64::try_from(candidate.clamp(lower, upper)).unwrap());
        self.boundary_notice();
    }

    fn boundary_notice(&mut self) {
        self.notice = format!(
            "Boundary {} selected; {} retained event(s) would be removed",
            self.through.0,
            self.removal_count()
        );
    }

    fn removal_count(&self) -> u64 {
        self.through
            .0
            .saturating_sub(self.selection.retained_from.0)
            .saturating_add(1)
    }

    pub fn draw_in(&self, frame: &mut Frame<'_>, area: Rect, focused: bool) {
        let [body, footer] =
            Layout::vertical([Constraint::Min(1), Constraint::Length(2)]).areas(area);
        let title = if focused {
            " Hermes stream compaction • focused "
        } else {
            " Hermes stream compaction "
        };
        let danger = Style::default().fg(Color::Red).add_modifier(Modifier::BOLD);
        let lines = vec![
            Line::styled("PERMANENT RETENTION DELETION", danger),
            Line::from(format!(
                "observed retained/published: {}/{}",
                self.selection.retained_from.0, self.selection.published_edge.0
            )),
            Line::from(format!("compact through: {}", self.through.0)),
            Line::from(format!("estimated removals: {}", self.removal_count())),
            Line::from("Pantheon does not infer a safe consumer watermark."),
            Line::from("Hermes re-observes both boundaries on the mutation connection."),
            Line::from(format!(
                "exact confirmation: {}",
                self.request().confirmation()
            )),
            Line::from("Uncertain delivery is never retried; refresh to reconcile."),
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

    pub const fn disposition(&self) -> CompactionDisposition {
        self.disposition
    }

    pub fn notice(&self) -> &str {
        &self.notice
    }

    pub const fn through(&self) -> EventSequenceV1 {
        self.through
    }
}

fn classify<E: std::fmt::Debug>(
    result: Result<CompactionOutcomeV1, CompactError<E>>,
) -> WorkerReply {
    match result {
        Ok(CompactionOutcomeV1::Compacted {
            removed,
            retained_from,
        }) => WorkerReply {
            disposition: CompactionDisposition::Applied {
                removed,
                retained_from: retained_from.0,
            },
            notice: format!(
                "Hermes compaction applied; removed {removed}, retained from {}",
                retained_from.0
            ),
        },
        Ok(CompactionOutcomeV1::Identity { retained_from }) => WorkerReply {
            disposition: CompactionDisposition::Applied {
                removed: 0,
                retained_from: retained_from.0,
            },
            notice: format!(
                "Hermes compaction was already applied; retained from {}",
                retained_from.0
            ),
        },
        Err(CompactError::MutationTransport(error)) => WorkerReply {
            disposition: CompactionDisposition::OutcomeUnknown,
            notice: format!("Hermes compaction outcome unknown: {error:?}"),
        },
        Err(error @ CompactError::StreamChanged { .. })
        | Err(error @ CompactError::StreamMissing)
        | Err(error @ CompactError::ObservationRefused(_))
        | Err(error @ CompactError::MutationRefused(_)) => WorkerReply {
            disposition: CompactionDisposition::Refused,
            notice: format!("Hermes compaction refused: {error:?}"),
        },
        Err(error) => WorkerReply {
            disposition: CompactionDisposition::Failed,
            notice: format!("Hermes compaction failed before a receipt: {error:?}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use hermes_protocol::{CommandV1, OutcomeV1, ReplyV1, StreamIdV1, StreamObservationV1};

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

    fn selection() -> CompactionSelection {
        CompactionSelection {
            stream: StreamIdV1([5; 16]),
            retained_from: EventSequenceV1(10),
            published_edge: EventSequenceV1(250),
        }
    }
    fn observation() -> StreamObservationV1 {
        StreamObservationV1 {
            retained_from: EventSequenceV1(10),
            published_edge: Some(EventSequenceV1(250)),
            retained_events: 241,
            consumer_count: 1,
            stalled_consumers: 0,
            max_consumer_backlog: 0,
        }
    }
    fn settle(component: &mut CompactionComponent) {
        for _ in 0..1_000 {
            component.poll();
            if component.disposition() != CompactionDisposition::Pending {
                return;
            }
            std::thread::yield_now();
        }
        panic!("compaction worker did not settle");
    }

    #[test]
    fn selected_boundary_is_reobserved_then_compacted_once() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let worker_commands = Arc::clone(&commands);
        let mut component = CompactionComponent::new_with_connector(
            move || {
                Ok::<_, &'static str>((
                    SessionHandleV1(0),
                    Port {
                        commands: worker_commands,
                        replies: vec![
                            Ok(ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(
                                observation(),
                            )))),
                            Ok(ReplyV1::Accepted(OutcomeV1::Compacted(
                                CompactionOutcomeV1::Compacted {
                                    removed: 101,
                                    retained_from: EventSequenceV1(111),
                                },
                            ))),
                        ],
                    },
                ))
            },
            selection(),
        );
        component.update(CompactionInput::NextPage, true);
        assert_eq!(component.through(), EventSequenceV1(110));
        component.update(CompactionInput::Confirm, true);
        assert!(commands.lock().unwrap().is_empty());
        component.update(CompactionInput::Arm, true);
        component.update(CompactionInput::Confirm, true);
        settle(&mut component);
        assert_eq!(
            component.disposition(),
            CompactionDisposition::Applied {
                removed: 101,
                retained_from: 111
            }
        );
        let commands = commands.lock().unwrap();
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[0], CommandV1::StreamObservation { .. }));
        assert!(matches!(
            commands[1],
            CommandV1::Compact {
                through: EventSequenceV1(110),
                ..
            }
        ));
    }

    #[test]
    fn boundary_is_clamped_and_uncertain_mutation_is_never_retried() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let worker_commands = Arc::clone(&commands);
        let mut component = CompactionComponent::new_with_connector(
            move || {
                Ok::<_, &'static str>((
                    SessionHandleV1(0),
                    Port {
                        commands: worker_commands,
                        replies: vec![
                            Ok(ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(
                                observation(),
                            )))),
                            Err("lost"),
                        ],
                    },
                ))
            },
            selection(),
        );
        component.update(CompactionInput::PreviousPage, true);
        assert_eq!(component.through(), EventSequenceV1(10));
        component.update(CompactionInput::Last, true);
        assert_eq!(component.through(), EventSequenceV1(250));
        component.update(CompactionInput::Arm, true);
        component.update(CompactionInput::Confirm, true);
        settle(&mut component);
        assert_eq!(
            component.disposition(),
            CompactionDisposition::OutcomeUnknown
        );
        component.update(CompactionInput::Confirm, true);
        assert_eq!(commands.lock().unwrap().len(), 2);
    }
}
