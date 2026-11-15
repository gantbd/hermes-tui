//! Composable, read-only Ratatui presentation over Hermes's public client.
#![forbid(unsafe_code)]

use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use hermes_client::{ReadOnlyClient, ServicePort};
use hermes_protocol::{
    OperationalSignalsV1, OutcomeV1, ReplyV1, SessionHandleV1, StatusV1, UsageV1,
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
        let (tx, rx) = sync_channel(1);
        let (reply_tx, reply_rx) = sync_channel(1);
        std::thread::spawn(move || {
            let mut client = ReadOnlyClient::new(session, port);
            while rx.recv().is_ok() {
                let result = observe(&mut client);
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
                vec![
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
                ]
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
    Ok(Observation {
        status,
        usage,
        signals,
    })
}
