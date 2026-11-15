//! Exact-confirmation guard for bounded Hermes event disclosure.
#![forbid(unsafe_code)]

use hermes_client::{ClientError, DisclosureClient, ServicePort};
use hermes_protocol::{
    EventSequenceV1, OutcomeV1, ReadBatchV1, RefusalV1, ReplyV1, SessionHandleV1, StreamIdV1,
};

pub const CAPABILITY: &str = "ledger.events.disclose.v1";
pub const MAX_EVENTS: u64 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DisclosureRequest {
    pub stream: StreamIdV1,
    pub start: EventSequenceV1,
    pub limit: u64,
}

impl DisclosureRequest {
    pub fn confirmation(self) -> String {
        format!(
            "DISCLOSE HERMES STREAM {} FROM {} LIMIT {}",
            hex(&self.stream.0),
            self.start.0,
            self.limit
        )
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum DisclosureError<E> {
    ConfirmationMismatch { required: String },
    InvalidLimit,
    Transport(E),
    Refused(RefusalV1),
    InvalidReply,
}

pub fn disclose<P: ServicePort>(
    session: SessionHandleV1,
    port: P,
    request: DisclosureRequest,
    confirmation: &str,
) -> Result<ReadBatchV1, DisclosureError<P::Error>> {
    if request.limit == 0 || request.limit > MAX_EVENTS {
        return Err(DisclosureError::InvalidLimit);
    }
    let required = request.confirmation();
    if confirmation != required {
        return Err(DisclosureError::ConfirmationMismatch { required });
    }
    let mut client =
        DisclosureClient::new(session, port, MAX_EVENTS).ok_or(DisclosureError::InvalidLimit)?;
    match client.read(request.stream, request.start, request.limit) {
        Ok(ReplyV1::Accepted(OutcomeV1::Read(batch))) => Ok(batch),
        Ok(ReplyV1::Refused(refusal)) => Err(DisclosureError::Refused(refusal)),
        Ok(_) => Err(DisclosureError::InvalidReply),
        Err(ClientError::Limit) => Err(DisclosureError::InvalidLimit),
        Err(ClientError::Transport(error)) => Err(DisclosureError::Transport(error)),
    }
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use hermes_protocol::{CommandV1, ReadGapV1};

    use super::*;

    struct Port {
        commands: Rc<RefCell<Vec<CommandV1>>>,
        reply: Result<ReplyV1, &'static str>,
    }
    impl ServicePort for Port {
        type Error = &'static str;
        fn execute(
            &mut self,
            _: SessionHandleV1,
            command: CommandV1,
        ) -> Result<ReplyV1, Self::Error> {
            self.commands.borrow_mut().push(command);
            self.reply.clone()
        }
    }

    fn request() -> DisclosureRequest {
        DisclosureRequest {
            stream: StreamIdV1([5; 16]),
            start: EventSequenceV1(3),
            limit: 4,
        }
    }

    #[test]
    fn exact_confirmation_performs_one_bounded_read() {
        let commands = Rc::new(RefCell::new(Vec::new()));
        let batch = ReadBatchV1 {
            events: Vec::new(),
            retained_from: EventSequenceV1(5),
            published_edge: Some(EventSequenceV1(8)),
            gap: Some(ReadGapV1::BeforeRetention {
                requested: EventSequenceV1(3),
                retained_from: EventSequenceV1(5),
            }),
        };
        assert_eq!(
            disclose(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    reply: Ok(ReplyV1::Accepted(OutcomeV1::Read(batch.clone()))),
                },
                request(),
                "DISCLOSE HERMES STREAM 05050505050505050505050505050505 FROM 3 LIMIT 4",
            ),
            Ok(batch)
        );
        assert_eq!(commands.borrow().len(), 1);
        assert!(matches!(
            commands.borrow()[0],
            CommandV1::Read { limit: 4, .. }
        ));
    }

    #[test]
    fn wrong_confirmation_and_invalid_limit_never_disclose() {
        for (request, confirmation) in [
            (request(), "disclose"),
            (
                DisclosureRequest {
                    limit: MAX_EVENTS + 1,
                    ..request()
                },
                "anything",
            ),
        ] {
            let commands = Rc::new(RefCell::new(Vec::new()));
            assert!(
                disclose(
                    SessionHandleV1(0),
                    Port {
                        commands: Rc::clone(&commands),
                        reply: Err("must not execute"),
                    },
                    request,
                    confirmation,
                )
                .is_err()
            );
            assert!(commands.borrow().is_empty());
        }
    }

    #[test]
    fn transport_failure_is_returned_without_retry() {
        let commands = Rc::new(RefCell::new(Vec::new()));
        assert_eq!(
            disclose(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    reply: Err("unavailable"),
                },
                request(),
                &request().confirmation(),
            ),
            Err(DisclosureError::Transport("unavailable"))
        );
        assert_eq!(commands.borrow().len(), 1);
    }
}
