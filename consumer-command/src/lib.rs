//! Same-connection exact-confirmation guard for one Hermes acknowledgement.
#![forbid(unsafe_code)]

use hermes_client::{ClientError, ConsumerClient, ServicePort};
use hermes_protocol::{
    ConsumerCursorV1, ConsumerIdV1, EventSequenceV1, OutcomeV1, RefusalV1, ReplyV1,
    SessionHandleV1, StreamIdV1,
};

pub const CAPABILITY: &str = "ledger.acknowledge.v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcknowledgeRequest {
    pub consumer: ConsumerIdV1,
    pub stream: StreamIdV1,
    pub expected_next: EventSequenceV1,
    pub through: EventSequenceV1,
}

impl AcknowledgeRequest {
    pub fn confirmation(self) -> String {
        format!(
            "ACKNOWLEDGE HERMES CONSUMER {} STREAM {} FROM {} THROUGH {}",
            hex(&self.consumer.0),
            hex(&self.stream.0),
            self.expected_next.0,
            self.through.0
        )
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum AcknowledgeError<E> {
    ConfirmationMismatch { required: String },
    InvalidRange,
    ObservationTransport(E),
    CursorMissing,
    CursorChanged { actual: EventSequenceV1 },
    ObservationRefused(RefusalV1),
    MutationTransport(E),
    MutationRefused(RefusalV1),
    InvalidReply,
}

pub fn acknowledge<P: ServicePort>(
    session: SessionHandleV1,
    port: P,
    request: AcknowledgeRequest,
    confirmation: &str,
) -> Result<ConsumerCursorV1, AcknowledgeError<P::Error>> {
    if request.through < request.expected_next {
        return Err(AcknowledgeError::InvalidRange);
    }
    let required = request.confirmation();
    if confirmation != required {
        return Err(AcknowledgeError::ConfirmationMismatch { required });
    }
    let mut client = ConsumerClient::new(session, port, 1).expect("nonzero read bound");
    let current = match client.cursor(request.consumer, request.stream) {
        Ok(ReplyV1::Accepted(OutcomeV1::Cursor(Some(cursor)))) => cursor,
        Ok(ReplyV1::Accepted(OutcomeV1::Cursor(None))) => {
            return Err(AcknowledgeError::CursorMissing);
        }
        Ok(ReplyV1::Refused(refusal)) => {
            return Err(AcknowledgeError::ObservationRefused(refusal));
        }
        Ok(_) => return Err(AcknowledgeError::InvalidReply),
        Err(ClientError::Transport(error)) => {
            return Err(AcknowledgeError::ObservationTransport(error));
        }
        Err(ClientError::Limit) => unreachable!("cursor has no caller-selected limit"),
    };
    if current.next_sequence != request.expected_next {
        return Err(AcknowledgeError::CursorChanged {
            actual: current.next_sequence,
        });
    }
    match client.acknowledge(request.consumer, request.stream, request.through) {
        Ok(ReplyV1::Accepted(OutcomeV1::Acknowledged(cursor)))
            if request
                .through
                .0
                .checked_add(1)
                .is_some_and(|next| cursor.next_sequence.0 == next) =>
        {
            Ok(cursor)
        }
        Ok(ReplyV1::Refused(refusal)) => Err(AcknowledgeError::MutationRefused(refusal)),
        Ok(_) => Err(AcknowledgeError::InvalidReply),
        Err(ClientError::Transport(error)) => Err(AcknowledgeError::MutationTransport(error)),
        Err(ClientError::Limit) => unreachable!("acknowledgement has no caller-selected limit"),
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

    use hermes_protocol::CommandV1;

    use super::*;

    struct Port {
        commands: Rc<RefCell<Vec<CommandV1>>>,
        replies: Vec<Result<ReplyV1, &'static str>>,
    }
    impl ServicePort for Port {
        type Error = &'static str;
        fn execute(
            &mut self,
            _: SessionHandleV1,
            command: CommandV1,
        ) -> Result<ReplyV1, Self::Error> {
            self.commands.borrow_mut().push(command);
            self.replies.remove(0)
        }
    }

    fn request() -> AcknowledgeRequest {
        AcknowledgeRequest {
            consumer: ConsumerIdV1([6; 16]),
            stream: StreamIdV1([5; 16]),
            expected_next: EventSequenceV1(1),
            through: EventSequenceV1(1),
        }
    }

    fn cursor(next: u64) -> ConsumerCursorV1 {
        ConsumerCursorV1 {
            consumer: ConsumerIdV1([6; 16]),
            stream: StreamIdV1([5; 16]),
            next_sequence: EventSequenceV1(next),
        }
    }

    #[test]
    fn exact_confirmation_observes_then_acknowledges_once() {
        let commands = Rc::new(RefCell::new(Vec::new()));
        assert_eq!(
            acknowledge(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    replies: vec![
                        Ok(ReplyV1::Accepted(OutcomeV1::Cursor(Some(cursor(1))))),
                        Ok(ReplyV1::Accepted(OutcomeV1::Acknowledged(cursor(2)))),
                    ],
                },
                request(),
                &request().confirmation(),
            ),
            Ok(cursor(2))
        );
        assert!(matches!(commands.borrow()[0], CommandV1::Cursor { .. }));
        assert!(matches!(
            commands.borrow()[1],
            CommandV1::Acknowledge { .. }
        ));
        assert_eq!(commands.borrow().len(), 2);
    }

    #[test]
    fn confirmation_and_cursor_fences_prevent_mutation() {
        let commands = Rc::new(RefCell::new(Vec::new()));
        assert!(matches!(
            acknowledge(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    replies: vec![],
                },
                request(),
                "acknowledge",
            ),
            Err(AcknowledgeError::ConfirmationMismatch { .. })
        ));
        assert!(commands.borrow().is_empty());

        assert_eq!(
            acknowledge(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    replies: vec![Ok(ReplyV1::Accepted(OutcomeV1::Cursor(Some(cursor(2)))))]
                },
                request(),
                &request().confirmation(),
            ),
            Err(AcknowledgeError::CursorChanged {
                actual: EventSequenceV1(2)
            })
        );
        assert_eq!(commands.borrow().len(), 1);
    }

    #[test]
    fn mutation_transport_failure_is_returned_without_retry() {
        let commands = Rc::new(RefCell::new(Vec::new()));
        assert_eq!(
            acknowledge(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    replies: vec![
                        Ok(ReplyV1::Accepted(OutcomeV1::Cursor(Some(cursor(1))))),
                        Err("lost"),
                    ],
                },
                request(),
                &request().confirmation(),
            ),
            Err(AcknowledgeError::MutationTransport("lost"))
        );
        assert_eq!(commands.borrow().len(), 2);
    }
}
