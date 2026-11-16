//! Same-connection exact-state guard for one Hermes compaction.
#![forbid(unsafe_code)]

use hermes_client::{OperatorClient, ReadOnlyClient, ServicePort};
use hermes_protocol::{
    CompactionOutcomeV1, EventSequenceV1, OutcomeV1, RefusalV1, ReplyV1, SessionHandleV1,
    StreamIdV1, StreamObservationV1,
};

pub const CAPABILITY: &str = "ledger.compact.v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactRequest {
    pub stream: StreamIdV1,
    pub expected_retained_from: EventSequenceV1,
    pub expected_published_edge: EventSequenceV1,
    pub through: EventSequenceV1,
}

impl CompactRequest {
    pub fn confirmation(self) -> String {
        format!(
            "COMPACT HERMES STREAM {} RETAINED FROM {} PUBLISHED EDGE {} THROUGH {}",
            hex(&self.stream.0),
            self.expected_retained_from.0,
            self.expected_published_edge.0,
            self.through.0
        )
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum CompactError<E> {
    ConfirmationMismatch { required: String },
    InvalidRange,
    ObservationTransport(E),
    StreamMissing,
    StreamChanged { actual: StreamObservationV1 },
    ObservationRefused(RefusalV1),
    MutationTransport(E),
    MutationRefused(RefusalV1),
    InvalidReply,
}

pub fn compact<P: ServicePort>(
    session: SessionHandleV1,
    port: P,
    request: CompactRequest,
    confirmation: &str,
) -> Result<CompactionOutcomeV1, CompactError<P::Error>> {
    if request.through < request.expected_retained_from
        || request.through > request.expected_published_edge
    {
        return Err(CompactError::InvalidRange);
    }
    let required = request.confirmation();
    if confirmation != required {
        return Err(CompactError::ConfirmationMismatch { required });
    }
    let mut observer = ReadOnlyClient::new(session, port);
    let current = match observer.stream_observation(request.stream) {
        Ok(ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(value)))) => value,
        Ok(ReplyV1::Accepted(OutcomeV1::StreamObservation(None))) => {
            return Err(CompactError::StreamMissing);
        }
        Ok(ReplyV1::Refused(refusal)) => {
            return Err(CompactError::ObservationRefused(refusal));
        }
        Ok(_) => return Err(CompactError::InvalidReply),
        Err(error) => return Err(CompactError::ObservationTransport(error)),
    };
    if current.retained_from != request.expected_retained_from
        || current.published_edge != Some(request.expected_published_edge)
    {
        return Err(CompactError::StreamChanged { actual: current });
    }
    let mut operator = OperatorClient::new(session, observer.into_port());
    match operator.compact(request.stream, request.through) {
        Ok(ReplyV1::Accepted(OutcomeV1::Compacted(outcome))) => Ok(outcome),
        Ok(ReplyV1::Refused(refusal)) => Err(CompactError::MutationRefused(refusal)),
        Ok(_) => Err(CompactError::InvalidReply),
        Err(error) => Err(CompactError::MutationTransport(error)),
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

    fn request() -> CompactRequest {
        CompactRequest {
            stream: StreamIdV1([5; 16]),
            expected_retained_from: EventSequenceV1(0),
            expected_published_edge: EventSequenceV1(1),
            through: EventSequenceV1(0),
        }
    }
    fn observation(retained: u64, edge: u64) -> StreamObservationV1 {
        StreamObservationV1 {
            retained_from: EventSequenceV1(retained),
            published_edge: Some(EventSequenceV1(edge)),
            retained_events: edge - retained + 1,
            consumer_count: 1,
            stalled_consumers: 0,
            max_consumer_backlog: 1,
        }
    }

    #[test]
    fn exact_confirmation_observes_then_compacts_once() {
        let commands = Rc::new(RefCell::new(Vec::new()));
        let outcome = CompactionOutcomeV1::Compacted {
            removed: 1,
            retained_from: EventSequenceV1(1),
        };
        assert_eq!(
            compact(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    replies: vec![
                        Ok(ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(
                            observation(0, 1),
                        )))),
                        Ok(ReplyV1::Accepted(OutcomeV1::Compacted(outcome))),
                    ],
                },
                request(),
                &request().confirmation(),
            ),
            Ok(outcome)
        );
        assert!(matches!(
            commands.borrow()[0],
            CommandV1::StreamObservation { .. }
        ));
        assert!(matches!(commands.borrow()[1], CommandV1::Compact { .. }));
        assert_eq!(commands.borrow().len(), 2);
    }

    #[test]
    fn confirmation_and_stream_fences_prevent_compaction() {
        let commands = Rc::new(RefCell::new(Vec::new()));
        assert!(matches!(
            compact(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    replies: vec![]
                },
                request(),
                "compact",
            ),
            Err(CompactError::ConfirmationMismatch { .. })
        ));
        assert!(commands.borrow().is_empty());
        assert!(matches!(
            compact(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    replies: vec![Ok(ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(
                        observation(1, 1),
                    ))))],
                },
                request(),
                &request().confirmation(),
            ),
            Err(CompactError::StreamChanged { .. })
        ));
        assert_eq!(commands.borrow().len(), 1);
    }

    #[test]
    fn mutation_transport_failure_is_returned_without_retry() {
        let commands = Rc::new(RefCell::new(Vec::new()));
        assert_eq!(
            compact(
                SessionHandleV1(0),
                Port {
                    commands: Rc::clone(&commands),
                    replies: vec![
                        Ok(ReplyV1::Accepted(OutcomeV1::StreamObservation(Some(
                            observation(0, 1),
                        )))),
                        Err("lost"),
                    ],
                },
                request(),
                &request().confirmation(),
            ),
            Err(CompactError::MutationTransport("lost"))
        );
        assert_eq!(commands.borrow().len(), 2);
    }
}
