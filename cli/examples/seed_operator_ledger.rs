#![cfg(unix)]
#![forbid(unsafe_code)]

use std::path::PathBuf;

use hermes_ledger::{
    CorrelationId, EventEnvelopeV1, EventSequence, IdempotencyKey, Limits, ProducerId, SchemaId,
    StreamId, persistence::CheckpointBudgetV1,
};

fn envelope(idempotency: u8, payload: u8) -> EventEnvelopeV1 {
    EventEnvelopeV1::new(
        ProducerId::new([6; 16]),
        SchemaId::new([7; 16]),
        IdempotencyKey::new([idempotency; 16]),
        CorrelationId::new([9; 16]),
        None,
        vec![payload],
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1).map(PathBuf::from);
    let directory = args.next().ok_or("ledger directory required")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let mut ledger =
        hermes_local::create_new(&directory, Limits::default(), CheckpointBudgetV1::default())
            .map_err(|error| std::io::Error::other(format!("ledger failed: {error:?}")))?;
    let stream = StreamId::new([5; 16]);
    ledger
        .append(stream, None, envelope(8, 10))
        .map_err(|error| std::io::Error::other(format!("seed failed: {error:?}")))?;
    ledger
        .append(stream, Some(EventSequence(0)), envelope(11, 12))
        .map_err(|error| std::io::Error::other(format!("seed failed: {error:?}")))?;
    Ok(())
}
