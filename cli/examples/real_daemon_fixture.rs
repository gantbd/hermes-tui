#![cfg(unix)]
#![forbid(unsafe_code)]

use std::{convert::Infallible, os::unix::net::UnixStream, path::PathBuf, time::Duration};

use hermes_ledger::{
    CorrelationId, EventEnvelopeV1, IdempotencyKey, Limits, ProducerId, SchemaId, StreamId,
    persistence::CheckpointBudgetV1,
};
use hermes_local_daemon::unix::{Authenticator, Daemon};
use hermes_service::{
    AuthenticatedSessionV1, AuthorizationActionV1, AuthorizationResourceV1, Authorizer, Service,
    ServiceLimitsV1,
};
use hermes_wire::LimitsV1;

struct Authentication;
impl Authenticator for Authentication {
    type Error = Infallible;
    fn authenticate(&mut self, _: &UnixStream) -> Result<AuthenticatedSessionV1, Self::Error> {
        Ok(AuthenticatedSessionV1 { subject: [1; 16] })
    }
}

struct Policy;
impl Authorizer for Policy {
    type Error = Infallible;
    fn authorize(
        &mut self,
        subject: [u8; 16],
        _: AuthorizationActionV1,
        _: AuthorizationResourceV1,
    ) -> Result<bool, Self::Error> {
        Ok(subject == [1; 16])
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1).map(PathBuf::from);
    let directory = args.next().ok_or("ledger directory required")?;
    let socket = args.next().ok_or("socket path required")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let mut ledger =
        hermes_local::create_new(&directory, Limits::default(), CheckpointBudgetV1::default())
            .map_err(|error| std::io::Error::other(format!("ledger failed: {error:?}")))?;
    ledger
        .append(
            StreamId::new([5; 16]),
            None,
            EventEnvelopeV1::new(
                ProducerId::new([6; 16]),
                SchemaId::new([7; 16]),
                IdempotencyKey::new([8; 16]),
                CorrelationId::new([9; 16]),
                None,
                vec![10],
            ),
        )
        .map_err(|error| std::io::Error::other(format!("seed failed: {error:?}")))?;
    let service = Service::new(ledger, Policy, ServiceLimitsV1::default())
        .ok_or_else(|| std::io::Error::other("service configuration refused"))?;
    let mut daemon = Daemon::bind_new(
        socket,
        service,
        Authentication,
        LimitsV1::default(),
        Duration::from_secs(5),
        8,
    )?;
    daemon
        .serve_next()
        .map_err(|error| std::io::Error::other(format!("daemon failed: {error:?}")))?;
    Ok(())
}
