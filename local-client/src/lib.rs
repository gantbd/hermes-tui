//! Bounded private Unix client for Hermes's selected local daemon.
#![forbid(unsafe_code)]

use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    time::Duration,
};

use hermes_client::ServicePort;
use hermes_protocol::{CommandV1, ReplyV1, SessionHandleV1};
use hermes_wire::{LimitsV1, decode_ready, decode_reply, encode_request};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientLimits {
    pub max_frame_bytes: usize,
    pub max_requests: usize,
    pub io_timeout: Duration,
}

impl Default for ClientLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 2 * 1024 * 1024,
            max_requests: 1_024,
            io_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientError {
    InvalidLimits,
    NotSent(io::ErrorKind),
    OutcomeUnknown(io::ErrorKind),
    InvalidReady,
    InvalidReply,
    RequestLimit,
    SessionMismatch,
}

pub const CONNECTION_SESSION: SessionHandleV1 = SessionHandleV1(0);

pub struct LocalPort {
    stream: UnixStream,
    limits: LimitsV1,
    requests_left: usize,
}

impl LocalPort {
    pub fn connect(path: impl AsRef<Path>, limits: ClientLimits) -> Result<Self, ClientError> {
        if limits.max_frame_bytes == 0 || limits.max_requests == 0 || limits.io_timeout.is_zero() {
            return Err(ClientError::InvalidLimits);
        }
        let mut stream = UnixStream::connect(path).map_err(|e| ClientError::NotSent(e.kind()))?;
        stream
            .set_read_timeout(Some(limits.io_timeout))
            .and_then(|()| stream.set_write_timeout(Some(limits.io_timeout)))
            .map_err(|e| ClientError::NotSent(e.kind()))?;
        let local = LimitsV1 {
            max_frame_bytes: limits.max_frame_bytes,
        };
        let bytes = read_frame(&mut stream, limits.max_frame_bytes)
            .map_err(|e| ClientError::NotSent(e.kind()))?;
        let ready = decode_ready(&bytes, local).map_err(|_| ClientError::InvalidReady)?;
        Ok(Self {
            stream,
            limits: LimitsV1 {
                max_frame_bytes: limits.max_frame_bytes.min(ready.max_frame_bytes as usize),
            },
            requests_left: limits.max_requests.min(ready.max_requests as usize),
        })
    }
}

impl ServicePort for LocalPort {
    type Error = ClientError;

    fn execute(
        &mut self,
        session: SessionHandleV1,
        command: CommandV1,
    ) -> Result<ReplyV1, Self::Error> {
        if session != CONNECTION_SESSION {
            return Err(ClientError::SessionMismatch);
        }
        if self.requests_left == 0 {
            return Err(ClientError::RequestLimit);
        }
        let request = encode_request(&command, self.limits)
            .map_err(|_| ClientError::NotSent(io::ErrorKind::InvalidData))?;
        write_frame(&mut self.stream, &request)
            .map_err(|e| ClientError::OutcomeUnknown(e.kind()))?;
        self.requests_left -= 1;
        let reply = read_frame(&mut self.stream, self.limits.max_frame_bytes)
            .map_err(|e| ClientError::OutcomeUnknown(e.kind()))?;
        decode_reply(&reply, self.limits).map_err(|_| ClientError::InvalidReply)
    }
}

fn write_frame(stream: &mut UnixStream, bytes: &[u8]) -> io::Result<()> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame length"))?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()
}

fn read_frame(stream: &mut UnixStream, maximum: usize) -> io::Result<Vec<u8>> {
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > maximum {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame limit"));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}
