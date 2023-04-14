
use std::{fmt, io};

use crate::PeerId;

#[derive(Debug)]
pub enum Error {
    NotStartedYet,
	/// No peer with given ID known. He must have been disconnected.
	PeerDisconnected(PeerId),
	/// We have already shut down.
	Shutdown,
	/// An I/O error.
	Io(io::Error),
	/// Can't reach the peer.
	PeerUnreachable(io::Error),
	/// The handshake with the peer is not finished.
	PeerHandshakePending,
}

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Error::Io(e)
	}
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Error::NotStartedYet => write!(f, "P2P hasn't started yet"),
			Error::PeerDisconnected(id) => write!(f, "peer disconnected: {}", id),
			Error::Shutdown => write!(f, "P2P is already shut down"),
			Error::Io(ref e) => write!(f, "I/O error: {}", e),
			Error::PeerUnreachable(ref e) => write!(f, "can't reach the peer: {}", e),
			Error::PeerHandshakePending => write!(f, "peer didn't finish the handshake yet"),
		}
	}
}
impl std::error::Error for Error {}

