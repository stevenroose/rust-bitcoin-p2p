
use std::{io, fmt};

#[derive(Debug)]
pub enum Error {
	Io(io::Error),
	Runtime(erin::Error),
	/// Error dialing a peer.
	Dial(io::Error),
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		fmt::Debug::fmt(self, f)
	}
}

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error { Error::Io(e) }
}

impl From<erin::Error> for Error {
	fn from(e: erin::Error) -> Error { Error::Runtime(e) }
}

impl std::error::Error for Error {}
