//! This module exports a single interface for mpsc channels for
//! crossbeam- and std-based ones.

#[cfg(feature = "crossbeam-channel")]
pub use self::crossbeam_mpsc::{bounded, Receiver, SendError, Sender, SyncSender, TrySendError};
#[cfg(not(feature = "crossbeam-channel"))]
pub use self::std_mpsc::{bounded, Receiver, SendError, Sender, SyncSender, TrySendError};

#[cfg(not(feature = "crossbeam-channel"))]
mod std_mpsc {
	pub use std::sync::mpsc::{self, Receiver, SendError, Sender, SyncSender, TrySendError};

	pub fn bounded<T>(size: usize) -> (SyncSender<T>, Receiver<T>) {
		mpsc::sync_channel(size)
	}
}

#[cfg(feature = "crossbeam-channel")]
mod crossbeam_mpsc {
	pub use crossbeam_channel::{self, Receiver, SendError, Sender, TrySendError};

	pub struct SyncSender<T>(Sender<T>);

	impl<T> SyncSender<T> {
		pub fn send(&self, t: T) -> Result<(), SendError<T>> {
			self.0.send(t)
		}

		pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
			self.0.try_send(t)
		}
	}

	pub fn bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
		crossbeam_channel::bounded(size)
	}
}
