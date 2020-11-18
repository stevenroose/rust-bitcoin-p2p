use std::io;
use std::time::Duration;
use std::sync::mpsc;

use rand_distr::{Distribution, Poisson};

use crate::{Event, Listener};

/// Get a poisson sample averaging the given number.
pub fn poisson_u64(avg: u64) -> u64 {
	debug_assert!(avg > 0);
	let dist = Poisson::new(avg as f64).expect("positive");
	dist.sample(&mut rand::thread_rng()) as u64
}

/// Get a poisson sample averaging the given [Duration].
pub fn poisson_duration(avg: Duration) -> Duration {
	Duration::from_secs(poisson_u64(avg.as_secs()))
}

macro_rules! or {
	($opt:expr, $other:tt) => {
		match $opt {
			Some(v) => v,
			None => $other,
		}
	};
}

/// An error from calling the [WakerSender::send] method.
#[derive(Debug)]
pub enum WakerSenderError<T> {
	/// An error sending on the sender.
	Send(mpsc::SendError<T>),
	/// An error waking up the waker.
	Wake(io::Error),
}

/// A wrapper of an [mpsc::Sender] with a `mio` waker so that a thread can be
/// woken up when an item is sent on the channel.
#[derive(Debug)]
pub struct WakerSender<T> {
	sender: mpsc::Sender<T>,
	waker: mio::Waker,
}

impl<T> WakerSender<T> {
	/// Create a new [WakerSender].
	pub fn new(sender: mpsc::Sender<T>, waker: mio::Waker) -> WakerSender<T> {
		WakerSender {
			sender: sender,
			waker: waker,
		}
	}

	/// Send on the channel.
	pub fn send(&self, item: T) -> Result<(), WakerSenderError<T>> {
		self.sender.send(item).map_err(WakerSenderError::Send)?;
		self.waker.wake().map_err(WakerSenderError::Wake)?;
		Ok(())
	}
}

impl Listener for WakerSender<Event> {
	fn event(&mut self, event: &Event) -> bool {
		match self.send(event.clone()) {
			Ok(()) => true,
			// The channel disconnected.
			Err(WakerSenderError::Send(_)) => false,
			// The waker has I/O problems, the mio::Poll might no longer exist.
			Err(WakerSenderError::Wake(_)) => false,
		}
	}
}
