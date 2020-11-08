
//TODO(stevenroose) remove later
#![allow(unused)]

#[macro_use]
extern crate log;


mod processor;


use std::{fmt, io, net, sync, thread};
use std::sync::{Arc, Mutex};

use bitcoin::network::message::NetworkMessage;

use processor::Ctrl;

#[derive(Debug)]
pub enum Error {
	/// We have already shut down.
	Shutdown,
	/// An I/O error.
	Io(io::Error),
}

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Error::Io(e)
	}
}

/// A peer identifier.
///
/// Can never be 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub(crate) usize);

impl fmt::Display for PeerId {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

pub enum PeerType {
	Inbound,
	Outbound,
}

/// Configuration options for P2P.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
	pub network: bitcoin::Network,
}

/// The main struct coordinating all P2P activity.
pub struct P2P {
	config: Config,

	/// Tally of the next peer ID to use.
	next_peer_id: sync::atomic::AtomicUsize,

	/// Handle to send control messages to the processor thread.
	ctrl_tx: Mutex<sync::mpsc::Sender<Ctrl>>,
	/// Waker to wake up the processor thread.
	waker: mio::Waker,

	/// The incoming message channel.
	/// It's an Option so that the user can take it away.
	in_rx: Mutex<Option<sync::mpsc::Receiver<(PeerId, NetworkMessage)>>>,
	/// The outgoing message channel.
	out_tx: Mutex<sync::mpsc::Sender<(PeerId, NetworkMessage)>>,

	/// A global signal for our thread to shut down.
	quit: sync::atomic::AtomicBool,
}

impl P2P {
	/// Instantiate a P2P coordinator.
	pub fn new(config: Config) -> Result<Arc<P2P>, Error> {
		let poll = mio::Poll::new()?;
		let waker = mio::Waker::new(poll.registry(), processor::WAKE_TOKEN)?;

		// Create the control message channel.
		let (ctrl_tx, ctrl_rx) = sync::mpsc::channel();

		// Create the incoming message channel.
		let (in_tx, in_rx) = sync::mpsc::channel(); //TODO(stevenroose) make sync

		// Create the outgoing message channel.
		let (out_tx, out_rx) = sync::mpsc::channel(); //TODO(stevenroose) make sync

		let p2p = Arc::new(P2P {
			config: config,
			next_peer_id: sync::atomic::AtomicUsize::new(1),
			ctrl_tx: Mutex::new(ctrl_tx),
			waker: waker,
			in_rx: Mutex::new(Some(in_rx)),
			out_tx: Mutex::new(out_tx),
			quit: sync::atomic::AtomicBool::new(false),
		});

		let p2p_cloned = p2p.clone();
		thread::spawn(|| processor::processor(
			p2p_cloned, poll, ctrl_rx, in_tx, out_rx,
		));

		Ok(p2p)
	}

	/// Assign the next [PeerId].
	fn next_peer_id(&self) -> PeerId {
		PeerId(self.next_peer_id.fetch_add(1, sync::atomic::Ordering::Relaxed))
	}

	/// Ensure we are still running, returning an error otherwise.
	fn ensure_up(&self) -> Result<(), Error> {
		if self.quit.load(sync::atomic::Ordering::Relaxed) {
			Err(Error::Shutdown)
		} else {
			Ok(())
		}
	}

	/// Shut down the p2p operation. Any subsequent call will be a no-op.
	pub fn shutdown(&self) {
		self.quit.store(true, sync::atomic::Ordering::Relaxed);
		let _ = self.waker.wake(); //TODO(stevenroose) log error?
	}

	/// Add a new peer from an opened TCP stream.
	//TODO(stevenroose) consider taking both mio and std TcpStreams using a trait
	pub fn add_peer(&self, conn: mio::net::TcpStream, peer_type: PeerType) -> Result<PeerId, Error> {
		self.ensure_up()?;
		let id = self.next_peer_id();

		// If this errors, it means that shutdown was called between our check
		// of ensure_up and now.
		let _ = self.ctrl_tx.lock().unwrap().send(Ctrl::AddPeer { id: id, conn: conn });

		Ok(id)
	}

	/// Take the incoming message channel out of the P2P struct.
	/// This can be done only once.
	pub fn take_incoming_message_channel(&self) -> Option<sync::mpsc::Receiver<(PeerId, NetworkMessage)>> {
		self.in_rx.lock().unwrap().take()
	}
}
