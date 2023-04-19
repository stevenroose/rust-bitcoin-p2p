// #![deny(unused)]

#[macro_use]
extern crate log;

#[macro_use]
mod utils;

pub mod constants;
pub mod connmgr;

mod config;
mod error;
mod logic;
mod processor;
mod scheduler;

mod mio_io;

pub use config::Config;
pub use error::Error;

use std::{fmt, io, net};
use std::collections::HashMap;
use std::sync::{atomic, mpsc, Arc, Mutex};

use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message_network::VersionMessage;

use processor::{Processor, Ctrl};
use mio_io::{ProcessorThread, ProcessorThreadError, WakerSender};

/// The return type of the [Listener::event] method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerResult {
    /// All normal.
    Ok,
    /// The listener asks for itself to be removed.
    RemoveMe,
}

/// Trait used to listen to events.
///
/// The following types implement this trait:
/// - [std::sync::mpsc::Sender<Event>]
/// - [std::sync::mpsc::SyncSender<Event>] (dropping events when full)
/// - [dyn for <'e> FnMut(&'e Event) -> bool + Send], a closure or function taking an Event,
///   returning false when the listener should be removed.
pub trait Listener: Send + 'static {
	/// Handle a new incoming event.
	///
	/// It's very important that the execution of this method is fast and
	/// certainly never blocks the thread. That's why it's probably advised to
	/// not implement this trait on your actual handler but on a channel to
	/// queue the events or an async function.
	fn event(&mut self, event: &Event) -> ListenerResult;
}

impl Listener for mpsc::Sender<Event> {
	fn event(&mut self, event: &Event) -> ListenerResult {
		match self.send(event.clone()) {
            Ok(_) => ListenerResult::Ok,
            Err(_) => ListenerResult::RemoveMe,
        }
	}
}

impl Listener for mpsc::SyncSender<Event> {
	fn event(&mut self, event: &Event) -> ListenerResult {
		match self.try_send(event.clone()) {
			Ok(()) => ListenerResult::Ok,
			Err(mpsc::TrySendError::Full(e)) => {
				warn!("Sync channel event listener full; dropping event: {:?}", e);
				ListenerResult::Ok
			}
			Err(mpsc::TrySendError::Disconnected(_)) => ListenerResult::RemoveMe,
		}
	}
}

impl<F: for<'e> FnMut(&'e Event) -> ListenerResult + Send + 'static> Listener for F {
	fn event(&mut self, event: &Event) -> ListenerResult {
		self(event)
	}
}

/// A peer identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub(crate) usize);

impl fmt::Display for PeerId {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

/// A signal received from P2P about a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
	/// The new peer connected, performed the handshake and can be
	/// communicated with.
	Connected(PeerId),
	/// The peer disconnected.
	Disconnected(PeerId),
	/// The peer sent a message.
	Message(PeerId, NetworkMessage),
}

impl Event {
	/// The peer this event is about.
	pub fn peer(&self) -> PeerId {
		match self {
			Event::Connected(p) => *p,
			Event::Disconnected(p) => *p,
			Event::Message(p, _) => *p,
		}
	}
}

/// Whether a peer was inbound or outbound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerType {
	Inbound,
	Outbound,
}

/// The internal state for a peer.
#[derive(Debug)]
pub struct PeerState {
	addr: net::SocketAddr,
	local_addr: net::SocketAddr,
	peer_type: PeerType,

	/// Whether this peer prefers to get sent headers first.
	send_headers: bool,

	/// Handshake state for this peer.
	handshake: logic::handshake::Handshake,

	/// Ping statistics to disconnect stale peers.
	ping_stats: logic::pingpong::PingStats,

	/// Our view of the peer's inventory.
	inventory: logic::inventory::PeerInventory,
}

/// Used to add new peers.
pub trait IntoMioTcpStream {
	fn into_mio_tcp_stream(self) -> Result<mio::net::TcpStream, io::Error>;
}
impl IntoMioTcpStream for mio::net::TcpStream {
	fn into_mio_tcp_stream(self) -> Result<mio::net::TcpStream, io::Error> {
		Ok(self)
	}
}
impl IntoMioTcpStream for std::net::TcpStream {
	fn into_mio_tcp_stream(self) -> Result<mio::net::TcpStream, io::Error> {
        self.set_nonblocking(true)?;
		Ok(mio::net::TcpStream::from_std(self))
	}
}

/// The main struct coordinating all P2P activity.
pub struct P2P {
	config: Config,

	/// Tally of the next peer ID to use.
	next_peer_id: atomic::AtomicUsize,

	/// Some consensus-level state for each peer.
	//TODO(stevenroose) consider RwLock?
	peers: Mutex<HashMap<PeerId, PeerState>>,

	/// Handle to send control messages to the processor thread.
	ctrl_tx: Mutex<Option<WakerSender<Ctrl>>>,

	/// The current block height of our client to advertise to peers.
	block_height: atomic::AtomicU32,

    /// The thread we are running in. This can be [None] if the user wants
    /// to manually manage their threads.
    thread: Option<ProcessorThread>,
}

impl P2P {
	/// Instantiate a P2P coordinator.
	pub fn new(config: Config) -> Result<P2P, ProcessorThreadError> {
		// Create the control message channel.
		let (ctrl_tx, ctrl_rx) = mpsc::channel();

		let proc = Processor::new(config.clone(), ctrl_rx);
        let thread = ProcessorThread::new(format!("p2p_{}", config.network))?;
        thread.add_processor(Box::new(proc))?;

		Ok(P2P {
			config: config,
			next_peer_id: atomic::AtomicUsize::new(1),
			peers: Mutex::new(HashMap::new()),
			ctrl_tx: Mutex::new(None),
			block_height: atomic::AtomicU32::new(0),
            thread: Some(thread),
		})
	}

	/// Get the configuration of the P2P instance.
	pub fn config(&self) -> &Config {
		&self.config
	}

	/// Set the latest block height to advertise to peers.
	pub fn set_height(&self, block_height: u32) {
		self.block_height.store(block_height, atomic::Ordering::Relaxed);
	}

	/// Assign the next [PeerId].
	fn next_peer_id(&self) -> PeerId {
		PeerId(self.next_peer_id.fetch_add(1, atomic::Ordering::Relaxed))
	}

	/// Check if the peer is known, returning an error if it doesn't.
	fn ensure_known_peer(&self, peer: PeerId) -> Result<(), Error> {
		if !self.peers.lock().unwrap().contains_key(&peer) {
			Err(Error::PeerDisconnected(peer))
		} else {
			Ok(())
		}
	}

	/// Queue a new control message to the processor thread.
	fn send_ctrl(&self, ctrl: Ctrl) -> Result<(), Error> {
		self.ctrl_tx.lock().unwrap().as_ref().ok_or(Error::NotStartedYet)?
            .send(ctrl).map_err(|_| Error::Shutdown)
	}

	/// Shut down the p2p operation. Any subsequent call will be a no-op.
	pub fn shutdown(&self) -> Result<(), Error> {
		self.send_ctrl(Ctrl::Shutdown).map_err(|_| Error::Shutdown)
	}

	/// Add a new peer from an opened TCP stream.
	pub fn add_peer<S: IntoMioTcpStream>(
		&self,
		conn: S,
		peer_type: PeerType,
	) -> Result<PeerId, Error> {
		let conn = conn.into_mio_tcp_stream()?;
		if let Some(err) = conn.take_error()? {
			return Err(Error::PeerUnreachable(err));
		}
		let addr = conn.peer_addr()?;

		let id = self.next_peer_id();
		let mut state = PeerState {
			addr: addr,
			local_addr: conn.local_addr()?,
			peer_type: peer_type,
			send_headers: false,
			handshake: Default::default(),
			ping_stats: Default::default(),
			inventory: logic::inventory::PeerInventory::new(&self.config),
		};

		let start_height = self.block_height.load(atomic::Ordering::Relaxed);
		let version =
			logic::handshake::make_version_msg(&self.config, id, &mut state, start_height);
		state.handshake.version_sent = true;
		assert!(self.peers.lock().unwrap().insert(id, state).is_none());

		self.send_ctrl(Ctrl::Connect(id, addr, conn))?;
		self.send_ctrl(Ctrl::SendMsg(id, NetworkMessage::Version(version)))?;

		Ok(id)
	}

	/// Connect to the given peer.
	pub fn connect_peer(&self, addr: net::SocketAddr) -> Result<PeerId, Error> {
		let conn = mio::net::TcpStream::connect(addr).map_err(Error::PeerUnreachable)?;
		self.add_peer(conn, PeerType::Outbound)
	}

	/// Disconnect the given peer.
	pub fn disconnect_peer(&self, peer: PeerId) -> Result<(), Error> {
		self.ensure_known_peer(peer)?;

		self.send_ctrl(Ctrl::Disconnect(peer))
	}

	/// Disconnect the peer and don't reconnect to it.
	pub fn ban_peer(&self, peer: PeerId) -> Result<(), Error> {
		self.disconnect_peer(peer)?;
		//TODO(stevenroose) keep an LRU list of banned peers.
		Ok(())
	}

	/// The number of connected peers.
	pub fn nb_connected_peers(&self) -> usize {
		self.peers.lock().unwrap().len()
	}

	/// Get the peer's version message.
	pub fn peer_version(&self, peer: PeerId) -> Result<Option<VersionMessage>, Error> {
		match self.peers.lock().unwrap().get(&peer) {
			Some(s) => Ok(s.handshake.version.clone()),
			None => Err(Error::PeerDisconnected(peer)),
		}
	}

	/// Send a message to the given peer.
	pub fn send_message(&self, peer: PeerId, msg: NetworkMessage) -> Result<(), Error> {
		let peers_lock = self.peers.lock().unwrap();
		let state = match peers_lock.get(&peer) {
			Some(s) => s,
			None => return Err(Error::PeerDisconnected(peer)),
		};

		if !state.handshake.finished() {
			return Err(Error::PeerHandshakePending);
		}

		self.send_ctrl(Ctrl::SendMsg(peer, msg))
	}

	/// Broadcast the message to all peers.
	pub fn broadcast_message(&self, msg: NetworkMessage) -> Result<(), Error> {
		self.send_ctrl(Ctrl::BroadcastMsg(msg))
	}

	/// Add an inventory item to send to the peer.
	/// They are trickled with randomized intervals.
	///
	/// Blocks are not queued up, but sent immediatelly.
	/// Don't use this to send `inv` messages directly, f.e. when replying to `mempool`.
	/// Just use `send_message` for that.
	pub fn queue_inventory(&self, peer: PeerId, inv: Inventory) -> Result<(), Error> {
		let mut peers_lock = self.peers.lock().unwrap();
		let state = match peers_lock.get_mut(&peer) {
			Some(s) => s,
			None => return Err(Error::PeerDisconnected(peer)),
		};

		if !state.handshake.finished() {
			return Err(Error::PeerHandshakePending);
		}

		if let Some(msg) = logic::inventory::queue_inventory(state, inv) {
			self.send_ctrl(Ctrl::SendMsg(peer, msg))?;
		}
		Ok(())
	}

	/// Broadcast an inventory item to all peers.
	/// They are tricled with randomized intervals.
	///
	/// Blocks are not queued up, but sent immediatelly.
	pub fn broadcast_inventory(&self, inv: Inventory) -> Result<(), Error> {
		//TODO(stevenroose) consider doing this in the processor instead
		for (peer, state) in self.peers.lock().unwrap().iter_mut() {
			if let Some(msg) = logic::inventory::queue_inventory(state, inv) {
				self.send_ctrl(Ctrl::SendMsg(*peer, msg))?;
			}
		}

		Ok(())
	}

	/// Add a new listener.
	///
	/// See the documentation on the [Listener] trait to see what common
	/// types implement this trait.
	///
	/// If you simply want to get an [std::mpsc::Receiver] for all events, use
	/// [create_listener_channel].
	pub fn add_listener(&self, listener: impl Listener) -> Result<(), Error> {
		self.send_ctrl(Ctrl::AddListener(Box::new(listener)))
	}
	// pub fn add_listener_function(&self, listener: impl for<'e> FnMut(&'e Event) -> bool + Send + 'static) -> Result<(), Error> {
	//	self.send_ctrl(Ctrl::AddListener(Box::new(listener)))
	// }

	/// Create a new [std::mpsc::channel] and register it as a listener.
	///
	/// Note that this channel is unbounded so failure to read its content
	/// causes increased memory usage.
	pub fn create_listener_channel(&self) -> Result<mpsc::Receiver<Event>, Error> {
		let (tx, rx) = mpsc::channel();
		self.add_listener(tx)?;
		Ok(rx)
	}
}
