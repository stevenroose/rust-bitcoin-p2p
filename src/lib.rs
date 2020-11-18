#![deny(unused)]

#[macro_use]
extern crate log;

#[macro_use]
mod utils;

pub mod constants;

mod config;
mod error;
mod logic;
mod processor;

pub use config::Config;
pub use error::Error;

use std::{fmt, net, thread};
use std::collections::HashMap;
use std::sync::{atomic, mpsc, Arc, Mutex};

use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message_network::VersionMessage;

use processor::Ctrl;
use utils::WakerSender;


/// Trait used to listen to events.
///
/// The following types implement this trait:
/// - [std::sync::mpsc::Sender<Event>]
/// - [std::sync::mpsc::SyncSender<Event>] (dropping events when full)
// /// - [Fn(&Event) -> bool]
pub trait Listener: Send {
	/// Handle a new incoming event.
	///
	/// Should return false if the listener should be removed, true otherwise.
	///
	/// It's very important that the execution of this method is fast and
	/// certainly never blocks the thread. That's why it's probably advised to
	/// not implement this trait on your actual handler but on a channel to
	/// queue the events or an async function.
	fn event(&mut self, event: &Event) -> bool;
}

impl Listener for mpsc::Sender<Event> {
	fn event(&mut self, event: &Event) -> bool {
		self.send(event.clone()).is_ok()
	}
}

impl Listener for mpsc::SyncSender<Event> {
	fn event(&mut self, event: &Event) -> bool {
		match self.try_send(event.clone()) {
			Ok(()) => true,
			Err(mpsc::TrySendError::Full(e)) => {
				warn!("Sync channel event listener full; dropping event: {:?}", e);
				true
			}
			Err(mpsc::TrySendError::Disconnected(_)) => false,
		}
	}
}

// pub type EventHandler = FnMut(&Event) -> bool;

// impl Listener for EventHandler {
// 	fn event(&mut self, event: &Event) -> bool {
// 		self(event)
// 	}
// }

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
	fn into_mio_tcp_stream(self) -> mio::net::TcpStream;
}
impl IntoMioTcpStream for mio::net::TcpStream {
	fn into_mio_tcp_stream(self) -> mio::net::TcpStream {
		self
	}
}
impl IntoMioTcpStream for std::net::TcpStream {
	fn into_mio_tcp_stream(self) -> mio::net::TcpStream {
		mio::net::TcpStream::from_std(self)
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
	ctrl_tx: Mutex<WakerSender<Ctrl>>,

	/// The current block height of our client to advertise to peers.
	block_height: atomic::AtomicU32,
}

impl P2P {
	/// Instantiate a P2P coordinator.
	pub fn new(config: Config) -> Result<Arc<P2P>, Error> {
		// Create the control message channel.
		let (ctrl_tx, ctrl_rx) = mpsc::channel();

		// Create the processor thread and receive the waker.
		let (processor, waker) = processor::Thread::new(ctrl_rx)?;
		let ctrl_tx = WakerSender::new(ctrl_tx, waker);

		let p2p = Arc::new(P2P {
			config: config,
			next_peer_id: atomic::AtomicUsize::new(1),
			peers: Mutex::new(HashMap::new()),
			ctrl_tx: Mutex::new(ctrl_tx),
			block_height: atomic::AtomicU32::new(0),
		});

		let p2p_cloned = p2p.clone();
		thread::Builder::new()
			.name("bitcoin_p2p_thread".into())
			.spawn(|| processor.run(p2p_cloned))?;

		Ok(p2p)
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
		self.ctrl_tx.lock().unwrap().send(ctrl).map_err(|_| Error::Shutdown)
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
		let conn = conn.into_mio_tcp_stream();
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
	pub fn add_listener<L: Listener + 'static>(&self, listener: L) -> Result<(), Error> {
		self.send_ctrl(Ctrl::AddListener(Box::new(listener)))
	}
}
