#![deny(unused)]

#[macro_use]
extern crate log;

pub mod constants;
#[macro_use]
mod utils;

mod config;
mod logic;
mod processor;

pub use config::Config;

use std::{fmt, io, net, thread};
use std::collections::HashMap;
use std::sync::{atomic, mpsc, Arc, Mutex};

use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message_network::VersionMessage;

use processor::Ctrl;

#[derive(Debug)]
pub enum Error {
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
			Error::PeerDisconnected(id) => write!(f, "peer disconnected: {}", id),
			Error::Shutdown => write!(f, "P2P is already shut down"),
			Error::Io(ref e) => write!(f, "I/O error: {}", e),
			Error::PeerUnreachable(ref e) => write!(f, "can't reach the peer: {}", e),
			Error::PeerHandshakePending => write!(f, "peer didn't finish the handshake yet"),
		}
	}
}
impl std::error::Error for Error {}

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

/// A signal received from a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum P2PEvent {
	/// The new peer connected, performed the handshake and can be
	/// communicated with.
	Connected(PeerId),
	/// The peer disconnected.
	Disconnected(PeerId),
	/// The peer sent a message.
	Message(PeerId, NetworkMessage),
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
	ctrl_tx: Mutex<mpsc::Sender<Ctrl>>,
	/// Waker to wake up the processor thread.
	waker: mio::Waker,

	/// The incoming event channel.
	/// It's an Option so that the user can take it away.
	event_rx: Mutex<Option<mpsc::Receiver<P2PEvent>>>,

	/// The current block height of our client to advertise to peers.
	block_height: atomic::AtomicU32,

	/// A global signal for our thread to shut down.
	quit: atomic::AtomicBool,
}

impl P2P {
	/// Instantiate a P2P coordinator.
	pub fn new(config: Config) -> Result<Arc<P2P>, Error> {
		let poll = mio::Poll::new()?;
		let waker = mio::Waker::new(poll.registry(), processor::WAKE_TOKEN)?;

		// Create the control message channel.
		let (ctrl_tx, ctrl_rx) = mpsc::channel();

		// Create the incoming message channel.
		let (event_tx, event_rx) = mpsc::channel(); //TODO(stevenroose) make sync

		let p2p = Arc::new(P2P {
			config: config,
			next_peer_id: atomic::AtomicUsize::new(1),
			peers: Mutex::new(HashMap::new()),
			ctrl_tx: Mutex::new(ctrl_tx),
			waker: waker,
			event_rx: Mutex::new(Some(event_rx)),
			block_height: atomic::AtomicU32::new(0),
			quit: atomic::AtomicBool::new(false),
		});

		let p2p_cloned = p2p.clone();
		thread::spawn(|| processor::processor(p2p_cloned, poll, ctrl_rx, event_tx));

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

	/// Ensure we are still running, returning an error otherwise.
	fn ensure_up(&self) -> Result<(), Error> {
		if self.quit.load(atomic::Ordering::Relaxed) {
			Err(Error::Shutdown)
		} else {
			Ok(())
		}
	}

	/// Check if the peer is known, returning an error if it doesn't.
	fn ensure_known_peer(&self, peer: PeerId) -> Result<(), Error> {
		if !self.peers.lock().unwrap().contains_key(&peer) {
			Err(Error::PeerDisconnected(peer))
		} else {
			Ok(())
		}
	}

	/// Shut down the p2p operation. Any subsequent call will be a no-op.
	pub fn shutdown(&self) {
		self.quit.store(true, atomic::Ordering::Relaxed);
		let _ = self.waker.wake(); //TODO(stevenroose) log error?
	}

	/// Add a new peer from an opened TCP stream.
	pub fn add_peer<S: IntoMioTcpStream>(
		&self,
		conn: S,
		peer_type: PeerType,
	) -> Result<PeerId, Error> {
		self.ensure_up()?;
		let conn = conn.into_mio_tcp_stream();
		if let Some(err) = conn.take_error()? {
			return Err(Error::PeerUnreachable(err));
		}

		let id = self.next_peer_id();
		let mut state = PeerState {
			addr: conn.peer_addr()?,
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

		// If this errors, it means that shutdown was called between our check
		// of ensure_up and now.
		let _ = self.ctrl_tx.lock().unwrap().send(Ctrl::Connect(id, conn));

		// Then send the version message to start the handshake.
		self.send_control(Ctrl::SendMsg(id, NetworkMessage::Version(version)));

		Ok(id)
	}

	/// Connect to the given peer.
	pub fn connect_peer(&self, addr: net::SocketAddr) -> Result<PeerId, Error> {
		let conn = mio::net::TcpStream::connect(addr).map_err(Error::PeerUnreachable)?;
		self.add_peer(conn, PeerType::Outbound)
	}

	/// Disconnect the given peer.
	pub fn disconnect_peer(&self, peer: PeerId) -> Result<(), Error> {
		self.ensure_up()?;
		self.ensure_known_peer(peer)?;

		let _ = self.ctrl_tx.lock().unwrap().send(Ctrl::Disconnect(peer));
		Ok(())
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

	/// Take the incoming event channel out of the P2P struct.
	/// This can be done only once.
	pub fn take_event_channel(&self) -> Option<mpsc::Receiver<P2PEvent>> {
		self.event_rx.lock().unwrap().take()
	}

	/// Queue a new control message to the processor thread.
	fn send_control(&self, ctrl: Ctrl) {
		self.ctrl_tx.lock().unwrap().send(ctrl).expect("processor quit");
		self.waker.wake().expect("processor waker error");
	}

	/// Send a message to the given peer.
	pub fn send_message(&self, peer: PeerId, msg: NetworkMessage) -> Result<(), Error> {
		self.ensure_up()?;
		let peers_lock = self.peers.lock().unwrap();
		let state = match peers_lock.get(&peer) {
			Some(s) => s,
			None => return Err(Error::PeerDisconnected(peer)),
		};

		if !state.handshake.finished() {
			return Err(Error::PeerHandshakePending);
		}

		self.send_control(Ctrl::SendMsg(peer, msg));
		Ok(())
	}

	/// Broadcast the message to all peers.
	pub fn broadcast_message(&self, msg: NetworkMessage) -> Result<(), Error> {
		self.ensure_up()?;
		self.send_control(Ctrl::BroadcastMsg(msg));
		Ok(())
	}

	/// Add an inventory item to send to the peer.
	/// They are trickled with randomized intervals.
	///
	/// Blocks are not queued up, but sent immediatelly.
	/// Don't use this to send `inv` messages directly, f.e. when replying to `mempool`.
	/// Just use `send_message` for that.
	pub fn queue_inventory(&self, peer: PeerId, inv: Inventory) -> Result<(), Error> {
		self.ensure_up()?;
		let mut peers_lock = self.peers.lock().unwrap();
		let state = match peers_lock.get_mut(&peer) {
			Some(s) => s,
			None => return Err(Error::PeerDisconnected(peer)),
		};

		if !state.handshake.finished() {
			return Err(Error::PeerHandshakePending);
		}

		if let Some(msg) = logic::inventory::queue_inventory(state, inv) {
			self.send_control(Ctrl::SendMsg(peer, msg));
		}
		Ok(())
	}

	/// Broadcast an inventory item to all peers.
	/// They are tricled with randomized intervals.
	///
	/// Blocks are not queued up, but sent immediatelly.
	pub fn broadcast_inventory(&self, inv: Inventory) -> Result<(), Error> {
		self.ensure_up()?;

		//TODO(stevenroose) consider doing this in the processor instead
		for (peer, state) in self.peers.lock().unwrap().iter_mut() {
			if let Some(msg) = logic::inventory::queue_inventory(state, inv) {
				self.send_control(Ctrl::SendMsg(*peer, msg));
			}
		}

		Ok(())
	}
}
