
use std::{io, fmt, net, ptr, thread};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};

use lru::LruCache;

use crate::{Event, PeerId, PeerType};
use crate::utils::{self, WakerSender};

const WAKE_TOKEN: mio::Token = mio::Token(0);

/// Connection manager related errors.
#[derive(Debug)]
pub enum Error {
	/// The connection manager is already shut down.
	Shutdown,
	/// An I/O error.
	Io(io::Error),
}

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Error::Io(e)
	}
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Error::Shutdown => write!(f, "ConnectionManager is already shut down"),
			Error::Io(ref e) => write!(f, "I/O error: {}", e),
		}
	}
}
impl std::error::Error for Error {}

/// [ConnectionManager] configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Target number of outbound peer connections.
    pub target_outbound_peers: usize,

    /// Maximum number of inbound peer connections.
    pub max_inbound_peers: usize,

	/// Maximum number of disconnected peers to remember.
	pub disconnected_peers_cache_size: usize,
}
//TODO(stevenroose) default

/// Closure used to add peers.
pub type AddPeerFn = dyn FnMut(mio::net::TcpStream, PeerType) -> Result<PeerId, crate::Error> + Send;

/// Closure used to remove peers.
pub type RemovePeerFn = dyn FnMut(PeerId) -> Result<(), crate::Error> + Send;

/// A listener for the connection manager.
#[derive(Debug)]
pub struct ConnMgrListener(WakerSender<Event>);

impl crate::Listener for ConnMgrListener {
	fn event(&mut self, event: &Event) -> bool {
		// Skip Message events.
		if let Event::Message(..) = event {
			//TODO(stevenroose) perhaps we want some messages?
			return true;
		}

		match self.0.send(event.clone()) {
			Ok(()) => true,
			// The channel disconnected.
			Err(utils::WakerSenderError::Send(_)) => false,
			// The waker has I/O problems, the mio::Poll might no longer exist.
			Err(utils::WakerSenderError::Wake(_)) => false,
		}
	}
}

#[derive(Debug)]
struct PeerInfo {
	addr: net::SocketAddr,
	peer_type: PeerType,
	handshake: bool,
}

/// Manager the amount of connections to maintain
/// and listen for incoming connections etc.
pub struct ConnectionManager {
	config: Config,

    /// The interfaces we are listening on.
    listeners: Mutex<HashSet<net::SocketAddr>>,

	/// The set of connected peers.
	connected: Mutex<HashMap<PeerId, PeerInfo>>,
	/// The set of recently disconnected peers.
	disconnected: Mutex<LruCache<net::SocketAddr, ()>>,

	/// Handle to the control msg channel.
	ctrl_tx: Mutex<WakerSender<Ctrl>>,
}

impl ConnectionManager {
	/// Create a new [ConnectionManager] that starts upon creation.
	///
	/// Also returns an event [Listener] that should be registred as a
	/// listener with the [P2P].
	pub fn new<A, R>(
		config: Config,
		add_peer: A,
		remove_peer: R,
	) -> Result<(Arc<ConnectionManager>, ConnMgrListener), io::Error>
	where
		A: FnMut(mio::net::TcpStream, PeerType) -> Result<PeerId, crate::Error> + Send + 'static,
		R: FnMut(PeerId) -> Result<(), crate::Error> + Send + 'static,
	{
		let (ctrl_tx, ctrl_rx) = mpsc::channel();
		let (event_tx, event_rx) = mpsc::channel();

		let (thread, poll_registry) = RuntimeData::new(
			ctrl_rx,
			event_rx,
			Box::new(add_peer),
			Box::new(remove_peer),
		)?;

		let ctrl_tx = WakerSender::new(ctrl_tx, mio::Waker::new(&poll_registry, WAKE_TOKEN)?);
		let event_tx = WakerSender::new(event_tx, mio::Waker::new(&poll_registry, WAKE_TOKEN)?);

		let mgr = Arc::new(ConnectionManager {
			connected: Mutex::new(HashMap::new()),
			disconnected: Mutex::new(LruCache::new(config.disconnected_peers_cache_size)),
			ctrl_tx: Mutex::new(ctrl_tx),
			config: config,
		});

		let mgr_cloned = mgr.clone();
		thread::Builder::new()
			.name("bitcoin_p2p_connmgr_thread".into())
			.spawn(|| thread.run(mgr_cloned))?;

		Ok((mgr, ConnMgrListener(event_tx)))
	}

    /// The set of listeners we are listening on.
    pub fn listeners(&self) -> HashSet<net::SocketAddr> {
        self.listeners.lock().unwrap().clone()
    }

    pub fn start(self: &Arc<ConnectionManager>) {
        //TODO(stevenroose) 
    }

	fn send_ctrl(&self, ctrl: Ctrl) -> Result<(), Error> {
		self.ctrl_tx.lock().unwrap().send(ctrl).map_err(|_| Error::Shutdown)
	}

    /// Add a new TCP listener.
    // dev note: It's important to use this method internally in the other methods
    // as it registers the listener in the local set.
    pub fn add_mio_listener(&self, listener: mio::net::TcpListener) -> Result<(), Error> {
        self.listeners.lock().unwrap().insert(listener.local_addr()?);
		self.send_ctrl(Ctrl::AddListener(addr, list))
    }

	/// Start listening for incoming connections on the given address.
	pub fn start_listening(&self, addr: net::SocketAddr) -> Result<(), Error> {
		self.add_mio_listener(mio::net::TcpListener::bind(addr)?)
	}

    /// Add a new listener using the [std::net::TcpListener] type.
    pub fn add_std_listener(&self, listener: net::TcpListener) -> Result<(), Error> {
        listener.set_nonblocking(true)?;
        self.add_mio_listener(mio::net::TcpListener::from_std(listener))
    }

	/// Stop listening on the given address.
	pub fn stop_listening(&self, addr: net::SocketAddr) -> Result<(), Error> {
        //TODO(stevenroose) should we error here if we notice we don't have this addr?
        self.listeners.lock().unwrap().remove(&addr);
		self.send_ctrl(Ctrl::RemoveListener(addr))
	}
}

enum Ctrl {
	/// Add a new TCP listener.
	AddListener(net::SocketAddr, mio::net::TcpListener),
	/// Remove a TCP listener.
	RemoveListener(net::SocketAddr),
}

struct RuntimeData {
	mgr: Arc<ConnectionManager>,

	poll: mio::Poll,
	ctrl_rx: mpsc::Receiver<Ctrl>,
	event_rx: mpsc::Receiver<Event>,
	
	// Closures to add and remove peers.
	add_peer_fn: Box<AddPeerFn>,
	remove_peer_fn: Box<RemovePeerFn>,
}

struct Listener {
	token: mio::Token,
	addr: net::SocketAddr,
	listener: mio::net::TcpListener,
}

impl RuntimeData {
	fn new(
		ctrl_rx: mpsc::Receiver<Ctrl>,
		event_rx: mpsc::Receiver<Event>,
		add_peer_fn: Box<AddPeerFn>,
		remove_peer_fn: Box<RemovePeerFn>,
	) -> Result<(RuntimeData, mio::Registry), io::Error> {
		let poll = mio::Poll::new()?;
		let registry = poll.registry().try_clone()?;

		let thread = RuntimeData {
			mgr: unsafe { Arc::from_raw(ptr::null()) },
			poll: poll,
			ctrl_rx: ctrl_rx,
			event_rx: event_rx,
			add_peer_fn: add_peer_fn,
			remove_peer_fn: remove_peer_fn,
		};
		Ok((thread, registry))
	}

	fn add_peer(&mut self, addr: net::SocketAddr, stream: mio::net::TcpStream, tp: PeerType) {
		let id = match (*self.add_peer_fn)(stream, tp) {
			Ok(id) => id,
			Err(e) => {
				warn!("Error adding peer with address {}: {}", addr, e);
				return;
			}
		};

		let info = PeerInfo {
			addr: addr,
			peer_type: tp,
			handshake: false,
		};
		if let Some(dup) = self.mgr.connected.lock().unwrap().insert(id, info) {
			error!("Duplicate peer ID {}: {} and {}", id, dup.addr, addr);
		}
	}

	fn peer_connected(&mut self, peer: PeerId) {
		if let Some(info) = self.mgr.connected.lock().unwrap().get_mut(&peer) {
			info.handshake = true;
			debug!("Peer {} ({}) finished handshake", peer, info.addr);
		} else {
			error!("Received Connected event for peer that we don't know: {}", peer);
		}
	}

	fn peer_disconnected(&mut self, peer: PeerId) {
		if let Some(info) = self.mgr.connected.lock().unwrap().remove(&peer) {
			debug!("Peer {} ({}) disconnected", peer, info.addr);
			self.mgr.disconnected.lock().unwrap().put(info.addr, ());
		} else {
			error!("Received Disconnected event for peer that we don't know: {}", peer);
		}
	}

	fn run(mut self, mgr: Arc<ConnectionManager>) {
		self.mgr = mgr;
		info!("ConnectionManager thread started");

		// A counter for mio tokens to keep.
		let mut token_counter = 0;

		// Here we'll keep all our TCP listeners.
		// Just keep a vector as we don't expect this to be big.
		let mut listeners = Vec::<Listener>::new();

		// To capture poll events.
		let mut events = mio::Events::with_capacity(1024);
		
		loop {
			events.clear();

			self.poll.poll(&mut events, None).expect("poll error");
			trace!("connmgr woken up");

			while let Ok(ctrl) = self.ctrl_rx.try_recv() {
				match ctrl {
					Ctrl::AddListener(addr, mut list) => {
						token_counter += 1;

						let token = mio::Token(token_counter);
						self.poll.registry().register(&mut list, token, mio::Interest::READABLE)
							.expect("TCP listener poll registry failed");

						// Check for duplicates.
						if listeners.iter().any(|l| l.addr == addr) {
							error!("Duplicate listener address: {}; ignoring", addr);
							continue;
						}
						
						listeners.push(Listener {
							token: token,
							addr: addr,
							listener: list,
						});
					}

					Ctrl::RemoveListener(addr) => {
						let len = listeners.len();
						listeners.retain(|l| l.addr != addr);
						if listeners.len() != len {
							debug!("Stopped listening on {}", addr);
						} else {
							error!("Asked to stop listening on {} but we are not", addr);
						}
					}
				}
			}

			while let Ok(event) = self.event_rx.try_recv() {
				match event {
					Event::Connected(peer) => self.peer_connected(peer),
					Event::Disconnected(peer) => self.peer_disconnected(peer),
					Event::Message(..) => unreachable!("our listener filters these"),
				}
			}

			'events:
			for event in events.iter().filter(|e| e.token() != WAKE_TOKEN) {
				let list = or!(listeners.iter_mut().find(|l| l.token == event.token()), continue);

				'accept:
				loop {
					match list.listener.accept() {
						Ok((stream, addr)) => {
							debug!("New TCP stream connected on {}: {}", list.addr, addr);
							self.add_peer(addr, stream, PeerType::Inbound);
						}
						Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
							break 'accept;
						}
						Err(e) => {
							error!("TCP listener error for {}: {}", list.addr, e);
							drop(list);
							listeners.retain(|l| l.token != event.token());
							continue 'events;
						}
					}
				}
			}
		}
	}
}
