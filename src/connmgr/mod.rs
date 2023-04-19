
use std::{io, fmt, net};
use std::collections::{HashMap, HashSet};
use std::sync::{mpsc, Arc, Mutex};
use std::num::NonZeroUsize;

use lru::LruCache;

use crate::{Event, ListenerResult, PeerId, PeerType};
use crate::mio_io::{
    IoProcessor, ProcessorThread, ProcessorThreadError, TokenTally, WakerSender, WakerSenderError,
    WAKE_TOKEN,
};

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
	pub disconnected_peers_cache_size: NonZeroUsize,
}
//TODO(stevenroose) default

/// Closure used to add peers.
pub type AddPeerFn = dyn FnMut(mio::net::TcpStream, PeerType) -> Option<PeerId> + Send + 'static;

/// A listener for the connection manager.
#[derive(Debug)]
pub struct ConnMgrListener(WakerSender<Event>);

impl crate::Listener for ConnMgrListener {
	fn event(&mut self, event: &Event) -> ListenerResult {
		// Skip Message events.
		if let Event::Message(..) = event {
			//TODO(stevenroose) perhaps we want some messages?
			return ListenerResult::Ok;
		}

		match self.0.send(event.clone()) {
			Ok(()) => ListenerResult::Ok,
			// The channel disconnected.
			Err(WakerSenderError::Send(_)) => ListenerResult::RemoveMe,
			// The waker has I/O problems, the mio::Poll might no longer exist.
			Err(WakerSenderError::Wake(_)) => ListenerResult::RemoveMe,
		}
	}
}

#[derive(Debug)]
struct PeerInfo {
	addr: net::SocketAddr,
	peer_type: PeerType,
	handshake: bool,
}

struct Data {
    /// The interfaces we are listening on.
    listeners: HashSet<net::SocketAddr>,

	/// The set of connected peers.
	connected: HashMap<PeerId, PeerInfo>,
	/// The set of recently disconnected peers.
	disconnected: LruCache<net::SocketAddr, ()>,
}

/// Manager the amount of connections to maintain
/// and listen for incoming connections etc.
pub struct ConnectionManager {
	config: Config,

    /// The data shared with the runtime.
    data: Arc<Mutex<Data>>,

	/// Handle to the control msg channel.
	ctrl_tx: WakerSender<Ctrl>,
}

impl ConnectionManager {
	/// Create a new [ConnectionManager] that starts inside the given
    /// [ProcessorThread] upon creation.
	pub fn start_in(
        thread: &ProcessorThread,
		config: Config,
		add_peer: Box<AddPeerFn>,
	) -> Result<(ConnectionManager, ConnMgrListener), ProcessorThreadError> {
        let data = Arc::new(Mutex::new(Data {
			connected: HashMap::new(),
			disconnected: LruCache::new(config.disconnected_peers_cache_size),
            listeners: HashSet::new(),
        }));

		let (ctrl_tx, ctrl_rx) = mpsc::channel();
		let (event_tx, event_rx) = mpsc::channel();
		let ctrl_tx = WakerSender::new(ctrl_tx, thread.waker()?);
		let event_tx = WakerSender::new(event_tx, thread.waker()?);

		let proc = Processor {
            data: data.clone(),
            ctrl_rx: ctrl_rx,
            event_rx: event_rx,
            tcp_listener_id_counter: 0,
            listeners: Vec::new(),
            add_peer_fn: add_peer,
        };
        thread.add_processor(Box::new(proc))?;

		let mgr = ConnectionManager {
            data: data,
			ctrl_tx: ctrl_tx,
			config: config,
		};

		Ok((mgr, ConnMgrListener(event_tx)))
	}

    /// The set of 
    pub fn listeners(&self) -> HashSet<net::SocketAddr> {
        self.data.lock().unwrap().listeners.clone()
    }

	fn send_ctrl(&self, ctrl: Ctrl) -> Result<(), Error> {
		self.ctrl_tx.send(ctrl).map_err(|_| Error::Shutdown)
	}

    /// Add a new TCP listener.
    pub fn add_mio_listener(&self, listener: mio::net::TcpListener) -> Result<(), Error> {
		self.send_ctrl(Ctrl::AddListener(listener.local_addr()?, listener))
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
		self.send_ctrl(Ctrl::RemoveListener(addr))
	}
}

enum Ctrl {
	/// Add a new TCP listener.
	AddListener(net::SocketAddr, mio::net::TcpListener),
	/// Remove a TCP listener.
	RemoveListener(net::SocketAddr),
}

struct Listener {
	token: mio::Token,
	addr: net::SocketAddr,
	listener: mio::net::TcpListener,
}

struct Processor {
    data: Arc<Mutex<Data>>,

	ctrl_rx: mpsc::Receiver<Ctrl>,
	event_rx: mpsc::Receiver<Event>,

    // Here we'll keep all our TCP listeners.
    // Just keep a vector as we don't expect this to be big.
    tcp_listener_id_counter: usize,
    listeners: Vec::<Listener>,
	
	// Closures to add and remove peers.
	add_peer_fn: Box<AddPeerFn>,
}

impl Processor {
	fn add_peer(&mut self, addr: net::SocketAddr, stream: mio::net::TcpStream, tp: PeerType) {
		if let Some(id) = (*self.add_peer_fn)(stream, tp) {
            let info = PeerInfo {
                addr: addr,
                peer_type: tp,
                handshake: false,
            };
            if let Some(dup) = self.data.lock().unwrap().connected.insert(id, info) {
                error!("Duplicate peer ID {}: {} and {}", id, dup.addr, addr);
            }
        }
	}

	fn peer_connected(&mut self, peer: PeerId) {
		if let Some(info) = self.data.lock().unwrap().connected.get_mut(&peer) {
			info.handshake = true;
			debug!("Peer {} ({}) finished handshake", peer, info.addr);
		} else {
			error!("Received Connected event for peer that we don't know: {}", peer);
		}
	}

	fn peer_disconnected(&mut self, peer: PeerId) {
		if let Some(info) = self.data.lock().unwrap().connected.remove(&peer) {
			debug!("Peer {} ({}) disconnected", peer, info.addr);
			self.data.lock().unwrap().disconnected.put(info.addr, ());
		} else {
			error!("Received Disconnected event for peer that we don't know: {}", peer);
		}
	}
}

impl IoProcessor for Processor {
	fn wakeup(&mut self, tt: &TokenTally, rg: &mio::Registry, ev: &mio::Events) {
        trace!("ConnectionManager runtime called");

        while let Ok(ctrl) = self.ctrl_rx.try_recv() {
            match ctrl {
                Ctrl::AddListener(addr, mut list) => {
                    self.tcp_listener_id_counter += 1;

                    let token = tt.next();
                    rg.register(&mut list, token.into(), mio::Interest::READABLE)
                        .expect("TCP listener poll registry failed");

                    // Check for duplicates.
                    if self.listeners.iter().any(|l| l.addr == addr) {
                        error!("Duplicate listener address: {}; ignoring", addr);
                        continue;
                    }
                    
                    self.listeners.push(Listener {
                        token: token,
                        addr: addr,
                        listener: list,
                    });
                }

                Ctrl::RemoveListener(addr) => {
                    let len = self.listeners.len();
                    self.listeners.retain(|l| l.addr != addr);
                    if self.listeners.len() != len {
                        debug!("Stopped listening on {}", addr);
                    } else {
                        error!("Asked to stop listening on {} but we are not", addr);
                    }
                }
            }
        }

        //TODO(stevenroose) 
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                Event::Connected(peer) => self.peer_connected(peer),
                Event::Disconnected(peer) => self.peer_disconnected(peer),
                Event::Message(..) => unreachable!("our listener filters these"),
            }
        }

        // borrowck workaround
        let mut new_peers = Vec::new();
        'events:
        for event in ev.iter().filter(|e| e.token() != WAKE_TOKEN) {
            let list = or!(self.listeners.iter_mut().find(|l| l.token == event.token()), continue);
            let addr = list.addr;

            'accept:
            loop {
                match list.listener.accept() {
                    Ok((stream, new_addr)) => {
                        debug!("New TCP stream connected on {}: {}", addr, new_addr);
                        new_peers.push((new_addr, stream, PeerType::Inbound));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        break 'accept;
                    }
                    Err(e) => {
                        error!("TCP listener error for {}: {}", addr, e);
                        drop(list);
                        self.listeners.retain(|l| l.token != event.token());
                        continue 'events;
                    }
                }
            }
        }
        for (addr, stream, tp) in new_peers {
            self.add_peer(addr, stream, tp);
        }
	}
}
