//! Peer manager. Handles peer negotiation (handshake).
//!
//! The steps for an *outbound* handshake are:
//!
//!   1. Send `version` message.
//!   2. Expect `version` message from remote.
//!   3. Expect `verack` message from remote.
//!   4. Send `verack` message.
//!
//! The steps for an *inbound* handshake are:
//!
//!   1. Expect `version` message from remote.
//!   2. Send `version` message.
//!   3. Send `verack` message.
//!   4. Expect `verack` message from remote.
//!

use std::{io, net};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_network::VersionMessage;
use crossbeam_channel as chan;

use crate::{DisconnectReason, Error, Height, Link, PeerId, Whitelist, Wire};
use crate::addrmgr::{self, AddressSource, Domain, Source};
use crate::events::{Dispatch, EventSource, Listener, ListenResult};
use crate::peer;
use crate::time::TimeExt;

//TODO(stevenroose) probably should also make this generic over a Peer trait like PingMgr

/// A time offset, in seconds.
//NB Can't use Duration because this is signed.
type TimeOffset = i64;

/// An event originating in the peer manager.
#[derive(Debug, Clone)]
pub enum Event {
	/// Connecting to a peer found from the specified source.
	Connecting {
		id: PeerId,
		source: Source,
		services: ServiceFlags,
	},
	/// A new peer has connected and is ready to accept messages.
	/// This event is triggered *before* the peer handshake
	/// has successfully completed.
	Connected {
		id: PeerId,
		link: Link,
		/// The peer handle.
		handle: Arc<peer::Peer>,
	},
	/// The `version` message was received from a peer.
	VersionReceived {
		/// The peer's id.
		peer: PeerId,
		/// The version message.
		msg: VersionMessage,
		/// The peer handle.
		handle: Arc<peer::Peer>,
	},
	/// A peer has successfully negotiated (handshaked).
	Negotiated {
		/// The peer's id.
		peer: PeerId,
		/// Connection link.
		link: Link,
		/// Services offered by negotiated peer.
		services: ServiceFlags,
		/// Peer user agent.
		user_agent: String,
		/// Peer height.
		height: Height,
		/// Protocol version.
		version: u32,
		/// The peer handle.
		handle: Arc<peer::Peer>,
	},
	/// Connection attempt failed.
	ConnectionFailed(PeerId, Arc<std::io::Error>),
	/// A peer has been disconnected.
	Disconnected(PeerId, DisconnectReason),
}

/// Peer manager configuration.
#[derive(Debug, Clone)]
pub struct Config {
	/// Protocol version.
	pub protocol_version: u32,

	/// Peer whitelist.
	///
	/// Default value: empty.
	pub whitelist: Whitelist,

	/// Services offered by this implementation.
	pub services: ServiceFlags,

	/// Whether we want to receive relay `inv` messages.
	///
	/// Default value: true.
	pub request_relay: bool,

	/// Peer addresses to persist connections with.
	///
	/// Default value: empty.
	pub persistent: Vec<net::SocketAddr>,

	/// Services required by peers.
	///
	/// Default value: TODO(stevenroose)
	pub required_services: ServiceFlags,

	/// Peer services preferred. We try to maintain as many
	/// connections to peers with these services.
	///
	/// Default value: TODO(stevenroose)
	pub preferred_services: ServiceFlags,

	/// Target number of outbound peer connections.
	///
	/// Default value: 8.
	pub target_outbound_peers: usize,

	/// Maximum number of inbound peer connections.
	///
	/// Default value: 16.
	pub max_inbound_peers: usize,

	/// Maximum time to wait between reconnection attempts.
	///
	/// Default value: 60 seconds.
	pub retry_max_wait: Duration,

	/// Minimum time to wait between reconnection attempts.
	///
	/// Default value: 1 seconds.
	pub retry_min_wait: Duration,

	/// Our user agent.
	///
	/// Default value: TODO(stevenroose)
	pub user_agent: &'static str,

	/// Supported communication domains.
	///
	/// Default value: all.
	pub domains: Vec<Domain>,

	/// Listen on these interfaces for incoming connections.
	///
	/// Default value: empty.
	pub listen: Vec<net::SocketAddr>,

	/// Config to be used for new peers.
	///
	/// Default value: default peer config.
	pub peer_config: peer::Config,

	/// Time to wait for a new connection.
	///
	/// Default value: 6 seconds.
	pub connection_timeout: Duration,

	/// Time after which to abort trying to connect to a new peer.
	///
	/// Default value: 500 milliseconds
	//TODO(stevenroose) we should really not have a blocking dial call and should use mio
	pub dial_timeout: Duration,

	/// Time to wait for response during peer handshake before disconnecting the peer.
	///
	/// Default value: 12 seconds.
	pub handshake_timeout: Duration,

	/// Time between idle operations.
	///
	/// Default value: 60 seconds.
	pub idle_interval: Duration,

	/// Maximum height difference for a stale peer, to maintain the connection (2 weeks).
	///
	/// Default value: 2016.
	pub max_stale_height_difference: Height,
}

impl Default for Config {
	fn default() -> Config {
		Config {
			protocol_version: 0, //TODO(stevenroose) use bitcoin::network::constants::PROTOCOL_VERSION?
			whitelist: Whitelist::default(),
			services: ServiceFlags::NONE, //TODO(stevenroose) fill in sane
			request_relay: true,
			persistent: Vec::new(),
			required_services: ServiceFlags::NONE, //TODO(stevenroose) fill in sane
			preferred_services: ServiceFlags::NONE, //TODO(stevenroose) fill in sane
			target_outbound_peers: 8,
			max_inbound_peers: 16,
			retry_max_wait: Duration::from_secs(60),
			retry_min_wait: Duration::from_secs(1),
			user_agent: "", //TODO(stevenroose) fill in
			domains: Domain::all(),
			listen: Vec::new(),
			peer_config: peer::Config::default(),
			connection_timeout: Duration::from_secs(6),
			dial_timeout: Duration::from_millis(500),
			handshake_timeout: Duration::from_secs(12),
			idle_interval: Duration::from_secs(60),
			max_stale_height_difference: 2016,
		}
	}
}

/// A peer connection. Peers that haven't yet sent their `version` message are stored as
/// connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
	/// The peer's address.
	pub addr: net::SocketAddr,
	/// Local peer address.
	pub local_addr: net::SocketAddr,
	/// Whether this is an inbound or outbound peer connection.
	pub link: Link,
	/// Connected since this time.
	pub since: SystemTime,
}

/// Peer state.
#[derive(Debug, Clone)]
pub enum PeerState {
	/// A connection is being attempted.
	Connecting {
		/// Time the connection was attempted.
		time: SystemTime,
	},
	/// A connection is established.
	Connected {
		/// Connection.
		conn: Connection,
		/// Peer information, if a `version` message was received.
		info: Option<PeerInfo>,
	},
	/// The connection is being closed.
	Disconnecting,
}

/// Peer negotiation (handshake) state.
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
enum HandshakeState {
	/// Received "version" and waiting for "verack" message from remote.
	ReceivedVersion { since: SystemTime },
	/// Received "verack". Handshake is complete.
	ReceivedVerack { since: SystemTime },
}

/// A peer with protocol information.
#[derive(Debug, Clone)]
pub struct PeerInfo {
	/// The peer's best height.
	pub height: Height,
	/// The peer's services.
	pub services: ServiceFlags,
	/// Peer user agent string.
	pub user_agent: String,
	/// An offset in seconds, between this peer's clock and ours.
	/// A positive offset means the peer's clock is ahead of ours.
	pub time_offset: TimeOffset,
	/// Whether this peer relays transactions.
	pub relay: bool,
	/// Whether this peer supports BIP-339.
	pub wtxidrelay: bool,
	/// The max protocol version supported by both the peer and nakamoto.
	pub version: u32,
	/// Whether this is a persistent peer.
	pub persistent: bool,

	/// Peer nonce. Used to detect self-connections.
	nonce: u64,
	/// Peer handshake state.
	state: HandshakeState,
}

impl PeerInfo {
	/// Check whether the peer has finished negotiating and received our `version`.
	pub fn is_negotiated(&self) -> bool {
		matches!(self.state, HandshakeState::ReceivedVerack { .. })
	}
}

/// Manages peer connections and handshake.
pub struct PeerManager {
	cfg: Config,
	ctrl_tx: chan::Sender<Ctrl>,
	process: erin::ProcessHandle,
}

impl PeerManager {
	pub fn start_in(
		rt: &Arc<erin::Runtime>,
		config: Config,
		addresses: impl AddressSource + Send + 'static,
		rng: fastrand::Rng,
	) -> Result<PeerManager, Error> {
		let (ctrl_tx, ctrl_rx) = chan::unbounded();
		
		let mut tcp_listeners = Vec::with_capacity(config.listen.len());
		for itf in &config.listen {
			//TODO(stevenroose) improve error messaging here; anyhow? module-level Error type?
			tcp_listeners.push(net::TcpListener::bind(itf)?);
		}

		let processor = Processor::new(
			config.clone(),
			ctrl_rx,
			ctrl_tx.clone(),
			addresses,
			tcp_listeners,
			rng,
			rt.clone(),
		);
		let process = rt.add_process(Box::new(processor))?;

		Ok(PeerManager {
			cfg: config,
			ctrl_tx: ctrl_tx,
			process: process,
		})
	}

	pub fn config(&self) -> &Config {
		&self.cfg
	}

	//TODO(stevenroose) expose peers here somehow

	/// Bind to the given interface to listen for new inbound peers.
	pub fn bind(&self, addr: net::SocketAddr) -> Result<(), io::Error> {
		let list = net::TcpListener::bind(addr)?;
		list.set_nonblocking(true)?;
		self.add_tcp_listener(list);
		Ok(())
	}

	/// Add a new event listener.
	pub fn listen(&self, listener: impl Listener<Event>) {
		self.add_listener(Box::new(listener));
	}

	/// Add a new TCP listener to listen to.
	///
	/// The caller must set the listener to non-blocking mode before calling this method.
	///
	/// It is advised to use utility method [bind] instead.
	pub fn add_tcp_listener(&self, listener: net::TcpListener) {
		self.send_ctrl(Ctrl::AddTcpListener(listener));
	}
}

impl EventSource<Event> for PeerManager {
	fn add_listener(&self, listener: Box<dyn Listener<Event>>) {
		self.send_ctrl(Ctrl::AddListener(listener))
	}
}

trait SendCtrl {
	fn send_ctrl(&self, ctrl: Ctrl);
}

impl SendCtrl for PeerManager {
	fn send_ctrl(&self, ctrl: Ctrl) {
		//TODO(stevenroose) should we handle these errors?
		let _ = self.ctrl_tx.send(ctrl);
		//TODO(stevenroose) this is UnixStream::as_raw_fd erroring
		let _ = self.process.wake();
	}
}

impl SendCtrl for (chan::Sender<Ctrl>, erin::Waker) {
	fn send_ctrl(&self, ctrl: Ctrl) {
		//TODO(stevenroose) should we handle these errors?
		let _ = self.0.send(ctrl);
		//TODO(stevenroose) this is UnixStream::as_raw_fd erroring
		let _ = self.1.wake();
	}
}

pub trait PeerManagerControl {
	fn update_tip(&self, height: Height);

	fn whitelist(&self, ip: net::IpAddr);

	fn connect(&self, addr: net::SocketAddr);

	/// Disconnect the peer.
	///
	/// This is equivalent on calling [Peer::disconnect] on the peer handle.
	fn disconnect(&self, id: PeerId);

	/// Disconnect the peer, providing a reason.
	///
	/// This is equivalent on calling [Peer::disconnect_with_reason] on the peer handle.
	fn disconnect_with_reason(&self, id: PeerId, reason: DisconnectReason);

	fn peer_connected(&self, id: PeerId);

	fn peer_disconnected(&self, peer: PeerId, reason: DisconnectReason);

	fn received_version(&self, id: PeerId, ver: VersionMessage);

	fn received_wtxidrelay(&self, id: PeerId);

	fn received_verack(&self, id: PeerId);
}

impl<T: SendCtrl> PeerManagerControl for T {
	fn update_tip(&self, height: Height) {
		self.send_ctrl(Ctrl::UpdateTip(height));
	}

	fn whitelist(&self, ip: net::IpAddr) {
		self.send_ctrl(Ctrl::Whitelist(ip));
	}

	fn connect(&self, addr: net::SocketAddr) {
		self.send_ctrl(Ctrl::Connect(addr));
	}

	fn disconnect(&self, id: PeerId) {
		self.disconnect_with_reason(id, DisconnectReason::Command);
	}

	fn disconnect_with_reason(&self, id: PeerId, reason: DisconnectReason) {
		self.send_ctrl(Ctrl::Disconnect(id, reason));
	}

	fn peer_connected(&self, id: PeerId) {
		self.send_ctrl(Ctrl::PeerConnected(id));
	}

	fn peer_disconnected(&self, peer: PeerId, reason: DisconnectReason) {
		self.send_ctrl(Ctrl::PeerDisconnected(peer, reason));
	}

	fn received_version(&self, id: PeerId, ver: VersionMessage) {
		self.send_ctrl(Ctrl::ReceivedVersion(id, ver));
	}

	fn received_wtxidrelay(&self, id: PeerId) {
		self.send_ctrl(Ctrl::ReceivedWtxidRelay(id));
	}

	fn received_verack(&self, id: PeerId) {
		self.send_ctrl(Ctrl::ReceivedVerack(id));
	}
}

enum Ctrl {
	AddListener(Box<dyn Listener<Event>>),
	UpdateTip(Height),
	Whitelist(net::IpAddr),
	Connect(net::SocketAddr),
	Disconnect(PeerId, DisconnectReason),
	AddTcpListener(net::TcpListener),

	PeerConnected(PeerId),
	PeerDisconnected(PeerId, DisconnectReason),
	ReceivedVersion(PeerId, VersionMessage),
	ReceivedWtxidRelay(PeerId),
	ReceivedVerack(PeerId),
}

#[derive(Debug)]
struct TrackedPeer {
	handle: Arc<peer::Peer>, //TODO(stevenroose) do trait like pingmgr?
	state: PeerState,
}

impl TrackedPeer {
	fn conn(&self) -> Option<&Connection> {
		match self.state {
			PeerState::Connected { ref conn, .. } => Some(conn),
			_ => None,
		}
	}

	fn info(&self) -> Option<(&PeerInfo, &Connection)> {
		match self.state {
			PeerState::Connected { ref conn, info: Some(ref info) } => Some((info, conn)),
			_ => None,
		}
	}

	fn services(&self) -> Option<ServiceFlags> {
		self.info().map(|(info, _)| info.services)
	}

	fn is_inbound(&self) -> bool {
		match self.state {
			PeerState::Connected { ref conn, .. } if conn.link.is_inbound() => true,
			_ => false,
		}
	}

	fn is_outbound(&self) -> bool {
		match self.state {
			PeerState::Connected { ref conn, .. } if conn.link.is_outbound() => true,
			_ => false,
		}
	}

	fn is_connecting(&self) -> bool {
		match self.state {
			PeerState::Connecting { .. } => true,
			_ => false,
		}
	}

	fn is_connected(&self) -> bool {
		match self.state {
			PeerState::Connected { .. } => true,
			_ => false,
		}
	}

	fn is_disconnecting(&self) -> bool {
		match self.state {
			PeerState::Disconnecting => true,
			_ => false,
		}
	}

	fn is_negotiated(&self) -> bool {
		match self.state {
			PeerState::Connected { info: Some(ref info), .. } => info.is_negotiated(),
			_ => false,
		}
	}

	fn disconnect(&mut self, reason: DisconnectReason) {
		if !self.is_disconnecting() {
			self.handle.disconnect_with_reason(reason);
			self.state = PeerState::Disconnecting;
		}
	}
}

enum Timer {
	Idle,
	Peer(PeerId),
	RetryPersistent(net::SocketAddr),
}

struct Processor<Addrs> {
	/// Peer manager configuration.
	cfg: Config,
	ctrl_rx: chan::Receiver<Ctrl>,
	ctrl_tx: chan::Sender<Ctrl>,
	dispatch: Dispatch<Event>,
	addrs: Addrs,

	tcp_listeners: Vec<(net::TcpListener, erin::IoToken)>,

	tip: Height,
	/// Last time we were idle.
	last_idle: Option<SystemTime>,
	/// Connection states.
	peers: HashMap<net::SocketAddr, TrackedPeer>,
	/// Peers that have been disconnected and a retry attempt is scheduled.
	persistent_retry: HashMap<net::SocketAddr, usize>,
	rng: fastrand::Rng,

	// erin stuff
	erin_rt: Arc<erin::Runtime>,
	timers: HashMap<erin::TimerToken, Timer>,
}

impl<A: AddressSource> Processor<A> {
	fn new(
		config: Config,
		ctrl_rx: chan::Receiver<Ctrl>,
		ctrl_tx: chan::Sender<Ctrl>,
		addrs: A,
		listeners: Vec<net::TcpListener>,
		rng: fastrand::Rng,
		erin_rt: Arc<erin::Runtime>,
	) -> Processor<A> {
		Processor {
			cfg: config,
			ctrl_rx: ctrl_rx,
			ctrl_tx: ctrl_tx,
			dispatch: Dispatch::new(),
			addrs: addrs,
			tcp_listeners: listeners.into_iter().map(|l| (l, erin::IoToken::NULL)).collect(),
			tip: 0,
			last_idle: None,
			peers: HashMap::new(),
			persistent_retry: HashMap::new(),
			rng: rng,
			erin_rt: erin_rt,
			timers: HashMap::new(),
		}
	}

	//TODO(stevenroose) do this somewhere
	// fn initialize(&mut self, addrs: &mut A) {
	// 	let peers = self.cfg.persistent.clone();

	// 	for addr in peers {
	// 		if !self.connect(&addr) {
	// 			// TODO: Return error here, or send event.
	// 			panic!(
	// 				"{}: unable to connect to persistent peer: {}",
	// 				source!(),
	// 				addr
	// 			);
	// 		}
	// 	}
	// 	self.maintain_connections(addrs);
	// }

	fn update_tip(&mut self, height: Height) {
		self.tip = height;
	}

	/// A persistent peer has been .
	fn persistent_disconnected(
		&mut self,
		addr: net::SocketAddr,
		set_retry_timer: impl FnOnce(Duration),
	) {
		let attempts = self.persistent_retry.entry(addr).or_default();

		let delay = Duration::from_secs(2u64.saturating_pow(*attempts as u32))
			.clamp(self.cfg.retry_min_wait, self.cfg.retry_max_wait);

		*attempts += 1;

		set_retry_timer(delay);
	}

	/// Retry connecting persistent peer connection.
	fn retry_persistent(
		&mut self,
		addr: net::SocketAddr,
		ctrl: (chan::Sender<Ctrl>, erin::Waker),
		set_retry_timer: impl FnOnce(Duration),
	) {
		// If it's no longer in the retry map it means the last
		// attempt was succesful.
		if self.persistent_retry.contains_key(&addr) {
			if !self.connect(addr, ctrl) {
				error!(target: "p2p", "Couldn't establish connection with {addr}");
				self.persistent_disconnected(addr, set_retry_timer);
			}
		}
	}

	fn allow_connection(&self, addr: net::SocketAddr) -> bool {
		let whitelisted = self.cfg.whitelist.contains_ip(&addr.ip())
			|| addrmgr::is_local(addr.ip());

		// Don't allow connections to unsupported domains.
		if !whitelisted && !self.cfg.domains.contains(&Domain::for_address(addr)) {
			return false;
		}

		if let Some(peer) = self.peers.get(&addr) {
			// Allow duplicate only if disconnecting. (reconnect)
			if peer.is_disconnecting() {
				true
			}  else {
				false
			}
		} else {
			true
		}
	}

	/// Register the peer's listener so that the [PeerManager] receives the 
	/// relevant notifications.
	fn register_peer_listener(
		&self,
		id: PeerId, peer: &impl EventSource<peer::Event>,
		ctrl: impl SendCtrl + Send + 'static,
	) {
		peer.add_listener(Box::new(move |e: &_| {
			match e {
				peer::Event::Connected => ctrl.peer_connected(id),
				peer::Event::Disconnected(reason) => ctrl.peer_disconnected(id, reason.clone()),
				peer::Event::Message(m) => match m {
					NetworkMessage::Version(m) => ctrl.received_version(id, m.clone()),
					NetworkMessage::Verack => ctrl.received_verack(id),
					NetworkMessage::WtxidRelay => ctrl.received_wtxidrelay(id),
					_ => {},
				},
			}
			ListenResult::Ok
		}));
	}

	fn incoming(
		&mut self,
		addr: net::SocketAddr,
		stream: net::TcpStream,
		ctrl: (chan::Sender<Ctrl>, erin::Waker),
	) {
		if !self.allow_connection(addr) {
			debug!("Not allowing incoming connection from {}", addr);
			if let Err(e) = stream.shutdown(net::Shutdown::Both) {
				warn!("I/O error shutting down TCP stream from {}: {}", addr, e);
			}
			return;
		}

		let nb_inbound = self.peers.values().filter(|p| p.is_connected() && p.is_inbound()).count();
		if nb_inbound >= self.cfg.max_inbound_peers {
			debug!("Not allowing new incoming connection because limit reached: {}",
				self.cfg.max_inbound_peers,
			);
			if let Err(e) = stream.shutdown(net::Shutdown::Both) {
				warn!("I/O error shutting down TCP stream from {}: {}", addr, e);
			}
			return;
		}

		let local_addr = match stream.local_addr() {
			Ok(a) => a,
			Err(e) => {
				error!("Error getting local addr from TCP stream from {}: {}", addr, e);
				return;
			}
		};

		match peer::Peer::start_in(
			&self.erin_rt,
			self.cfg.peer_config.clone(),
			stream,
		) {
			Ok(peer) => {
				let id = peer.id();
				self.register_peer_listener(id, &peer, ctrl);
				let peer = Arc::new(peer);
				self.peers.insert(id, TrackedPeer {
					handle: peer,
					state: PeerState::Connected {
						conn: Connection {
							addr: addr,
							local_addr: local_addr,
							link: Link::Inbound,
							since: SystemTime::now(),
						},
						info: None,
					},
				});
			}
			Err(e) => error!("Error starting new peer in runtime: {}", e),
		}
	}

	fn connect(
		&mut self,
		addr: net::SocketAddr,
		ctrl: (chan::Sender<Ctrl>, erin::Waker),
	) -> bool {
		if !self.allow_connection(addr) {
			return false;
		}
		
		let ret = peer::Peer::dial_in(
			&self.erin_rt,
			self.cfg.peer_config.clone(),
			addr,
			Some(self.cfg.dial_timeout),
		);
		match ret {
			Ok(peer) => {
				self.register_peer_listener(peer.id(), &peer, ctrl);
				let peer = Arc::new(peer);
				self.peers.insert(peer.id(), TrackedPeer {
					handle: peer,
					state: PeerState::Connecting { time: SystemTime::now() },
				});

				true
			}
			Err(e) => {
				error!("Error dialing peer at {}: {}", addr, e);
				false
			}
		}
	}

	fn disconnect(&mut self, id: PeerId, reason: DisconnectReason) {
		if let Some(peer) = self.peers.get_mut(&id) {
			peer.disconnect(reason);
		}
	}

	/// Called when a peer connected.
	fn peer_connected(&mut self, id: PeerId, set_handshake_timer: impl FnOnce(Duration)) {
		match self.peers.get_mut(&id) {
			Some(peer) if peer.is_inbound() => {
				debug_assert!(peer.is_connected()); // inbound peers start off connected
				self.dispatch.dispatch(&Event::Connected {
					id, link: Link::Inbound, handle: peer.handle.clone(),
				});

				// Set a timeout for receiving the `version` message.
				set_handshake_timer(self.cfg.handshake_timeout);
			}
			Some(peer) if peer.is_outbound() && !peer.is_connected() => {
				debug_assert!(peer.is_connecting());

				let local_addr = peer.handle.local_addr();

				// TODO: There is a chance that we simultaneously connect to a peer that is connecting
				// to us. This would create two connections to the same peer, one outbound and one
				// inbound. To prevent this, we could look at IPs when receiving inbound connections,
				// to check whether we are already connected to the peer.

				peer.state = PeerState::Connected {
					conn: Connection {
						addr: id,
						local_addr: local_addr,
						link: Link::Outbound,
						since: SystemTime::now(),
					},
					info: None,
				};
				self.persistent_retry.remove(&id);
				self.dispatch.dispatch(&Event::Connected {
					id, link: Link::Outbound, handle: peer.handle.clone(),
				});

				// Then start the handshake protocol.
				Wire::version(&*peer.handle, make_version_msg(
					&self.cfg, id, local_addr, self.rng.u64(..), self.tip, SystemTime::now(),
				));
				// Set a timeout for receiving the `version` message.
				set_handshake_timer(self.cfg.handshake_timeout);
			}
			Some(peer) => warn!("Unexpected connected notification for peer {}: {:?}", id, peer),
			None => error!("Peer connected notification for unknown peer: {}", id),
		}
	}

	/// Called when a peer disconnected.
	fn peer_disconnected(
		&mut self,
		id: PeerId,
		reason: DisconnectReason,
		set_retry_timer: impl FnOnce(Duration),
	) {
		if let Some(peer) = self.peers.remove(&id) {
			if peer.is_connecting() {
				// If we haven't yet established a connection, the disconnect reason
				// should always be a `ConnectionError`.
				if let DisconnectReason::ConnectionError(err) = reason {
					self.dispatch.dispatch(&Event::ConnectionFailed(id, err));
				} else {
					panic!("Peer disconnected before it was connected: {} (reason {})", id, reason);
				}
			} else {
				self.dispatch.dispatch(&Event::Disconnected(id, reason));
			}
		}

		if self.cfg.persistent.contains(&id) {
			self.persistent_disconnected(id, set_retry_timer);
		}

		//TODO(stevenroose) somehow make sure we still have enough connections after disconnecting
		//maybe call maintain_connections?
	}

	/// Called when a `version` message was received.
	fn received_version(
		&mut self,
		id: PeerId,
		msg: VersionMessage,
	) {
		let now = SystemTime::now();

		let VersionMessage {
			// Peer's best height.
			start_height,
			// Peer's local time.
			timestamp,
			// Highest protocol version understood by the peer.
			version,
			// Services offered by this peer.
			services,
			// User agent.
			user_agent,
			// Peer nonce.
			nonce,
			// Our address, as seen by the remote peer.
			receiver,
			// Relay node.
			relay,
			..
		} = msg.clone();

		let target = self.cfg.target_outbound_peers;
		let preferred = self.cfg.preferred_services;
		let required = self.cfg.required_services;
		let whitelisted = self.cfg.whitelist.contains(&id.ip(), &user_agent)
			|| addrmgr::is_local(id.ip());

		if self.peers.contains_key(&id) {
			trace!("Received version message from {}: {:?}", id, msg);
			assert!(self.peers[&id].conn().is_some(), "version message from peer not connected");

			self.dispatch.dispatch(&Event::VersionReceived {
				peer: id,
				msg: msg.clone(),
				handle: self.peers[&id].handle.clone(),
			});

			let is_outbound = self.peers[&id].conn().unwrap().link.is_outbound();

			// Don't support peers with too old of a protocol version.
			if version < super::MIN_PROTOCOL_VERSION {
				self.peers.get_mut(&id).unwrap()
					.disconnect(DisconnectReason::PeerProtocolVersion(version));
				return;
			}

			// Don't connect to peers that don't support the services we require.
			if is_outbound && !whitelisted && !services.has(required) {
				self.peers.get_mut(&id).unwrap()
					.disconnect(DisconnectReason::PeerServices(services));
				return;
			}
			// If the peer is too far behind, there's no use connecting to it, we'll
			// have to wait for it to catch up.
			let behind = self.tip.saturating_sub(start_height as u64);
			if is_outbound && !whitelisted
				&& behind > self.cfg.max_stale_height_difference
			{
				self.peers.get_mut(&id).unwrap()
					.disconnect(DisconnectReason::PeerHeight(start_height as Height));
				return;
			}
			// Check for self-connections. We only need to check one link direction,
			// since in the case of a self-connection, we will see both link directions.
			for peer in self.peers.values() {
				if let Some((info, conn)) = peer.info() {
					if conn.link.is_outbound() && info.nonce == nonce {
						self.peers.get_mut(&id).unwrap()
							.disconnect(DisconnectReason::SelfConnection);
						return;
					}
				}
			}

			// If this peer doesn't have the preferred services, and we already have enough peers,
			// disconnect this peer.
			let nb_negotiated = self.peers.values()
				.filter(|p| p.is_outbound() && p.is_negotiated())
				.count();
			if is_outbound && !services.has(preferred) && nb_negotiated >= target {
				self.peers.get_mut(&id).unwrap()
					.disconnect(DisconnectReason::ConnectionLimit);
				return;
			}

			// Record the address this peer has of us.
			if let Ok(addr) = receiver.socket_addr() {
				self.addrs.record_local_address(addr);
			}

			let peer = self.peers.get_mut(&id).unwrap();
			let conn = peer.conn().unwrap();
			match conn.link {
				Link::Inbound => {
					Wire::version(&*peer.handle, make_version_msg(
						&self.cfg, conn.addr, conn.local_addr, nonce, self.tip, now,
					));
					peer.handle.wtxid_relay();
					peer.handle.verack();
					peer.handle.send_headers();
				}
				Link::Outbound => {
					peer.handle.wtxid_relay();
					peer.handle.verack();
					peer.handle.send_headers();
				}
			}
			let persistent = self.cfg.persistent.contains(&conn.addr);

			// replacing the old entry
			peer.state = PeerState::Connected {
				conn: conn.clone(),
				info: Some(PeerInfo {
					nonce: nonce,
					height: start_height as Height,
					time_offset: timestamp - now.block_time() as i64,
					services: services,
					persistent: persistent,
					user_agent: user_agent,
					state: HandshakeState::ReceivedVersion { since: now },
					relay: relay,
					wtxidrelay: false,
					version: u32::min(self.cfg.protocol_version, version),
				}),
			};
		}
	}

	/// Called when a `wtxidrelay` message was received.
	fn received_wtxidrelay(&mut self, id: PeerId) {
		if let Some(peer) = self.peers.get_mut(&id) {
			if let PeerState::Connected { info: Some(ref mut info), .. } = peer.state {
				match info.state {
					HandshakeState::ReceivedVersion { .. } => info.wtxidrelay = true,
					_ => self.disconnect(id, DisconnectReason::PeerMisbehaving(
						"`wtxidrelay` must be received before `verack`",
					)),
				}
			}
		}
	}

	fn received_verack(&mut self, id: PeerId) {
		if let Some(peer) = self.peers.get_mut(&id) {
			if let PeerState::Connected { ref conn, info: Some(ref mut info) } = peer.state {
				if let HandshakeState::ReceivedVersion { .. } = info.state {
					self.dispatch.dispatch(&Event::Negotiated {
						peer: id,
						link: conn.link,
						services: info.services,
						user_agent: info.user_agent.clone(),
						height: info.height,
						version: info.version,
						handle: peer.handle.clone(),
					});

					info.state = HandshakeState::ReceivedVerack { since: SystemTime::now() };
				} else {
					peer.disconnect(DisconnectReason::PeerMisbehaving(
						"unexpected `verack` message received",
					));
				}
			}
		}
	}

	fn whitelist(&mut self, addr: net::IpAddr) {
		self.cfg.whitelist.addresses.insert(addr);
	}

	/// Given the current peer state and targets, calculate how many new connections we should
	/// make.
	fn delta(&self) -> usize {
		// Peers with our preferred services.
		let primary = self.peers.values().filter(|p| {
			let services = p.services().unwrap_or(ServiceFlags::NONE);
			p.is_negotiated() && p.is_outbound() && services.has(self.cfg.preferred_services)
		}).count();
		// Peers only with required services, which we'd eventually want to drop in favor of peers
		// that have all services.
		let outbound = self.peers.values().filter(|p| {
			p.is_negotiated() && p.is_outbound()
		}).count();
		let secondary = outbound - primary;
		// Connected peers that have not yet completed handshake.
		let all_connected = self.peers.values().filter(|p| p.is_connected()).count();
		let connected = all_connected - primary - secondary;
		// Connecting peers.
		let connecting = self.peers.values().filter(|p| p.is_connecting()).count();

		// We connect up to the target number of peers plus an extra margin equal to the number of
		// target divided by two. This ensures we have *some* connections to
		// primary peers, even if that means exceeding our target. When a secondary peer is
		// dropped, if we have our target number of primary peers connected, there is no need
		// to replace the connection.
		//
		// Above the target count, all peer connections without the preferred services are
		// automatically dropped. This ensures we never have more than the target of secondary
		// peers.
		let target = self.cfg.target_outbound_peers;
		let unknown = connecting + connected;
		let total = primary + secondary + unknown;
		let max = target + target / 2;

		// If we are somehow connected to more peers than the target or maximum,
		// don't attempt to connect to more. This can happen if the client has been
		// requesting connections to specific peers.
		if total > max || primary + unknown > target {
			return 0;
		}

		usize::min(max - total, target - (primary + unknown))
	}

	/// Attempt to maintain a certain number of outbound peers.
	fn maintain_connections(
		&mut self,
		ctrl: (chan::Sender<Ctrl>, erin::Waker),
	) {
		let target = self.cfg.target_outbound_peers;
		if target == 0 {
			return;
		}

		let delta = self.delta();
		let nb_negotiated = self.peers.values()
			.filter(|p| p.is_outbound() && p.is_negotiated())
			.count();

		// Keep track of new addresses we're connecting to, and loop until
		// we've connected to enough addresses.
		let mut connecting = HashSet::new();

		while connecting.len() < delta {
			let sample = self.addrs.sample(self.cfg.preferred_services).or_else(|| {
				// Only try to connect to non-preferred peers if we are below our target.
				if nb_negotiated < target {
					self.addrs.sample(self.cfg.required_services)
						// If we can't find peers with any kind of useful services, then
						// perhaps we should connect to peers that may know of such peers. This
						// is especially important when doing an initial DNS sync, since DNS
						// addresses don't come with service information. This will draw from
						// that pool.
						.or_else(|| self.addrs.sample(ServiceFlags::NONE))
				} else {
					None
				}
			});
			if let Some((addr, source)) = sample {
				if let Ok(sockaddr) = addr.socket_addr() {
					if self.connect(sockaddr, ctrl.clone()) {
						connecting.insert(sockaddr);
						self.dispatch.dispatch(&Event::Connecting {
							id: sockaddr, source, services: addr.services,
						});
					}
				}
			} else {
				// We're completely out of addresses, give up.
				// TODO: Fetch from DNS seeds. Make sure we're able to add to address book
				// even though address manager doesn't like peers with no services if `insert`
				// is used.
				break;
			}
		}
	}
}

/// Create a `version` message for a peer.
pub fn make_version_msg(
	config: &Config,
	addr: net::SocketAddr,
	local_addr: net::SocketAddr,
	nonce: u64,
	start_height: Height,
	local_time: SystemTime,
) -> VersionMessage {
	VersionMessage {
		// Our max supported protocol version.
		version: config.protocol_version,
		// Local services.
		services: config.services,
		// Local time.
		timestamp: local_time.block_time() as i64,
		// Receiver address and services, as perceived by us.
		receiver: Address::new(&addr, ServiceFlags::NONE),
		// Local address (unreliable) and local services (same as `services` field)
		sender: Address::new(&local_addr, config.services),
		// A nonce to detect connections to self.
		nonce: nonce,
		// Our user agent string.
		user_agent: config.user_agent.to_owned(),
		// Our best height.
		start_height: start_height as i32,
		// Whether we want to receive transaction `inv` messages.
		relay: config.request_relay,
	}
}

impl<A: AddressSource + Send + 'static> erin::Process for Processor<A> {
	fn setup(&mut self, rt: &erin::RuntimeHandle) -> Result<(), erin::Exit> {
		let idle_token = rt.set_timer(self.cfg.idle_interval);
		self.timers.insert(idle_token, Timer::Idle);

		for (listener, token) in self.tcp_listeners.iter_mut() {
			*token = rt.register_io(listener, erin::interest::READ);
		}

		Ok(())
	}

	fn wakeup(&mut self, rt: &erin::RuntimeHandle, ev: erin::Events) -> Result<(), erin::Exit> {
		if ev.waker() {
			loop {
				match self.ctrl_rx.try_recv() {
					Ok(Ctrl::AddListener(l)) => self.dispatch.add_listener(l),
					Ok(Ctrl::UpdateTip(h)) => self.update_tip(h),
					Ok(Ctrl::Whitelist(ip)) => self.whitelist(ip),
					Ok(Ctrl::Connect(addr)) => {
						let ctrl = (self.ctrl_tx.clone(), rt.new_waker());
						let _ = self.connect(addr, ctrl);
					},
					Ok(Ctrl::Disconnect(id, reason)) => self.disconnect(id, reason),
					Ok(Ctrl::AddTcpListener(l)) => {
						let token = rt.register_io(&l, erin::interest::READ);
						self.tcp_listeners.push((l, token));
					},
					Ok(Ctrl::PeerConnected(id)) => {
						let mut timer = None;
						self.peer_connected(id, |d| timer = Some(rt.set_timer(d)));
						if let Some(t) = timer {
							self.timers.insert(t, Timer::Peer(id));
						}
					},
					Ok(Ctrl::PeerDisconnected(id, reason)) => {
						let mut timer = None;
						self.peer_disconnected(id, reason, |d| timer = Some(rt.set_timer(d)));
						if let Some(t) = timer {
							self.timers.insert(t, Timer::RetryPersistent(id as net::SocketAddr));
						}
					},
					Ok(Ctrl::ReceivedVersion(id, msg)) => self.received_version(id, msg),
					Ok(Ctrl::ReceivedWtxidRelay(id)) => self.received_wtxidrelay(id),
					Ok(Ctrl::ReceivedVerack(id)) => self.received_verack(id),
					Err(chan::TryRecvError::Empty) => break,
					Err(chan::TryRecvError::Disconnected) => {
						info!("PeerManager processor shutting down: ctrl channel closed");
						return Err(erin::Exit);
					}
				}
			}
		}

		for ev in ev.timers() {
			match self.timers.remove(&ev) {
				Some(Timer::Idle) => {
					let ctrl = (self.ctrl_tx.clone(), rt.new_waker());
					self.maintain_connections(ctrl);
					self.last_idle = Some(SystemTime::now());
					let new_token = rt.set_timer(self.cfg.idle_interval);
					self.timers.insert(new_token, Timer::Idle);
				}
				Some(Timer::Peer(id)) => {
					if let Some(peer) = self.peers.get_mut(&id) {
						match peer.state {
							PeerState::Connecting { time } => {
								// Time out all peers that have been idle in a
								// "connecting" state for too long.
								if time.saturating_elapsed() >= self.cfg.connection_timeout {
									peer.disconnect(DisconnectReason::PeerTimeout("connection"));
								}
							}
							PeerState::Connected { info: Some(ref info), .. } => match info.state {
								HandshakeState::ReceivedVersion { since } => {
									if since.saturating_elapsed() >= self.cfg.handshake_timeout {
										peer.disconnect(DisconnectReason::PeerTimeout("handshake"));
									}
								}
								HandshakeState::ReceivedVerack { .. } => {},
							}
							PeerState::Connected { info: None, ref conn } => {
								if conn.since.saturating_elapsed() >= self.cfg.handshake_timeout {
									peer.disconnect(DisconnectReason::PeerTimeout("handshake"));
								}
							}
							PeerState::Disconnecting => {}, // already disconnecting
						}
					}
				}
				Some(Timer::RetryPersistent(addr)) => {
					let mut timer = None;
					let ctrl = (self.ctrl_tx.clone(), rt.new_waker());
					self.retry_persistent(addr, ctrl, |d| timer = Some(rt.set_timer(d)));
					if let Some(t) = timer {
						self.timers.insert(t, Timer::RetryPersistent(addr));
					}
				}
				None => panic!("Unknown erin timer token: {:?}", ev),
			}
		}

		for ev in ev.io() {
			let list_idx = self.tcp_listeners.iter()
				.position(|(_, t)| *t == ev.token)
				.expect("unregistered I/O token");

			if ev.src.is_error() || ev.src.is_hangup() {
				// Let the subsequent read fail.
				let list = &self.tcp_listeners[list_idx].0;
				trace!("TCP listener error triggered: {:?}: {:?}", list.local_addr(), ev);
			}
			
			if ev.src.is_invalid() {
				// File descriptor was closed and is invalid.
				// Nb. This shouldn't happen. It means the source wasn't
				// properly unregistered, or there is a duplicate source.
				let list = &self.tcp_listeners[list_idx].0;
				error!("TCP listener is invalid, shutting down: {:?}", list.local_addr());
				return Err(erin::Exit);
			}

			if ev.src.is_readable() {
				loop {
					let listener = &self.tcp_listeners[list_idx].0;
					match listener.accept() {
						Ok((stream, addr)) => {
							let list_addr = listener.local_addr()
								.map(|a| a.to_string())
								.unwrap_or_else(|_| "<ERROR>".to_owned());
							info!("New incoming peer on interface {}: {}", list_addr, addr);
							drop(listener);
							let ctrl = (self.ctrl_tx.clone(), rt.new_waker());
							self.incoming(addr, stream, ctrl.clone());
						}
						Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
						Err(e) => {
							error!("Error listening on TCP interface: {}", e);
							drop(listener);
							self.tcp_listeners.swap_remove(list_idx);
						},
					}
				}
			}
		}

		Ok(())
	}

	fn shutdown(&mut self) {
		// nothing to do, all will be dropped
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::VecDeque;

	use nakamoto_common::bitcoin::network::address::Address;
	use nakamoto_common::block::time::RefClock;
	use nakamoto_test::assert_matches;

	mod util {
		use super::*;

		pub fn config() -> Config {
			Config {
				protocol_version: crate::fsm::PROTOCOL_VERSION,
				target_outbound_peers: TARGET_OUTBOUND_PEERS,
				max_inbound_peers: MAX_INBOUND_PEERS,
				domains: Domain::all(),
				user_agent: crate::fsm::USER_AGENT,
				persistent: vec![],
				retry_max_wait: Duration::from_mins(60),
				retry_min_wait: Duration::from_secs(1),
				services: ServiceFlags::NONE,
				preferred_services: ServiceFlags::COMPACT_FILTERS | ServiceFlags::NETWORK,
				required_services: ServiceFlags::NETWORK,
				whitelist: Whitelist::default(),
			}
		}
	}

	#[test]
	fn test_persistent_client_reconnect() {
		let rng = fastrand::Rng::with_seed(1);
		let time = RefClock::from(SystemTime::now());
		let height = 144;

		let local = ([99, 99, 99, 99], 9999).into();
		let remote = ([124, 43, 110, 1], 8333).into();

		let mut addrs = VecDeque::new();
		let cfg = Config {
			persistent: vec![remote],
			..util::config()
		};
		let mut peermgr = PeerManager::new(cfg, rng, Hooks::default(), (), time.clone());

		peermgr.initialize(&mut addrs);
		assert_eq!(peermgr.connecting().next(), Some(&remote));

		peermgr.peer_connected(remote, local, Link::Outbound, height);
		assert_eq!(
			peermgr.connected().map(|c| &c.socket.addr).next(),
			Some(&remote)
		);

		// Confirm first attempt
		peermgr.peer_disconnected(
			&remote,
			&mut addrs,
			DisconnectReason::PeerTimeout("").into(),
		);
		assert!(peermgr.is_disconnected(&remote));
		assert_eq!(peermgr.connected().next(), None);

		time.elapse(Duration::from_secs(1));
		peermgr.received_wake(&mut addrs);
		assert_eq!(peermgr.connecting().next(), Some(&remote));

		// Confirm exponential backoff after failed first attempt
		peermgr.peer_disconnected(
			&remote,
			&mut addrs,
			DisconnectReason::PeerTimeout("").into(),
		);
		assert!(peermgr.is_disconnected(&remote));
		assert_eq!(peermgr.connecting().next(), None);

		time.elapse(Duration::from_secs(1));
		peermgr.received_wake(&mut addrs);
		assert_eq!(peermgr.connecting().next(), None);

		time.elapse(Duration::from_secs(1));
		peermgr.received_wake(&mut addrs);
		assert_eq!(peermgr.connecting().next(), Some(&remote));
	}

	#[test]
	fn test_wtxidrelay_outbound() {
		let rng = fastrand::Rng::with_seed(1);
		let time = SystemTime::now();

		let mut addrs = VecDeque::new();
		let mut peermgr = PeerManager::new(util::config(), rng.clone(), Hooks::default(), (), time);

		let height = 144;
		let local = ([99, 99, 99, 99], 9999).into();
		let remote = ([124, 43, 110, 1], 8333).into();
		let version = VersionMessage {
			services: ServiceFlags::NETWORK,
			..peermgr.version(local, remote, rng.u64(..), height, time)
		};

		peermgr.initialize(&mut addrs);
		peermgr.connect(&remote);
		peermgr.peer_connected(remote, local, Link::Outbound, height);
		peermgr.received_version(&remote, version, height, &mut addrs);

		assert_matches!(
			peermgr.peers.get(&remote),
			Some(PeerState::Connected{peer: Some(p), ..}) if !p.wtxidrelay
		);

		peermgr.received_wtxidrelay(&remote);
		peermgr.received_verack(&remote, time);

		assert_matches!(
			peermgr.peers.get(&remote),
			Some(PeerState::Connected{peer: Some(p), ..}) if p.wtxidrelay
		);
	}

	#[test]
	fn test_wtxidrelay_misbehavior() {
		let rng = fastrand::Rng::with_seed(1);
		let time = SystemTime::now();

		let mut addrs = VecDeque::new();
		let mut peermgr = PeerManager::new(util::config(), rng.clone(), Hooks::default(), (), time);

		let height = 144;
		let local = ([99, 99, 99, 99], 9999).into();
		let remote = ([124, 43, 110, 1], 8333).into();
		let version = VersionMessage {
			services: ServiceFlags::NETWORK,
			..peermgr.version(local, remote, rng.u64(..), height, time)
		};

		peermgr.initialize(&mut addrs);
		peermgr.connect(&remote);
		peermgr.peer_connected(remote, local, Link::Outbound, height);
		peermgr.received_version(&remote, version, height, &mut addrs);
		peermgr.received_verack(&remote, time);
		peermgr.received_wtxidrelay(&remote);

		assert_matches!(peermgr.peers.get(&remote), Some(PeerState::Disconnecting));
	}

	#[test]
	fn test_connect_timeout() {
		let rng = fastrand::Rng::with_seed(1);
		let time = RefClock::from(SystemTime::now());

		let remote = ([124, 43, 110, 1], 8333).into();

		let mut addrs = VecDeque::new();
		let mut peermgr = PeerManager::new(util::config(), rng, Hooks::default(), (), time.clone());

		peermgr.initialize(&mut addrs);
		peermgr.connect(&remote);

		assert_eq!(peermgr.connecting().next(), Some(&remote));
		assert_eq!(peermgr.connecting().count(), 1);

		time.elapse(Duration::from_secs(1));
		peermgr.received_wake(&mut addrs);

		assert_eq!(peermgr.connecting().next(), Some(&remote));

		// After the timeout has elapsed, the peer should be disconnected.
		time.elapse(CONNECTION_TIMEOUT);
		peermgr.received_wake(&mut addrs);

		assert_eq!(peermgr.connecting().next(), None);
		assert!(matches!(
			peermgr.peers.get(&remote),
			Some(PeerState::Disconnecting)
		));
	}

	#[test]
	fn test_peer_dropped() {
		let rng = fastrand::Rng::with_seed(1);
		let time = SystemTime::now();
		let mut addrs = VecDeque::new();
		let mut peermgr = PeerManager::new(util::config(), rng.clone(), Hooks::default(), (), time);

		let height = 144;
		let local = ([99, 99, 99, 99], 9999).into();
		let remote = ([124, 43, 110, 1], 8333).into();
		let version = VersionMessage {
			services: ServiceFlags::NETWORK,
			..peermgr.version(local, remote, rng.u64(..), height, time)
		};

		peermgr.initialize(&mut addrs);
		peermgr.connect(&remote);
		peermgr.peer_connected(remote, local, Link::Outbound, height);
		peermgr.received_version(&remote, version, height, &mut addrs);

		let (_, conn) = peermgr.received_verack(&remote, time).unwrap();
		let socket = conn.socket;
		assert_eq!(socket.refs(), 2);

		peermgr
			.negotiated(Link::Outbound)
			.find(|(_, c)| c.socket.addr == remote)
			.unwrap();

		peermgr.received_wake(&mut addrs);
		assert!(!peermgr.is_disconnecting(&remote));
		assert_eq!(socket.refs(), 2);

		drop(socket);

		peermgr.received_wake(&mut addrs);
		assert!(peermgr.is_disconnecting(&remote));
	}

	#[test]
	fn test_disconnects() {
		let rng = fastrand::Rng::with_seed(1);
		let time = SystemTime::now();
		let height = 144;

		let services = ServiceFlags::NETWORK;
		let local = ([99, 99, 99, 99], 9999).into();
		let remote1 = ([124, 43, 110, 1], 8333).into();
		let remote2 = ([124, 43, 110, 2], 8333).into();
		let remote3 = ([124, 43, 110, 3], 8333).into();
		let remote4 = ([124, 43, 110, 4], 8333).into();
		let reason: network::Disconnect<DisconnectReason> =
			DisconnectReason::PeerTimeout("timeout").into();

		let mut addrs = VecDeque::new();
		let mut peermgr = PeerManager::new(util::config(), rng, Hooks::default(), (), time);

		peermgr.initialize(&mut addrs);
		peermgr.connect(&remote1);

		assert_eq!(peermgr.connecting().next(), Some(&remote1));
		assert_eq!(peermgr.connected().next(), None);

		peermgr.peer_connected(remote1, local, Link::Outbound, height);

		assert_eq!(peermgr.connecting().next(), None);
		assert_eq!(
			peermgr.connected().map(|c| &c.socket.addr).next(),
			Some(&remote1)
		);

		// Disconnect remote#1 after it has connected.
		addrs.push_back((Address::new(&remote2, services), Source::Dns));
		peermgr.peer_disconnected(&remote1, &mut addrs, reason.clone());

		assert!(peermgr.is_disconnected(&remote1));
		assert_eq!(peermgr.connected().next(), None);
		assert_eq!(
			peermgr.connecting().next(),
			Some(&remote2),
			"Disconnection triggers a new connection to remote#2"
		);

		// Disconnect remote#2 while still connecting.
		addrs.push_back((Address::new(&remote3, services), Source::Dns));
		peermgr.peer_disconnected(&remote2, &mut addrs, reason.clone());

		assert!(peermgr.is_disconnected(&remote2));
		assert_eq!(
			peermgr.connecting().next(),
			Some(&remote3),
			"Disconnection triggers a new connection to remote#3"
		);

		// Connect, then disconnect remote#3.
		addrs.push_back((Address::new(&remote4, services), Source::Dns));

		peermgr.peer_connected(remote3, local, Link::Outbound, height);
		peermgr.disconnect(remote3, DisconnectReason::Command);
		peermgr.peer_disconnected(&remote3, &mut addrs, reason);

		assert!(peermgr.is_disconnected(&remote3));
		assert_eq!(
			peermgr.connecting().next(),
			Some(&remote4),
			"Disconnection triggers a new connection to remote#4"
		);
	}

	#[test]
	fn test_connection_delta() {
		let target_outbound_peers = 4;
		let height = 144;
		let cfg = Config {
			target_outbound_peers,
			..util::config()
		};
		let rng = fastrand::Rng::with_seed(1);
		let time = SystemTime::now();
		let local = ([99, 99, 99, 99], 9999).into();

		let cases: Vec<((usize, usize, usize, usize), usize)> = vec![
			// outbound = 0/4 (0), connecting = 0/4
			((0, 0, 0, 0), target_outbound_peers),
			// outbound = 0/4 (0), connecting = 1/4
			((1, 0, 0, 0), target_outbound_peers - 1),
			// outbound = 0/4 (0), connecting = 3/4
			((1, 2, 0, 0), target_outbound_peers - 3),
			// outbound = 1/4 (0), connecting = 2/4
			((1, 1, 1, 0), 2),
			// outbound = 2/4 (1), connecting = 2/4
			((1, 1, 1, 1), 1),
			// outbound = 3/4 (1), connecting = 1/4
			((0, 1, 2, 1), 2),
			// outbound = 4/4 (1), connecting = 0/4, extra = 2
			((0, 0, 3, 1), 2),
			// outbound = 6/4 (3), connecting = 0/4
			((0, 0, 3, 3), 0),
			// outbound = 4/4 (4), connecting = 0/4
			((0, 0, 0, target_outbound_peers), 0),
			// outbound = 6/4 (2), connecting = 0/4
			((0, 0, 4, 2), 0),
			// outbound = 6/4 (3), connecting = 0/4
			((0, 0, 2, 4), 0),
			// outbound = 5/4 (2), connecting = 0/4, extra = 1
			((0, 0, 3, 2), 1),
			// outbound = 0/4 (0), connecting = 4/4
			((4, 0, 0, 0), 0),
			// outbound = 4/4 (0), connecting = 0/4, extra = 2
			((0, 0, 4, 0), 2),
			// outbound = 5/4 (3), connecting = 0/4, extra = 1
			((0, 0, 2, 3), 1),
			// outbound = 5/4 (3), connecting = 1/4, extra = 0
			((1, 0, 2, 3), 0),
			// outbound = 5/4 (3), connecting = 1/4, extra = 0
			((0, 1, 2, 3), 0),
		];

		for (case, delta) in cases {
			let (connecting, connected, required, preferred) = case;

			let mut addrs = VecDeque::new();
			let mut peermgr =
				PeerManager::new(cfg.clone(), rng.clone(), Hooks::default(), (), time);

			peermgr.initialize(&mut addrs);

			for i in 0..connecting {
				let remote = ([44, 44, 44, i as u8], 8333).into();
				peermgr.connect(&remote);
				assert!(peermgr.peers.contains_key(&remote));
			}
			for i in 0..connected {
				let remote = ([55, 55, 55, i as u8], 8333).into();
				peermgr.connect(&remote);
				peermgr.peer_connected(remote, local, Link::Outbound, height);
				assert!(peermgr.peers.contains_key(&remote));
			}
			for i in 0..required {
				let remote = ([66, 66, 66, i as u8], 8333).into();
				let version = VersionMessage {
					services: cfg.required_services,
					..peermgr.version(local, remote, rng.u64(..), height, time)
				};

				peermgr.connect(&remote);
				peermgr.peer_connected(remote, local, Link::Outbound, height);
				assert!(peermgr.peers.contains_key(&remote));

				peermgr.received_version(&remote, version, height, &mut addrs);
				assert!(peermgr.peers.contains_key(&remote));

				peermgr.received_verack(&remote, time).unwrap();
				assert_matches!(
					peermgr.peers.get(&remote).unwrap(),
					PeerState::Connected { peer: Some(p), .. } if p.is_negotiated()
				);
			}
			for i in 0..preferred {
				let remote = ([77, 77, 77, i as u8], 8333).into();
				let version = VersionMessage {
					services: cfg.preferred_services,
					..peermgr.version(local, remote, rng.u64(..), height, time)
				};

				peermgr.connect(&remote);
				peermgr.peer_connected(remote, local, Link::Outbound, height);
				assert!(peermgr.peers.contains_key(&remote));

				peermgr.received_version(&remote, version, height, &mut addrs);
				assert!(peermgr.peers.contains_key(&remote));

				peermgr.received_verack(&remote, time).unwrap();
				assert_matches!(
					peermgr.peers.get(&remote).unwrap(),
					PeerState::Connected { peer: Some(p), .. } if p.is_negotiated()
				);
			}
			assert_eq!(peermgr.delta(), delta, "{:?}", case);
		}
	}
}

