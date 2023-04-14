use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bitcoin::network::address::Address as BtcNetAddress;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_network::VersionMessage;
use hashbrown::HashMap;

use crate::channel;
use crate::error::Error;
use crate::message_handler::{PeerInventory, message_handler, PeerState};
use crate::network_handler::{network_handler, PeerIo};
use crate::peer::{Peer, PeerConfig, PeerId, PeerInfo, PeerType, TokenType};
use crate::utils::{WaitGroup, WakeupScheduler};
use crate::WAKE_TOKEN;

#[derive(Debug, Clone)]
pub struct PeerManagerConfig {
	/// The default config values for new peers.
	/// This is used when peers are added without a config given or
	/// this can be used to autocomplete [PeerConfig] structs.
	///
	/// ```rust,ignore
	/// let cfg = PeerConfig {
	///		relay: true,
	///		..peer_manager.default_peer_config()
	/// };
	/// ```
	pub default_peer_config: Option<PeerConfig>,

	/// The user agent to advertise to peers.
	pub user_agent: Cow<'static, str>,

	/// The timeout for the initial handshake procedure.
	pub handshake_timeout: Duration,
}

impl Default for PeerManagerConfig {
	fn default() -> Self {
		PeerManagerConfig {
			default_peer_config: None,
			user_agent: "/rust-bitcoin-p2p/".into(),
			handshake_timeout: Duration::from_secs(30),
		}
	}
}

pub(crate) struct PeerManagerState {
	pub config: PeerManagerConfig,
	/// Set of peers, indexed by their Token/ID.
	pub peers: RwLock<HashMap<PeerId, Arc<Peer>>>,
}

impl PeerManagerState {
	pub fn remove_disconnected_peers(&self) {
		self.peers.write().unwrap().retain(|_, peer| peer.connected());
	}
}

pub struct PeerManager {
	next_peer_id: Mutex<usize>,
	state: Arc<PeerManagerState>,
	// Manage the network_handler thread.
	network_handler_peers_tx: channel::SyncSender<(PeerId, PeerIo)>,
	network_handler_registry: mio::Registry,
	network_handler_waker: mio::Waker,
	// Manager the message_handler thread.
	message_handler_peers_tx: channel::SyncSender<(PeerId, PeerState)>,
	message_handler_registry: mio::Registry,
	message_handler_waker: mio::Waker,
	// these fields are used to synchronize the shutdown of the threads
	wg: WaitGroup,
	quit: Arc<AtomicBool>,
}

impl PeerManager {
	pub fn new(config: PeerManagerConfig) -> Result<PeerManager, Error> {
		let state = Arc::new(PeerManagerState {
			config: config,
			peers: RwLock::new(HashMap::new()),
		});

		let wg = WaitGroup::new();
		let quit = Arc::new(AtomicBool::new(false));

		let nh_poll = mio::Poll::new()?;
		let nh_registry = nh_poll.registry().try_clone()?;
		let (nh_tx, nh_rx) = channel::bounded(1);
		let nh_tb = thread::Builder::new().name("bitcoin_p2p_network_handler".to_owned());
		let nh_quit = quit.clone();
		let nh_wg = wg.clone();
		nh_tb
			.spawn(move || {
				network_handler(nh_poll, nh_rx, nh_quit);
				drop(nh_wg);
			})
			.expect("failed to spawn network_handler thread");

		let mh_poll = mio::Poll::new()?;
		let mh_registry = mh_poll.registry().try_clone()?;
		let mh_tb = thread::Builder::new().name("bitcoin_p2p_message_handler".to_owned());
		let (mh_tx, mh_rx) = channel::bounded(1);
		let mh_state = state.clone();
		let mh_quit = quit.clone();
		let mh_wg = wg.clone();
		mh_tb
			.spawn(move || {
				message_handler(mh_poll, mh_rx, mh_quit);
				drop(mh_wg);
			})
			.expect("failed to spawn message_handler thread");

		Ok(PeerManager {
			next_peer_id: Mutex::new(1), // 0 is special
			state: state,
			network_handler_peers_tx: nh_tx,
			network_handler_waker: mio::Waker::new(&nh_registry, WAKE_TOKEN)?,
			network_handler_registry: nh_registry,
			message_handler_peers_tx: mh_tx,
			message_handler_waker: mio::Waker::new(&mh_registry, WAKE_TOKEN)?,
			message_handler_registry: mh_registry,
			wg: wg,
			quit: quit,
		})
	}

	pub fn default_peer_config(&self) -> PeerConfig {
		if let Some(ref cfg) = self.state.config.default_peer_config {
			cfg.clone()
		} else {
			Default::default()
		}
	}

	fn get_next_peer_id(&self) -> PeerId {
		let mut next_peer_id = self.next_peer_id.lock().unwrap();
		let ret = *next_peer_id;
		*next_peer_id += 1;
		PeerId::new(ret)
	}

	fn make_version(&self, peer: &Peer, start_height: i32) -> VersionMessage {
		let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

		VersionMessage {
			version: peer.config.protocol_version,
			services: peer.config.services,
			timestamp: timestamp,
			receiver: BtcNetAddress::new(&peer.address(), 0),
			sender: BtcNetAddress::new(&peer.info.local_addr, 0),
			nonce: 0,
			user_agent: self.state.config.user_agent.as_ref().to_owned(),
			start_height: start_height,
			relay: peer.config.relay,
		}
	}

	fn start_handshake(
		&self,
		peer: &Peer,
		version: Option<VersionMessage>,
		start_height: i32,
	) -> Result<(), Error> {
		peer.send_msg(NetworkMessage::Version(
			version.unwrap_or(self.make_version(peer, start_height)),
		))?;
		Ok(())
	}

	/// Add a new peer to the manager after the conncetion has been established.
	/// This will perform the handshake.
	pub fn add_peer(
		&self,
		conn: mio::net::TcpStream,
		config: Option<PeerConfig>,
		peer_type: PeerType,
		start_height: i32,
	) -> Result<Arc<Peer>, Error> {
		let id = self.get_next_peer_id();
		let config = config.unwrap_or(self.default_peer_config());
		let info = PeerInfo {
			id: id,
			addr: conn.peer_addr()?,
			local_addr: conn.local_addr()?,
			peer_type: peer_type,
		};

		let (msg_tx, msg_rx) = channel::bounded(42); //TODO(stevenroose) n
		let (net_tx, net_rx) = channel::bounded(42); //TODO(stevenroose) n

		// Notify the network handler.
		self.network_handler_peers_tx.send((
			id,
			PeerIo {
				conn: conn,
				network: config.network,
				in_rx: net_rx,
				out_tx: net_tx,
				msgh_waker: mio::Waker::new(
					&self.message_handler_registry,
					id.token(TokenType::MsgIn),
				)?,
				in_buf: Vec::new(),
				out_buf: Vec::new(),
			},
		));
		self.network_handler_waker.wake()?;

		let inv = Arc::new(RwLock::new(PeerInventory::new(100))); //TODO(stevenroose) capacity
		let state = PeerState {
			id: id,
			config: config.clone(), //TODO(stevenroose) consider arc
			net_tx: net_tx,
			net_rx: net_rx,
			msg_tx: msg_tx,
			msg_rx: msg_rx,
			inventory: inv.clone(),
		};
		self.message_handler_waker.wake()?;

		let peer = Peer::new(
			info,
			config,
			msg_tx,
			mio::Waker::new(&self.network_handler_registry, id.token(TokenType::NetworkWaker))?,
			mio::Waker::new(&self.message_handler_registry, id.token(TokenType::MsgOut))?,
			inv,
		)?;
		self.state.peers.write().unwrap().insert(id, peer.clone());

		self.start_handshake(&peer, None, start_height)?;
		Ok(peer)
	}

	pub fn shutdown(self) {
		info!("PeerManager shutting down...");
		self.quit.store(true, AtomicOrdering::Release);
		for (_, peer) in self.state.peers.write().unwrap().drain() {
			peer.disconnect();
		}
		if let Err(e) = self.network_handler_waker.wake() {
			error!("Error waking up network_handler thread: {}", e);
		}
		if let Err(e) = self.message_handler_waker.wake() {
			error!("Error waking up message_handler thread: {}", e);
		}
		self.wg.wait();
		info!("PeerManager shutdown complete");
	}
}
