use std::borrow::Cow;
use std::collections::LinkedList;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::{cmp, fmt};

use bitcoin::hashes::sha256d;
use bitcoin::network::message::{CommandString, NetworkMessage};
use bitcoin::network::message_blockdata::{InvType, Inventory};
use bitcoin::network::message_network::{Reject, RejectReason, VersionMessage};
use lru::LruCache;

#[cfg(feature = "crossbeam-channel")]
use crossbeam_channel::Sender;
#[cfg(not(feature = "crossbeam-channel"))]
use std::sync::mpsc::{Sender, SyncSender};

use crate::constants;
use crate::error::Error;
use crate::channel;

use crate::message_handler::PeerInventory;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum TokenType {
	Disconnect,
	MsgIn,
	MsgOut,
	NetworkIo,
	NetworkWaker,
}

const NB_TOKEN_TYPES: usize = 5;

/// A peer identifier.
///
/// Can never be 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(usize);

impl PeerId {
	pub(crate) fn new(id: usize) -> PeerId {
		assert_ne!(id, 0);
		assert!(id < usize::max_value() / NB_TOKEN_TYPES, "too many peers!");
		PeerId(id)
	}

	pub(crate) fn from_token(token: mio::Token) -> (PeerId, TokenType) {
		assert_ne!(token, ::WAKE_TOKEN);
		(
			PeerId::new(token.0 / NB_TOKEN_TYPES),
			match token.0 % NB_TOKEN_TYPES {
				0 => TokenType::Disconnect,
				1 => TokenType::MsgIn,
				2 => TokenType::MsgOut,
				3 => TokenType::NetworkIo,
				4 => TokenType::NetworkWaker,
				_ => unreachable!(),
			},
		)
	}

	pub(crate) fn token(&self, token_type: TokenType) -> mio::Token {
		mio::Token(
			self.0 * NB_TOKEN_TYPES
				+ match token_type {
					TokenType::Disconnect => 0,
					TokenType::MsgIn => 1,
					TokenType::MsgOut => 2,
					TokenType::NetworkIo => 3,
					TokenType::NetworkWaker => 4,
				},
		)
	}
}

impl fmt::Display for PeerId {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		fmt::Display::fmt(&self.0, f)
	}
}

pub type OnMessageCallback = fn(Arc<Peer>, NetworkMessage);

/// Specifies the way incoming messages are delivered to the user.
/// System messages like pings and handshake messages are not delivered
/// unless the peer config instructs so.
#[derive(Debug, Clone)]
pub enum MessageDelivery {
	/// Discard all non-system messages.
	Discard,
	/// Callback that will be called for all incoming messages.
	/// This callback is called from a single processing thread, which means:
	/// - Only a single callback can be called across all [Peer]s from the same
	///   [PeerManager].
	/// - As long as this method doesn't return, no other messages
	///   can be processed.
	Callback(OnMessageCallback),
	/// Push all messages into this channel.
	/// Messages are pushed from a single processing thread, which means:
	/// - Only a single send can be performed across all [Peer]s from the same
	///   [PeerManager].
	/// - As long as this channel can't accept the message, no other messages
	///   can be sent.
	#[cfg(feature = "crossbeam-channel")]
	Channel(Sender<NetworkMessage>),
	#[cfg(not(feature = "crossbeam-channel"))]
	Channel(Arc<Mutex<Sender<NetworkMessage>>>),
	/// See [Channel].
	#[cfg(not(feature = "crossbeam-channel"))]
	SyncChannel(SyncSender<NetworkMessage>),
}

#[derive(Debug, Clone)]
pub struct PeerConfig {
	/// Bitcoin network this peer is connected with.
	///
	/// Default value: mainnet.
	pub network: bitcoin::Network,

	/// Specify how messages should be delivered.
	pub delivery: MessageDelivery,

	/// Protocol version to announce to peers.
	///
	/// Default value: 70012.
	pub protocol_version: u32,
	/// Service flags to announce to peers.
	///
	/// Default value: 0.
	pub services: u64,
	/// Whether or not to request peers to relay transactions.
	///
	/// Default value: true.
	pub relay: bool,

	/// Set this to not handle the handshake so the user can handle it manually.
	///
	/// Default value: false.
	//TODO(stevenroose) add testing for this
	pub manual_handshake: bool,
	/// Set this to not handle ping messages so the user can handle them manually.
	///
	/// Default value: false.
	//TODO(stevenroose) add testing for this
	pub manual_pingpong: bool,

	/// The interval at which to trickle data to peers.
	///
	/// Default value: 10 seconds.
	pub trickle_interval: Duration,

	/// The interval at which to ping the peer.
	///
	/// Default value: 2 minutes.
	pub ping_interval: Duration,

	/// The maximum size of the known inventory to keep for this peer.
	pub max_inventory_size: usize,
}

impl Default for PeerConfig {
	fn default() -> Self {
		PeerConfig {
			network: bitcoin::Network::Bitcoin,
			delivery: MessageDelivery::Discard,
			protocol_version: 70012,
			services: 0,
			relay: true,
			manual_handshake: false,
			manual_pingpong: false,
			trickle_interval: Duration::from_secs(10),
			ping_interval: Duration::from_secs(2 * 60),
			max_inventory_size: 1000,
		}
	}
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum PeerType {
	Inbound,
	Outbound,
}

#[derive(Debug, Default)]
pub(crate) struct HandshakeStatus {
	pub received_version: bool,
	pub received_verack: bool,
	pub protocol_version: u32,
	pub version_msg: Option<VersionMessage>,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct PeerInfo {
	pub id: PeerId,
	pub addr: SocketAddr,
	pub local_addr: SocketAddr,
	pub peer_type: PeerType,
}

impl fmt::Display for PeerInfo {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"Peer(#{} {} {})",
			self.id,
			self.addr,
			match self.peer_type {
				PeerType::Inbound => "ib",
				PeerType::Outbound => "ob",
			}
		)
	}
}

//TODO(stevenroose) does it make sense to use an inner or so
//to get rid of these pub(crate) mess?
pub struct Peer {
	// These fields can be public because users can never mutate Peers.
	/// Public info about the [Peer].
	pub info: PeerInfo,
	/// Configuration of the [Peer].
	pub config: PeerConfig,

	/// Keep track of the connected status of the peer.
	pub(crate) connected: AtomicBool,

	pub(crate) msg_tx: channel::SyncSender<NetworkMessage>,

	/// Progress on the handshake progress.
	//TODO(stevenroose) check that nothing can be done while not finished
	//f.e. incoming messages
	pub(crate) handshake: RwLock<HandshakeStatus>,
	/// Ping/pong statistics.
	/// Tuple of the last nonce and the last succesful ping timestamp.
	pub(crate) ping_stats: Mutex<(u64, Instant)>,
	/// If the peer wants header msgs instead of inventory vectors for blocks.
	pub(crate) prefers_headers: AtomicBool,

	pub(crate) network_handler_waker: mio::Waker,
	pub(crate) message_handler_waker: mio::Waker,

	inventory: Arc<RwLock<PeerInventory>>,
}

impl Peer {
	pub(crate) fn new(
		info: PeerInfo,
		config: PeerConfig,
		msg_tx: channel::SyncSender<NetworkMessage>,
		network_handler_waker: mio::Waker,
		message_handler_waker: mio::Waker,
		inventory: Arc<RwLock<PeerInventory>>,
	) -> Result<Arc<Peer>, Error> {
		Ok(Arc::new(Peer {
			info: info,
			connected: AtomicBool::new(true),
			handshake: RwLock::new(HandshakeStatus::default()),
			ping_stats: Mutex::new((0, Instant::now())),
			prefers_headers: AtomicBool::new(false),
			msg_tx: msg_tx,
			network_handler_waker: network_handler_waker,
			message_handler_waker: message_handler_waker,
			config: config,
			inventory: inventory,
		}))
	}

	pub fn address(&self) -> SocketAddr {
		self.info.addr
	}

	pub fn pending_handshake(&self) -> bool {
		if self.config.manual_handshake {
			return false;
		}
		let status = self.handshake.read().unwrap();
		!status.received_version || !status.received_verack
	}

	/// Zero means not known yet.
	pub fn protocol_version(&self) -> u32 {
		self.handshake.read().unwrap().protocol_version
	}

	pub fn version_message(&self) -> Option<VersionMessage> {
		//TODO(stevenroose) find a way to return a reference here
		//as this getter might be used frequently
		self.handshake.read().unwrap().version_msg.clone()
	}

	pub fn prefers_headers(&self) -> bool {
		self.prefers_headers.load(AtomicOrdering::Acquire)
	}

	pub fn connected(&self) -> bool {
		self.connected.load(AtomicOrdering::Acquire)
	}

	pub fn disconnect(&self) {
		// First set connected to false so that any threads waking up ignore this peer.
		if !self.connected.compare_and_swap(true, false, AtomicOrdering::Release) {
			debug!("Called disconnect on {} while already disconnected", self);
			return;
		}

		// Wake up the network handler who will close the TCP connection.
		if let Err(e) = self.network_handler_waker.wake() {
			error!("Error waking up network_handler while disconnecting peer {}: {}", self, e);
		}

		// Ultimately we wake up the message handler. This is so that it
		// receives an event with this peer and it can remove it from the
		// PeerManager's peer list.
		if let Err(e) = self.message_handler_waker.wake() {
			error!("Error waking up message_handler while disconnecting peer {}: {}", self, e);
		}
	}

	fn ensure_can_send_safely(&self) -> Result<(), Error> {
		if !self.connected() {
			return Err(Error::Disconnected);
		}
		if self.pending_handshake() {
			return Err(Error::HandshakePending);
		}
		Ok(())
	}

	pub fn send_msg(&self, msg: NetworkMessage) -> Result<(), Error> {
		self.ensure_can_send_safely()?;
		self.msg_tx.send(msg);
		self.network_handler_waker.wake()?;
		Ok(())
	}

	pub fn tru_send_msg(&self, msg: NetworkMessage) -> Result<(), Error> {
		self.ensure_can_send_safely()?;
		self.msg_tx.try_send(msg)?;
		self.network_handler_waker.wake()?;
		Ok(())
	}

	// pub fn recv_msg(&self) -> Result<Option<NetworkMessage>, Error> {
	//	let mut queue = self.inbound_msgs.lock().unwrap();
	//	if let Some(msg) = queue.pop_front() {
	//		// Wake the message handler to process next message.
	//		if !queue.is_empty() {
	//			self.message_handler_waker.wake()?;
	//		}
	//		Ok(Some(msg))
	//	} else {
	//		Ok(None)
	//	}
	// }

	// pub fn wait_recv_msg(&self, timeout: Duration) -> Result<NetworkMessage, Error> {
	//	let start = SystemTime::now();
	//	let mut queue = self.inbound_msgs.lock().unwrap();
	//	if let Some(msg) = queue.pop_front() {
	//		// Wake the message handler to process next message.
	//		if !queue.is_empty() {
	//			self.message_handler_waker.wake()?;
	//		}
	//		return Ok(msg);
	//	}
	//	let time_passed = SystemTime::now().duration_since(start).unwrap();
	//	let timeout_left = match timeout.checked_sub(time_passed) {
	//		Some(t) => t,
	//		None => return Err(Error::TimedOut),
	//	};
	//	let (mut queue, result) = self.inbound_notify.wait_timeout(queue, timeout_left).unwrap();
	//	if result.timed_out() {
	//		return Err(Error::TimedOut);
	//	}
	//	// The only scenario where the Condvar is notified without any new
	//	// messages in the queue is when the peer is disconnected.
	//	match queue.pop_front() {
	//		Some(m) => {
	//			// Wake the message handler to process next message.
	//			if !queue.is_empty() {
	//				self.message_handler_waker.wake()?;
	//			}
	//			Ok(m)
	//		}
	//		None => {
	//			if !self.connected() {
	//				Err(Error::Disconnected)
	//			} else {
	//				panic!("severe error: mutex condvar released with empty queue");
	//			}
	//		}
	//	}
	// }

	/// Return true if the item was already known.
	pub fn add_known_inventory(&self, hash: sha256d::Hash) -> bool {
		self.known_inventory.lock().unwrap().put(hash, ()).is_some()
	}

	pub fn send_inventory(&self, inv_type: InvType, hash: sha256d::Hash) -> Result<(), Error> {
		self.ensure_can_send_safely()?;
		if self.known_inventory.lock().unwrap().contains(&hash) {
			trace!("Peer {} already knowns about {}, so we're not sending it", self, hash);
		}

		let inv = Inventory {
			//TODO(stevenroose) no clone after https://github.com/rust-bitcoin/rust-bitcoin/pull/357
			inv_type: inv_type.clone(),
			hash: hash,
		};

		// If it's a block, we send it right away.
		// Other inv types, we add to a queue to send later in bunch.
		if inv_type == InvType::Block || inv_type == InvType::WitnessBlock {
			self.send_msg(NetworkMessage::Inv(vec![inv]))?;
		} else {
			self.inv_queue.lock().unwrap().push_back(inv);
		}
		Ok(())
	}

	pub fn send_reject_msg<R>(
		&self,
		command: CommandString,
		reason_code: RejectReason,
		reason: R,
		hash: Option<sha256d::Hash>,
	) -> Result<(), Error>
	where
		R: Into<Cow<'static, str>>,
	{
		if self.protocol_version() < constants::REJECT_VERSION {
			//TODO(stevenroose) CommandString will implement display
			debug!("Not sending '{}' reject message because protocol version is low", command.0);
			return Ok(());
		}

		//TODO(stevenroose) redo this after https://github.com/rust-bitcoin/rust-bitcoin/pull/357/

		//if command == CommandString("block".into()) || command == CommandString("tx".into()) {
		//	if hash.is_none() {
		//		warn!(
		//			"Trying to send reject message for '{}' command without a hash specified.",
		//			command
		//		);
		//	}
		//}

		let msg = Reject {
			message: command.0,
			ccode: reason_code,
			reason: reason.into().into_owned(),
			hash: hash.unwrap_or_default(),
		};
		self.send_msg(NetworkMessage::Reject(msg))?;
		Ok(())
	}
}

impl fmt::Display for Peer {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		fmt::Display::fmt(&self.info, f)
	}
}

impl fmt::Debug for Peer {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		fmt::Display::fmt(self, f)
	}
}

impl cmp::PartialEq for Peer {
	fn eq(&self, other: &Self) -> bool {
		cmp::PartialEq::eq(&self.info.id, &other.info.id)
	}
}

impl cmp::Eq for Peer {}

impl cmp::PartialOrd for Peer {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		cmp::PartialOrd::partial_cmp(&self.info.id, &other.info.id)
	}
}

impl cmp::Ord for Peer {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		cmp::Ord::cmp(&self.info.id, &other.info.id)
	}
}
