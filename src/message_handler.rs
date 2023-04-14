use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{RwLock, Arc};
use std::time::Instant;
use std::{cmp, io, mem};
use std::collections::HashMap;
use std::collections::LinkedList;

use bitcoin::hashes::sha256d;
use bitcoin::network::message_blockdata::{InvType, Inventory};
use bitcoin::network::message::{CommandString, NetworkMessage};
use bitcoin::network::message_network::RejectReason;
use rand::RngCore;
use lru::LruCache;

use crate::channel;
use crate::constants;
use crate::manager::PeerManagerState;
use crate::peer::{MessageDelivery, Peer, PeerId, TokenType, PeerConfig};
use crate::utils::WakeupScheduler;
use crate::WAKE_TOKEN;

pub struct PeerInventory {
	pub inv_queue: LinkedList<Inventory>,
	pub known_inventory: LruCache<sha256d::Hash, ()>,
}

impl PeerInventory {
	pub fn new(capacity: usize) -> PeerInventory {
		PeerInventory {
			inv_queue: LinkedList::new(),
			known_inventory: LruCache::new(capacity),
		}
	}
}

pub struct PeerState {
	pub id: PeerId,
	pub config: PeerConfig,
	pub net_tx: channel::SyncSender<NetworkMessage>,
	pub net_rx: channel::Receiver<NetworkMessage>,
	pub msg_tx: channel::SyncSender<NetworkMessage>,
	pub msg_rx: channel::Receiver<NetworkMessage>,

	pub inventory: Arc<RwLock<PeerInventory>>,
}

type Peers = HashMap<PeerId, PeerState>;

fn reject_duplicate(peer: &Arc<Peer>, command: &'static str) {
	let _ = peer.send_reject_msg(
		CommandString(command.into()),
		RejectReason::DUPLICATE,
		format!("duplicate {} message", command),
		None,
	);
	peer.disconnect();
}

/// Returns true if the next message can be handled.
fn handle_message(state: &Arc<PeerManagerState>, peer: &Arc<Peer>, msg: NetworkMessage) -> bool {
	match msg {
		NetworkMessage::Version(ver) if !peer.config.manual_handshake => {
			if !peer.pending_handshake() {
				reject_duplicate(peer, "version"); //TODO(stevenroose) msg.command()
				return false;
			}
			let mut handshake = peer.handshake.write().unwrap();
			handshake.protocol_version = std::cmp::min(peer.config.protocol_version, ver.version);
			//TODO(stevenroose) can we serve non-witness peers??
			handshake.version_msg = Some(ver);
			handshake.received_version = true;

			// Then reply with a verack.
			if let Err(e) = peer.send_msg(NetworkMessage::Verack) {
				warn!("{} Error sending Verack message during handshake: {}", peer, e);
				peer.disconnect();
			}
			return true;
		}

		NetworkMessage::Verack if !peer.config.manual_handshake => {
			if !peer.pending_handshake() {
				reject_duplicate(peer, "verack"); //TODO(stevenroose) msg.command()
				return false;
			}
			let mut handshake = peer.handshake.write().unwrap();
			handshake.received_verack = true;

			// Handshake finished, start scheduling pings.
			state.ping_schedule.lock().unwrap().schedule(
				peer.clone(),
				Instant::now().checked_add(peer.config.ping_interval).expect("time overflow"),
			);
			return true;
		}

		NetworkMessage::Ping(ping) if !peer.config.manual_pingpong => {
			if peer.protocol_version() <= constants::BIP0031_VERSION {
				return true;
			}
			// On a ping message, we just reply with a pong with the same nonce.
			trace!("{} replying to ping with nonce {}", peer, ping);
			if let Err(e) = peer.send_msg(NetworkMessage::Pong(ping)) {
				warn!("{} Error replying to ping message: {}", peer, e);
			}
			return true;
		}

		NetworkMessage::Pong(pong) if !peer.config.manual_pingpong => {
			if peer.protocol_version() <= constants::BIP0031_VERSION {
				return true;
			}
			let mut stats = peer.ping_stats.lock().unwrap();
			if stats.0 != 0 && stats.0 == pong {
				trace!("{} pong'ed ping with nonce {}", peer, pong);
				stats.0 = 0;
				stats.1 = Instant::now();
			}
			return true;
		}

		NetworkMessage::SendHeaders => {
			debug!("{} prefers headers ('sendheaders')", peer);
			peer.prefers_headers.store(true, AtomicOrdering::Release);
			return true;
		}

		NetworkMessage::Inv(ref items) => {
			// Add the items to the known inventory of the peer, but still
			// pass the message to the handler.
			let mut known_inventory = peer.known_inventory.lock().unwrap();
			for item in items.iter() {
				known_inventory.put(item.hash, ());
			}
		}
		_ => {}
	}

	match peer.config.delivery {
		MessageDelivery::Discard => true,
		MessageDelivery::Callback(callback) => {
			let move_peer = peer.clone();
			trace!(
				"{} Calling callback method for with a {:?} message",
				peer,
				//TODO(stevenroose) wait for https://github.com/rust-bitcoin/rust-bitcoin/pull/357/
				"placeholder" //msg.command(),
			);
			//TODO(stevenroose) measure the time spent in the callback and print
			// a warning if it exceeds some limit
			callback(move_peer, msg);
			true
		}
		#[cfg(feature = "crossbeam-channel")]
		MessageDelivery::Channel(ref sender) => {
			if let Err(e) = sender.send(msg) {
				warn!("{} Failed to send msg over delivery channel; disconnecting: {}", peer, e);
				peer.disconnect();
				false
			} else {
				true
			}
		}
		#[cfg(not(feature = "crossbeam-channel"))]
		MessageDelivery::Channel(ref sender_mtx) => {
			if let Err(e) = sender_mtx.lock().unwrap().send(msg) {
				warn!("{} Failed to send msg over delivery channel; disconnecting: {}", peer, e);
				peer.disconnect();
				false
			} else {
				true
			}
		}
		#[cfg(not(feature = "crossbeam-channel"))]
		MessageDelivery::SyncChannel(ref sender) => {
			if let Err(e) = sender.send(msg) {
				warn!("{} Failed to send msg over delivery channel; disconnecting: {}", peer, e);
				peer.disconnect();
				false
			} else {
				true
			}
		}
	}
}

#[derive(Debug)]
struct Scheduling {
	/// A queue of scheduled ping messages to go out.
	/// Since all additions are of the exact same time, we can just add them
	/// at the end and take the closest from the front.
	/// A new peer is first scheduled right after the initial handshake is
	/// succesfully performed.
	pub ping_schedule: WakeupScheduler,
	pub trickle_schedule: WakeupScheduler,
}

impl Scheduling {
	fn new() -> Scheduling {
		Scheduling {
			ping_schedule: WakeupScheduler::new(),
			trickle_schedule: WakeupScheduler::new(),
		}
	}

	fn next_wakeup(&self) -> Option<Instant> {
        let min = |w1, w2| match (w1, w2) {
            (Some(w), None) => Some(w),
            (None, Some(w)) => Some(w),
            (Some(w1), Some(w2)) => if w1 < w2 { Some(w1) } else { Some(w2) },
            (None, None) => None,
        };

        let mut wakeup = self.ping_schedule.next_wakeup();
        wakeup = min(wakeup, self.trickle_schedule.next_wakeup());
        wakeup
	}

	fn handle_trickles(&mut self, peers: &mut Peers) {
		let now = Instant::now();
		let mut schedule = self.trickle_schedule;
		'peers: while !schedule.is_empty() && schedule.next_wakeup().unwrap() < now {
			let id = schedule.pop().unwrap().1;
			let peer = match peers.get(&id) {
				Some(p) => p,
				None => continue,
			};

			// It's subtly important here that we take the next two locks in this
			// other because they are taken in the same order in Peer::send_inventory.
			let mut peer_inv = peer.inventory.write().unwrap();
			let mut known_inventory = peer_inv.known_inventory;
			let mut queue = peer_inv.inv_queue;

			let mut invs =
				Vec::with_capacity(cmp::min(queue.len(), peer.config.max_inventory_size));
			while let Some(inv) = queue.pop_front() {
				// Check if the peer already knows the item by now.
				// Skip if it does; add it if it doesn't.
				if known_inventory.put(inv.hash, ()).is_some() {
					continue;
				}

				invs.push(inv);

				// Send out an inv message if the inventory size limit is reached.
				if invs.len() == peer.config.max_inventory_size {
					let to_send = mem::replace(
						&mut invs,
						Vec::with_capacity(cmp::min(queue.len(), peer.config.max_inventory_size)),
					);
					if let Err(e) = peer.net_tx.send(NetworkMessage::Inv(to_send)) {
						error!("{} Error sending inv message: {}", peer.id, e);
						//TODO(stevenroose) disconnect
						continue 'peers;
					}
				}
			}

			if let Err(e) = peer.net_tx.send(NetworkMessage::Inv(invs)) {
				error!("{} Error sending inv message: {}", peer.id, e);
				//TODO(stevenroose) disconnect
				continue;
			}

			drop(known_inventory);
			drop(queue);
			// Schedule next trickle.
			//TODO(stevenroose) add trickly interval variance
			let next = now.checked_add(peer.config.ping_interval).expect("time overflow");
			schedule.schedule(peer.id, next);
		}
	}

	//TODO(stevenroose) consider keeping track of the last received message time from a peer
	// and not sending pings if that time is recent
	fn handle_scheduled_pings(state: &Arc<PeerManagerState>) {
		let now = Instant::now();
		let mut schedule = state.ping_schedule.lock().unwrap();
		while !schedule.is_empty() && schedule.next_wakeup().unwrap() < now {
			let peer = schedule.pop().unwrap().1;
			if !peer.connected() {
				continue;
			}

			// Send a ping to the peer.
			let mut stats = peer.ping_stats.lock().unwrap();
			let nonce = rand::thread_rng().next_u64();
			trace!("{} sending ping with nonce {}", peer, nonce);
			if let Err(e) = peer.send_msg(NetworkMessage::Ping(nonce)) {
				error!("{} Error sending ping message: {}", peer, e);
				peer.disconnect();
				continue;
			} else {
				stats.0 = nonce;
			}
			drop(stats);

			// Schedule next ping.
			//TODO(stevenroose) consider adding ping interval variance
			let next = now.checked_add(peer.config.ping_interval).expect("time overflow");
			schedule.schedule(peer, next);
		}
	}
}

//TODO(stevenroose) impl
pub fn message_handler(
	mut poll: mio::Poll,
	new_peer_rx: channel::Receiver<(PeerId, PeerState)>,
	quit: Arc<AtomicBool>,
) {
	trace!("message_handler started");
	let mut events = mio::Events::with_capacity(1024);

	let mut schedule = Scheduling::new();

	loop {
		// Wait until an event happens.
		trace!("message_handler polling...");
		events.clear();
		let timeout = schedule.next_wakeup().map(|i| i - Instant::now());
		match poll.poll(&mut events, timeout) {
			Ok(_) => {}
			Err(e) if e.kind() == io::ErrorKind::TimedOut => {
				//TODO(stevenroose) this might actually happen normally
				panic!("Unexpected io::TimedOut error from poll: '{}'; events: {:?}", e, events);
			}
			Err(e) => {
				panic!("Poll returned error: {}", e);
			}
		};

		if quit.load(AtomicOrdering::Acquire) {
			debug!("message_handler thread shutting down");
			break;
		}

		// Handle scheduled trickles.
		schedule.handle_trickles();
		// Handle scheduled pings.
		schedule.handle_scheduled_pings(&state);

		// Keep the lock during the operations on purpose. Not sure if this is good.
		'events: for event in events.iter() {
			if event.token() == WAKE_TOKEN {
				continue; // We were woken up by the waker.
			}

			let (id, token_tp) = PeerId::from_token(event.token());
			assert_eq!(token_tp, TokenType::Message);
			let peer = if let Some(peer) = state.peers.read().unwrap().get(&id) {
				peer.clone()
			} else {
				//TODO(stevenroose) should this be allowed?
				panic!("message_handler woke up with token of unknown peer: {:?}", event.token());
				//continue;
			};

			if !peer.connected() {
				debug!("message_handler processing disconnected peer {}", peer);
				assert!(state.peers.write().unwrap().remove(&id).is_some());
				continue;
			}

			// Try handling as many messages as possible.
			while let Some(msg) = peer.inbound_msgs.lock().unwrap().pop_front() {
				let next = handle_message(&state, &peer, msg);
				if !next || !peer.connected() {
					break;
				}
			}
		}
	}
}
