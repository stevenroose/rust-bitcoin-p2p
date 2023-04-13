//! Manage a view on a peer's item inventory and schedule trickles
//! to send him new items.

use std::cmp;
use std::collections::{BinaryHeap, HashSet};
use std::time::Instant;

use bitcoin::hashes::{sha256d, Hash};
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::Inventory;
use lru::LruCache;

use crate::logic::Reactions;
use crate::utils;
use crate::{Config, PeerId, PeerState, PeerType};

/// Get the sha256d::Hash of the inv item.
//TODO(stevenroose) replace with https://github.com/rust-bitcoin/rust-bitcoin/pull/515
fn inv_hash(inv: &Inventory) -> sha256d::Hash {
	match *inv {
		Inventory::Error => sha256d::Hash::all_zeros(),
		Inventory::Transaction(t) => t.to_raw_hash(),
		Inventory::Block(b) => b.to_raw_hash(),
		Inventory::CompactBlock(b) => b.to_raw_hash(),
		Inventory::WTx(t) => t.to_raw_hash(),
		Inventory::WitnessTransaction(t) => t.to_raw_hash(),
		Inventory::WitnessBlock(b) => b.to_raw_hash(),
        Inventory::Unknown { hash, .. } => sha256d::Hash::from_byte_array(hash),
	}
}

/// Managing our view of a peer's inventory.
#[derive(Debug)]
pub struct PeerInventory {
	/// The peer's inventory known to us.
	pub known_inventory: LruCache<sha256d::Hash, ()>,
	/// Queue of inv items to send to the peer.
	pub inv_queue: HashSet<Inventory>,
}

impl PeerInventory {
	/// Create a new [PeerInventory] with the given capacity of items.
	pub fn new(config: &Config) -> PeerInventory {
		PeerInventory {
			known_inventory: LruCache::new(config.max_inventory_size),
			//TODO(stevenroose) this is currently unbounded, as seems Core's
			inv_queue: HashSet::new(),
		}
	}
}

/// Handle a received `inv` message from the peer.
pub fn handle_inv(state: &mut PeerState, items: &[Inventory]) {
	for item in items {
		state.inventory.known_inventory.put(inv_hash(item), ());
	}
}

/// Queue a new inventory item for sending.
/// Returns a message to send to the peer.
///
/// Blocks are always sent immediatelly, txs are queued.
pub fn queue_inventory(state: &mut PeerState, inv: Inventory) -> Option<NetworkMessage> {
	let hash = inv_hash(&inv);
	if state.inventory.known_inventory.contains(&hash) {
		trace!("Peer already has inventory item {:?}, not sending", inv);
	}

	match inv {
		// Send blocks right away.
		Inventory::Block(_) | Inventory::WitnessBlock(_) => {
			trace!("Directly relaying inv item {:?}", inv);
			Some(NetworkMessage::Inv(vec![inv]))
		}
		Inventory::CompactBlock(_) => {
            //TODO(stevenroose) compact blocks
            None
		}
		Inventory::Transaction(_) | Inventory::WitnessTransaction(_) | Inventory::WTx(_) => {
			trace!("Queued inv item {:?} for peer", inv);
			state.inventory.inv_queue.insert(inv);
			None
		}
        Inventory::Unknown { .. } => None,
		Inventory::Error => None,
	}
}

/// Handle a scheduled trigger to trickle our inventory to the peer.
pub fn scheduled_trickle(
	react: &mut Reactions,
	config: &Config,
	peer: PeerId,
	state: &mut PeerState,
) {
	// Schedule next trickle.
	let interval = utils::poisson_duration(match state.peer_type {
		PeerType::Inbound => config.avg_inventory_broadcast_interval_inbound,
		PeerType::Outbound => config.avg_inventory_broadcast_interval_outbound,
	});
	react.schedule_trickle(peer, Instant::now() + interval);

	//TODO(stevenroose) Core trickles all mempool txs (bip35)

	if state.inventory.inv_queue.is_empty() {
		return; // nothing to do
	}

	// Use a heap because we don't to fetch all items and the heap is built in
	// O(n) and yields items in O(log n).
	let mut items = BinaryHeap::with_capacity(state.inventory.inv_queue.len());
	for inv in state.inventory.inv_queue.iter() {
		// Take all queued invs and order them by random.
		//TODO(stevenroose) Core orders by fee, but we don't know fee. consider closure
		items.push((rand::random::<u32>(), *inv));
	}

	let mut invs = Vec::with_capacity(cmp::min(config.max_inventory_broadcast_size, items.len()));
	while let Some((_, inv)) = items.pop() {
		invs.push(inv);
		state.inventory.inv_queue.remove(&inv);
		state.inventory.known_inventory.put(inv_hash(&inv), ());

		if invs.len() == config.max_inventory_broadcast_size {
			break;
		}
	}
	react.send(peer, NetworkMessage::Inv(invs));
}
