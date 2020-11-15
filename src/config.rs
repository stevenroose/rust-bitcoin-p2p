use std::time::Duration;

use bitcoin::network::constants::{Network, ServiceFlags, PROTOCOL_VERSION};

/// Configuration options for P2P.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
	/// The Bitcoin network to operate in.
	///
	/// Default value: mainnet.
	pub network: Network,

	/// The protocol version to advertise to peers.
	///
	/// Default value: [bitcoin::network::constants::PROTOCOL_VERSION].
	pub protocol_version: u32,

	/// Service flags to announce to peers.
	///
	/// Default value: NONE.
	pub services: ServiceFlags,

	/// Whether to ask peers to relay txs.
	///
	/// Default value: false.
	pub relay: bool,

	/// Whether we want peers to send us headers instead of block invs.
	/// Note that this only takes effect for peers that support sendheaders.
	///
	/// Default value: false.
	pub send_headers: bool,

	/// Whether we want peers to send us compact blocks instead of block invs.
	/// Note that this only takes effect for peers that support compact blocks.
	///
	/// Default value: false.
	pub send_compact_blocks: bool,

	/// The user agent to advertise to peers.
	///
	/// Default value: "rust_bitcoin/bitcoin_p2p".
	pub user_agent: String,

	/// The maximum size of the pending messages queue for each peer.
	///
	/// Default value: 25.
	pub max_msg_queue_size: usize,

	/// The interval at which to ping peers.
	///
	/// Default value: 2 minutes.
	pub ping_interval: Duration,

	/// The maximum capacity of the known inventory to keep for each peer.
	///
	/// Default value: 100.
	pub max_inventory_size: usize,

	/// The maximum number of inventory items to broadcast each interval.
	///
	/// Equivalent to Core's `INVENTORY_BROADCAST_MAX` (35).
	///
	/// Default value: 35.
	pub max_inventory_broadcast_size: usize,

	/// The average interval between broadcasting inventory.
	///
	/// Equivalent to Core's `INVENTORY_BROADCAST_INTERVAL` (5 secs).
	///
	/// Default value: 5 seconds.
	pub avg_inventory_broadcast_interval_inbound: Duration,

	/// The average interval between broadcasting inventory.
	///
	/// Equivalent to half of Core's `INVENTORY_BROADCAST_INTERVAL` (2 secs).
	///
	/// Default value: 2 seconds.
	pub avg_inventory_broadcast_interval_outbound: Duration,
}

impl Default for Config {
	fn default() -> Config {
		Config {
			network: Network::Bitcoin,
			protocol_version: PROTOCOL_VERSION,
			services: ServiceFlags::NONE,
			relay: false,
			send_headers: false,
			send_compact_blocks: false,
			user_agent: "rust_bitcoin/bitcoin_p2p".to_owned(),
			max_msg_queue_size: 25,
			ping_interval: Duration::from_secs(2 * 60),
			max_inventory_size: 100,
			max_inventory_broadcast_size: 35,
			avg_inventory_broadcast_interval_inbound: Duration::from_secs(5),
			avg_inventory_broadcast_interval_outbound: Duration::from_secs(2),
		}
	}
}
