//! Bitcoin p2p library.

#[macro_use]
extern crate log;

pub mod peer;
pub mod addrmgr;
pub mod peermgr;
pub mod pingmgr;

mod error;
pub use error::Error;

mod events;
pub use events::{EventSource, Listener, ListenResult};

mod p2p;
pub use p2p::P2P;

mod time;

/// Identifies a peer.
pub type PeerId = std::net::SocketAddr;

/// A block height.
pub type Height = u64;

use std::{fmt, io, net};
use std::collections::HashSet;
use std::sync::Arc;

use bitcoin::{BlockHash, BlockHeader, Transaction};
use bitcoin::consensus::encode;
use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::network::message_filter::{CFHeaders, CFilter, GetCFHeaders, GetCFilters};
use bitcoin::network::message_network::VersionMessage;

/// Minimum supported peer protocol version.
/// This version includes support for the `sendheaders` feature.
pub const MIN_PROTOCOL_VERSION: u32 = 70012;

/// Link direction of the peer connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
	/// Inbound conneciton.
	Inbound,
	/// Outbound connection.
	Outbound,
}

impl Link {
	/// Check whether the link is outbound.
	pub fn is_outbound(&self) -> bool {
		*self == Link::Outbound
	}

	/// Check whether the link is inbound.
	pub fn is_inbound(&self) -> bool {
		*self == Link::Inbound
	}
}

/// Bitcoin wire protocol.
//TODO(stevenroose) fill in missing messages
pub trait Wire {
	/// Send a raw message.
	fn message(&self, msg: NetworkMessage);

	// Handshake messages //////////////////////////////////////////////////////

	/// Send a `version` message.
	fn version(&self, msg: VersionMessage) {
		self.message(NetworkMessage::Version(msg));
	}

	/// Send a `verack` message.
	fn verack(&self) {
		self.message(NetworkMessage::Verack);
	}

	/// Send a BIP-339 `wtxidrelay` message.
	fn wtxid_relay(&self) {
		self.message(NetworkMessage::WtxidRelay);
	}

	/// Send a `sendheaders` message.
	fn send_headers(&self) {
		self.message(NetworkMessage::SendHeaders);
	}

	// Ping/pong ///////////////////////////////////////////////////////////////

	/// Send a `ping` message.
	fn ping(&self, nonce: u64) {
		self.message(NetworkMessage::Ping(nonce));
	}

	/// Send a `pong` message.
	fn pong(&self, nonce: u64) {
		self.message(NetworkMessage::Pong(nonce));
	}

	// Addresses //////////////////////////////////////////////////////////////

	/// Send a `getaddr` message.
	fn get_addr(&self) {
		self.message(NetworkMessage::GetAddr);
	}

	/// Send an `addr` message.
	fn addr(&self, addrs: Vec<(u32, Address)>) { //TODO(stevenroose) typing of time
		self.message(NetworkMessage::Addr(addrs));
	}

	// Compact block filters ///////////////////////////////////////////////////

	/// Get compact filter headers from peer, starting at the start height,
	/// and ending at the stop hash.
	fn get_cfheaders(&self, start_height: Height, stop_hash: BlockHash) {
		self.message(NetworkMessage::GetCFHeaders(GetCFHeaders {
			filter_type: 0x00,
			start_height: start_height as u32,
			stop_hash,
		}));
	}

	/// Get compact filters from a peer.
	fn get_cfilters(&self, start_height: Height, stop_hash: BlockHash) {
		self.message(NetworkMessage::GetCFilters(GetCFilters {
			filter_type: 0x00,
			start_height: start_height as u32,
			stop_hash,
		}));
	}

	/// Send compact filter headers to a peer.
	fn cfheaders(&self, headers: CFHeaders) {
		self.message(NetworkMessage::CFHeaders(headers));
	}

	/// Send a compact filter to a peer.
	fn cfilter(&self, filter: CFilter) {
		self.message(NetworkMessage::CFilter(filter));
	}

	// Header sync /////////////////////////////////////////////////////////////

	/// Get headers from a peer.
	fn get_headers(&self, pver: u32, locators: Vec<BlockHash>, stop_hash: BlockHash) {
		self.message(NetworkMessage::GetHeaders(GetHeadersMessage {
			version: pver,
			// Starting hashes, highest heights first.
			locator_hashes: locators,
			// Using the zero hash means *fetch as many blocks as possible*.
			stop_hash: stop_hash,
		}));
	}

	/// Send headers to a peer.
	fn headers(&self, headers: Vec<BlockHeader>) {
		self.message(NetworkMessage::Headers(headers));
	}

	// Inventory ///////////////////////////////////////////////////////////////

	/// Sends an `inv` message to a peer.
	fn inv(&self, items: Vec<Inventory>) {
		self.message(NetworkMessage::Inv(items));
	}

	/// Sends a `getdata` message to a peer.
	fn get_data(&self, items: Vec<Inventory>) {
		self.message(NetworkMessage::GetData(items));
	}

	/// Sends a `tx` message to a peer.
	fn tx(&self, tx: Transaction) {
		self.message(NetworkMessage::Tx(tx));
	}
}

/// Extension trait for all types that implemenet [Wire].
///
/// We have an extension trait because we want [Wire] to be boxable and
/// the method(s) in this trait take generic parameters.
pub trait WireExt: Wire {
	/// Send any other user-defined wire message type that converts into
	/// a [NetworkMessage].
	///
	/// Usually these types convert into the [NetworkMessage::Other] variant.
	fn other(&self, msg: impl Into<NetworkMessage>) { self.message(msg.into()) }
}

impl<T: Wire> WireExt for T {}

/// Peer whitelist.
#[derive(Debug, Clone, Default)]
pub struct Whitelist {
	/// Trusted addresses.
	pub addresses: HashSet<net::IpAddr>,
	/// Trusted user-agents.
	pub user_agents: HashSet<String>,
}

impl Whitelist {
	fn contains(&self, addr: &net::IpAddr, user_agent: &str) -> bool {
		self.addresses.contains(addr) || self.user_agents.contains(user_agent)
	}
	fn contains_ip(&self, addr: &net::IpAddr) -> bool {
		self.addresses.contains(addr)
	}
}

/// Disconnect reason.
#[derive(Debug, Clone)]
pub enum DisconnectReason {
	/// Failed to make initial connection.
	ConnectionFailed,
	/// An error in the underlying connection with the peer.
	ConnectionError(Arc<io::Error>),
	/// Peer is misbehaving.
	PeerMisbehaving(&'static str),
	/// Peer protocol version is too old or too recent.
	PeerProtocolVersion(u32),
	/// Peer doesn't have the required services.
	PeerServices(ServiceFlags),
	/// Peer chain is too far behind.
	PeerHeight(u64),
	/// Peer magic is invalid.
	PeerMagic(u32),
	/// Peer timed out.
	PeerTimeout(&'static str),
	/// Connection to self was detected.
	SelfConnection,
	/// Inbound connection limit reached.
	ConnectionLimit,
	/// Error trying to decode incoming message.
	DecodeError(Arc<encode::Error>), // Arc for Clone
	/// Peer was forced to disconnect by external command.
	Command,
	/// Peer was disconnected for another reason.
	Other(&'static str),
}
//TODO(stevenroose) check if we use all of them

impl DisconnectReason {
	/// Check whether the disconnect reason is transient, ie. may no longer be applicable
	/// after some time.
	pub fn is_transient(&self) -> bool {
		match self {
			Self::ConnectionLimit | Self::PeerTimeout(_) | Self::PeerHeight(_) => true,
			_ => false,
		}
	}
}

impl fmt::Display for DisconnectReason {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::ConnectionFailed => write!(f, "failed to connect"),
			Self::ConnectionError(e) => write!(f, "connection error: {}", e),
			Self::PeerMisbehaving(reason) => write!(f, "peer misbehaving: {}", reason),
			Self::PeerProtocolVersion(_) => write!(f, "peer protocol version mismatch"),
			Self::PeerServices(_) => write!(f, "peer doesn't have the required services"),
			Self::PeerHeight(_) => write!(f, "peer is too far behind"),
			Self::PeerMagic(magic) => write!(f, "received message with invalid magic: {}", magic),
			Self::PeerTimeout(s) => write!(f, "peer timed out: {:?}", s),
			Self::SelfConnection => write!(f, "detected self-connection"),
			Self::ConnectionLimit => write!(f, "inbound connection limit reached"),
			Self::DecodeError(err) => write!(f, "message decode error: {}", err),
			Self::Command => write!(f, "received external command"),
			Self::Other(reason) => write!(f, "{}", reason),
		}
	}
}

#[cfg(test)]
mod test {
	use std::mem;

	use super::*;

	#[test]
	fn disconnect_reason_size() {
		//! A test to make sure we notice when this size changes.
		assert_eq!(mem::size_of::<DisconnectReason>(), 32);
	}

	#[test]
	fn wire_ensure_boxable() {
		fn test(_: Box<dyn Wire>) {}
	}
}
