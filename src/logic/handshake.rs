use std::sync::{atomic, Arc};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_network::VersionMessage;

use crate::logic::Reactions;
use crate::{constants, Config, PeerId, PeerState, P2P};

/// Some state kept for handshaking with the peer.
#[derive(Debug, Default)]
pub struct Handshake {
	/// The peer's version message.
	pub version: Option<VersionMessage>,

	/// The agreed protocol version.
	pub pver: u32,

	/// We have sent our version.
	pub version_sent: bool,
	/// Peer has acked our version.
	pub version_acked: bool,
}

impl Handshake {
	/// Is the handshake finished.
	pub fn finished(&self) -> bool {
		self.version.is_some() && self.version_sent && self.version_acked
	}
}

/// Prepare a `version` message to send to the peer.
pub fn make_version_msg(
	config: &Config,
	peer: PeerId,
	state: &PeerState,
	start_height: u32,
) -> VersionMessage {
	let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

	let msg = VersionMessage {
		version: config.protocol_version,
		services: config.services,
		timestamp: timestamp,
		receiver: bitcoin::network::address::Address::new(&state.addr, ServiceFlags::NONE),
		sender: bitcoin::network::address::Address::new(&state.local_addr, config.services),
		nonce: rand::random(),
		user_agent: config.user_agent.clone(),
		start_height: start_height as i32,
		relay: config.relay,
	};

	info!("Prepared version message to peer {} ({}): {:?}", peer, state.addr, msg);
	msg
}

/// Send messages to request features from the peer.
fn request_features(react: &mut Reactions, config: &Config, peer: PeerId, state: &mut PeerState) {
	let pver = state.handshake.pver;

	if config.send_headers && pver >= constants::VERSION_SENDHEADERS {
		react.send(peer, NetworkMessage::SendHeaders);
	}

	if config.send_compact_blocks && pver >= constants::VERSION_COMPACT_BLOCKS {
		//TODO(stevenroose) implement when BIP152 messages are merged
		//https://github.com/rust-bitcoin/rust-bitcoin/pull/249
	}
}

/// Handle receiving a version message from a peer.
/// Returns a message to send in response.
pub fn handle_version(
	react: &mut Reactions,
	p2p: &Arc<P2P>,
	peer: PeerId,
	state: &mut PeerState,
	ver: VersionMessage,
) {
	if state.handshake.version.is_some() {
		debug!("Peer {} sent duplicate version. Disconnecting...", peer);
		react.disconnect(peer);
		return;
	}

	info!("Peer {} ({}) sent us version: {:?}", peer, state.addr, ver);
	state.handshake.pver = std::cmp::min(ver.version, p2p.config.protocol_version);
	state.handshake.version = Some(ver);
	react.send(peer, NetworkMessage::Verack);

	if !state.handshake.version_sent {
		let start_height = p2p.block_height.load(atomic::Ordering::Relaxed);
		let version = make_version_msg(&p2p.config, peer, state, start_height);
		react.send(peer, NetworkMessage::Version(version));
		state.handshake.version_sent = true;
	}

	if state.handshake.finished() {
		request_features(react, &p2p.config, peer, state);
	}
}

/// Handle receiving a verack message from a peer.
/// Returns true if the handshake is done.
pub fn handle_verack(react: &mut Reactions, config: &Config, peer: PeerId, state: &mut PeerState) {
	if state.handshake.version_acked {
		debug!("Peer {} sent duplicate verack. Disconnecting...", peer);
		react.disconnect(peer);
		return;
	}

	if !state.handshake.version_sent {
		debug!("Peer {} sent verack before version was sent. Disconnecting...", peer);
		react.disconnect(peer);
		return;
	}

	state.handshake.version_acked = true;

	if state.handshake.finished() {
		request_features(react, config, peer, state);
	}

	// Schedule the first ping.
    if let Some(int) = config.ping_interval {
	    react.schedule_ping(peer, Instant::now() + int);
    }
}
