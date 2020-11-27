use std::time::Instant;

use bitcoin::network::message::NetworkMessage;

use crate::constants;
use crate::logic::Reactions;
use crate::{Config, PeerId, PeerState};

/// Some statistics we keep on pinging for peers.
#[derive(Debug, Default)]
pub struct PingStats {
	last_nonce: Option<u64>,
	replied: bool,
}

/// Handle a scheduled trigger to send a ping to the peer.
pub fn scheduled_ping(react: &mut Reactions, config: &Config, peer: PeerId, state: &mut PeerState) {
	// First check if the peer replied to out last ping.
	if !state.ping_stats.replied && state.ping_stats.last_nonce.is_some() {
		debug!("Disconnecting peer {} because he failed to reply to ping", peer);
		react.disconnect(peer);
		return;
	}

	// Schedule next ping.
	react.schedule_ping(peer, Instant::now() + config.ping_interval);

	let nonce = rand::random();
	state.ping_stats.last_nonce = Some(nonce);
	state.ping_stats.replied = false;

	trace!("Sending ping with nonce {} to peer {}", nonce, peer);
	react.send(peer, NetworkMessage::Ping(nonce));
}

/// Handle a ping message received from the peer.
pub fn handle_ping(react: &mut Reactions, peer: PeerId, state: &mut PeerState, nonce: u64) {
	trace!("Received ping with nonce {} from peer {}", nonce, peer);

	if state.handshake.pver > constants::VERSION_PONG {
		trace!("Replying with pong to {}", peer);
		react.send(peer, NetworkMessage::Pong(nonce));
	}
}

/// Handle a pong message received from the peer.
pub fn handle_pong(react: &mut Reactions, peer: PeerId, state: &mut PeerState, nonce: u64) {
	trace!("Received pong with nonce {} from peer {}", nonce, peer);

	if state.handshake.pver < constants::VERSION_PONG {
		debug!("Peer {} sent a pong but reported non-pong PVER", peer);
		react.disconnect(peer);
		return;
	}

	match state.ping_stats.last_nonce {
		Some(last_nonce) if last_nonce == nonce => {
			state.ping_stats.replied = true;
		}
		Some(last_nonce) => {
			debug!(
				"Peer {} replied with wrong pong nonce ({} instead of {})",
				peer, nonce, last_nonce,
			);
			react.disconnect(peer);
		}
		None => {
			debug!("Peer {} sent an unrequested pong", peer);
			react.disconnect(peer);
		}
	}
}
