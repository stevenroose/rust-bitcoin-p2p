//! All business logic that bitcoin-p2p handles internally is
//! organized in this module and its submodules.

pub mod handshake;
pub mod inventory;
pub mod pingpong;

use std::mem;
use std::time::Instant;

use bitcoin::network::message::NetworkMessage;

use crate::PeerId;

/// Enum used for logic methods that can require multiple actions.
#[derive(Debug, PartialEq, Eq)]
enum Action {
	/// Send the given message.
	Send(PeerId, NetworkMessage),
	/// Disconnect for the given reason.
	Disconnect(PeerId),
	/// Schedule an event.
	Schedule(PeerId, Event, Instant),
    // TODO(stevenroose) 
    // sfa
    // sfad
    // saf
}

/// Multiple actions to be made.
//TODO(stevenroose) consider making into a LinkedList for more efficient draining
// this makes more sense when linked_list_entry lands and has a remove method
pub struct Reactions(Vec<Action>);

impl Reactions {
	/// Create an empty actions.
	pub fn new() -> Reactions {
		Reactions(Vec::new())
	}

	/// The number of action items.
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Add an action.
	fn add(&mut self, a: Action) {
		self.0.push(a);
	}

	/// Add a send message action.
	pub fn send(&mut self, peer: PeerId, msg: NetworkMessage) {
		self.add(Action::Send(peer, msg));
	}

	/// Add a disconnect action for the peer.
	pub fn disconnect(&mut self, peer: PeerId) {
		self.add(Action::Disconnect(peer));
	}

	/// Schedule a ping.
	pub fn schedule_ping(&mut self, peer: PeerId, time: Instant) {
		self.add(Action::Schedule(peer, Event::Ping, time));
	}

	/// Schedule a trickle.
	pub fn schedule_trickle(&mut self, peer: PeerId, time: Instant) {
		self.add(Action::Schedule(peer, Event::Trickle, time));
	}

	/// Insert all schedule actions into the scheduler.
	pub fn schedule_all(&mut self, scheduler: &mut Scheduler) {
		self.0.retain(|a| {
			if let Action::Schedule(peer, event, time) = a {
				scheduler.schedule(*peer, *event, *time);
				false
			} else {
				true
			}
		});
	}

	/// Get all the disconnect items.
	pub fn take_disconnects(&mut self) -> Vec<PeerId> {
		let mut ret = Vec::new();
		self.0.retain(|a| {
			if let Action::Disconnect(peer) = a {
				ret.push(*peer);
				false
			} else {
				true
			}
		});
		ret
	}

	/// Empty all actions, returning an iterator over them.
	pub fn drain_messages(&mut self) -> impl Iterator<Item = (PeerId, NetworkMessage)> {
		let actions = mem::replace(&mut self.0, Vec::new());
		actions.into_iter().filter_map(|a| match a {
			Action::Send(peer, msg) => Some((peer, msg)),
			_ => None,
		})
	}
}
