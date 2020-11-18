//! All business logic that bitcoin-p2p handles internally is
//! organized in this module and its submodules.

pub mod handshake;
pub mod inventory;
pub mod pingpong;

use std::{cmp, mem};
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bitcoin::network::message::NetworkMessage;

use crate::{PeerId, P2P};

/// The time to run ahead of schedule. This avoids accidentally
/// report wake-up calls in the past.
const RUN_AHEAD: Duration = Duration::from_millis(100);

/// An event that can be scheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Event {
	Ping,
	Trickle,
}

/// A scheduled event.
///
/// [Ord] is implemented such that older ones have higher priority.
#[derive(Debug, PartialEq, Eq)]
struct Slot {
	time: Instant,
	peer: PeerId,
	event: Event,
}

impl cmp::PartialOrd<Slot> for Slot {
	fn partial_cmp(&self, other: &Slot) -> Option<cmp::Ordering> {
		Some(cmp::Ord::cmp(self, other))
	}
}
impl cmp::Ord for Slot {
	fn cmp(&self, other: &Slot) -> cmp::Ordering {
		self.time.cmp(&other.time).reverse()
	}
}

#[derive(Debug)]
pub struct Scheduler(BinaryHeap<Slot>);

impl Scheduler {
	/// Create a new scheduler.
	pub fn new() -> Scheduler {
		Scheduler(BinaryHeap::new())
	}

	/// Schedule a new event.
	fn schedule(&mut self, peer: PeerId, event: Event, time: Instant) {
		trace!("Scheduling {:?} event for peer {} at {:?}", event, peer, time);
		self.0.push(Slot {
			time,
			peer,
			event,
		});
	}

	/// Get the time of the next scheduled event, if any.
	pub fn next_event(&self) -> Option<Instant> {
		self.0.peek().map(|s| s.time)
	}

	/// Handle all due events.
	pub fn handle_events_due(&mut self, p2p: &Arc<P2P>, react: &mut Reactions) {
		#[allow(unused)] //TODO(stevenroose) cargo complains about this
		let mut peers_lock = None;
		macro_rules! get_state {
			($peer:expr) => {{
				peers_lock = Some(p2p.peers.lock().unwrap());
				or!(peers_lock.as_mut().unwrap().get_mut($peer), continue)
			}};
		}

		loop {
			let cutoff = Instant::now() + RUN_AHEAD;
			if self.0.is_empty() || self.0.peek().unwrap().time > cutoff {
				break;
			}

			let Slot { time: _, peer, event } = self.0.pop().unwrap();
			match event {
				Event::Ping => {
					let state = get_state!(&peer);
					pingpong::scheduled_ping(react, &p2p.config, peer, state);
				}
				Event::Trickle => {
					let state = get_state!(&peer);
					inventory::scheduled_trickle(react, &p2p.config, peer, state);
				}
			}
		}
	}
}

/// Enum used for logic methods that can require multiple actions.
#[derive(Debug, PartialEq, Eq)]
enum Action {
	/// Send the given message.
	Send(PeerId, NetworkMessage),
	/// Disconnect for the given reason.
	Disconnect(PeerId),
	/// Schedule an event.
	Schedule(PeerId, Event, Instant),
}

/// Multiple actions to be made.
//TODO(stevenroose) consider making into a LinkedList for more efficient draining
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
