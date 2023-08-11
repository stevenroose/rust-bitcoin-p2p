//! Ping manager.
//!
//! Detects dead peer connections and responds to peer `ping` messages.
//!
//! *Implementation of BIP 0031.*
//!

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bitcoin::network::message::NetworkMessage;
use crossbeam_channel as chan;
use fastrand;
use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::{DisconnectReason, PeerId, Wire};
use crate::events::{EventSource, ListenResult};
use crate::peer;
use crate::time::TimeExt;

/// The version from which the pong message is supported.
const PONG_VERSION: u32 = 60000;

#[derive(Clone, Debug)]
pub struct Config {
	/// The interval at which to ping.
	///
	/// Default value: 2 minutes.
	pub ping_interval: Duration,

	/// The timeout within which to expect a pong.
	///
	/// Default value: 30 seconds.
	pub ping_timeout: Duration,

	/// The number of latencies to keep track of when calculating
	/// average peer latencies.
	///
	/// Default value: 64.
	pub max_recorded_latencies: usize,
}

impl Default for Config {
	fn default() -> Config {
		Config {
			ping_interval: Duration::from_secs(2 * 60),
			ping_timeout: Duration::from_secs(30),
			max_recorded_latencies: 64,
		}
	}
}

/// Abstraction for peers that the [PingManager] is responsible for.
//TODO(stevenroose) should we rename this to deduplicate this name
pub trait Peer: EventSource<peer::Event> + Send + Sync + 'static {
	/// Get the peer's ID.
	fn id(&self) -> PeerId;
	/// Send the peer a `ping` message.
	fn ping(&self, nonce: u64);
	/// Send the peer a `pong` message.
	fn pong(&self, nonce: u64);
	/// Disconnect the peer.
	fn disconnect(&self, reason: DisconnectReason);
}

impl Peer for peer::Peer {
	fn id(&self) -> PeerId { peer::Peer::id(self) }
	fn ping(&self, nonce: u64) { Wire::ping(self, nonce); }
	fn pong(&self, nonce: u64) { Wire::pong(self, nonce); }
	fn disconnect(&self, reason: DisconnectReason) {
		self.disconnect_with_reason(reason);
	}
}

#[derive(Debug, Clone, Default)]
pub struct PingStats {
	latencies: VecDeque<Duration>,
}

impl PingStats {
	/// Calculate the average latency of this peer.
	pub fn latency(&self) -> Duration {
		let sum = self.latencies.iter().sum::<Duration>();

		sum / self.latencies.len() as u32
	}

	fn record_latency(&mut self, sample: Duration, limit: usize) {
		self.latencies.push_front(sample);
		self.latencies.truncate(limit);
	}
}

/// Detects dead peer connections.
//TODO(stevenroose) do we want this manager to emit ping and pong events?
pub struct PingManager {
	ctrl_tx: chan::Sender<Ctrl>,
	process: erin::ProcessHandle,
	stats: Arc<RwLock<HashMap<PeerId, PingStats>>>,
}

impl PingManager {
	pub fn start_in(
		rt: &erin::Runtime,
		config: Config,
		rng: fastrand::Rng,
	) -> Result<PingManager, erin::Error> {
		let (ctrl_tx, ctrl_rx) = chan::unbounded();
		let stats = Arc::new(RwLock::new(HashMap::new()));
		
		let processor = Processor::new(config, ctrl_rx, ctrl_tx.clone(), stats.clone(), rng);
		let process = rt.add_process(Box::new(processor))?;

		Ok(PingManager { ctrl_tx, process, stats })
	}

	pub fn ping_stats(&self, peer: PeerId) -> Option<PingStats> {
		self.stats.read().get(&peer).cloned()
	}

	pub fn all_ping_stats(&self) -> HashMap<PeerId, PingStats> {
		self.stats.read().clone()
	}

	pub fn shutdown(&self) {
		let _ = self.process.shutdown();
	}
}

trait SendCtrl {
	fn send_ctrl(&self, ctrl: Ctrl);
}

impl SendCtrl for PingManager {
	fn send_ctrl(&self, ctrl: Ctrl) {
		//TODO(stevenroose) should we handle these errors?
		let _ = self.ctrl_tx.send(ctrl);
		//TODO(stevenroose) this is UnixStream::as_raw_fd erroring
		let _ = self.process.wake();
	}
}

impl SendCtrl for (chan::Sender<Ctrl>, erin::Waker) {
	fn send_ctrl(&self, ctrl: Ctrl) {
		//TODO(stevenroose) should we handle these errors?
		let _ = self.0.send(ctrl);
		//TODO(stevenroose) this is UnixStream::as_raw_fd erroring
		let _ = self.1.wake();
	}
}

/// A trait with internal control methods for the [PingManager].
///
/// Users will usually not have to use these directly.
pub trait PingManagerControl {
	fn add_peer(&self, peer: Arc<dyn Peer>, protocol_version: u32);

	fn peer_disconnected(&self, peer: PeerId);

	fn received_ping(&self, peer: PeerId, nonce: u64);

	fn received_pong(&self, peer: PeerId, nonce: u64);
}

impl<T: SendCtrl> PingManagerControl for T {
	fn add_peer(&self, peer: Arc<dyn Peer>, protocol_version: u32,
	) {
		self.send_ctrl(Ctrl::AddPeer { peer, pver: protocol_version });
	}

	fn peer_disconnected(&self, peer: PeerId) {
		self.send_ctrl(Ctrl::PeerDisconnected { peer });
	}

	fn received_ping(&self, peer: PeerId, nonce: u64) {
		self.send_ctrl(Ctrl::ReceivedPing { peer, nonce });
	}

	fn received_pong(&self, peer: PeerId, nonce: u64) {
		self.send_ctrl(Ctrl::ReceivedPong { peer, nonce });
	}
}

enum Ctrl {
	AddPeer {
		peer: Arc<dyn Peer>,
		pver: u32,
	},
	PeerDisconnected {
		peer: PeerId,
	},
	ReceivedPing {
		peer: PeerId,
		nonce: u64,
	},
	ReceivedPong {
		peer: PeerId,
		nonce: u64,
	},
}

#[derive(Debug)]
enum Status {
	AwaitingPong { nonce: u64, since: SystemTime },
	Idle { since: SystemTime },
}

struct State {
	peer: Arc<dyn Peer>,
	pver: u32,
	status: Status,
}

struct Processor {
	cfg: Config,
	ctrl_rx: chan::Receiver<Ctrl>,
	// We need to keep a sender too to give out when we register new peers.
	ctrl_tx: chan::Sender<Ctrl>,
	peers: HashMap<PeerId, State>,
	stats: Arc<RwLock<HashMap<PeerId, PingStats>>>,
	rng: fastrand::Rng,
	
	// erin stuff

	timers: HashMap<erin::TimerToken, PeerId>,
}

impl Processor {
	fn new(
		config: Config,
		ctrl_rx: chan::Receiver<Ctrl>,
		ctrl_tx: chan::Sender<Ctrl>,
		stats: Arc<RwLock<HashMap<PeerId, PingStats>>>,
		rng: fastrand::Rng,
	) -> Processor {
		Processor {
			cfg: config,
			ctrl_rx: ctrl_rx,
			ctrl_tx: ctrl_tx,
			peers: HashMap::new(),
			stats: stats,
			timers: HashMap::new(),
			rng: rng,
		}
	}

	fn add_peer(&mut self, peer: Arc<dyn Peer>, pver: u32) {
		let nonce = self.rng.u64(..);
		let now = SystemTime::now();

		// Send first ping.
		peer.ping(nonce);

		self.stats.write().insert(peer.id(), PingStats::default());
		self.peers.insert(
			peer.id(),
			State {
				peer: peer,
				pver: pver,
				status: Status::AwaitingPong { nonce, since: now },
			},
		);
	}

	fn register_peer_listener(&self, peer: &dyn Peer, ctrl: impl SendCtrl + Send + 'static) {
		let id = peer.id();
		peer.add_listener(Box::new(move |ev: &_| {
			match ev {
				// We ignore this because peers should be negotiated before they're added.
				peer::Event::Connected => {},
				peer::Event::Disconnected(_) => ctrl.peer_disconnected(id),
				peer::Event::Message(m) => match m {
					NetworkMessage::Ping(n) => ctrl.received_ping(id, *n),
					NetworkMessage::Pong(n) => ctrl.received_pong(id, *n),
					_ => {},
				}
			}
			ListenResult::Ok
		}));
	}

	fn peer_disconnected(&mut self, peer: PeerId) {
		self.peers.remove(&peer);
		self.stats.write().remove(&peer);
	}

	fn handle_ping(&mut self, peer: PeerId, nonce: u64) {
		if let Some(s) = self.peers.get(&peer) {
			trace!("Received ping ({}) from peer {}", nonce, peer);
			if s.pver >= PONG_VERSION {
				s.peer.pong(nonce);
			}
		}
	}

	/// Called when a `pong` is received.
	fn handle_pong(&mut self, peer: PeerId, nonce: u64) {
		if let Some(s) = self.peers.get_mut(&peer) {
			if s.pver < PONG_VERSION {
				debug!("Peer {} sent a pong message but reported pre-pong version", peer);
				let r = "peer sent pong message but reported pre-pong protocol version";
				s.peer.disconnect(DisconnectReason::PeerMisbehaving(r));
			}

			match s.status {
				Status::AwaitingPong { nonce: last_nonce, since } => {
					if nonce == last_nonce {
						s.status = Status::Idle { since: SystemTime::now() };
						if let Some(stats) = self.stats.write().get_mut(&peer) {
							let latency = since.saturating_elapsed();
							stats.record_latency(latency, self.cfg.max_recorded_latencies);
						}
					}
				}
				// Unsolicited or redundant `pong`. Ignore.
				Status::Idle { .. } => {}
			}
		}
	}

	fn handle_timer(
		&mut self,
		set_timer: &mut dyn FnMut(SystemTime),
		peer: PeerId,
	) {
		let s = match self.peers.get_mut(&peer) {
			Some(s) => s,
			None => return, // probably already disconnected
		};

		let now = SystemTime::now();
		match s.status {
			Status::AwaitingPong { since, .. } => {
				// TODO: By using nonces we should be able to overlap ping messages.
				// This would allow us to only disconnect a peer after N ping messages
				// are sent in a row with no reply.

				// A ping was sent and we're waiting for a `pong`. If too much
				// time has passed, we consider this peer dead, and disconnect
				// from them.
				if since.saturating_elapsed() >= self.cfg.ping_timeout {
					s.peer.disconnect(DisconnectReason::PeerTimeout("ping"));
				}
			}
			Status::Idle { since } => {
				// We aren't waiting for any `pong`. Check whether enough time has passed since we
				// received the last `pong`, and if so, send a new `ping`.
				if since.saturating_elapsed() >= self.cfg.ping_interval {
					let nonce = self.rng.u64(..);

					s.peer.ping(nonce);
					set_timer(now + self.cfg.ping_timeout);
					set_timer(now + self.cfg.ping_interval);

					s.status = Status::AwaitingPong { nonce, since: now };
				}
			}
		}
	}
}

impl erin::Process for Processor {
	fn wakeup(&mut self, rt: &erin::RuntimeHandle, ev: erin::Events) -> Result<(), erin::Exit> {
		if ev.waker() {
			loop {
				match self.ctrl_rx.try_recv() {
					Ok(Ctrl::AddPeer { peer, pver }) => {
						let ctrl = (self.ctrl_tx.clone(), rt.new_waker());
						self.register_peer_listener(&*peer, ctrl);
						self.add_peer(peer, pver);
					}
					Ok(Ctrl::PeerDisconnected { peer }) => self.peer_disconnected(peer),
					Ok(Ctrl::ReceivedPing { peer, nonce }) => self.handle_ping(peer, nonce),
					Ok(Ctrl::ReceivedPong { peer, nonce }) => self.handle_pong(peer, nonce),
					Err(chan::TryRecvError::Empty) => break,
					Err(chan::TryRecvError::Disconnected) => {
						info!("PingManager processor shutting down: ctrl channel closed");
						return Err(erin::Exit);
					}
				}
			}
		}

		for timer in ev.timers() {
			let peer = self.timers.remove(&timer).expect("erin reported unknown timer");

			// This is a little bit ugly to work around borrowck.
			let mut timers = SmallVec::<[erin::TimerToken; 2]>::new();
			self.handle_timer(&mut |t| timers.push(rt.set_alarm(t)), peer);
			for timer in timers {
				self.timers.insert(timer, peer);
			}
		}

		Ok(())
	}

	fn shutdown(&mut self) {
		// nothing to do, all will be dropped
	}
}
