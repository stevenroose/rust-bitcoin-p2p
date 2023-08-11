//!
//! The peer-to-peer address manager.
//!

mod store;
pub use self::store::{AddressSource, KnownAddress, Source, Store};

use std::net;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::NetworkMessage;
use crossbeam_channel as chan;
use parking_lot::Mutex;

use crate::{DisconnectReason, PeerId, Link, Wire};
use crate::events::{Dispatch, EventSource, Listener, ListenResult};
use crate::time::TimeExt;
use crate::peer;

/// Communication domain of a network socket.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Domain {
	/// IPv4.
	IPV4,
	/// IPv6.
	IPV6,
}

impl Domain {
	/// All domains.
	pub fn all() -> Vec<Self> {
		vec![Self::IPV4, Self::IPV6]
	}

	/// Returns the domain for `address`.
	pub const fn for_address(address: net::SocketAddr) -> Domain {
		match address {
			net::SocketAddr::V4(_) => Domain::IPV4,
			net::SocketAddr::V6(_) => Domain::IPV6,
		}
	}
}

/// Address manager configuration.
#[derive(Clone, Debug)]
pub struct Config {
	/// Services required from peers.
	pub required_services: ServiceFlags,
	/// Any peer having any of these services will be ignored.
	pub ignore_services: ServiceFlags,
	/// Communication domains we're interested in.
	pub domains: Vec<Domain>,
	/// How long before a sampled address can be returned again.
	///
	/// Default value: 3 minutes.
	pub sample_timeout: Duration,
	/// Interval at which to check if we should request new addresses.
	///
	/// Default value: 1 minute.
	pub request_interval: Duration,
	/// Interval at which to idle.
	///
	/// When idling, we flush the address store.
	///
	/// Default value: 1 minute.
	pub idle_interval: Duration,
	/// Maximum number of addresses expected in a `addr` message.
	///
	/// Default value: 1000.
	pub max_addr_addresses: usize,
	/// Maximum number of addresses we store for a given address range.
	///
	/// Default value: 256.
	pub max_range_size: usize,

}

impl Default for Config {
	fn default() -> Self {
		Self {
			required_services: ServiceFlags::NONE,
			ignore_services: ServiceFlags::NONE,
			domains: Domain::all(),
			sample_timeout: Duration::from_secs(3 * 60),
			request_interval: Duration::from_secs(60),
			idle_interval: Duration::from_secs(60),
			max_addr_addresses: 1000,
			max_range_size: 256,
		}
	}
}

/// An event emitted by the address manager.
#[derive(Clone, Debug)]
pub enum Event {
	/// Peer addresses have been received.
	AddressesReceived {
		/// Number of addresses received.
		count: usize,
		/// Source of addresses received.
		source: Source,
	},
	/// Address book exhausted.
	AddressBookExhausted,
	/// An error was encountered.
	Error(String),
}

/// Abstraction for peers that the [AddressManager] uses as addr sources.
//TODO(stevenroose) should we rename this to deduplicate this name
pub trait Peer: EventSource<peer::Event> + Send {
	/// Get the peer's ID.
	fn id(&self) -> PeerId;
	/// Send the peer a `getaddr` message.
	fn send_getaddr(&self);
	/// Send the peer an `addr` message.
	fn send_addr(&self, addrs: Vec<(u32, Address)>);
	/// Disconnect the peer.
	fn disconnect(&self, reason: DisconnectReason);
}

impl Peer for peer::Peer {
	fn id(&self) -> PeerId { peer::Peer::id(self) }
	fn send_getaddr(&self) { Wire::get_addr(self) }
	fn send_addr(&self, addrs: Vec<(u32, Address)>) { Wire::addr(self, addrs) }
	fn disconnect(&self, reason: DisconnectReason) {
		self.disconnect_with_reason(reason);
	}
}

/// Iterator over addresses.
pub struct Iter<F>(F);

impl<F> Iterator for Iter<F>
where
	F: FnMut() -> Option<(Address, Source)>,
{
	type Item = (Address, Source);

	fn next(&mut self) -> Option<Self::Item> {
		(self.0)()
	}
}

/// Internal state shared by the handle and the processor.
struct State<Store> {
	store: Store,
	connected: HashSet<net::IpAddr>,
	bans: HashSet<net::IpAddr>,
	address_ranges: HashMap<u8, HashSet<net::IpAddr>>,
	// maybe can be outside of state
	local_addrs: HashSet<net::SocketAddr>,
	last_idle: Option<SystemTime>,
	rng: fastrand::Rng,
}

impl<S: Store> State<S> {
	fn new(store: S, rng: fastrand::Rng, max_range_size: usize) -> State<S> {
		let ips = store.iter().map(|(ip, _)| *ip).collect::<Vec<_>>();
		let mut s = State {
			store: store,
			rng: rng,
			connected: HashSet::new(),
			bans: HashSet::new(),
			address_ranges: HashMap::new(),
			local_addrs: HashSet::new(),
			last_idle: None,
		};
		for ip in ips.iter() {
			s.populate_address_ranges(max_range_size, *ip);
		}
		s
	}

	fn is_empty(&self) -> bool {
		self.store.is_empty() || self.address_ranges.is_empty()
	}

	fn is_exhausted(&self, timeout: Duration) -> bool {
		let time = match self.last_idle {
			Some(t) => t,
			None => return true, // never idled yet
		};

		for (addr, ka) in self.store.iter() {
			// Unsuccessful attempt to connect.
			if ka.last_attempt.is_some() && ka.last_success.is_none() {
				continue;
			}
			match ka.last_sampled {
				Some(s) if time.saturating_duration_since(s) < timeout => continue,
				None => continue,
				Some(_) => {},
			}
			if !self.connected.contains(addr) {
				return false;
			}
		}
		true
	}

	fn ban(&mut self, ip: net::IpAddr) -> bool {
		debug_assert!(!self.connected.contains(&ip));

		let key = addr_key(ip);
		let address_ranges = &mut self.address_ranges;
		let bans = &mut self.bans;
		let store = &mut self.store;
		if let Some(range) = address_ranges.get_mut(&key) {
			range.remove(&ip);

			// TODO: Persist bans.
			store.remove(&ip);
			bans.insert(ip);

			if range.is_empty() {
				address_ranges.remove(&key);
			}
			return true;
		}
		false
	}

	/// Pick an address at random from the set of known addresses.
	///
	/// This function tries to ensure a good geo-diversity of addresses, such that an adversary
	/// controlling a disproportionately large number of addresses in the same address range does
	/// not have an advantage over other peers.
	///
	/// This works under the assumption that adversaries are *localized*.
	fn sample(
		&mut self,
		domains: &[Domain],
		timeout: Duration,
		services: ServiceFlags,
	) -> Option<(Address, Source)> {
		self.sample_with(domains, timeout, |ka: &KnownAddress| {
			if ka.addr.services.has(services) {
				true
			} else {
				match ka.source {
					Source::Dns => {
						// If we've negotiated with this peer and it hasn't signaled the
						// required services, we know not to return it.
						// DNS-sourced addresses don't include service information,
						// so we won't be including these until we know the services.
						false
					}
					Source::Imported => {
						// We expect that imported addresses will always include the correct
						// service information. Hence, if this one doesn't have the necessary
						// services, it's safe to skip.
						false
					}
					Source::Peer(_) => {
						// Peer-sourced addresses come with service information. It's safe to
						// skip this address if it doesn't have the required services.
						false
					}
				}
			}
		})
	}

	/// Sample an address using the provided predicate. Only returns addresses which are `true`
	/// according to the predicate.
	fn sample_with(
		&mut self,
		domains: &[Domain],
		timeout: Duration,
		predicate: impl Fn(&KnownAddress) -> bool,
	) -> Option<(Address, Source)> {
		if self.is_empty() {
			return None;
		}
		let time = match self.last_idle {
			Some(t) => t,
			None => return None,
		};

		let mut ranges: Vec<_> = self.address_ranges.values().collect();
		self.rng.shuffle(&mut ranges);

		// First select a random address range.
		for range in ranges.drain(..) {
			assert!(!range.is_empty());

			let mut ips: Vec<_> = range.iter().collect();
			self.rng.shuffle(&mut ips);

			// Then select a random address in that range.
			for ip in ips.drain(..) {
				let ka = self.store.get_mut(ip).expect("address must exist");

				// If the address domain is unsupported, skip it.
				// Nb. this currently skips Tor addresses too.
				if !ka
					.addr
					.socket_addr()
					.map_or(false, |a| domains.contains(&Domain::for_address(a)))
				{
					continue;
				}

				// If the address was already attempted unsuccessfully, skip it.
				if ka.last_attempt.is_some() && ka.last_success.is_none() {
					continue;
				}
				// If we recently sampled this address, don't return it again.
				let last_sampled = ka.last_sampled.unwrap_or(UNIX_EPOCH);
				if time.saturating_duration_since(last_sampled) < timeout {
					continue;
				}
				// If we're already connected to this address, skip it.
				if self.connected.contains(ip) {
					continue;
				}
				// If the provided filter doesn't pass, keep looking.
				if !predicate(ka) {
					continue;
				}
				// Ok, we've found a worthy address!
				ka.last_sampled = Some(time);

				return Some((ka.addr.clone(), ka.source));
			}
		}

		None
	}

	/// Populate address ranges with an IP. This may remove an existing IP if
	/// its range is full. Returns the range key that was used.
	fn populate_address_ranges(&mut self, max_range_size: usize, ip: net::IpAddr) -> u8 {
		let key = addr_key(ip);
		let range = self.address_ranges.entry(key).or_insert_with({
			//TODO(stevenroose) use our rng in this hashset
			|| HashSet::new()
		});

		// If the address range is already full, remove a random address
		// before inserting this new one.
		if range.len() == max_range_size {
			let ix = self.rng.usize(..range.len());
			let addr = range
				.iter()
				.cloned()
				.nth(ix)
				.expect("the range is not empty");

			range.remove(&addr);
			self.store.remove(&addr);
		}
		range.insert(ip);

		key
	}
}

/// Manages peer network addresses.
pub struct AddressManager<Store> {
	cfg: Config,
	ctrl_tx: chan::Sender<Ctrl>,
	process: erin::ProcessHandle,
	state: Arc<Mutex<State<Store>>>,
}

impl<S: Store + Send + 'static> AddressManager<S> {
	pub fn start_in(
		rt: &erin::Runtime,
		config: Config,
		store: S,
		rng: fastrand::Rng,
	) -> Result<AddressManager<S>, erin::Error> {
		let (ctrl_tx, ctrl_rx) = chan::unbounded();
		let state = Arc::new(Mutex::new(State::new(store, rng, config.max_range_size)));
		
		let processor = Processor::new(config.clone(), ctrl_rx, ctrl_tx.clone(), state.clone());
		let process = rt.add_process(Box::new(processor))?;

		Ok(AddressManager {
			cfg: config,
			ctrl_tx: ctrl_tx,
			process,
			state,
		})
	}

	/// The configuration used for this [AddressManager].
	pub fn config(&self) -> &Config {
		&self.cfg
	}

	/// Return an iterator over randomly sampled addresses.
	pub fn iter(&self, services: ServiceFlags) -> impl Iterator<Item = (Address, Source)> + '_ {
		// Each time the `next` function is called on this iterator, a new mutex
		// lock is taken so that a user that keeps this iterator around doesn't
		// block our Processor from locking the mutex.
		Iter(move || {
			self.state.lock().sample(&self.cfg.domains, self.cfg.sample_timeout, services)
		})
	}

	/// Check whether we have unused addresses.
	pub fn is_exhausted(&self) -> bool {
		self.state.lock().is_exhausted(self.cfg.sample_timeout)
	}

	/// The number of peers known.
	pub fn len(&self) -> usize {
		self.state.lock().store.len()
	}

	/// Whether there are any peers known to the address manager.
	pub fn is_empty(&self) -> bool {
		self.state.lock().is_empty()
	}

	/// Remove an address from the address book and prevent it from being sampled again.
	pub fn ban(&self, addr: net::IpAddr) -> bool {
		self.state.lock().ban(addr)
	}

	/// Pick an address at random from the set of known addresses.
	///
	/// This function tries to ensure a good geo-diversity of addresses, such that an adversary
	/// controlling a disproportionately large number of addresses in the same address range does
	/// not have an advantage over other peers.
	///
	/// This works under the assumption that adversaries are *localized*.
	pub fn sample(&self, services: ServiceFlags) -> Option<(Address, Source)> {
		self.state.lock().sample(&self.cfg.domains, self.cfg.sample_timeout, services)
	}

	/// Sample an address using the provided predicate. Only returns addresses which are `true`
	/// according to the predicate.
	pub fn sample_with(
		&self,
		predicate: impl Fn(&KnownAddress) -> bool,
	) -> Option<(Address, Source)> {
		self.state.lock().sample_with(&self.cfg.domains, self.cfg.sample_timeout, predicate)
	}

	/// Clear the address manager of all peers.
	#[cfg(test)]
	pub fn clear(&mut self) {
		let mut state = self.state.lock();
		state.store.clear();
		state.address_ranges.clear();
	}

	pub fn listen(&self, listener: impl Listener<Event>) {
		self.add_listener(Box::new(listener));
	}
}

impl<S> EventSource<Event> for AddressManager<S> {
	fn add_listener(&self, listener: Box<dyn Listener<Event>>) {
		self.send_ctrl(Ctrl::AddListener(listener))
	}
}

trait SendCtrl {
	fn send_ctrl(&self, ctrl: Ctrl);
}

impl<S> SendCtrl for AddressManager<S> {
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

/// A trait with internal control methods for the [AddressManager].
///
/// Users will usually not have to use these directly.
pub trait AddressManagerControl {
	/// Get addresses from peers.
	fn get_addresses(&self);

	/// Called when a peer connection is attempted.
	fn peer_attempted(&self, addr: net::SocketAddr);

	/// Called when a peer has connected.
	fn peer_connected(&self, peer: impl Peer + 'static);

	/// Called when a peer has handshaked.
	fn peer_negotiated(&self, id: PeerId, services: ServiceFlags, link: Link);

	/// Called when a peer signaled activity.
	fn peer_active(&self, id: PeerId);

	/// Called when a peer disconnected.
	fn peer_disconnected(&self, id: PeerId, reason: DisconnectReason);

	/// Called when we received a `getaddr` message from a peer.
	fn received_getaddr(&self, peer: PeerId);

	/// Called when we received an `addr` message from a peer.
	fn received_addr(&self, peer: PeerId, addrs: Vec<(u32, Address)>);
}

impl<T: SendCtrl> AddressManagerControl for T {
	fn get_addresses(&self) {
		self.send_ctrl(Ctrl::RequestAddrs);
	}

	fn peer_attempted(&self, addr: net::SocketAddr) {
		self.send_ctrl(Ctrl::PeerAttempted(addr));
	}

	fn peer_connected(&self, peer: impl Peer + 'static) {
		let id = peer.id();
		if !is_routable(id.ip()) || is_local(id.ip()) {
			return;
		}

		self.send_ctrl(Ctrl::PeerConnected(Box::new(peer)));
	}

	fn peer_negotiated(&self, id: PeerId, services: ServiceFlags, link: Link) {
		self.send_ctrl(Ctrl::PeerNegotiated(id, services, link));
	}

	fn peer_active(&self, id: PeerId) {
		self.send_ctrl(Ctrl::PeerActive(id));
	}

	fn peer_disconnected(&self, id: PeerId, reason: DisconnectReason) {
		self.send_ctrl(Ctrl::PeerDisconnected(id, reason));
	}

	fn received_getaddr(&self, peer: PeerId) {
		self.send_ctrl(Ctrl::ReceivedGetAddr(peer));
	}

	fn received_addr(&self, peer: PeerId, addrs: Vec<(u32, Address)>) {
		self.send_ctrl(Ctrl::ReceivedAddr(peer, addrs));
	}
}

impl<S: Store + Send + 'static> AddressSource for Arc<AddressManager<S>> {
    fn sample(&mut self, services: ServiceFlags) -> Option<(Address, Source)> {
        AddressManager::sample(&**self, services)
    }

    fn record_local_address(&mut self, addr: net::SocketAddr) {
        self.state.lock().local_addrs.insert(addr);
    }

    fn iter(&mut self, services: ServiceFlags) -> Box<dyn Iterator<Item = (Address, Source)> + '_> {
        Box::new(AddressManager::iter(&**self, services))
    }
}

enum Ctrl {
	AddListener(Box<dyn Listener<Event>>),
	RequestAddrs,
	PeerAttempted(net::SocketAddr),
	PeerConnected(Box<dyn Peer>),
	PeerNegotiated(PeerId, ServiceFlags, Link),
	PeerActive(PeerId),
	PeerDisconnected(PeerId, DisconnectReason),
	ReceivedGetAddr(PeerId),
	ReceivedAddr(PeerId, Vec<(u32, Address)>),
}

struct ConnectedPeer {
	handle: Box<dyn Peer>,
	is_source: bool,
}

impl std::ops::Deref for ConnectedPeer {
	type Target = Box<dyn Peer>;
	fn deref(&self) -> &Self::Target { &self.handle }
}

struct Processor<Store> {
	cfg: Config,

	ctrl_rx: chan::Receiver<Ctrl>,
	ctrl_tx: chan::Sender<Ctrl>,
	dispatch: Dispatch<Event>,
	request_timer: erin::TimerToken,
	idle_timer: erin::TimerToken,

	peers: HashMap<PeerId, ConnectedPeer>,
	state: Arc<Mutex<State<Store>>>,
}

impl<S: Store> Processor<S> {
	fn new(
		config: Config,
		ctrl_rx: chan::Receiver<Ctrl>,
		ctrl_tx: chan::Sender<Ctrl>,
		state: Arc<Mutex<State<S>>>,
	) -> Processor<S> {
		Processor {
			cfg: config,
			ctrl_rx: ctrl_rx,
			ctrl_tx: ctrl_tx,
			dispatch: Dispatch::new(),
			request_timer: erin::TimerToken::NULL,
			idle_timer: erin::TimerToken::NULL,
			peers: HashMap::new(),
			state: state,
		}
	}

	fn idle(&mut self) {
		let mut state = self.state.lock();
		// If it's been a while, save addresses to store.
		if let Err(err) = state.store.flush() {
			self.dispatch.dispatch(&Event::Error(format!("flush to disk failed: {}", err)));
		}
		state.last_idle = Some(SystemTime::now());
	}

	fn request(&mut self) {
		// If we're already using all the addresses we have available, we should fetch more.
		if self.state.lock().is_exhausted(self.cfg.sample_timeout) {
			self.dispatch.dispatch(&Event::AddressBookExhausted);
			self.get_addresses();
		}
	}

	fn get_addresses(&mut self) {
		for peer in self.peers.values() {
			if peer.is_source {
				peer.send_getaddr();
			}
		}
	}

	fn peer_attempted(&mut self, addr: net::SocketAddr) {
		// We're only interested in connection attempts for addresses we keep track of.
		if let Some(ka) = self.state.lock().store.get_mut(&addr.ip()) {
			ka.last_attempt = Some(SystemTime::now());
		}
	}

	fn peer_connected(&mut self, peer: Box<dyn Peer>) {
		let id = peer.id();
		self.peers.insert(id, ConnectedPeer {
			handle: peer,
			is_source: false,
		});
		self.state.lock().connected.insert(id.ip());
	}

	fn register_peer_listener(&self, peer: &dyn Peer, ctrl: impl SendCtrl + Send + 'static) {
		let id = peer.id();
		peer.add_listener(Box::new(move |ev: &_| {
			match ev {
				// We ignore this because we get peers only once they're connected.
				peer::Event::Connected => {},
				peer::Event::Disconnected(r) => ctrl.peer_disconnected(id, r.clone()),
				peer::Event::Message(m) => match m {
					NetworkMessage::GetAddr => ctrl.received_getaddr(id),
					NetworkMessage::Ping(_) => ctrl.peer_active(id),
					NetworkMessage::Pong(_) => ctrl.peer_active(id),
					_ => {},
				}
			}
			ListenResult::Ok
		}));
	}

	fn peer_disconnected(&mut self, id: PeerId, reason: DisconnectReason) {
		if self.peers.remove(&id).is_some() {
			let mut state = self.state.lock();
			state.connected.remove(&id.ip());

			// If the reason for disconnecting the peer suggests that we shouldn't try to
			// connect to this peer again, then remove the peer from the address book.
			// Otherwise, we leave it in the address buckets so that it can be chosen
			// in the future.
			if !reason.is_transient() {
				state.ban(id.ip());
			}
		}
	}

	fn peer_negotiated(&mut self, id: PeerId, services: ServiceFlags, link: Link) {
		if let Some(peer) = self.peers.get_mut(&id) {
			if link.is_outbound() {
				peer.is_source = true;
			}

			// We're only interested in peers we already know, eg. from DNS or peer
			// exchange. Peers should only be added to our address book if they are DNS seeds
			// or are discovered via a DNS seed.
			if let Some(ka) = self.state.lock().store.get_mut(&id.ip()) {
				// Only ask for addresses when connecting for the first time.
				if ka.last_success.is_none() {
					peer.send_getaddr();
				}
				// Keep track of when the last successful handshake was.
				let now = SystemTime::now();
				ka.last_success = Some(now);
				ka.last_active = Some(now);
				ka.addr.services = services;
			}
		}
	}

	fn peer_active(&mut self, id: PeerId) {
		if let Some(ka) = self.state.lock().store.get_mut(&id.ip()) {
			ka.last_active = Some(SystemTime::now());
		}
	}

	fn received_getaddr(&mut self, id: PeerId) {
		if let Some(peer) = self.peers.get(&id) {
			// TODO: We should only respond with peers who were last active within
			// the last 3 hours.
			let mut addrs = Vec::new();

			// Include one random address per address range.
			let state = self.state.lock();
			for range in state.address_ranges.values() {
				let ix = state.rng.usize(..range.len());
				let ip = range.iter().nth(ix).expect("index must be present");
				let ka = state.store.get(ip).expect("address must exist");

				addrs.push((
					ka.last_active.map(|t| t.block_time()).unwrap_or_default(),
					ka.addr.clone(),
				));
			}
			peer.send_addr(addrs);
		}
	}

	fn received_addr(&mut self, peer: PeerId, addrs: Vec<(u32, Address)>) {
		if addrs.is_empty() || addrs.len() > self.cfg.max_addr_addresses {
			// Peer misbehaving, got empty message or too many addresses.
			return;
		}

		let source = Source::Peer(peer);
		self.dispatch.dispatch(&Event::AddressesReceived {
			count: addrs.len(),
			source,
		});
		self.insert(addrs.into_iter(), source);
	}

	//TODO(stevenroose)	do we want to expose this?
	fn insert(&mut self, addrs: impl IntoIterator<Item = (u32, Address)>, source: Source) {
		let mut state = self.state.lock();

		let time = state.last_idle.expect("we idle on Processor init");

		for (last_active, addr) in addrs {
			// Ignore addresses that don't have the required services.
			if !addr.services.has(self.cfg.required_services) {
				continue;
			}
			// Ignore peers with these antiquated services offered.
			if addr.services.clone().remove(self.cfg.ignore_services) == ServiceFlags::NONE {
				continue;
			}
			// Ignore addresses that don't have a "last active" time.
			if last_active == 0 {
				continue;
			}
			// Ignore addresses that are too far into the future.
			if SystemTime::from_block_time(last_active) > time + Duration::from_secs(60 * 60) {
				continue;
			}
			// Ignore addresses from unsupported domains.
			let net_addr = match addr.socket_addr() {
				Ok(a) if self.cfg.domains.contains(&Domain::for_address(a)) => a,
				_ => continue,
			};
			let ip = net_addr.ip();

			// Ensure no self-connections.
			if state.local_addrs.contains(&net_addr) {
				continue;
			}
			// No banned addresses.
			if state.bans.contains(&ip) {
				continue;
			}

			// Ignore non-routable addresses if they come from a peer.
			if !is_routable(ip) {
				continue;
			}

			// Ignore local addresses.
			if is_local(ip) {
				continue;
			}

			let last_active = if last_active == 0 {
				// TODO: Cannot be 'None' due to above condition.
				None
			} else {
				Some(SystemTime::from_block_time(last_active))
			};

			// Record the address, and ignore addresses we already know.
			// Note that this should never overwrite an existing address.
			if !state.store.insert(ip, KnownAddress::new(addr.clone(), source, last_active)) {
				continue;
			}

			state.populate_address_ranges(self.cfg.max_range_size, net_addr.ip());
		}
	}
}

impl<S: Store + Send + 'static> erin::Process for Processor<S> {
	fn setup(&mut self, rt: &erin::RuntimeHandle) -> Result<(), erin::Exit> {
		self.idle(); // this sets last_idle to Some
		self.idle_timer = rt.set_timer(self.cfg.idle_interval);
		self.request_timer = rt.set_timer(self.cfg.request_interval);
		Ok(())
	}

	fn wakeup(&mut self, rt: &erin::RuntimeHandle, ev: erin::Events) -> Result<(), erin::Exit> {
		if ev.waker() {
			loop {
				match self.ctrl_rx.try_recv() {
					Ok(Ctrl::AddListener(l)) => self.dispatch.add_listener(l),
					Ok(Ctrl::RequestAddrs) => self.get_addresses(),
					Ok(Ctrl::PeerAttempted(addr)) => self.peer_attempted(addr),
					Ok(Ctrl::PeerConnected(peer)) => {
						let ctrl = (self.ctrl_tx.clone(), rt.new_waker());
						self.register_peer_listener(&*peer, ctrl);
						self.peer_connected(peer);
					}
					Ok(Ctrl::PeerNegotiated(id, fl, li)) => self.peer_negotiated(id, fl, li),
					Ok(Ctrl::PeerActive(id)) => self.peer_active(id),
					Ok(Ctrl::PeerDisconnected(id, reason)) => self.peer_disconnected(id, reason),
					Ok(Ctrl::ReceivedGetAddr(peer)) => self.received_getaddr(peer),
					Ok(Ctrl::ReceivedAddr(peer, addrs)) => self.received_addr(peer, addrs),
					Err(chan::TryRecvError::Empty) => break,
					Err(chan::TryRecvError::Disconnected) => {
						info!("AddressManager processor shutting down: ctrl channel closed");
						return Err(erin::Exit);
					}
				}
			}
		}

		for ev in ev.timers() {
			if ev == self.idle_timer {
				self.idle();
				self.idle_timer = rt.set_timer(self.cfg.idle_interval);
			} else if ev == self.request_timer {
				self.request();
				self.request_timer = rt.set_timer(self.cfg.request_interval);
			} else {
				panic!("Unknown erin timer token: {:?}", ev);
			}
		}

		Ok(())
	}

	fn shutdown(&mut self) {
		// nothing to do, all will be dropped
	}
}

//	////////////////////////////////////////////////////////////////////////////


/// Check whether an IP address is globally routable.
pub fn is_routable(addr: net::IpAddr) -> bool {
	match addr {
		net::IpAddr::V4(addr) => ipv4_is_routable(addr),
		net::IpAddr::V6(addr) => ipv6_is_routable(addr),
	}
}

/// Check whether an IP address is locally routable.
pub fn is_local(addr: net::IpAddr) -> bool {
	match addr {
		net::IpAddr::V4(addr) => {
			addr.is_private() || addr.is_loopback() || addr.is_link_local() || addr.is_unspecified()
		}
		net::IpAddr::V6(_) => false,
	}
}

/// Get the 8-bit key of an IP address. This key is based on the IP address's
/// range, and is used as a key to group IP addresses by range.
fn addr_key(ip: net::IpAddr) -> u8 {
	match ip {
		net::IpAddr::V4(ip) => {
			// Use the /16 range (first two components) of the IP address to key into the
			// range buckets.
			//
			// Eg. 124.99.123.1 and 124.54.123.1 would be placed in
			// different buckets, but 100.99.43.12 and 100.99.12.8
			// would be placed in the same bucket.
			let octets: [u8; 4] = ip.octets();
			let bits: u16 = (octets[0] as u16) << 8 | octets[1] as u16;

			(bits % u8::MAX as u16) as u8
		}
		net::IpAddr::V6(ip) => {
			// Use the first 32 bits of an IPv6 address to as a key.
			let segments: [u16; 8] = ip.segments();
			let bits: u32 = (segments[0] as u32) << 16 | segments[1] as u32;

			(bits % u8::MAX as u32) as u8
		}
	}
}

/// Check whether an IPv4 address is globally routable.
///
/// This code is adapted from the Rust standard library's `net::Ipv4Addr::is_global`. It can be
/// replaced once that function is stabilized.
fn ipv4_is_routable(addr: net::Ipv4Addr) -> bool {
	// Check if this address is 192.0.0.9 or 192.0.0.10. These addresses are the only two
	// globally routable addresses in the 192.0.0.0/24 range.
	if u32::from(addr) == 0xc0000009 || u32::from(addr) == 0xc000000a {
		return true;
	}
	!addr.is_private()
		&& !addr.is_loopback()
		&& !addr.is_link_local()
		&& !addr.is_broadcast()
		&& !addr.is_documentation()
		// Make sure the address is not in 0.0.0.0/8.
		&& addr.octets()[0] != 0
}

/// Check whether an IPv6 address is globally routable.
///
/// For now, this always returns `true`, as IPv6 addresses
/// are not fully supported.
fn ipv6_is_routable(_addr: net::Ipv6Addr) -> bool {
	true
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::fsm;
	use std::collections::HashMap;
	use std::iter;

	use nakamoto_common::block::time::RefClock;
	use nakamoto_common::network::Network;
	use quickcheck::TestResult;
	use quickcheck_macros::quickcheck;

	#[test]
	fn test_sample_empty() {
		let mut addrmgr = AddressManager::new(
			Config::default(),
			fastrand::Rng::new(),
			HashMap::new(),
			(),
			LocalTime::now(),
		);

		assert!(addrmgr.sample(ServiceFlags::NONE).is_none());
	}

	#[test]
	fn test_known_addresses() {
		let time = LocalTime::now();
		let mut addrmgr = AddressManager::new(
			Config::default(),
			fastrand::Rng::new(),
			HashMap::new(),
			(),
			time,
		);
		let source = Source::Dns;
		let services = ServiceFlags::NETWORK;

		addrmgr.initialize();
		addrmgr.insert(
			[
				(
					time.block_time(),
					Address::new(&([33, 33, 33, 33], 8333).into(), services),
				),
				(
					time.block_time(),
					Address::new(&([44, 44, 44, 44], 8333).into(), services),
				),
			],
			source,
		);

		let addr: &net::SocketAddr = &([33, 33, 33, 33], 8333).into();

		let ka = addrmgr.peers.get(&addr.ip()).unwrap();
		assert!(ka.last_success.is_none());
		assert!(ka.last_attempt.is_none());
		assert!(ka.last_active.is_some());
		assert!(ka.last_sampled.is_none());
		assert_eq!(
			ka.last_active,
			Some(LocalTime::from_block_time(time.block_time()))
		);

		addrmgr.peer_attempted(addr);

		let ka = addrmgr.peers.get(&addr.ip()).unwrap();
		assert!(ka.last_success.is_none());
		assert!(ka.last_attempt.is_some());
		assert!(ka.last_active.is_some());
		assert!(ka.last_sampled.is_none());

		// When a peer is connected, it is not yet considered a "success".
		addrmgr.peer_connected(addr);

		let ka = addrmgr.peers.get(&addr.ip()).unwrap();
		assert!(ka.last_success.is_none());
		assert!(ka.last_attempt.is_some());
		assert!(ka.last_active.is_some());
		assert!(ka.last_sampled.is_none());

		// Only when it is negotiated is it a "success".
		addrmgr.peer_negotiated(addr, services, Link::Outbound);

		let ka = addrmgr.peers.get(&addr.ip()).unwrap();
		assert!(ka.last_success.is_some());
		assert!(ka.last_attempt.is_some());
		assert!(ka.last_active.is_some());
		assert!(ka.last_sampled.is_none());
		assert_eq!(ka.last_active, ka.last_success);

		let (sampled, _) = addrmgr.sample(services).unwrap();
		assert_eq!(
			sampled.socket_addr().ok(),
			Some(([44, 44, 44, 44], 8333).into())
		);
		let ka = addrmgr.peers.get(&[44, 44, 44, 44].into()).unwrap();
		assert!(ka.last_success.is_none());
		assert!(ka.last_attempt.is_none());
		assert!(ka.last_active.is_some());
		assert!(ka.last_sampled.is_some());
	}

	#[test]
	fn test_is_exhausted() {
		let time = LocalTime::now();
		let mut addrmgr = AddressManager::new(
			Config::default(),
			fastrand::Rng::new(),
			HashMap::new(),
			(),
			time,
		);
		let source = Source::Dns;
		let services = ServiceFlags::NETWORK;

		addrmgr.initialize();
		addrmgr.insert(
			[(
				time.block_time(),
				Address::new(&net::SocketAddr::from(([33, 33, 33, 33], 8333)), services),
			)],
			source,
		);
		assert!(!addrmgr.is_exhausted());

		// Once a peer connects, it's no longer available as an address.
		addrmgr.peer_connected(&([33, 33, 33, 33], 8333).into());
		assert!(addrmgr.is_exhausted());

		addrmgr.insert(
			[(
				time.block_time(),
				Address::new(&net::SocketAddr::from(([44, 44, 44, 44], 8333)), services),
			)],
			source,
		);
		assert!(!addrmgr.is_exhausted());

		// If a peer has been attempted with no success, it should not count towards the available
		// addresses.
		addrmgr.peer_attempted(&([44, 44, 44, 44], 8333).into());
		assert!(addrmgr.is_exhausted());

		// If a peer has been connected to successfully, and then disconnected for a transient
		// reason, its address should be once again available.
		addrmgr.peer_connected(&([44, 44, 44, 44], 8333).into());
		addrmgr.peer_negotiated(&([44, 44, 44, 44], 8333).into(), services, Link::Outbound);
		addrmgr.peer_disconnected(
			&([44, 44, 44, 44], 8333).into(),
			fsm::DisconnectReason::PeerTimeout("timeout").into(),
		);
		assert!(!addrmgr.is_exhausted());
		assert!(addrmgr.sample(services).is_some());
		assert!(addrmgr.sample(services).is_none());
		assert!(addrmgr.is_exhausted());

		// If a peer has been attempted and then disconnected, it is not available
		// for sampling, even when the disconnect reason is transient.
		addrmgr.insert(
			[(
				time.block_time(),
				Address::new(&net::SocketAddr::from(([55, 55, 55, 55], 8333)), services),
			)],
			source,
		);
		addrmgr.sample(services);
		addrmgr.peer_attempted(&([55, 55, 55, 55], 8333).into());
		addrmgr.peer_connected(&([55, 55, 55, 55], 8333).into());
		addrmgr.peer_disconnected(
			&([55, 55, 55, 55], 8333).into(),
			fsm::DisconnectReason::PeerTimeout("timeout").into(),
		);
		assert!(addrmgr.sample(services).is_none());
	}

	#[test]
	fn test_disconnect_rediscover() {
		// Check that if we re-discover an address after permanent disconnection, we still know
		// not to connect to it.
		let time = LocalTime::now();
		let mut addrmgr = AddressManager::new(
			Config::default(),
			fastrand::Rng::new(),
			HashMap::new(),
			(),
			time,
		);
		let source = Source::Dns;
		let services = ServiceFlags::NETWORK;
		let addr: &net::SocketAddr = &([33, 33, 33, 33], 8333).into();

		addrmgr.initialize();
		addrmgr.insert([(time.block_time(), Address::new(addr, services))], source);

		let (sampled, _) = addrmgr.sample(services).unwrap();
		assert_eq!(sampled.socket_addr().ok(), Some(*addr));
		assert!(addrmgr.sample(services).is_none());

		addrmgr.peer_attempted(addr);
		addrmgr.peer_connected(addr);
		addrmgr.peer_negotiated(addr, services, Link::Outbound);
		addrmgr.peer_disconnected(
			addr,
			fsm::DisconnectReason::PeerMisbehaving("misbehaving").into(),
		);

		// Peer is now disconnected for non-transient reasons.
		// Receive from a new peer the same address we just disconnected from.
		addrmgr.received_addr(
			([99, 99, 99, 99], 8333).into(),
			vec![(time.block_time(), Address::new(addr, services))],
		);
		// It should not be returned from `sample`.
		assert!(addrmgr.sample(services).is_none());
	}

	#[quickcheck]
	fn prop_sample_no_duplicates(size: usize, seed: u64) -> TestResult {
		let clock = LocalTime::now();

		if size > 24 {
			return TestResult::discard();
		}

		let mut addrmgr = {
			let upstream = crate::fsm::output::Outbox::new(Network::Mainnet, 0);

			AddressManager::new(
				Config::default(),
				fastrand::Rng::with_seed(seed),
				HashMap::new(),
				upstream,
				clock,
			)
		};
		let time = LocalTime::now();
		let services = ServiceFlags::NETWORK;
		let mut addrs = vec![];

		for i in 0..size {
			addrs.push([96 + i as u8, 96 + i as u8, 96, 96]);
		}

		addrmgr.initialize();
		addrmgr.insert(
			addrs.iter().map(|a| {
				(
					time.block_time(),
					Address::new(&net::SocketAddr::from((*a, 8333)), services),
				)
			}),
			Source::Dns,
		);

		let mut sampled = HashSet::with_hasher(fastrand::Rng::with_seed(seed).into());
		for _ in 0..addrs.len() {
			let (addr, _) = addrmgr
				.sample(services)
				.expect("an address should be returned");
			sampled.insert(addr.socket_addr().unwrap().ip());
		}

		assert_eq!(
			sampled,
			addrs.iter().map(|a| (*a).into()).collect::<HashSet<_>>()
		);
		TestResult::passed()
	}

	#[test]
	fn test_max_range_size() {
		let services = ServiceFlags::NONE;
		let time = LocalTime::now();

		let mut addrmgr = AddressManager::new(
			Config::default(),
			fastrand::Rng::new(),
			HashMap::new(),
			(),
			time,
		);
		addrmgr.initialize();

		for i in 0..MAX_RANGE_SIZE + 1 {
			addrmgr.insert(
				iter::once((
					time.block_time(),
					Address::new(
						&([111, 111, (i / u8::MAX as usize) as u8, i as u8], 8333).into(),
						services,
					),
				)),
				Source::Dns,
			);
		}
		assert_eq!(
			addrmgr.len(),
			MAX_RANGE_SIZE,
			"we can't insert more than a certain amount of addresses in the same range"
		);

		addrmgr.insert(
			iter::once((
				time.block_time(),
				Address::new(&([129, 44, 12, 2], 8333).into(), services),
			)),
			Source::Dns,
		);
		assert_eq!(
			addrmgr.len(),
			MAX_RANGE_SIZE + 1,
			"inserting in another range is perfectly fine"
		);
	}

	#[test]
	fn test_addr_key() {
		assert_eq!(
			addr_key(&net::IpAddr::V4(net::Ipv4Addr::new(255, 0, 3, 4))),
			0
		);
		assert_eq!(
			addr_key(&net::IpAddr::V4(net::Ipv4Addr::new(255, 1, 3, 4))),
			1
		);
		assert_eq!(
			addr_key(&net::IpAddr::V4(net::Ipv4Addr::new(1, 255, 3, 4))),
			1
		);
	}

	#[test]
	fn test_insert() {
		use std::collections::HashMap;

		use nakamoto_common::bitcoin::network::address::Address;
		use nakamoto_common::bitcoin::network::constants::ServiceFlags;
		use nakamoto_common::block::time::LocalTime;
		use nakamoto_common::p2p::peer::Source;

		let cfg = Config::default();
		let time = LocalTime::now();
		let mut addrmgr = AddressManager::new(cfg, fastrand::Rng::new(), HashMap::new(), (), time);

		addrmgr.initialize();
		addrmgr.insert(
			vec![
				Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
				Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
				Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
			]
			.into_iter()
			.map(|a| (time.block_time(), a)),
			Source::Dns,
		);

		assert_eq!(addrmgr.len(), 3);

		addrmgr.insert(
			std::iter::once((
				time.block_time(),
				Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
			)),
			Source::Dns,
		);

		assert_eq!(addrmgr.len(), 3, "already known addresses are ignored");

		addrmgr.clear();
		addrmgr.insert(
			vec![Address::new(
				&([255, 255, 255, 255], 8333).into(),
				ServiceFlags::NONE,
			)]
			.into_iter()
			.map(|a| (time.block_time(), a)),
			Source::Dns,
		);

		assert!(
			addrmgr.is_empty(),
			"non-routable/non-local addresses are ignored"
		);
	}

	#[test]
	fn test_sample() {
		use std::collections::HashMap;

		use nakamoto_common::bitcoin::network::address::Address;
		use nakamoto_common::bitcoin::network::constants::ServiceFlags;
		use nakamoto_common::block::time::{Duration, LocalTime};
		use nakamoto_common::p2p::peer::Source;

		let cfg = Config::default();
		let clock = RefClock::from(LocalTime::now());
		let mut addrmgr =
			AddressManager::new(cfg, fastrand::Rng::new(), HashMap::new(), (), clock.clone());

		addrmgr.initialize();

		// Addresses controlled by an adversary.
		let adversary_addrs = vec![
			Address::new(&([111, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
			Address::new(&([111, 8, 43, 11], 8333).into(), ServiceFlags::NONE),
			Address::new(&([111, 8, 89, 6], 8333).into(), ServiceFlags::NONE),
			Address::new(&([111, 8, 124, 41], 8333).into(), ServiceFlags::NONE),
			Address::new(&([111, 8, 65, 4], 8333).into(), ServiceFlags::NONE),
			Address::new(&([111, 8, 161, 73], 8333).into(), ServiceFlags::NONE),
		];
		addrmgr.insert(
			adversary_addrs
				.iter()
				.cloned()
				.map(|a| (clock.block_time(), a)),
			Source::Dns,
		);

		// Safe addresses, controlled by non-adversarial peers.
		let safe_addrs = vec![
			Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
			Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
			Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
			Address::new(&([99, 129, 2, 15], 8333).into(), ServiceFlags::NONE),
		];
		addrmgr.insert(
			safe_addrs.iter().cloned().map(|a| (clock.block_time(), a)),
			Source::Dns,
		);

		// Keep track of how many times we pick a safe vs. an adversary-controlled address.
		let mut adversary = 0;
		let mut safe = 0;

		for _ in 0..99 {
			let (addr, _) = addrmgr.sample(ServiceFlags::NONE).unwrap();

			// Make sure we can re-sample the same addresses for the purpose of this example.
			clock.elapse(Duration::from_mins(60));
			addrmgr.received_wake();

			if adversary_addrs.contains(&addr) {
				adversary += 1;
			} else if safe_addrs.contains(&addr) {
				safe += 1;
			}
		}

		// Despite there being more adversary-controlled addresses, our safe addresses
		// are picked much more often.
		assert!(
			safe > adversary * 2,
			"safe addresses are picked twice more often"
		);
	}
}
