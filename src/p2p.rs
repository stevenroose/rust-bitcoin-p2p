
use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Error, PeerId};
use crate::events::{Dispatch, EventSource, Listener, ListenResult};

use crate::peer;
use crate::addrmgr::{self, AddressManager};
use crate::peermgr::{self, PeerManager};
use crate::pingmgr::{self, PingManager, PingManagerControl};

pub struct Config {
	address_mgr: Option<addrmgr::Config>,
	peer_mgr: Option<peermgr::Config>,
	ping_mgr: Option<pingmgr::Config>,
}

#[derive(Clone, Debug)]
pub enum Event {
	Peer(PeerId, peer::Event),
	AddrMgr(addrmgr::Event),
	PeerMgr(peermgr::Event),
}

//TODO(stevenroose) should we box the store?
/// A top-level construction for creating a complete p2p setup.
pub struct P2P<Store> {
	addr_mgr: Option<Arc<AddressManager<Store>>>,
	peer_mgr: Option<Arc<PeerManager>>,
	ping_mgr: Option<Arc<PingManager>>,

	dispatch: Arc<Mutex<Dispatch<Event>>>,
}

impl<S> P2P<S>
where
	S: addrmgr::Store + Send + 'static,
{
	pub fn setup_erin(config: Config, store: S) -> Result<P2P<S>, Error> {
		let rt = Arc::new(erin::Runtime::start()?);
		let rng = fastrand::Rng::new();

		let dispatch = Arc::new(Mutex::new(Dispatch::new()));

		let addr_mgr = if let Some(cfg) = config.address_mgr {
			let mgr = AddressManager::start_in(
				&rt, cfg.clone(), store, rng.clone(),
			)?;

			let dp = dispatch.clone();
			mgr.listen(move |e: &addrmgr::Event| {
				dp.lock().dispatch(&Event::AddrMgr(e.clone()));
				ListenResult::Ok
			});

			Some(Arc::new(mgr))
		} else {
			None
		};

		let ping_mgr = if let Some(cfg) = config.ping_mgr {
			let mgr = PingManager::start_in(&rt, cfg.clone(), rng.clone())?;
			Some(Arc::new(mgr))
		} else {
			None
		};

		let peer_mgr = if let Some(cfg) = config.peer_mgr {
			let mgr = PeerManager::start_in(
				&rt, cfg.clone(), addr_mgr.clone(), rng.clone(),
			)?;

			let dp = dispatch.clone();
			let pm = ping_mgr.clone();
			mgr.listen(move |e: &peermgr::Event| {
				match e {
					peermgr::Event::Connected {id, ref handle, .. } => {
						let id = *id;
						let dp = dp.clone();
						handle.listen(move |e: &peer::Event| {
							dp.lock().dispatch(&Event::Peer(id, e.clone()));
							ListenResult::Ok
						});
					}
					peermgr::Event::Negotiated { version, ref handle, .. } => {
						if let Some(ref ping_mgr) = pm {
							ping_mgr.add_peer(handle.clone(), *version);
						}
					}
					_ => {},
				}

				dp.lock().dispatch(&Event::PeerMgr(e.clone()));
				ListenResult::Ok
			});

			Some(Arc::new(mgr))
		} else {
			None
		};

		Ok(P2P {
			addr_mgr,
			peer_mgr,
			ping_mgr,
			dispatch,
		})
	}

	pub fn address_manager(&self) -> Option<&Arc<AddressManager<S>>> {
		self.addr_mgr.as_ref()
	}

	pub fn peer_manager(&self) -> Option<&Arc<PeerManager>> {
		self.peer_mgr.as_ref()
	}

	pub fn ping_manager(&self) -> Option<&Arc<PingManager>> {
		self.ping_mgr.as_ref()
	}

	pub fn listen(&self, listener: impl Listener<Event>) {
		self.add_listener(Box::new(listener));
	}

	/// Helper function to listen to events using a filter to filter
	/// away certain events.
	///
	/// Only events for which the filter returns true will be passed
	/// to the listener.
	///
	/// Please note that it is important that the filter function is
	/// fast and cannot block.
	pub fn listen_filter(
		&self,
		mut listener: impl Listener<Event>,
		mut filter: impl for<'e> FnMut(&'e Event) -> bool + Send + 'static,
	) {
		self.listen(move |event: &Event| {
			if filter(event) {
				listener.event(event)
			} else {
				ListenResult::Ok
			}
		})
	}
}

impl<S> EventSource<Event> for P2P<S> {
	fn add_listener(&self, listener: Box<dyn Listener<Event>>) {
		self.dispatch.lock().add_listener(listener);
	}
}
