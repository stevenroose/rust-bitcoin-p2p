
use std::{fmt, io, net};
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{self, AtomicU32};
use std::time::Duration;

use bitcoin;
// use bitcoin::network::constants::Magic;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::consensus::encode;
use crossbeam_channel as chan;
use erin;

use crate::{DisconnectReason, Error, PeerId};
use crate::events::{Dispatch, EventSource, Listener};


#[derive(Debug, Clone)]
pub struct Config {
	/// The magic value for the network.
	pub magic: u32,

	/// The maximum size of the output buffer we maintain.
	/// Buffers initialize size 0 but will be shrunk to this
	/// size when they grow over it.
	///
	/// Default value: 4 MiB.
	pub stream_out_buf_size_limit: usize,

	/// The maximum size of the input buffer we maintain.
	/// Buffers initialize size 0 but will be shrunk to this
	/// size when they grow over it.
	///
	/// Default value: 4 MiB.
	pub stream_in_buf_size_limit: usize,
}

impl Default for Config {
	fn default() -> Config {
		Config {
			magic: 0, //TODO(stevenroose) bitcoin::network::constant::Magic::BITCOIN,
			stream_out_buf_size_limit: 4 * 1024 * 1024,
			stream_in_buf_size_limit: 4 * 1024 * 1024,
		}
	}
}

#[derive(Clone, Debug)]
pub enum Event {
	Connected,
	//TODO(stevenroose) should this be an arc to avoid cloning 4MB blocks?
	//  arguably, our internal listeners won't clone this because we do filtering
	//  before sending on a channel, but users might use simple channels as listeners
	Message(NetworkMessage),
	Disconnected(DisconnectReason),
}

pub struct Peer {
	id: PeerId,
	local_addr: net::SocketAddr,
	ctrl_tx: chan::Sender<Ctrl>,
	waker: erin::Waker,
	pver: Arc<AtomicU32>,
}

impl Peer {
	pub fn start_in(
		rt: &erin::Runtime,
		config: Config,
		stream: net::TcpStream,
	) -> Result<Peer, Error> {
		let id = stream.peer_addr()?;
		let local_addr = stream.local_addr()?;
		let pver = Arc::new(AtomicU32::new(0));

		let (ctrl_tx, ctrl_rx) = chan::unbounded();
		let processor = Processor::new(config, id, stream, ctrl_rx);
		let waker = rt.add_process(Box::new(processor))?.into_waker();

		Ok(Peer { id, local_addr, ctrl_tx, waker, pver })
	}

	pub fn dial_in(
		rt: &erin::Runtime,
		config: Config,
		addr: net::SocketAddr,
		timeout: Option<Duration>,
	) -> Result<Peer, Error> {
		let stream = if let Some(to) = timeout {
			net::TcpStream::connect_timeout(&addr, to)
		} else {
			net::TcpStream::connect(&addr)
		}.map_err(Error::Dial)?;

		Self::start_in(rt, config, stream)
	}

	pub fn id(&self) -> PeerId {
		self.id
	}

	pub fn addr(&self) -> net::SocketAddr {
		self.id
	}

	pub fn local_addr(&self) -> net::SocketAddr {
		self.local_addr
	}

	/// The protocol version of this peer.
	///
	/// This returns [None] before a protocol version has been negotiated.
	pub fn version(&self) -> Option<u32> {
		match self.pver.load(atomic::Ordering::Relaxed) {
			0 => None,
			v => Some(v),
		}
	}

	/// Set the peer's protocol version.
	pub fn set_version(&self, protocol_version: u32) {
		self.pver.store(protocol_version, atomic::Ordering::Relaxed);
	}

	pub fn listen(&self, listener: impl Listener<Event>) {
		self.add_listener(Box::new(listener));
	}

	/// Queue a network message to be sent to the peer.
	pub fn send_message(&self, msg: impl Into<NetworkMessage>) {
		self.send_ctrl(Ctrl::SendMsg(msg.into()));
	}

	/// Start disconnecting this peer for the specified reason.
	pub fn disconnect_with_reason(&self, reason: DisconnectReason) {
		self.send_ctrl(Ctrl::Disconnect(reason));
	}

	/// Start disconnecting this peer.
	pub fn disconnect(&self) {
		self.disconnect_with_reason(DisconnectReason::Command);
	}
}

impl EventSource<Event> for Peer {
	fn add_listener(&self, listener: Box<dyn Listener<Event>>) {
		self.send_ctrl(Ctrl::AddListener(listener))
	}
}

trait SendCtrl {
	fn send_ctrl(&self, ctrl: Ctrl);
}

impl SendCtrl for Peer {
	fn send_ctrl(&self, ctrl: Ctrl) {
		//TODO(stevenroose) should we handle these errors?
		let _ = self.ctrl_tx.send(ctrl);
		//TODO(stevenroose) this is UnixStream::as_raw_fd erroring
		let _ = self.waker.wake();
	}
}

impl crate::Wire for Peer {
	fn message(&self, msg: NetworkMessage) {
		self.send_message(msg);
	}
}

impl fmt::Debug for Peer {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		fmt::Display::fmt(&self.id, f)
	}
}

enum Ctrl {
	SendMsg(NetworkMessage),
	AddListener(Box<dyn Listener<Event>>),
	Disconnect(DisconnectReason),
}

struct Processor {
	cfg: Config,
	id: PeerId,

	stream: net::TcpStream,
	ctrl_rx: chan::Receiver<Ctrl>,
	dispatch: Dispatch<Event>,

	// some I/O local vars
	token: erin::IoToken,
	out_buf: Vec<u8>, //TODO(stevenroose) use Bytes
	in_buf: Vec<u8>,
	is_connected: bool,
}

impl Processor {
	fn new(
		config: Config,
		id: PeerId,
		stream: net::TcpStream,
		ctrl_rx: chan::Receiver<Ctrl>,
	) -> Processor {
		Processor {
			cfg: config,
			id: id,
			stream: stream,
			ctrl_rx: ctrl_rx,
			dispatch: Dispatch::new(),

			token: erin::IoToken::NULL,
			out_buf: Vec::new(),
			in_buf: Vec::new(),
			is_connected: false,
		}
	}
}

impl Processor {
	/// Send the message to the peer or add the message to the queue.
	///
	/// Returns true if there is more to send to the peer and false otherwise.
	fn send_msg(&mut self, msg: NetworkMessage) -> Result<bool, DisconnectReason> {
		let try_write = self.out_buf.is_empty();

		let raw = RawNetworkMessage {
			magic: self.cfg.magic,
			payload: msg,
		};
		encode::Encodable::consensus_encode(&raw, &mut self.out_buf)
			.expect("buffers don't error");

		if self.is_connected && try_write {
			self.try_write()
		} else {
			Ok(true)
		}
	}

	fn disconnect(&mut self, reason: DisconnectReason) {
		let _ = self.stream.shutdown(net::Shutdown::Both);
		self.dispatch.dispatch(&Event::Disconnected(reason));
	}

	fn try_read(&mut self) -> Result<(), DisconnectReason> {
		match self.stream.read(&mut self.in_buf) {
			Ok(0) => {
				let err = io::Error::new(io::ErrorKind::ConnectionReset, "peer disconnected");
				Err(DisconnectReason::ConnectionError(Arc::new(err)))
			}
			Ok(_n) => {
				let mut start = 0;
				loop {
					match encode::deserialize_partial::<RawNetworkMessage>(&self.in_buf[start..]) {
						Ok((msg, end)) => {
							if msg.magic != self.cfg.magic {
								return Err(DisconnectReason::PeerMagic(msg.magic));
							}
							self.dispatch.dispatch(&Event::Message(msg.payload));
							start += end;
						}
						Err(encode::Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
							break;
						}
						Err(e) => return Err(DisconnectReason::DecodeError(e.into())),
					}
				}

				self.in_buf.copy_within(start.., 0);
				self.in_buf.truncate(self.in_buf.len() - start);
				self.in_buf.shrink_to(self.cfg.stream_in_buf_size_limit);
				Ok(())
			}
			Err(_) if !self.is_connected => Err(DisconnectReason::ConnectionFailed),
			Err(e) => Err(DisconnectReason::ConnectionError(e.into())),
		}
	}

	/// Try to write to the socket.
	///
	/// Returns true if there is more to send to the peer and false otherwise.
	fn try_write(&mut self) -> Result<bool, DisconnectReason> {
		if self.out_buf.is_empty() {
			return Ok(false);
		}

		match self.stream.write(&self.out_buf) {
			Ok(0) => Ok(true),
			Ok(n) => {
				self.out_buf.copy_within(n.., 0);
				self.out_buf.truncate(self.out_buf.len() - n);
				self.out_buf.shrink_to(self.cfg.stream_out_buf_size_limit);
				Ok(!self.out_buf.is_empty())
			}
			Err(e) if e.kind() == io::ErrorKind::WouldBlock
				|| e.kind() == io::ErrorKind::WriteZero =>
			{
				Ok(true)
			}
			Err(_) if !self.is_connected => Err(DisconnectReason::ConnectionFailed),
			Err(e) => Err(DisconnectReason::ConnectionError(e.into())),
		}
	}
}

/// Read and write interest for erin.
const RW: erin::Interest = erin::interest::READ | erin::interest::WRITE;

impl erin::Process for Processor {
	fn setup(&mut self, rt: &erin::RuntimeHandle) -> Result<(), erin::Exit> {
		// Initially we register for WRITE because we are only really connected when
		// we can write to the peer. We use this to send the connected event.
		self.token = rt.register_io(&self.stream, RW);
		Ok(())
	}

	fn wakeup(&mut self, rt: &erin::RuntimeHandle, ev: erin::Events) -> Result<(), erin::Exit> {
		if ev.waker() {
			loop {
				match self.ctrl_rx.try_recv() {
					Ok(Ctrl::SendMsg(msg)) => {
						match self.send_msg(msg) {
							Ok(true) => {
								rt.reregister_io(self.token, RW);
							}
							Ok(false) => {},
							Err(r) => {
								self.disconnect(r);
								return Err(erin::Exit);
							}
						}
					}
					Ok(Ctrl::AddListener(mut l)) => {
						// To make sure some listeners don't stall waiting for
						// a Connected event that already passed, we send it.
						//TODO(stevenroose) does this have adverse effects?
						if self.is_connected {
							l.event(&Event::Connected);
						}
						self.dispatch.add_listener(l);
					},
					Ok(Ctrl::Disconnect(r)) => {
						self.disconnect(r);
						debug!("Peer processor shutting down: disconnected");
						return Err(erin::Exit);
					},
					Err(chan::TryRecvError::Empty) => break,
					Err(chan::TryRecvError::Disconnected) => {
						debug!("Peer processor shutting down: ctrl channel closed");
						return Err(erin::Exit);
					}
				}
			}
		}

		for ev in ev.io() {
			assert_eq!(ev.token, self.token, "only ever registered one token");

			if ev.src.is_error() || ev.src.is_hangup() {
				// Let the subsequent read fail.
				trace!("{}: Socket error triggered: {:?}", self.id, ev);
			}

			if ev.src.is_readable() {
				if let Err(r) = self.try_read() {
					self.disconnect(r);
					return Err(erin::Exit);
				}
			}
			
			if ev.src.is_writable() {
				if !self.is_connected {
					// We are only really connected once we get the first WRITE event.
					self.dispatch.dispatch(&Event::Connected);
					self.is_connected = true;
				}

				match self.try_write() {
					Ok(true) => rt.reregister_io(self.token, RW),
					Ok(false) => rt.reregister_io(self.token, erin::interest::READ),
					Err(r) => {
						self.disconnect(r);
						return Err(erin::Exit);
					}
				}
			}

			if ev.src.is_invalid() {
				// File descriptor was closed and is invalid.
				// Nb. This shouldn't happen. It means the source wasn't
				// properly unregistered, or there is a duplicate source.
				error!("{}: Socket is invalid, shutting down", self.id);
				return Err(erin::Exit);
			}
		}

		Ok(())
	}

	fn shutdown(&mut self) {
		self.disconnect(DisconnectReason::Command);
	}
}
