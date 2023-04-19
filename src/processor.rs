//! 
use std::{io, ptr, net};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::{Cursor, Read, Write};
use std::sync::{mpsc, Arc};
use std::time::Instant;

use bitcoin::consensus::encode::{self, Decodable, Encodable};
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::constants::Magic;

use crate::{Event, Listener, ListenerResult, PeerId, P2P};
use crate::mio_io::{
    IoProcessor, ProcessorThread, ProcessorThreadError, TokenTally, WakerSender, WakerSenderError,
    WAKE_TOKEN,
};
use crate::logic::{self, Reactions, Scheduler};

/// Control message used to communicate with the processor thread.
pub enum Ctrl {
	/// Add a new listener.
	AddListener(Box<dyn Listener>),
	/// Add a new peer.
	Connect(PeerId, net::SocketAddr, mio::net::TcpStream),
	/// Disconnect the peer.
	Disconnect(PeerId),
	/// Send message to the peer.
	SendMsg(PeerId, NetworkMessage),
	/// Broadcast the message to all peers.
	BroadcastMsg(NetworkMessage),
	/// Order the processor thread to shutdown.
	Shutdown,
}

/// I/O related state for a peer.
struct PeerIo {
    /// The id of the peer.
    id: PeerId,
    /// During processing, when we notice this peer is dead, we will mark is dead
    /// and only in the end remove all dead peers.
    dead: bool,
	/// The peer's TCP address.
	addr: net::SocketAddr,
	/// The TCP connection.
	conn: mio::net::TcpStream,
    /// The mio poll token used for this connection.
    token: mio::Token,

	/// Buffers for reading and writing.
	/// These will never hold more than a single message.
	buf_in: Vec<u8>,
	buf_out: Vec<u8>,

	/// Queue for outgoing messages.
	queue_out: VecDeque<NetworkMessage>,
}

/// Error cases for [handle_read].
#[derive(Debug)]
enum ReadError {
	Disconnected,
	DoS(&'static str),
	Tcp(io::Error),
	Encode(encode::Error),
	WrongMagic(Magic),
}

/// This is a type of writer that will first try to write bytes to the TCP
/// stream and once that would block, writes the remainder into a buffer.
struct TryTcpWriter<'a> {
	stream: &'a mut mio::net::TcpStream,
	remain: &'a mut Vec<u8>,
}

impl<'a> io::Write for TryTcpWriter<'a> {
	fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
		// Once we started writing into the buffer,
		// never write to the stream again.
		if !self.remain.is_empty() {
			self.remain.write_all(buf).expect("vecs don't error");
			return Ok(buf.len());
		}

		// Try to write to the stream first.
		// If not everything could be written, write the rest to the buffer.
		let written = self.stream.write(buf)?;
		if written < buf.len() {
			trace!("Wrote {} bytes to the tcp stream, writing the rest to the buffer", written);
			self.remain.write_all(&buf[written..]).expect("vecs don't error");
		}

		Ok(buf.len())
	}

	fn flush(&mut self) -> Result<(), io::Error> {
		unimplemented!()
	}
}

/// Write queued messages for the peer into its TCP stream.
///
/// This method is outside of Thread for borrowck reasons.
fn handle_write(pio: &mut PeerIo, magic: Magic) -> Result<(), io::Error> {
	// Check if we have some leftover bytes from last write.
	let peer_buf = &mut pio.buf_out;
	if !peer_buf.is_empty() {
		trace!("Peer {} has write buffer of {} bytes. Writing...", pio.id, peer_buf.len());
		let written = pio.conn.write(&peer_buf)?;
		trace!("Written {} bytes to peer {}", written, pio.id);
		if written < peer_buf.len() {
			// Shift the remainder of the buffer to the front of it.
			// This is safe because we only move bytes from higher indices to lower indices.
			let remaining_bytes = peer_buf.len() - written;
			trace!("Peer {} still has {} bytes of write buffer left", pio.id, remaining_bytes);
			unsafe {
				ptr::copy(peer_buf[written..].as_ptr(), peer_buf[..].as_mut_ptr(), remaining_bytes);
				peer_buf.set_len(remaining_bytes);
			}
			return Ok(());
		}
	}
	peer_buf.clear();

	while let Some(msg) = pio.queue_out.pop_front() {
		trace!("Wiring {} msg to peer {}", msg.cmd(), pio.id);
		let raw_msg = RawNetworkMessage {
			magic: magic,
			payload: msg,
		};

		// Write the message into a writer that will first write into the TCP
		// stream and write the remainder into our peer buffer.
		let mut writer = TryTcpWriter {
			stream: &mut pio.conn,
			remain: peer_buf,
		};
		raw_msg.consensus_encode(&mut writer)?;

		if !peer_buf.is_empty() {
			break;
		}
	}
	Ok(())
}

/// Try queue the message to the peer and try send it over TCP if possible.
fn queue_msg(
    pio: &mut PeerIo,
    rg: &mio::Registry,
    config: &crate::config::Config,
    msg: NetworkMessage,
) {
    if pio.queue_out.len() >= config.max_msg_queue_size {
        debug!("Dropping {} message to peer {} ({}): full queue", msg.cmd(), pio.id, pio.addr);
        return;
    }

    pio.queue_out.push_back(msg);

    // If this is the first msg in the queue, write to the TCP stream.
    if pio.queue_out.len() == 1 && pio.buf_out.is_empty() {
        trace!("Writing immediatelly!");
        //TODO(stevenroose) deduplicate this section with the section from poll events
        if let Err(e) = handle_write(pio, config.network.magic()) {
            let addr = pio.addr;

            warn!("Error writing to peer {} ({}): {}", pio.id, addr, e);
            // TODO(stevenroose) 
            // self.react.disconnect(peer);
            return;
        }

        // If there's more to write left, reregister for writing.
        if !pio.buf_out.is_empty() || !pio.queue_out.is_empty() {
            trace!("Reregistering poll for peer {}", pio.id);
            let interest = mio::Interest::READABLE | mio::Interest::WRITABLE;
            rg.reregister(&mut pio.conn, pio.token, interest)
                .expect("TCP stream poll registry failed");
        }
    }
}

/// Dispatch this event al all listeners.
fn dispatch(listeners: &mut Vec<Box<dyn Listener>>, event: Event) {
    listeners.retain_mut(|l| {
        l.event(&event) == ListenerResult::Ok
    });
}

/// The main processing thread and all the state it keeps locally.
//TODO(stevenroose) need pub?
pub struct Processor {
    /// The main p2p config.
    //TODO(stevenroose) see if we need the entire config or something more local
    config: crate::config::Config,

	/// The channel on which we receive control messages.
	ctrl_rx: mpsc::Receiver<Ctrl>,

	/// The listeners to dispatch events to.
    //TODO(stevenroose) remove
    // /// We wrap this in a refcell (maybe temporarily) so that the
    // /// dispatch method doesn't need &mut because it's a pita.
	listeners: Vec<Box<dyn Listener>>,

    // This is our main I/O buffer. It's re-used for all TCP I/O.
    // This buffer will in practice grow to the size of the OS-level buffer
    // for TCP streams.
    main_buffer: Vec<u8>,

    peers: HashMap<PeerId, PeerIo>,
    token_index: HashMap<mio::Token, PeerId>,
}

impl Processor {
    pub fn new(
        config: crate::config::Config,
        ctrl_rx: mpsc::Receiver<Ctrl>,
    ) -> Processor {
		Processor {
            config: config,

			ctrl_rx: ctrl_rx,
			listeners: Vec::new(),

            main_buffer: Vec::with_capacity(bitcoin::consensus::encode::MAX_VEC_SIZE),
            peers: HashMap::new(),
            token_index: HashMap::new(),
		}
    }

	/// Handle an incoming message.
	///
	/// Some messages are handled internally, those that are not are pushed into
	/// the channel.
    //TODO(stevenroose) do we need this?
	pub fn handle_message(&self, peer: PeerId, msg: NetworkMessage) {
		debug!("Received {:?} message from {}", msg.cmd(), peer);

        //TODO(stevenroose) put these elsewhere

		// if let NetworkMessage::Version(ver) = msg {
		//	let mut peers_lock = self.p2p.peers.lock().unwrap();
		//	let state = or!(peers_lock.get_mut(&peer), return);
		//	logic::handshake::handle_version(&mut self.react, &self.p2p, peer, state, ver);
		//	if state.handshake.finished() {
		//		drop(state);
		//		drop(peers_lock);
		//		self.dispatch(Event::Connected(peer));
		//	}
		//	return;
		// }

		// if let NetworkMessage::Verack = msg {
		//	let mut peers_lock = self.p2p.peers.lock().unwrap();
		//	let state = or!(peers_lock.get_mut(&peer), return);
		//	// Store verack info and schedule first ping.
		//	logic::handshake::handle_verack(&mut self.react, &self.config, peer, state);
		//	if state.handshake.finished() {
		//		drop(state);
		//		drop(peers_lock);
		//		self.dispatch(Event::Connected(peer));
		//	}
		//	return;
		// }

		// if let NetworkMessage::Ping(nonce) = msg {
		//	let mut peers_lock = self.p2p.peers.lock().unwrap();
		//	let state = or!(peers_lock.get_mut(&peer), return);
		//	logic::pingpong::handle_ping(&mut self.react, peer, state, nonce);
		//	return;
		// }

		// if let NetworkMessage::Pong(nonce) = msg {
		//	let mut peers_lock = self.p2p.peers.lock().unwrap();
		//	let state = or!(peers_lock.get_mut(&peer), return);
		//	logic::pingpong::handle_pong(&mut self.react, peer, state, nonce);
		//	return;
		// }

		// if let NetworkMessage::SendHeaders = msg {
		//	let mut peers_lock = self.p2p.peers.lock().unwrap();
		//	let state = or!(peers_lock.get_mut(&peer), return);
		//	state.send_headers = true;
		//	return;
		// }

		// if let NetworkMessage::Inv(ref items) = msg {
		//	let mut peers_lock = self.p2p.peers.lock().unwrap();
		//	let state = or!(peers_lock.get_mut(&peer), return);
		//	logic::inventory::handle_inv(state, items);
		//	// don't return but pass the message to the user
		// }

		// self.dispatch(Event::Message(peer, msg));
	}

	/// Read new messages for this peer from his TCP stream.
	fn handle_read(
		&mut self,
		peer: PeerId,
	) -> Result<(), ReadError> {
        let buffer = &mut self.main_buffer;
		trace!("buffer: len={}; cap={}", buffer.len(), buffer.capacity());
        let pio = self.peers.get_mut(&peer).unwrap();

		buffer.clear(); // Set the len to 0.

		// First check if we have some leftover of the last read.
		let peer_buf = &mut pio.buf_in;
		let had_peer_buf = !peer_buf.is_empty();
		if had_peer_buf {
			buffer.write_all(&peer_buf).expect("vecs don't error");
			peer_buf.clear();
		}

		// We wrap the buffer in a cursor here, because borrowck doesn't understand
		// the buffer being freed when the cursor goes out of scope when the
		// 'parsing loop breaks.
		//TODO(stevenroose) try to clean that up!
		let mut cursor = Cursor::new(buffer);

		'reading:
		loop {
			// Reset the cursor position from wherever it was during the last iteration.
			cursor.set_position(0);

			// Then read from the TCP stream.
			// To call read, we need to create space in the buffer. We set len to the max capacity
			let len = cursor.get_ref().len();
			let capacity = cursor.get_ref().capacity();
			unsafe {
				cursor.get_mut().set_len(capacity);
			}
			let count = pio.conn.read(&mut cursor.get_mut()[len..]).map_err(ReadError::Tcp)?;

			if count == 0 {
				// In mio's non-blocking I/O, an `Ok(0)` means EOF.
				return Err(ReadError::Disconnected);
			}

			debug_assert!(count + len <= cursor.get_ref().capacity());
			unsafe {
				cursor.get_mut().set_len(count + len);
			}
			trace!("Peer {} read {} bytes", peer, count);

			// Then try to parse messages in the buffer.
			let full_buffer = cursor.get_ref().len() == cursor.get_ref().capacity();
			loop {
				let start_pos = cursor.position() as usize;
				let raw_msg = match RawNetworkMessage::consensus_decode(&mut cursor) {
					Ok(m) => m,
					Err(encode::Error::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
						// To avoid DoS, make sure the message fits in our buffer.
						if start_pos == 0 && !had_peer_buf && full_buffer {
							// The message takes the full buffer, which is sized at the max msg size!
							return Err(ReadError::DoS(
								"sending messages larger than the max size"
							));
						}

						// We only have a partial message in our buffer.
						if !full_buffer {
							// If we didn't fill our buffer, we need to wait
							// for the remainder of the message, so simply put
							// it in the peer buffer and we're done.
							peer_buf.write_all(&cursor.get_mut()[start_pos..])
								.expect("vecs don't error");
							break 'reading;
						} else {
							// Otherwise, we can actually fetch more. We move the
							// remaining bytes back to the front of the buffer.
							// This is safe because we only move bytes from
							// higher indices to lower indices.
							let remaining_bytes = cursor.get_ref().len() - start_pos;
							unsafe {
								ptr::copy(
									cursor.get_ref()[start_pos..].as_ptr(),
									cursor.get_mut()[..].as_mut_ptr(),
									remaining_bytes,
								);
							}
							continue 'reading;
						}
					}
					Err(e) => return Err(ReadError::Encode(e)),
				};

				if raw_msg.magic != self.config.network.magic() {
					return Err(ReadError::WrongMagic(raw_msg.magic));
				}

				trace!("Queueing {} msg for peer {}", raw_msg.cmd(), peer);
                dispatch(&mut self.listeners, Event::Message(peer, raw_msg.payload));

				// Check if we reached the end of our buffer.
				if cursor.position() as usize == cursor.get_ref().len() {
					if !full_buffer {
						// If we didn't read our entire buffer before,
						// the TCP stream is empty.
						break 'reading;
					} else {
						// Otherwise, clear the buffer and read more.
						cursor.get_mut().clear();
						continue 'reading;
					}
				}
			}
			// We want to enforce that this is unreachable. The compiler actually knows it's
			// unreachable, but we can't ask the compiler to tell us when we broke that invariant.
			// When the never type is stabilized, we can do that by wrapping the inner loop with
			// `let _: ! = loop { ... };` So let's wait for that.
			#[allow(unreachable_code)]
			{ unreachable!("we should never get to this point!"); }
		}

		Ok(())
	}

	/// A method to safely disconnect a peer.
	/// This will also remove the peer from the P2P map and notify the [done]
	/// condvar before releasing the P2P peers lock.
	fn disconnect_peer<'a>(&mut self, peer: PeerId) {
		if let Some(pio) = self.peers.remove(&peer) {
			info!("Disconnecting peer {} with address {}", peer, pio.addr);
			if let Err(e) = pio.conn.shutdown(net::Shutdown::Both) {
				debug!("Error shutting down connection with {}: {}", pio.addr, e);
			}
            self.token_index.remove(&pio.token);

            //TODO(stevenroose) 
			// self.p2p.peers.lock().unwrap().remove(&peer).expect("peer must exist");
			dispatch(&mut self.listeners, Event::Disconnected(peer));
			info!("Succesfully disconnected peer {} with address {}", peer, pio.addr);
		} else {
			warn!("Already disconnected peer {}", peer);
		}
	}

    fn broadcast_msg(&mut self, rg: &mio::Registry, msg: NetworkMessage) {
        for pio in self.peers.values_mut().filter(|pio| !pio.dead) {
            queue_msg(pio, rg, &self.config, msg.clone());
        }
    }
}

/// Use this to safely get a peer from the peer list.
///
/// This method prints log messages when the peer was not found, so that the
/// caller doesn't have to do this.
///
/// We have this as a top level function instead of a [Processor] getter
/// so that a [&mut PeerIo] can be retrieved without needing a mutable borrow
/// of the entire [Processor].
fn safe_get_pio(peers: &mut HashMap<PeerId, PeerIo>, peer: PeerId) -> Option<&mut PeerIo> {
    if let Some(pio) = peers.get_mut(&peer) {
        if !pio.dead {
            Some(pio)
        } else {
            warn!("Reference made to peer that already disconnected: {}", peer);
            None
        }
    } else {
        error!("Reference made to unknown peer: {}", peer);
        None
    }
}

impl IoProcessor for Processor {
    fn wakeup(&mut self, tt: &TokenTally, rg: &mio::Registry, ev: &mio::Events) {
        //TODO(stevenroose) fix scheduler in IoProcessor
        // let timeout = self.scheduler.next_event()
        //     .map(|t| t.checked_duration_since(Instant::now()).expect("event in the far future"));
        // trace!("processor waiting for poll, with timeout {:?}", timeout);
        // self.poll.poll(&mut events, timeout).expect("poll error");
        // trace!("processor woken up");

        // First handle control messages so that we can disconnect peers early
        // and create peers that might already have messages queued.
        while let Ok(ctrl) = self.ctrl_rx.try_recv() {
            match ctrl {
                Ctrl::AddListener(listener) => self.listeners.push(listener),

                Ctrl::Connect(id, addr, mut conn) => {
                    debug!("Handling Ctrl::Connect {}: {}", id, addr);

                    // Register the TCP stream in our Poll.
                    let token = tt.next();
                    rg.register(&mut conn, mio::Token(id.0), mio::Interest::READABLE)
                        .expect("TCP stream poll registry failed");

                    let pio = PeerIo {
                        id: id,
                        dead: false,
                        addr: addr,
                        conn: conn,
                        token: token,
                        buf_in: Vec::new(),
                        buf_out: Vec::new(),
                        queue_out: VecDeque::with_capacity(self.config.max_msg_queue_size),
                    };
                    assert!(self.peers.insert(id, pio).is_none(), "duplicate peer id: {}", id);
                }

                Ctrl::Disconnect(peer) => self.disconnect_peer(peer),

                Ctrl::SendMsg(peer, msg) => {
                    if let Some(pio) = safe_get_pio(&mut self.peers, peer) {
                        queue_msg(pio, rg, &self.config, msg)
                    }
                }

                Ctrl::BroadcastMsg(msg) => self.broadcast_msg(rg, msg),

                Ctrl::Shutdown => {
                    info!("Processor thread received shutdown signal; shutting down...");
                    //TODO(stevenroose) should we close all tcp connections?
                    return;
                }
            }
        }

        // In between, disconnect missing peers.
        // TODO(stevenroose) 
        // for peer in self.react.take_disconnects() {
        //     self.disconnect_peer(peer);
        // }

        // Then perform all I/O events for reading.
        for e in ev.iter().filter(|e| e.is_readable()) {
            let peer = *or!(self.token_index.get(&e.token()), continue);
            trace!("Readable event for peer {}", peer);
            if let Err(err) = self.handle_read(peer) {
                let addr = self.peers[&peer].addr;
                match err {
                    ReadError::Disconnected => {
                        info!("Peer {} ({}) disconnected.", peer, addr);
                        self.disconnect_peer(peer);
                    }
                    ReadError::DoS(e) => {
                        warn!("DoS error from peer {} ({}): {}", peer, addr, e);
                        //TODO(stevenroose) should ban
                        self.disconnect_peer(peer);
                    }
                    ReadError::Encode(e) => {
                        warn!("Encoding error from peer {} ({}): {}", peer, addr, e);
                        //TODO(stevenroose) should ban
                        self.disconnect_peer(peer);
                    }
                    ReadError::WrongMagic(m) => {
                        warn!("Peer {} ({}) uses bad network magic: 0x{:08x}", peer, addr, m);
                        //TODO(stevenroose) should ban
                        self.disconnect_peer(peer);
                    }
                    ReadError::Tcp(e) => {
                        warn!("Error reading from peer {} ({}): {}", peer, addr, e);
                        self.disconnect_peer(peer);
                    }
                }
            }
        }

        //TODO(stevenroose) 
        // In between reading and writing, disconnect misbehaving peers.
        // for peer in self.react.take_disconnects() {
        //     self.disconnect_peer(peer);
        // }

        // Then perform all I/O events for writing.
        for e in ev.iter().filter(|e| e.is_writable()) {
            let peer = *or!(self.token_index.get(&e.token()), continue);
            let pio = self.peers.get_mut(&peer).unwrap();
            trace!("Writable event for peer {} ({})", peer, pio.addr);

            if let Err(e) = handle_write(pio, self.config.network.magic()) {
                let addr = pio.addr;

                warn!("Error writing to peer {} ({}): {}", peer, addr, e);
                drop(pio);
                self.disconnect_peer(peer);
                continue;
            }

            if pio.buf_out.is_empty() && pio.queue_out.is_empty() {
                // Nothing more to write, we deregister the WRITABLE interest.
                trace!("Deregistering write for peer {}", peer);
                let interest = mio::Interest::READABLE;
                rg.reregister(&mut pio.conn, mio::Token(peer.0), interest)
                    .expect("TCP stream poll registry failed");
            }
        }

        //TODO(stevenroose) fix these

        // Handle any scheduled events.
        // self.scheduler.handle_events_due(&self.p2p, &mut self.react);
        // Schedule all schedule actions queued by the logic.
        // self.react.schedule_all(&mut self.scheduler);

        // // Also disconnect bad peers.
        // for peer in self.react.take_disconnects() {
        //     self.disconnect_peer(peer);
        // }

        // // Then queue new outgoing messages.
        // trace!("Scheduler and handler queued {} messages", self.react.len());
        // for (peer, msg) in self.react.drain_messages() {
        //     let pio = or!(self.peers.get_mut(&peer), continue);
        //     self.queue_msg(peer, pio, msg);
        // }

        // assert_eq!(self.react.len(), 0, "the action queue should be empty!");
    }
}
