
use std::{io, ptr, net};
use std::collections::{HashMap, VecDeque};
use std::io::{Cursor, Read, Write};
use std::sync::{atomic, mpsc, Arc};
use std::time::Instant;

use bitcoin::consensus::encode::{self, Decodable, Encodable};
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};

use crate::logic::{self, Reactions, Scheduler};
use crate::{P2PEvent, PeerId, P2P};

pub const WAKE_TOKEN: mio::Token = mio::Token(0);

/// Control message used to communicate with the processor thread.
pub enum Ctrl {
	/// Add a new peer.
	Connect(PeerId, mio::net::TcpStream),
	/// Disconnect the peer.
	Disconnect(PeerId),
	/// Send message to the peer.
	SendMsg(PeerId, NetworkMessage),
	/// Broadcast the message to all peers.
	BroadcastMsg(NetworkMessage),
}

/// I/O related state for a peer.
struct PeerIo {
	/// The peer's TCP address.
	addr: net::SocketAddr,
	/// The TCP connection.
	conn: mio::net::TcpStream,

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
	WrongMagic(u32),
}

/// Read new messages for this peer from his TCP stream.
fn handle_read(
	p2p: &Arc<P2P>,
	react: &mut Reactions,
	event_tx: &mpsc::Sender<P2PEvent>,
	peer: PeerId,
	pio: &mut PeerIo,
	buf: &mut Vec<u8>,
) -> Result<(), ReadError> {
	trace!("buffer: len={}; cap={}", buf.len(), buf.capacity());
	buf.clear(); // Set the len to 0.

	// First check if we have some leftover of the last read.
	let peer_buf = &mut pio.buf_in;
	let had_peer_buf = !peer_buf.is_empty();
	if !peer_buf.is_empty() {
		buf.write_all(&peer_buf).expect("vecs don't error");
		peer_buf.clear();
	}

	// We wrap the buffer in a cursor here, because borrowck doesn't understand
	// the buffer being freed when the cursor goes out of scope when the
	// 'parsing loop breaks.
	//TODO(stevenroose) try to clean that up!
	let mut cursor = Cursor::new(buf);

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
		debug_assert!(count + len <= cursor.get_ref().capacity());
		unsafe {
			cursor.get_mut().set_len(count + len);
		}
		trace!("Peer {} read {} bytes", peer, count);

		if count == 0 {
			// In mio's non-blocking I/O, an `Ok(0)` means EOF.
			return Err(ReadError::Disconnected);
		}

		// Then try to parse messages in the buffer.
		let full_buffer = cursor.get_ref().len() == cursor.get_ref().capacity();
		'parsing:
		loop {
			let start_pos = cursor.position() as usize;
			let raw_msg = match RawNetworkMessage::consensus_decode(&mut cursor) {
				Ok(m) => m,
				Err(encode::Error::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
					// To avoid DoS, make sure the message fits in our buffer.
					if start_pos == 0 && !had_peer_buf && full_buffer {
						// The message takes the full buffer, which is sized at the max msg size!
						return Err(ReadError::DoS("sending messages larger than the max size"));
					}

					// We only have a partial message in our buffer.
					if !full_buffer {
						// If we didn't fill our buffer, we need to wait for the remainder of the
						// message, so simply put it in the peer buffer and we're done.
						peer_buf.write_all(&cursor.get_mut()[start_pos..]).expect("vecs don't error");
						break 'reading;
					} else {
						// Otherwise, we can actually fetch more. We move the remaining bytes
						// back to the front of the buffer.
						// This is safe because we only move bytes from higher indices to lower indices.
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
				Err(encode::Error::UnrecognizedNetworkCommand(ref cmd)) => {
					//TODO(stevenroose) I think this will change with the NetworkMessage::Unknown variant
					warn!("{} Ignoring unknown '{}' message", peer, cmd);
					continue 'parsing;
				}
				Err(e) => return Err(ReadError::Encode(e)),
			};

			if raw_msg.magic != p2p.config.network.magic() {
				return Err(ReadError::WrongMagic(raw_msg.magic));
			}

			trace!("Queueing {} msg for peer {}", raw_msg.cmd(), peer);
			logic::handle_message(p2p, react, event_tx, peer, raw_msg.payload);

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
		// So we want to enforce that this is unreachable. The compiler actually knows it's
		// unreachable, but we can't ask the compiler to tell us when we broke that invariant.
		// When the never type is stabilized, we can do that by wrapping the inner loop with
		// `let _: ! = loop { ... };` So let's wait for that.
		#[allow(unreachable_code)]
		{ unreachable!("we should never get to this point!"); }
	}
	
	Ok(())
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
fn handle_write(p2p: &Arc<P2P>, peer: PeerId, pio: &mut PeerIo) -> Result<(), io::Error> {
	// Check if we have some leftover bytes from last write.
	let peer_buf = &mut pio.buf_out;
	if !peer_buf.is_empty() {
		trace!("Peer {} has write buffer of {} bytes. Writing...", peer, peer_buf.len());
		let written = pio.conn.write(&peer_buf)?;
		trace!("Written {} bytes to peer {}", written, peer);
		if written < peer_buf.len() {
			// Shift the remainder of the buffer to the front of it.
			// This is safe because we only move bytes from higher indices to lower indices.
			let remaining_bytes = peer_buf.len() - written;
			trace!("Peer {} still has {} bytes of write buffer left", peer, remaining_bytes);
			unsafe {
				ptr::copy(peer_buf[written..].as_ptr(), peer_buf[..].as_mut_ptr(), remaining_bytes);
				peer_buf.set_len(remaining_bytes);
			}
			return Ok(());
		}
	}
	peer_buf.clear();

	while let Some(msg) = pio.queue_out.pop_front() {
		trace!("Wiring {} msg to peer {}", msg.cmd(), peer);
		let raw_msg = RawNetworkMessage {
			magic: p2p.config.network.magic(),
			payload: msg,
		};

		// Write the message into a writer that will first write into the TCP
		// stream and write the remainder into our peer buffer.
		let mut writer = TryTcpWriter {
			stream: &mut pio.conn,
			remain: peer_buf,
		};
		//TODO(stevenroose) change this to `?` after the Encodable signature changed
		match raw_msg.consensus_encode(&mut writer) {
			Ok(_) => {}
			Err(encode::Error::Io(err)) => return Err(err),
			Err(_) => panic!("these should be impossible, see TODO about signature"),
		};

		if !peer_buf.is_empty() {
			break;
		}
	}
	Ok(())
}

/// Try queue the message to the peer and try send it over TCP if possible.
fn queue_msg(
	p2p: &Arc<P2P>,
	react: &mut Reactions,
	poll: &mut mio::Poll,
	peer: PeerId,
	pio: &mut PeerIo,
	msg: NetworkMessage,
) {
	if pio.queue_out.len() >= p2p.config.max_msg_queue_size {
		debug!("Dropping {} message to peer {} ({}): full queue", msg.cmd(), peer, pio.addr);
		return;
	}

	pio.queue_out.push_back(msg);

	// If this is the first msg in the queue, write to the TCP stream.
	if pio.queue_out.len() == 1 && pio.buf_out.is_empty() {
		trace!("Writing immediatelly!");
		//TODO(stevenroose) deduplicate this section with the section from poll events
		if let Err(e) = handle_write(&p2p, peer, pio) {
			let addr = pio.addr;
			drop(pio); // To be able to do `&mut peers`.

			warn!("Error writing to peer {} ({}): {}", peer, addr, e);
			react.disconnect(peer);
			return;
		}

		// If there's more to write left, reregister for writing.
		if !pio.buf_out.is_empty() || !pio.queue_out.is_empty() {
			trace!("Reregistering poll for peer {}", peer);
			let interest = mio::Interest::READABLE | mio::Interest::WRITABLE;
			poll.registry().reregister(&mut pio.conn, mio::Token(peer.0), interest)
				.expect("TCP stream poll registry failed");
		}
	}
}

/// A method to safely disconnect a peer.
/// This will also remove the peer from the P2P map and notify the [done]
/// condvar before releasing the P2P peers lock.
fn disconnect_peer<'a>(
	p2p: &'a Arc<P2P>,
	peers: &mut HashMap<PeerId, PeerIo>,
	event_tx: &mpsc::Sender<P2PEvent>,
	peer: PeerId,
) {
	if let Some(pio) = peers.remove(&peer) {
		info!("Disconnecting peer {} with address {}", peer, pio.addr);
		if let Err(e) = pio.conn.shutdown(net::Shutdown::Both) {
			debug!("Error shutting down connection with {}: {}", pio.addr, e);
		}

		p2p.peers.lock().unwrap().remove(&peer).expect("peer must exist");
		event_tx.send(P2PEvent::Disconnected(peer)).expect("event channel broken");
		info!("Succesfully disconnected peer {} with address {}", peer, pio.addr);
	} else {
		warn!("Already disconnected peer {}", peer);
	}
}

/// The main function of the processing thread.
/// It's using non-blocking I/O so most of the time it's waiting for OS events.
pub fn processor(
	p2p: Arc<P2P>,
	mut poll: mio::Poll,
	ctrl_rx: mpsc::Receiver<Ctrl>,
	event_tx: mpsc::Sender<P2PEvent>,
) {
	trace!("processor starting...");
	let mut events = mio::Events::with_capacity(1024);
	let mut peers = HashMap::new();

	// The schedule for all the timers.
	let mut scheduler = Scheduler::new();

	// A queue of actions that logic handling schedule during our event loop.
	let mut react = Reactions::new();

	// This is our main I/O buffer. It's re-used for all TCP I/O.
	// This buffer will in practice grow to the size of the OS-level buffer
	// for TCP streams.
	let mut buffer = Vec::with_capacity(bitcoin::consensus::encode::MAX_VEC_SIZE);

	loop {
		events.clear();

		let timeout = scheduler.next_event()
			.map(|t| t.checked_duration_since(Instant::now()).expect("event in the far future"));
		trace!("processor waiting for poll, with timeout {:?}", timeout);
		poll.poll(&mut events, timeout).expect("poll error");
		trace!("processor woken up");

		if p2p.quit.load(atomic::Ordering::Relaxed) {
			info!("P2P processing thread received quit signal; exiting.");
			return;
		}

		// First handle control messages so that we can disconnect peers early
		// and create peers that might already have messages queued.
		for ctrl in ctrl_rx.try_iter() {
			match ctrl {

				Ctrl::Connect(id, mut conn) => {
					let addr = match conn.peer_addr() {
						Ok(a) => a,
						Err(e) => {
							error!("Error getting peer addr from new TCP connection: {}", e);
							continue;
						}
					};
					debug!("Handling Ctrl::Connect {}: {}", id, addr);

					// Register the TCP stream in our Poll.
					let interest = mio::Interest::READABLE;
					poll.registry().register(&mut conn, mio::Token(id.0), interest)
						.expect("TCP stream poll registry failed");

					let pio = PeerIo {
						addr: addr,
						conn: conn,
						buf_in: Vec::new(),
						buf_out: Vec::new(),
						queue_out: VecDeque::with_capacity(p2p.config.max_msg_queue_size),
					};
					assert!(peers.insert(id, pio).is_none(), "duplicate peer id: {}", id);
				}

				Ctrl::Disconnect(peer) => {
					disconnect_peer(&p2p, &mut peers, &event_tx, peer);
				}

				Ctrl::SendMsg(peer, msg) => {
					let pio = or_continue!(peers.get_mut(&peer));
					queue_msg(&p2p, &mut react, &mut poll, peer, pio, msg);
				}

				Ctrl::BroadcastMsg(msg) => {
					for (peer, pio) in peers.iter_mut() {
						queue_msg(&p2p, &mut react, &mut poll, *peer, pio, msg.clone());
					}
				}

			}
		}

		// In between, disconnect missing peers.
		for peer in react.take_disconnects() {
			disconnect_peer(&p2p, &mut peers, &event_tx, peer);
		}

		// Then perform all I/O events for reading.
		for event in events.iter().filter(|e| e.is_readable() && e.token() != WAKE_TOKEN) {
			let peer = PeerId(event.token().0);
			let pio = or_continue!(peers.get_mut(&peer));
			trace!("Readable event for peer {}", peer);

			if let Err(err) = handle_read(&p2p, &mut react, &event_tx, peer, pio, &mut buffer) {
				let addr = pio.addr;
				drop(pio); // To be able to do `&mut peers`.

				match err {
					ReadError::Disconnected => {
						info!("Peer {} ({}) disconnected.", peer, addr);
						disconnect_peer(&p2p, &mut peers, &event_tx, peer);
					}
					ReadError::DoS(e) => {
						warn!("DoS error from peer {} ({}): {}", peer, addr, e);
						//TODO(stevenroose) should ban
						disconnect_peer(&p2p, &mut peers, &event_tx, peer);
					}
					ReadError::Encode(e) => {
						warn!("Encoding error from peer {} ({}): {}", peer, addr, e);
						//TODO(stevenroose) should ban
						disconnect_peer(&p2p, &mut peers, &event_tx, peer);
					}
					ReadError::WrongMagic(m) => {
						warn!("Peer {} ({}) uses bad network magic: 0x{:08x}", peer, addr, m);
						//TODO(stevenroose) should ban
						disconnect_peer(&p2p, &mut peers, &event_tx, peer);
					}
					ReadError::Tcp(e) => {
						warn!("Error reading from peer {} ({}): {}", peer, addr, e);
						disconnect_peer(&p2p, &mut peers, &event_tx, peer);
					}
				}
			}
		}

		// In between reading and writing, disconnect misbehaving peers.
		for peer in react.take_disconnects() {
			disconnect_peer(&p2p, &mut peers, &event_tx, peer);
		}

		// Then perform all I/O events for writing.
		for event in events.iter().filter(|e| e.is_readable() && e.token() != WAKE_TOKEN) {
			let peer = PeerId(event.token().0);
			let pio = or_continue!(peers.get_mut(&peer));
			trace!("Writable event for peer {}", peer);

			if let Err(e) = handle_write(&p2p, peer, pio) {
				let addr = pio.addr;
				drop(pio); // To be able to do `&mut peers`.

				warn!("Error writing to peer {} ({}): {}", peer, addr, e);
				disconnect_peer(&p2p, &mut peers, &event_tx, peer);
				continue;
			}

			if pio.buf_out.is_empty() && pio.queue_out.is_empty() {
				// Nothing more to write, we deregister the WRITABLE interest.
				trace!("Deregistering write for peer {}", peer);
				let interest = mio::Interest::READABLE;
				poll.registry().reregister(&mut pio.conn, mio::Token(peer.0), interest)
					.expect("TCP stream poll registry failed");
			}
		}

		// Handle any scheduled events.
		scheduler.handle_events_due(&p2p, &mut react);

		// Schedule all schedule actions queued by the logic.
		react.schedule_all(&mut scheduler);

		// Also disconnect bad peers.
		for peer in react.take_disconnects() {
			disconnect_peer(&p2p, &mut peers, &event_tx, peer);
		}

		// Then queue new outgoing messages.
		trace!("Scheduler and handler queued {} messages", react.len());
		for (peer, msg) in react.drain_messages() {
			let pio = or_continue!(peers.get_mut(&peer));
			queue_msg(&p2p, &mut react, &mut poll, peer, pio, msg);
		}

		assert_eq!(react.len(), 0, "the action queue should be empty!");
	}
}
