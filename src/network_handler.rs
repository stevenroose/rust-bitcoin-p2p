use std::collections::HashMap;
use std::io::{self, Cursor, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;

use bitcoin::consensus::encode::{self, Decodable, Encodable};
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};

use crate::channel;
use crate::error::Error;
use crate::peer::{PeerId, TokenType};
use crate::WAKE_TOKEN;

/// All the data needed for the [network_manager] to handle I/O with a peer.
///
/// All this data is entirely owned by the network_manager.
pub struct PeerIo {
	pub conn: mio::net::TcpStream,
	pub network: bitcoin::Network,
	pub in_rx: channel::Receiver<NetworkMessage>,
	pub out_tx: channel::SyncSender<NetworkMessage>,
	pub msgh_waker: mio::Waker,
	pub in_buf: Vec<u8>,
	pub out_buf: Vec<u8>,
}

fn handle_read(id: PeerId, pio: &mut PeerIo, buf: &mut Vec<u8>) -> Result<(), Error> {
	trace!("buffer: len={}; cap={}", buf.len(), buf.capacity());
	buf.resize(buf.capacity(), 0);
	let mut inbound_buf = pio.out_buf;
	if !inbound_buf.is_empty() {
		if let Err(e) = buf.write_all(&inbound_buf) {
			// This should really not return an error.
			panic!("Error writing inbound peer {} buf into read buf: {}", id, e);
		}
		inbound_buf.clear();
	}
	let count = pio.conn.read(&mut buf[..])?;
	if count == 0 {
		debug!("Peer {} read 0 bytes; peer seems disconnected", id);
		return Ok(());
		//TODO(stevenroose) disconnect here
		//return Err(Error::Disconnected);
	}
	buf.resize(count, 0);
	trace!("Peer {} read {} bytes", id, count);
	let mut cursor = Cursor::new(buf);
	loop {
		let pos = cursor.position() as usize;
		let raw_msg =
			match RawNetworkMessage::consensus_decode(&mut cursor) {
				Ok(m) => m,
				Err(encode::Error::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
					// The message is longer than the remainder of the buffer.
					//TODO(stevenroose) optimize this and avoid DoS
					let buf = cursor.into_inner();
					if let Err(e) = pio.in_buf.write_all(&buf[pos..]) {
						panic!("Error writing remaining {} bytes of read buf into peer inbound buf: {}",
                        buf.len() - pos, e);
					}
					break;
				}
				Err(encode::Error::UnrecognizedNetworkCommand(ref cmd)) => {
					debug!("{} Ignoring unknown '{}' message", id, cmd);
					continue;
				}
				Err(e) => return Err(e.into()),
			};
		if raw_msg.magic != pio.network.magic() {
			return Err(Error::WrongNetworkMagic(raw_msg.magic));
		}

		pio.out_tx.send(raw_msg.payload);
		if let Err(e) = pio.msgh_waker.wake() {
			panic!("Error waking up message handler: {}", e);
		}

		//TODO(stevenroose) halt if too many messages

		if cursor.position() as usize == cursor.get_ref().len() {
			break;
		}
	}
	inbound_buf.shrink_to_fit();
	Ok(())
}

/// Returns [true] if all was written, [false] if there is more to write.
fn handle_write(id: PeerId, pio: &mut PeerIo, buf: &mut Vec<u8>) -> Result<(), Error> {
	// Check if we have some leftover bytes from last write.
	let mut peer_buf = &mut pio.out_buf;
	if !peer_buf.is_empty() {
		trace!("Peer {} write buffer of {} bytes. Writing...", id, peer_buf.len());
		let written = pio.conn.write(&peer_buf)?;
		trace!("Written {} bytes to peer {}", written, id);
		if written < peer_buf.len() {
			*peer_buf = peer_buf[written..].to_vec();
			trace!("Peer {} Still {} bytes of write buffer left", id, peer_buf.len());
			return Ok(());
		}
	}
	peer_buf.clear();

	while let Ok(msg) = pio.in_rx.try_recv() { //TODO(stevenroose) handle error?
		let raw_msg = RawNetworkMessage {
			magic: pio.network.magic(),
			payload: msg,
		};
		let msg_size = raw_msg.consensus_encode(&mut *buf).unwrap();
		trace!("Peer {} trying to write {} bytes...", id, msg_size);
		let written = pio.conn.write(buf)?;
		trace!("Peer {} written {} bytes", id, written);
		if written < msg_size {
			// Only create new peer buffer if we don't fit in the existing one.
			if msg_size - written > peer_buf.capacity() {
				*peer_buf = Vec::with_capacity(msg_size - written);
			}
			peer_buf.extend_from_slice(&buf[written..]);
			peer_buf.shrink_to_fit();
			break;
		}
		buf.clear();
	}
	Ok(())
}

// Specify for what kind of errors we should disconnect the TCP stream.
fn should_disconnect(err: &Error) -> bool {
	match err {
		Error::Disconnected => true,
		Error::WrongNetworkMagic(..) => true,
		Error::Encoding(..) => true,
		Error::Io(ref err) => match err.kind() {
			io::ErrorKind::UnexpectedEof => true,
			io::ErrorKind::ConnectionAborted => true,
			io::ErrorKind::ConnectionRefused => true,
			io::ErrorKind::ConnectionReset => true,
			io::ErrorKind::WouldBlock => false,
			_ => false,
		},
		_ => false,
	}
}

fn disconnect_peer(id: PeerId, pio: PeerIo, poll: &mio::Poll) {
	debug!("Disconnecting TCP for peer {} seen by network_handler", id);
	if let Err(e) = poll.registry().deregister(&mut pio.conn) {
		error!("Error deregistering poll while disconnecting peer {}: {}", id, e);
	}
	// Then close the TCP stream.
	if let Err(e) = pio.conn.shutdown(std::net::Shutdown::Both) {
		error!("Error closing TCP connection with {}: {}", id, e)
}

pub fn network_handler(
	mut poll: mio::Poll,
	new_peer_rx: channel::Receiver<(PeerId, PeerIo)>,
	quit: Arc<AtomicBool>,
) {
	trace!("network_handler started");

	let mut peers = HashMap::new();
	let mut events = mio::Events::with_capacity(1024);
	let mut buffer = Vec::with_capacity(1024 * 1024);

	loop {
		// Wait until an event happens.
		trace!("network_handler polling...");
		events.clear();
		match poll.poll(&mut events, None) {
			Ok(_) => {}
			Err(e) if e.kind() == io::ErrorKind::TimedOut => {
				//TODO(stevenroose) this might actually happen normally
				panic!("Unexpected io::TimedOut error from poll: {}", e);
			}
			Err(e) => {
				panic!("Poll returned error: {}", e);
			}
		};

		if quit.load(AtomicOrdering::Acquire) {
			debug!("network_handler thread shut down");
			break;
		}
		if events.is_empty() {
			panic!("Woke up without events, this shouldn't happen.");
		}

		for event in events.iter() {
			if event.token() == WAKE_TOKEN {
				// We were woken up by the waker. This means we have new peers.
				while let Ok((id, pio)) = new_peer_rx.try_recv() {
					poll.registry().register(
						&mut pio.conn,
						id.token(TokenType::NetworkIo),
						mio::Interest::READABLE | mio::Interest::WRITABLE,
					);
					peers.insert(id, pio);
				}
				continue;
			}

			let (id, token_tp) = PeerId::from_token(event.token());

			if token_tp == TokenType::Disconnect {
				disconnect_peer(id, peers.remove(&id).unwrap_or_else(|| {
					//TODO(stevenroose) should this be allowed?
					panic!(
						"network_handler woke up with token of unknown peer: {:?}",
						event.token()
					);
				}), &poll);
				continue;
			}

			let pio = peers.get(&id).unwrap_or_else(|| {
				//TODO(stevenroose) should this be allowed?
				panic!("network_handler woke up with token of unknown peer: {:?}", event.token());
			});

			if token_tp == TokenType::NetworkIo && event.is_readable() {
				trace!("network_handler got readable event for peer: {}", id);
				buffer.clear();
				if let Err(e) = handle_read(id, &mut pio, &mut buffer) {
					if e.is_would_block() {
						continue;
					}

					warn!("{} Error while reading from TCP stream: {}", id, e);
					if should_disconnect(&e) {
						info!("Disconnecting peer {}", id);
						drop(pio);
						disconnect_peer(id, peers.remove(&id).unwrap(), &poll);
					}
					continue;
				}
			}

			if token_tp == TokenType::NetworkWaker || event.is_writable() {
				trace!("network_handler got writable event ({:?}) for peer: {}", token_tp, id);
				buffer.clear();
				if let Err(e) = handle_write(id, &mut pio, &mut buffer) {
					if e.is_would_block() {
						continue;
					}

					warn!("{} Error while writing to TCP stream: {}", id, e);
					if should_disconnect(&e) {
						info!("Disconnecting peer {}", id);
						disconnect_peer(id, peers.remove(&id).unwrap(), &poll);
					}
					continue;
				}
			}
		}

		buffer.resize(1024 * 1024, 0);
		buffer.shrink_to_fit();
	}
}
