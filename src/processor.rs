
use std::{io, sync};
use std::collections::HashMap;
use std::sync::Arc;
use std::io::{Cursor, Read, Write};

use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::consensus::encode::{self, Decodable, Encodable};

use crate::{PeerId, P2P};

pub const WAKE_TOKEN: mio::Token = mio::Token(0);

/// Control message used to communicate with the processor thread.
pub enum Ctrl {
	/// Add a new peer.
	AddPeer {
		id: PeerId,
		conn: mio::net::TcpStream,
	}
}

struct PeerIo {
	conn: mio::net::TcpStream,
	in_buf: Vec<u8>,
}

#[derive(Debug)]
enum ReadError {
	DoS,
	Io(io::Error),
	Encode(encode::Error),
	WrongMagic(u32),
}

impl From<io::Error> for ReadError {
	fn from(e: io::Error) -> ReadError {
		ReadError::Io(e)
	}
}

impl From<encode::Error> for ReadError {
	fn from(e: encode::Error) -> ReadError {
		ReadError::Encode(e)
	}
}

fn handle_read(
	p2p: &Arc<P2P>,
	msg_tx: &sync::mpsc::Sender<(PeerId, NetworkMessage)>,
	id: PeerId,
	pio: &mut PeerIo,
	buf: &mut Vec<u8>,
) -> Result<(), ReadError> {
	trace!("buffer: len={}; cap={}", buf.len(), buf.capacity());
	buf.clear(); // Set the len to 0.

	// First check if we have some leftover of the last read.
	let peer_buf = &mut pio.in_buf;
	let had_peer_buf = !peer_buf.is_empty();
	if !peer_buf.is_empty() {
		buf.write_all(&peer_buf).expect("vecs don't error");
		peer_buf.clear();
	}

	// Then read from the stream.
	// To call read, we need to create space in the buffer. We set len to the max capacity
	let len = buf.len();
	unsafe { buf.set_len(buf.capacity()); }
	let count = pio.conn.read(&mut buf[len..])?;
	assert!(count + len <= buf.capacity());
	unsafe { buf.set_len(count + len); }
	trace!("Peer {} read {} bytes", id, count);

	if count == 0 {
		debug!("Peer {} read 0 bytes; peer seems disconnected", id);
		return Ok(());
		//TODO(stevenroose) disconnect here
		//return Err(Error::Disconnected);
	}

	// Then try to parse messages in the buffer.
	let full_buffer = buf.len() == buf.capacity();
	let mut cursor = Cursor::new(buf);
	loop {
		let pos = cursor.position() as usize;
		let raw_msg = match RawNetworkMessage::consensus_decode(&mut cursor) {
			Ok(m) => m,
			Err(encode::Error::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
				// To avoid DoS, make sure the message fits in our buffer.
				if pos == 0 && !had_peer_buf && full_buffer {
					// The message takes the full buffer!
					return Err(ReadError::DoS);
				}

				// The message is longer than the remainder of the buffer.
				//TODO(stevenroose) optimize this and avoid DoS
				let buf = cursor.into_inner();
				peer_buf.write_all(&buf[pos..]).expect("vecs don't error");
				break;
			}
			Err(encode::Error::UnrecognizedNetworkCommand(ref cmd)) => {
				debug!("{} Ignoring unknown '{}' message", id, cmd);
				continue;
			}
			Err(e) => return Err(e.into()),
		};
		if raw_msg.magic != p2p.config.network.magic() {
			return Err(ReadError::WrongMagic(raw_msg.magic));
		}

		//TODO(stevenroose) probably error here if queue is full
		//TODO(stevenroose) handle message first
		msg_tx.send((id, raw_msg.payload));

		if cursor.position() as usize == cursor.get_ref().len() {
			break;
		}
	}
	
	//TODO(stevenroose) does this make sense?
	//peer_buf.shrink_to_fit();
	Ok(())
}

/// The main function of the processing thread.
/// It's using non-blocking I/O so most of the time it's waiting for OS events.
pub fn processor(
	p2p: Arc<P2P>,
	mut poll: mio::Poll,
	ctrl_rx: sync::mpsc::Receiver<Ctrl>,
	in_tx: sync::mpsc::Sender<(PeerId, NetworkMessage)>,
	out_rx: sync::mpsc::Receiver<(PeerId, NetworkMessage)>,
) {
	let mut events = mio::Events::with_capacity(1024);
	let mut peers = HashMap::new();

	// This is our main I/O buffer. It's re-used for all TCP I/O.
	let mut buffer = Vec::with_capacity(bitcoin::consensus::encode::MAX_VEC_SIZE);

	loop {
		events.clear();
		poll.poll(&mut events, None).expect("poll error"); //TODO(stevenroose) handle?

		if p2p.quit.load(sync::atomic::Ordering::Relaxed) {
			info!("P2P processing thread received quit signal; exiting.");
			return;
		}

		// Filter over events that represent TCP streams.
		for event in events.iter().filter(|e| e.token() != WAKE_TOKEN) {
			let peer = PeerId(event.token().0);
			let pio = peers.get_mut(&peer).expect("token for non-existing peer");

			if event.is_readable() {
				trace!("Readable event for peer {}", peer);
				if let Err(e) = handle_read(&p2p, &in_tx, peer, pio, &mut buffer) {
					//TODO(stevenroose) handle
					error!("handle_read error: {:?}", e);
				}
			}

			if event.is_writable() {
				trace!("Writable event for peer {}", peer);
				//TODO(stevenroose) finish
			}
		}

		for ctrl in ctrl_rx.try_iter() {
			match ctrl {
				Ctrl::AddPeer { id, mut conn } => {
					// Register the TCP stream in our Poll.
					poll.registry().register(&mut conn, mio::Token(id.0), mio::Interest::READABLE)
						.expect("TCP stream poll registry failed");

					let pio = PeerIo {
						conn: conn,
						in_buf: Vec::new(),
					};
					assert!(peers.insert(id, pio).is_none(), "duplicate peer id: {}", id);
				}
			}
		}
	}
}
