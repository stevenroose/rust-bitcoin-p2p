#[macro_use]
extern crate log;

use std::thread;
use std::time::Duration;

use bitcoin::network::constants::ServiceFlags;
use bitcoin_p2p::*;

fn setup_logger() {
	fern::Dispatch::new()
		.format(|out, message, record| {
			out.finish(format_args!(
				"{}[{}][{}] {}",
				chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
				record.target(),
				record.level(),
				message
			))
		})
		.level(log::LevelFilter::Trace)
		.level_for("mio", log::LevelFilter::Debug)
		.chain(std::io::stdout())
		.apply()
		.expect("logger");
}



fn main() {
    info!("Setting up logging...");
	setup_logger();

    info!("Instantiating...");
	let p2p = P2P::new(Config {
		network: bitcoin::Network::Signet,
		ping_interval: Duration::from_secs(3 * 60),
		services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
		..Default::default()
	}).unwrap();
    p2p.start();
    info!("Sleeping...");
	thread::sleep(Duration::from_secs(3));

	info!("Connecting TCP...");
	let conn1 = mio::net::TcpStream::connect("127.0.0.1:38333".parse().unwrap()).unwrap();
	thread::sleep(Duration::from_secs(3));
	info!("Adding peer");
	let _p1 = p2p.add_peer(conn1, PeerType::Outbound).expect("adding peer");
	thread::sleep(Duration::from_secs(3));

	// Add logger.
	// fn log_event(event: &Event) -> bool {
	// 	trace!("New event received: {:?}", event);
	// 	true
	// }
	let log_event = |event: &Event| {
		trace!("New event received: {:?}", event);
		true
	};
	info!("Adding listener");
	p2p.add_listener(log_event).unwrap();

	info!("Creating channel");
	let receiver = p2p.create_listener_channel().unwrap();
	for event in receiver.iter() {
		trace!("Event: {:?}", event);
		if let Event::Connected(peer) = event {
			info!("Peer {} connected!", peer);
		}

		if let Event::Message(peer, msg) = event {
			debug!("Received {} message from {}", msg.cmd(), peer);
		}
	}
}
