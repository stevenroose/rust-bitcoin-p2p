#[macro_use]
extern crate log;

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
	setup_logger();

	let p2p = P2P::new(Config {
		network: bitcoin::Network::Regtest,
		ping_interval: Duration::from_secs(3 * 60),
		services: ServiceFlags::NETWORK | ServiceFlags::WITNESS,
		..Default::default()
	}).unwrap();

	let conn1 = mio::net::TcpStream::connect("127.0.0.1:18444".parse().unwrap()).unwrap();
	let _p1 = p2p.add_peer(conn1, PeerType::Outbound).expect("adding peer");

	let receiver = p2p.create_listener_channel();
	for event in receiver.iter() {
		info!("Received event: {:?}", event);
	}
}
