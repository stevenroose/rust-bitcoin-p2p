
mod p2p_ext;
pub mod prelude;
use prelude::*;

pub fn test_config() -> Config {
    Config {
	    network: Network::Regtest,
        ..Default::default()
    }
}

pub fn run_client(config: Config) -> Arc<P2P> {
    let ret = P2P::new(config);
    ret.start().unwrap();
    ret
}

