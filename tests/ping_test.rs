
mod common;
use common::prelude::*;

#[test]
fn ping_test() {
    let sbj = common::run_client(Config {
        ping_interval: Some(Duration::from_millis(100)),
        ..common::test_config()
    });

    let agt = common::run_client(Config {
        ping_interval: None,
        ..common::test_config()
    });


}
