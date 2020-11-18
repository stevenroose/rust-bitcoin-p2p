
bitcoin-p2p
===========


This is a Rust library implementation of the Bitcoin p2p protocol.


# Features

- [BIP 0014: Protocol Version and User Agent](https://github.com/bitcoin/bips/blob/master/bip-0014.mediawiki)
- [BIP 0031: Pong Messages](https://github.com/bitcoin/bips/blob/master/bip-0031.mediawiki)
- [BIP 0060: Fixed Length "version" Message (Relay-Transactions Field)](https://github.com/bitcoin/bips/blob/master/bip-0060.mediawiki)
- [BIP 0130: sendheaders message](https://github.com/bitcoin/bips/blob/master/bip-0130.mediawiki)
- [BIP 0144: Segregated Witness (Peer Services)](https://github.com/bitcoin/bips/blob/master/bip-0144.mediawiki)


## TODO

- peer banning
- fee filters: https://github.com/bitcoin/bips/blob/master/bip-0133.mediawiki
- reject peers that don't support segwit (ver 70013) (rust-bitcoin can't serialize without witness)
- finish compact blocks support after https://github.com/rust-bitcoin/rust-bitcoin/pull/249
- addrv2 https://github.com/bitcoin/bips/blob/master/bip-0155.mediawiki
  https://github.com/rust-bitcoin/rust-bitcoin/pull/449
- block filters
- support WASM-threads with compile flags

