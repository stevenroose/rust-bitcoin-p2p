


# Some design decisions made

## Components

The following components exist: 

- `AddressManager`
- `PeerManager`
- `PingManager`
- `Peer`

Every component is structured as follows:

### Public

* There is a `Config` struct that contains all configuration options for the
  manager.
* There is a "handle" struct that has the components name.
* Generally handles will be Arc'ed by the caller right after constructing.
  (The `P2P` utility f.e. only exposes Arc'ed handles.)
* Methods intended for internal use are exposed through a `XxxManagerControl`
  trait.

### Internal

* There is a `Processor` struct that will live inside the runtime, so that
  there is direct access to the internal fields without locks.
* Communication from the handle to the processor happens over channels.
* Sometimes we want to expose some data through the handle, this data
  will unfortunately have to be stored in a shared `State` behind a lock.
* Components can expose events through a `Dispatch` object stored in the
  processor, users can listen by calling the `add_listener` methods on the
  handles.
* There is a private `SendCtrl` trait that is used to actually send `Ctrl`
  messages. Usually it is implemented by the Manager itself and by the pair of
  `(chan::Sender<Ctrl>, erin::Waker)`. So that event listeners can be
  registered from within the processor without needing the manager's handle.
