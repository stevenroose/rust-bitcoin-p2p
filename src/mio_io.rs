
use std::{io, thread};
use std::sync::{atomic, mpsc};

use mio;

pub const WAKE_TOKEN: mio::Token = mio::Token(0);

/// An error from calling the [WakerSender::send] method.
#[derive(Debug)]
pub enum WakerSenderError<T> {
	/// An error sending on the sender.
	Send(mpsc::SendError<T>),
	/// An error waking up the waker.
	Wake(io::Error),
}

/// A wrapper of an [mpsc::Sender] with a `mio` waker so that a thread can be
/// woken up when an item is sent on the channel.
#[derive(Debug)]
pub struct WakerSender<T> {
	sender: mpsc::Sender<T>,
	waker: mio::Waker,
}

impl<T> WakerSender<T> {
	/// Create a new [WakerSender].
	pub fn new(sender: mpsc::Sender<T>, waker: mio::Waker) -> WakerSender<T> {
		WakerSender {
			sender: sender,
			waker: waker,
		}
	}

	/// Send on the channel.
	pub fn send(&self, item: T) -> Result<(), WakerSenderError<T>> {
		self.sender.send(item).map_err(WakerSenderError::Send)?;
		self.waker.wake().map_err(WakerSenderError::Wake)?;
		Ok(())
	}
}

impl crate::Listener for WakerSender<crate::Event> {
	fn event(&mut self, event: &crate::Event) -> crate::ListenerResult {
		match self.send(event.clone()) {
			Ok(()) => crate::ListenerResult::Ok,
			// The channel disconnected.
			Err(WakerSenderError::Send(_)) => crate::ListenerResult::RemoveMe,
			// The waker has I/O problems, the mio::Poll might no longer exist.
			Err(WakerSenderError::Wake(_)) => crate::ListenerResult::RemoveMe,
		}
	}
}

//TODO(stevenroose) need atomic?
#[derive(Debug)]
pub struct TokenTally {
    tally: atomic::AtomicUsize,
}

impl TokenTally {
    /// Create a new [TokenTally] that starts with value 1.
    pub fn new() -> TokenTally {
        TokenTally {
            tally: atomic::AtomicUsize::new(WAKE_TOKEN.0 + 1),
        }
    }

    /// Get the next token to use.
    pub fn next(&self) -> mio::Token {
        let t = self.tally.fetch_add(1, atomic::Ordering::AcqRel);
        assert_ne!(t, usize::max_value(), "mio token overflow");
        mio::Token(t)
    }
}

/// Trait to be implemented by processors to be managed by a [ProcessorThread].
pub trait IoProcessor: Send + 'static {
    /// This method will be called whenever the thread wakes up.
    /// It is up to the processor to determine if there are any events relevant
    /// for this processor. Note that any [Waker] using [WAKE_TOKEN] wakes up
    /// this thread also.
    fn wakeup(&mut self, tt: &TokenTally, rg: &mio::Registry, ev: &mio::Events);
}

pub enum ProcessorThreadError {
    AlreadyShutdown,
	Io(io::Error),
    Other(String),
}

impl From<io::Error> for ProcessorThreadError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// A thread that can execute multiple [IoProcessor] instances.
pub struct ProcessorThread {
    /// Thread to use to send new processors to the thread.
    /// Closing this channel will cause the thread to shut down.
    proc_tx: mpsc::Sender<Box<dyn IoProcessor>>,
    registry: mio::Registry,
    waker: mio::Waker,
    join_handle: thread::JoinHandle<()>,
}

impl ProcessorThread {
    /// Create a new named [ProcessorThread].
    pub fn new(name: String) -> Result<ProcessorThread, ProcessorThreadError> {
        let mut poll = mio::Poll::new()?;
        let rg = poll.registry().try_clone()?;
        let (proc_tx, proc_rx) = mpsc::channel::<Box<dyn IoProcessor>>();
        let waker = mio::Waker::new(poll.registry(), WAKE_TOKEN)?;

        let jh = thread::Builder::new().name(name).spawn(move || {
            let mut ev = mio::Events::with_capacity(1024);
            let tt = TokenTally::new();
            let mut procs = Vec::new();
            
            loop {
                ev.clear();

                //TODO(stevenroose) timeout, probably do scheduler here
                poll.poll(&mut ev, None).expect("poll error");

                // Start with our own mgmt channel so that we shutdown fast
                // and give new procs a chance to register tokens right away.
                loop {
                    match proc_rx.try_recv() {
                        Ok(new_proc) => procs.push(new_proc),
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => return,
                    }
                }

                for proc in procs.iter_mut() {
                    proc.wakeup(&tt, poll.registry(), &ev);
                }
            }
        })?;

        Ok(ProcessorThread {
            proc_tx: proc_tx,
            registry: rg,
            waker: waker,
            join_handle: jh,
        })
    }

    /// Try to add the processor to the thread.
    pub fn add_processor(&self, proc: Box<dyn IoProcessor>) -> Result<(), ProcessorThreadError> {
        self.proc_tx.send(proc).map_err(|_| ProcessorThreadError::AlreadyShutdown)?;
        self.waker.wake()?;
        Ok(())
    }

    /// Create a new waker for this thread.
    pub(crate) fn waker(&self) -> Result<mio::Waker, io::Error> {
        mio::Waker::new(&self.registry, WAKE_TOKEN)
    }

    /// Will trigger shutdown and block until it is finished.
    pub fn shutdown(self) -> Result<(), ProcessorThreadError> {
        drop(self.proc_tx);
        self.waker.wake()?;
        self.join_handle.join()
            .map_err(|e| ProcessorThreadError::Other(format!("error joining thread: {:?}", e)))?;
        Ok(())
    }
}
