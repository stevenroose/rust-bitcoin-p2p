//! A basic implementation of a thread wait group.

use std::sync::{Arc, Condvar, Mutex};

/// Enables a thread to wait for other threads to finish.
///
/// Add threads with Clone and remove with Drop.
pub struct WaitGroup {
	inner: Arc<WaitGroupInner>,
}

/// [WaitGroup] inner state.
struct WaitGroupInner {
	count: Mutex<usize>,
	condvar: Condvar,
}

impl WaitGroup {
	/// Create a new [WaitGroup].
	pub fn new() -> WaitGroup {
		WaitGroup {
			inner: Arc::new(WaitGroupInner {
				count: Mutex::new(1),
				condvar: Condvar::new(),
			}),
		}
	}

	/// Wait (and block) for all other threads in the waitgroup to finish.
	pub fn wait(self) {
		if *self.inner.count.lock().unwrap() == 1 {
			return;
		}

		let inner = self.inner.clone();
		drop(self);

		let mut count = inner.count.lock().unwrap();
		while *count > 0 {
			count = inner.condvar.wait(count).unwrap();
		}
	}
}

impl Drop for WaitGroup {
	fn drop(&mut self) {
		let mut count = self.inner.count.lock().unwrap();
		*count -= 1;

		if *count == 0 {
			self.inner.condvar.notify_all();
		}
	}
}

impl Clone for WaitGroup {
	fn clone(&self) -> WaitGroup {
		let mut count = self.inner.count.lock().unwrap();
		*count += 1;

		WaitGroup {
			inner: self.inner.clone(),
		}
	}
}
