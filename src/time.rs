
use std::time::{UNIX_EPOCH, Duration, SystemTime};

/// Extension crate for time types.
pub(crate) trait TimeExt {
	/// Create a time from a block timestamp.
	fn from_block_time(block_time: u32) -> Self;

	/// Return the block timestamp for this time.
	fn block_time(&self) -> u32;

	/// Returns the duration since the other time, returning the zero duration
	/// if the other time is in the future.
	fn saturating_duration_since(&self, other: SystemTime) -> Duration;

	/// Same as [SystemTime::elapsed], but saturating to 0 if negative.
	fn saturating_elapsed(&self) -> Duration;
}

impl TimeExt for SystemTime {
	fn from_block_time(block_time: u32) -> Self {
		UNIX_EPOCH + Duration::from_secs(block_time as u64)
	}

	fn block_time(&self) -> u32 {
		self.duration_since(UNIX_EPOCH).unwrap().as_secs().try_into().expect("block time overflow")
	}

	fn saturating_duration_since(&self, other: SystemTime) -> Duration {
		self.duration_since(other).unwrap_or_default()
	}

	fn saturating_elapsed(&self) -> Duration {
		SystemTime::now().saturating_duration_since(*self)
	}
}

