use std::time::Duration;

use rand_distr::{Distribution, Poisson};

/// Get a poisson sample averaging the given number.
pub fn poisson_u64(avg: u64) -> u64 {
	debug_assert!(avg > 0);
	let dist = Poisson::new(avg as f64).expect("positive");
	dist.sample(&mut rand::thread_rng()) as u64
}

/// Get a poisson sample averaging the given [Duration].
pub fn poisson_duration(avg: Duration) -> Duration {
	Duration::from_secs(poisson_u64(avg.as_secs()))
}

/// Get the value and call `continue` when it's [None];
macro_rules! or_continue {
	($opt:expr) => {
		match $opt {
			Some(v) => v,
			None => continue,
		}
	};
}
