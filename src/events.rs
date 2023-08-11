

use std::fmt::Debug;
use std::sync::mpsc;

use crossbeam_channel as chan;

/// The return type of the [Listener::event] method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenResult {
	/// All normal.
	Ok,
	/// The listener asks for itself to be removed.
	RemoveMe,
}

/// Trait used to listen to events.
///
/// The following types implement this trait:
/// - [crossbeam_channel::Sender<Event>]
/// - [std::sync::mpsc::Sender<Event>]
/// - [std::sync::mpsc::SyncSender<Event>] (dropping events when full)
/// - [dyn for <'e> FnMut(&'e Event) -> ListenResult + Send], a closure or
///   function taking an Event, returning a [ListenResult] indicating when
///   the listener should be removed.
///
/// Read the documentation for the [event] method carefully when
/// implementing this trait.
pub trait Listener<E>: Send + 'static {
	/// Handle a new incoming event.
	///
	/// It's very important that the execution of this method is fast and
	/// certainly never blocks the thread. That's why it's probably advised to
	/// not implement this trait on your actual handler but on a channel to
	/// queue the events or an async function.
	fn event(&mut self, event: &E) -> ListenResult;
}

impl<E: Send + Clone + Debug + 'static> Listener<E> for chan::Sender<E> {
	fn event(&mut self, event: &E) -> ListenResult {
		match self.try_send(event.clone()) {
			Ok(_) => ListenResult::Ok,
			Err(chan::TrySendError::Full(e)) => {
				warn!("Sync channel event listener full; dropping event: {:?}", e);
				ListenResult::Ok
			}
			Err(chan::TrySendError::Disconnected(_)) => ListenResult::RemoveMe,
		}
	}
}

impl<E: Send + Clone + 'static> Listener<E> for mpsc::Sender<E> {
	fn event(&mut self, event: &E) -> ListenResult {
		match self.send(event.clone()) {
			Ok(()) => ListenResult::Ok,
			Err(mpsc::SendError(_event)) => ListenResult::RemoveMe,
		}
	}
}

impl<E: Send + Clone + Debug + 'static> Listener<E> for mpsc::SyncSender<E> {
	fn event(&mut self, event: &E) -> ListenResult {
		match self.try_send(event.clone()) {
			Ok(()) => ListenResult::Ok,
			Err(mpsc::TrySendError::Full(e)) => {
				warn!("Sync channel event listener full; dropping event: {:?}", e);
				ListenResult::Ok
			}
			Err(mpsc::TrySendError::Disconnected(_event)) => ListenResult::RemoveMe,
		}
	}
}

impl<E, F> Listener<E> for F
where
	F: for<'e> FnMut(&'e E) -> ListenResult + Send + 'static,
{
	fn event(&mut self, event: &E) -> ListenResult {
		self(event)
	}
}

/// A type that emits certain types of events.
pub trait EventSource<E> {
	fn add_listener(&self, listener: Box<dyn Listener<E>>);
}

/// An event dispatch.
pub struct Dispatch<E> {
	listeners: Vec<Box<dyn Listener<E>>>,
}

impl<E: 'static> Dispatch<E> {
	/// Create a new dispatch.
	pub fn new() -> Dispatch<E> {
		Dispatch {
			listeners: Vec::new(),
		}
	}

	/// Add a new listener to the dispatch.
	pub fn add_listener(&mut self, listener: Box<dyn Listener<E>>) {
		self.listeners.push(listener);
	}

	/// Dispatch a new event to all listeners.
	pub fn dispatch(&mut self, event: &E) {
		// A loop that allows removing listeners efficiently.
		let mut i = 0;
		while i < self.listeners.len() {
			if self.listeners[i].event(event) == ListenResult::RemoveMe {
				let _ = self.listeners.swap_remove(i);
				// don't increment i because i is replaced with last element
				// self.listeners.len() automatically decreased
			} else {
				i += 1;
			}
		}
	}
}
