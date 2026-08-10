//! Embeddable, disposable Function Service worker core.
//!
//! This crate deliberately contains no durable store or Sandbox Engine client.
//! Function Service owns durable history and fencing; a language runner owns
//! this core only for the lifetime of its sandbox process.

pub mod io;
pub mod model;
pub mod state_machine;

pub mod agent;
pub mod runtime;
