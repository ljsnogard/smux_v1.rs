#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
#![feature(min_specialization)]
#![no_std]

#[cfg(test)]
extern crate std;

pub mod connection;
pub mod handshake;
