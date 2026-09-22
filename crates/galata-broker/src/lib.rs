//! Where normalised events leave the process.
//!
//! ```text
//!   markets.<venue>.<ticker>.<kind>     one instrument, one dataset
//!   status.<venue>                      what a capture process says about
//!                                       itself, on its own root
//! ```
//!
//! **Two roots so that one can be taken without the other.** A dashboard
//! subscribes `status.>` and is granted no market subject at all: market data
//! for a screen comes from the record, which holds it bounded, because *an
//! identity that could read every venue's firehose is exactly what a password
//! on an operator's laptop should not be*. See [`table`](grants::table).
//!
//! **This crate depends on [`galata_wire`] and nothing else of the workspace.**
//! A component that reads the stream links no columnar format to do it — which
//! is the wall the predecessor's `algo-fast` reached for and did not get,
//! because eighteen crates reached the bus through the crate that also owned
//! the archive writer.
//!
//! # The boot asymmetry
//!
//! ```text
//!   Unreachable   the broker did not answer. An OUTAGE, which the record
//!                 survives: archive-before-publish means capture keeps
//!                 running, measured over nine hours with no broker at all.
//!
//!   Rejected      the broker answered and refused this identity. A
//!                 MISCONFIGURATION. It will never fix itself, and the status
//!                 surface that would report it is a publish too.
//! ```
//!
//! Both used to arrive as one error, which is why *running for a day archiving
//! everything and publishing nothing* looked exactly like *running for a day
//! with a broker outage*. [`ConnectRefusal`] is the only place they are told
//! apart, and it is told once, at connect.
//!
//! **Publishing and consuming are different authorities.** [`NatsPublisher`]
//! and [`NatsSubscriber`] hold separate connections: datawatch publishes and
//! never consumes, and a component that only folds should not hold a handle
//! that can write.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]

pub mod encode;
pub mod grants;
pub mod identity;
pub mod publisher;
pub mod subject;
pub mod subscriber;

pub use encode::{DecodeError, decode, encode};
pub use grants::{Grant, Grants, ROOTS, password_var, to_nats_config};
pub use identity::BrokerIdentity;
pub use publisher::{ConnectRefusal, NatsPublisher, NullPublisher, PublishError, Publisher};
pub use subject::Subject;
pub use subscriber::NatsSubscriber;
