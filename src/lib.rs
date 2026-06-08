#![allow(
    clippy::manual_strip,
    clippy::cloned_ref_to_slice_refs,
    clippy::match_like_matches_macro,
    clippy::large_enum_variant,
    clippy::should_implement_trait,
    clippy::too_many_arguments,
    clippy::ptr_arg
)]

// LIVEN library entry point

pub mod client;
pub mod codec;
pub mod config;
pub mod executor;
pub mod parser;
pub mod security;
pub mod server;
pub mod storage;
pub mod types;
