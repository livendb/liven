#![forbid(unsafe_code)]
#![allow(
    clippy::manual_strip,
    clippy::cloned_ref_to_slice_refs,
    clippy::match_like_matches_macro,
    clippy::large_enum_variant,
    clippy::should_implement_trait,
    clippy::too_many_arguments,
    clippy::ptr_arg,
    clippy::vec_init_then_push
)]

mod cli;

#[cfg(feature = "tui")]
mod tui;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cli::run_cli().await
}
