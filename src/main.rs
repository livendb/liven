#![forbid(unsafe_code)]
#![allow(
    clippy::manual_strip,
    clippy::cloned_ref_to_slice_refs,
    clippy::match_like_matches_macro,
    clippy::large_enum_variant,
    clippy::should_implement_trait,
    clippy::too_many_arguments,
    clippy::ptr_arg,
    clippy::vec_init_then_push,
    clippy::collapsible_if
)]

mod cli;

#[cfg(feature = "tui")]
mod tui;

use tracing_subscriber::{EnvFilter, fmt};

/// Initialize logging with tracing-subscriber
/// Defaults to info level, can be overridden with RUST_LOG environment variable
fn init_logging() {
    // Default to info level, override with RUST_LOG if set
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt::fmt()
        .with_env_filter(filter)
        .with_target(true) // Show module names
        .with_thread_ids(false) // Keep output clean
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check if this is a status command - if so, run it without logging
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "status" {
        // Run status command without initializing logging
        cli::run_cli().await
    } else {
        // Initialize logging for all other commands
        init_logging();
        cli::run_cli().await
    }
}
