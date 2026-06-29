// LIVEN — High-Velocity Embedded Database
//
// Copyright (c) 2026, LIVEN Maintainers <team@livendb.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//
// Commercial licenses are available. Contact: team@livendb.com

// Crate-level clippy allowances for intentional or pre-existing patterns.
#![allow(
    clippy::manual_strip,
    clippy::cloned_ref_to_slice_refs,
    clippy::match_like_matches_macro,
    clippy::needless_return,
    clippy::module_name_repetitions,
    clippy::ptr_arg,
    clippy::empty_line_after_doc_comments,
    clippy::manual_div_ceil,
    clippy::large_enum_variant,
    clippy::should_implement_trait,
    clippy::too_many_arguments,
    clippy::collapsible_if,
    clippy::vec_init_then_push
)]
#![doc = include_str!("rust-crate-api.md")]

pub mod client;
pub mod codec;
pub mod config;
pub mod embed;
pub mod error;
pub mod executor;
pub mod import_export;
pub mod parser;
pub mod query;
pub mod security;
pub mod storage;
pub mod sysinfo;
pub mod types;
pub use embed::Liven;

#[cfg(feature = "server")]
pub mod server;
