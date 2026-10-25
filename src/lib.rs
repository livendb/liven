// LIVEN — High-Velocity Embedded Database
//
// Copyright 2026 LIVEN Maintainers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
#[cfg(debug_assertions)]
mod power_loss;
pub mod query;
pub mod security;
pub mod storage;
pub mod sysinfo;
pub mod types;
pub use embed::Liven;

#[cfg(feature = "server")]
pub mod server;
