# Contributing to LivenDB

Thanks for your interest in contributing! Liven is a high-velocity embedded database engine written in Rust — every contribution helps make it faster, safer, and more capable.

## Getting Started

```bash
git clone https://github.com/livendb/liven
cd liven
cargo build
cargo test
```

For the full stack (including the web dashboard):
```bash
cd ui && npm ci --legacy-peer-deps && npm run build && cd ..
cargo build --release
```

## Development Workflow

1. **Fork** the repository and create a feature branch.
2. **Write** your code, tests, and update docs as needed.
3. **Run the checks** before pushing:
   ```bash
   cargo fmt --all -- --check
   cargo clippy --lib --tests -- -D warnings
   cargo test --lib --tests
   ```
4. **Submit** a pull request to `main`.

## Commit Style

- Use present-tense, concise messages: `add vector similarity index`
- Reference issues where applicable: `fix #42 — handle empty segment on restart`

## Testing

- Unit tests live alongside the code in `#[cfg(test)]` modules.
- Property tests use `proptest`; integration tests go in `tests/`.
- Benchmarks are in `benches/` and use `criterion`.

## Code Style

We follow standard Rust conventions enforced by `cargo fmt` and `cargo clippy`. Key guidelines:

- Prefer explicit error types over `unwrap()`/`expect()` in library code.
- Use `thiserror` for error definitions.
- Keep functions focused — extract helpers rather than growing deeply nested logic.
- Document public API surface with doc comments.

## Feature Flags

Liven uses Cargo feature flags for binary size control:

| Feature  | What it enables                  |
|----------|----------------------------------|
| `server` | REST API + WebSocket + Web UI    |
| `tui`    | Interactive terminal dashboard   |
| `tls`    | mTLS support                     |

The `full` feature enables everything. For embedded use, build with `--no-default-features`.

## Reporting Issues

- Use the [GitHub issue tracker](https://github.com/livendb/liven/issues).
- Include your OS, Rust version (`rustc --version`), and Liven version.
- Attach a minimal reproduction case when possible.

## Pull Requests

- Keep PRs focused — one feature or fix per pull request.
- Ensure all tests pass and formatting/lint checks are green.
- Add or update tests for behavior changes.
- Update relevant documentation.

## License

By contributing, you agree that your contributions will be licensed under the [SSPL 1.0](LICENSE-SSPL).
