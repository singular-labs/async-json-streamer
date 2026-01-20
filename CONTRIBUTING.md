# Contributing

Thanks for contributing to asyncjsonstream.

## Development setup

- Install stable Rust and Cargo.
- Clone the repository.

```bash
cargo test
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

## Pull requests

- Keep changes focused and well-scoped.
- Add tests for bug fixes and new behavior.
- Update docs and CHANGELOG when appropriate.
- Ensure `cargo fmt` and `cargo clippy` are clean.

## Reporting issues

Please include:

- Rust version (`rustc -V`)
- Platform (OS + CPU)
- A minimal reproduction case
- Expected vs actual behavior

## Code of conduct

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).
