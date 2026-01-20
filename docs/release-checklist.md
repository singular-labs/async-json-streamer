# Release checklist

## Pre-release

- [ ] Update `CHANGELOG.md` with release date and highlights.
- [ ] Ensure `Cargo.toml` metadata is accurate (repo, docs, license, keywords).
- [ ] Verify MSRV (Rust 1.74+) on CI.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo clippy --all-targets --all-features -- -D warnings`.
- [ ] Run `cargo test --all-features`.
- [ ] Run `cargo doc --no-deps`.
- [ ] Review README examples and API docs.

## Publish

- [ ] `cargo publish --dry-run`
- [ ] `cargo publish`
- [ ] Create git tag `v0.1.0` and GitHub release notes.

## Post-release

- [ ] Update crates.io README (verify badges).
- [ ] Announce release and link docs.rs.
