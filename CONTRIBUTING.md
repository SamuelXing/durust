# Contributing to durare

Thanks for your interest! Bug reports, feature requests, and pull requests are
all welcome.

## Development setup

You need Rust **1.88 or later** (the MSRV, enforced in CI).

```bash
git clone https://github.com/SamuelXing/durare
cd durare
cargo test
```

`cargo test` runs the suite against the in-memory and SQLite backends with no
setup at all. The Postgres portion of the suite is gated on `DATABASE_URL`; to
run it too, point that at any scratch database:

```bash
docker run -d --name durare-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo test
```

The suite creates and migrates the `dbos` schema itself.

## Before you submit

CI enforces all four of these — running them locally first saves a round trip:

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings   # includes the missing_docs gate
cargo test                                   # unit + integration + doc-tests
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

A few conventions:

- **Every public item is documented** (`#![warn(missing_docs)]` + CI). Prefer
  runnable doc examples; use `no_run` for snippets that need a live database,
  and avoid `ignore` — ignored examples rot.
- **User-visible changes get a `CHANGELOG.md` entry** under `[Unreleased]`.
- Tests for behavior touching storage should cover the backends it affects —
  in-memory, SQLite, and Postgres where applicable.
- Commit messages: a short imperative summary line; the body explains *why*.

## Releases

Maintainers cut releases following [RELEASING.md](RELEASING.md).

## License

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed under Apache-2.0 and MIT, without any additional terms or
conditions.
