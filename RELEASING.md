# Releasing

`durare` is a two-crate workspace: the proc-macro crate **`durare-macros`** and
the library **`durare`**, which depends on it. They are versioned in lockstep and
published together, macros first.

During `0.x` the API is allowed to change: a release with breaking changes bumps
the **minor** version (`0.1 → 0.2`), a backward-compatible release bumps the
**patch** version (`0.1.0 → 0.1.1`).

## One-time setup

- A [crates.io](https://crates.io) account, added as an owner of both crates
  (`cargo owner --list durare`).
- A local API token: `cargo login` (paste the token when prompted — it is not
  echoed). Treat the token as a secret; if it is ever exposed, revoke it at
  <https://crates.io/settings/tokens> and log in again.

## Steps

### 1. Bump the version

Set the same `version` in both crates, and the macro dependency's version to
match:

- `Cargo.toml` → `[package] version`
- `durare-macros/Cargo.toml` → `[package] version`
- `Cargo.toml` → `durare-macros = { path = "durare-macros", version = "X.Y" }`

Keep `rust-version` (MSRV) accurate in both manifests if the floor moved; CI has a
job pinned to it.

### 2. Update the changelog

In `CHANGELOG.md`, rename the `## [Unreleased]` heading to `## [X.Y.Z] - YYYY-MM-DD`
and update the link reference at the bottom to point at the tag. Start a fresh
`## [Unreleased]` section for the next cycle.

### 3. Pre-flight checks

Run the same gates CI does, plus a package inspection:

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings      # also enforces #![warn(missing_docs)]
cargo test                                      # in-memory + sqlite; Postgres if DATABASE_URL is set
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# Inspect exactly what will ship — no internal docs, CI config, or planning files:
cargo package --list -p durare | grep -E '^\.github/|^\.cargo/|API_REVIEW|ROADMAP|PARITY' \
  && echo "!! internal files would ship — fix [package] exclude" || echo "package contents clean"

# Dry-run both publishes (no upload). A clean tree is required; see the note below.
cargo publish -p durare-macros --dry-run
cargo publish -p durare --dry-run
```

### 4. Commit and tag

Commit the version/changelog changes and merge to `main` before publishing, so
the tag points at exactly what ships:

```bash
git tag -a vX.Y.Z -m "durare X.Y.Z"
git push origin vX.Y.Z
```

### 5. Publish — macros first

The order matters: `durare` depends on `durare-macros`, so the macro version must
already be on the registry when `durare` is verified.

```bash
cargo publish -p durare-macros
cargo publish -p durare
```

### 6. After publishing

- Create a GitHub Release from the tag, using the changelog section as the body.
- `docs.rs` builds the API docs automatically; confirm <https://docs.rs/durare>
  came up green.

## Gotchas

- **Publish order is not optional.** Publishing `durare` before `durare-macros`
  (or before the new macro version is indexed) fails verification — the macro
  dependency cannot be resolved from the registry.
- **Keep internal files out of the tarball.** `[package] exclude` in `Cargo.toml`
  drops `.github/`, `.cargo/`, and the internal planning docs (`API_REVIEW.md`,
  `ROADMAP.md`, `PARITY.md`). `cargo package --list` is the source of truth for
  what ships — check it, don't assume.
- **Do not paper over a dirty tree with `--allow-dirty`.** `cargo publish` refuses
  to package uncommitted changes that would end up in the crate. If it complains,
  either commit the change or add the path to `exclude` — reach for `--allow-dirty`
  only for a file you have deliberately, verifiably decided to ship as-is.
- **A published version is immutable.** You can `cargo yank` a bad release (which
  stops new dependents from selecting it) but you cannot overwrite or delete it.
  Dry-run first.
- **Crate names are permanent.** Once published, a name cannot be freed or
  transferred to a different crate; ownership can only be handed to another
  account via `cargo owner`.
