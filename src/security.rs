//! Security posture: what durare trusts, exposes, and guarantees.
//!
//! Durable execution puts your application's state in a database and,
//! optionally, opens two operational surfaces — an admin HTTP server and a
//! conductor websocket client. This guide is the trust map: the invariants
//! the SDK maintains, the surfaces you choose to open, how the secrets you
//! hand it are handled, and how its dependencies are policed.
//!
//! # The dynamic-SQL invariant
//!
//! **No caller-supplied string ever becomes SQL text.** Every runtime value —
//! workflow ids and names, queue names, topics, event/stream keys, step
//! names, schedule names, filter values — reaches the database as a bind
//! parameter. The fragments that *are* interpolated into query strings are
//! compile-time constants: column lists, dialect keywords, and clauses
//! selected from `&'static str` tables by enum or boolean flags. The one
//! identifier that comes from configuration — the Postgres system-tables
//! schema — is validated as a plain identifier (`[A-Za-z_][A-Za-z0-9_]*`)
//! before it is stored, and rejected otherwise.
//!
//! The invariant is enforced from the outside rather than by pinning the
//! source: the `tests/security.rs` sweep pushes hostile strings (embedded
//! quotes, statement terminators, `DROP TABLE`, comment markers) through
//! every string-typed public input on both SQL backends and asserts they
//! round-trip verbatim as data, with the system tables intact afterwards.
//!
//! One boundary note: [`transaction`](crate::DurableContext::transaction)
//! steps run *your* SQL. The invariant covers every statement durare builds;
//! statements your step builds are yours to parameterize.
//!
//! # Secrets
//!
//! The SDK receives two secrets and holds both by the same rule — never
//! logged, never exposed through formatting:
//!
//! - **The database URL** is passed through to the sqlx pool and not retained;
//!   no durare log line or error message echoes it.
//! - **The conductor API key** lives in [`ConductorConfig`](crate::ConductorConfig),
//!   which deliberately implements no `Debug` — a `{:?}` of it is a compile
//!   error (pinned by a `compile_fail` doctest on the field). Connection
//!   failures log the transport error, never the URL that embeds the key.
//!
//! Inject both from the environment or a secret store at startup; neither
//! belongs in configuration files under version control.
//!
//! What the *database* sees is a different boundary: workflow inputs,
//! outputs, and step results are checkpointed to the system tables in
//! plaintext — that is what makes replay and cross-SDK tooling work. Anyone
//! who can read the database can read every payload, so the database sits
//! **inside** the trust boundary: protect it like the application data it
//! now holds (TLS to the server, least-privilege roles, encryption at rest),
//! and encrypt fields at the application layer before returning them from a
//! step if even database operators must not read them. Logs and trace spans
//! stay outside that boundary — span fields carry ids, names, and statuses,
//! never payload values.
//!
//! # Network exposure
//!
//! A plain engine listens on **nothing**: durare is a library, and every
//! network surface is opt-in behind a cargo feature.
//!
//! | Surface | Feature | Direction | Auth |
//! |---|---|---|---|
//! | [`AdminServer`](crate::AdminServer) | `admin` | inbound HTTP | none — network-layer only |
//! | [`Conductor`](crate::Conductor) | `conductor` | outbound `wss://` only | API key in the connect URL |
//! | Database | `postgres`/`sqlite` | outbound | whatever the URL carries — prefer TLS (`sslmode`) |
//!
//! The admin API is unauthenticated by design (it matches the other DBOS
//! SDKs, so shared tooling works) and binds all interfaces by default, which
//! orchestrator health probes require. Treat the port like a database port:
//! private network, firewall or network policy, and loopback binding via
//! [`AdminServer::start_on`](crate::AdminServer::start_on) when only the
//! machine itself should reach it:
//!
//! ```ignore
//! use std::net::Ipv4Addr;
//! let admin = AdminServer::start_on(engine.clone(), (Ipv4Addr::LOCALHOST, 3001).into()).await?;
//! ```
//!
//! The conductor opens no listener at all — it dials out over TLS and serves
//! management commands across that connection, so no inbound rule is needed.
//!
//! # Supply chain
//!
//! CI enforces two independent checks on every push and on a daily schedule:
//! `cargo audit` (RustSec advisories against the lockfile) and `cargo deny`
//! (advisories again, plus license allow-listing and registry/source
//! pinning — configuration in `deny.toml` at the repo root). An advisory or
//! an unvetted license entering the tree fails CI rather than shipping.
//! Dependency updates land deliberately: patch bumps ride the regular
//! release cadence; a new dependency or a major bump gets reviewed like code.
