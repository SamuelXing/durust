//! Transactional steps: SQL writes and the step checkpoint in **one** commit —
//! exactly-once, even across a crash.
//!
//! # The problem
//!
//! An ordinary [step](crate::DurableContext::step) performs its side effect,
//! *then* commits its checkpoint — two writes, and a crash between them
//! re-runs the step on replay. That is the [at-least-once
//! window](crate::durability#the-at-least-once-window), and for most effects
//! (HTTP calls, emails) idempotency keys are the answer. But when the effect
//! *is a write to the workflow database*, there is a better answer: run the
//! SQL **inside the same database transaction as the checkpoint**. Either both
//! commit or neither does — there is no window. That is what
//! [`DurableContext::transaction`] does.
//!
//! This example runs against a real SQLite database. Note what the second
//! `start` under the same workflow id does — and does not do — to the balance:
//!
//! ```
//! use durare::{DurableContext, DurableEngine, Result, SqliteProvider, WorkflowOptions, params};
//! use std::sync::Arc;
//!
//! #[durare::workflow]
//! async fn transfer(ctx: DurableContext, amount: i64) -> Result<i64> {
//!     ctx.transaction("move_funds", move |tx| Box::pin(async move {
//!         // Demo setup — a real app would have its schema already.
//!         tx.execute("CREATE TABLE IF NOT EXISTS accounts (name TEXT PRIMARY KEY, balance INTEGER)", &params![]).await?;
//!         tx.execute("INSERT OR IGNORE INTO accounts VALUES ('alice', 100), ('bob', 100)", &params![]).await?;
//!
//!         // The writes and this step's checkpoint commit atomically.
//!         tx.execute("UPDATE accounts SET balance = balance - ? WHERE name = 'alice'", &params![amount]).await?;
//!         tx.execute("UPDATE accounts SET balance = balance + ? WHERE name = 'bob'", &params![amount]).await?;
//!         let row = tx.query_one("SELECT balance FROM accounts WHERE name = 'bob'", &params![]).await?;
//!         Ok(row.get::<i64>("balance"))
//!     })).await
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! # let path = std::env::temp_dir().join(format!("durare-tx-guide-{}.db", std::process::id()));
//! # for ext in ["", "-wal", "-shm"] {
//! #     std::fs::remove_file(format!("{}{ext}", path.display())).ok();
//! # }
//! # let url = format!("sqlite://{}", path.display());
//! let engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
//!
//! let handle = engine.start_with(Transfer, 30, WorkflowOptions::with_id("tx-1")).await?;
//! assert_eq!(handle.await?, 130); // bob: 100 + 30
//!
//! // Same workflow id again: the recorded outcome is returned — the SQL
//! // does NOT run twice. Bob has 130, not 160.
//! let replay = engine.start_with(Transfer, 30, WorkflowOptions::with_id("tx-1")).await?;
//! assert_eq!(replay.await?, 130);
//! # for ext in ["", "-wal", "-shm"] {
//! #     std::fs::remove_file(format!("{}{ext}", path.display())).ok();
//! # }
//! # Ok(())
//! # }
//! ```
//!
//! # How it works
//!
//! The body runs on the workflow database's own pool. The engine opens one
//! transaction, hands the body a [`Tx`], and — after the body returns —
//! inserts the step's checkpoint **inside that same transaction**, then
//! commits. On replay, the recorded outcome is read back and returned without
//! running the body at all. One consequence of the single-transaction model:
//! the tables you touch must live in the **same database** as the `dbos`
//! system schema.
//!
//! Transactions require a SQL backend — [`PostgresProvider`] or
//! [`SqliteProvider`]. On [`InMemoryProvider`] they return an error.
//!
//! # Failure semantics
//!
//! If the body returns an error, its SQL **rolls back** — a failed
//! transactional step never leaves partial writes. The *error itself* is still
//! checkpointed (in a separate write, outside the aborted transaction), so the
//! failure is durable too: a replay yields the same error without re-running
//! the body, exactly like an ordinary failed step.
//!
//! # Conflicts and isolation
//!
//! [`TransactionOptions`] selects an [`IsolationLevel`]
//! (`ReadCommitted` — the default — `RepeatableRead`, or `Serializable`) and a
//! read-only hint. Under the stronger levels the database may abort a
//! transaction with a serialization conflict (Postgres `40001`/`40P01`, SQLite
//! `BUSY`/`LOCKED`); the engine **retries the whole transaction on a fresh
//! one** with backoff, which is why the body is `Fn` rather than `FnOnce` —
//! it must be re-runnable. Capture `Copy` values freely; clone anything else
//! inside the closure.
//!
//! Conflict retries are separate from *application* retries: a body **error**
//! is not retried by default, but [`TransactionOptions::max_retries`] (with an
//! optional [`retry_if`](TransactionOptions::retry_if) predicate) re-runs the
//! body on a new transaction with exponential backoff, and only the final
//! outcome is checkpointed.
//!
//! # Writing the SQL
//!
//! The [`Tx`] API is dialect-agnostic: write `?` placeholders (rewritten to
//! `$1, $2, …` on Postgres), bind with [`params!`](crate::params), and read
//! rows via [`Row::get`] / [`Row::try_get`]. `execute` returns the affected
//! row count; `query_one` / `query_opt` / `query_all` cover the read shapes.
//!
//! Prefer the attribute form for named, reusable transaction functions:
//! `#[durare::transaction]` wraps an
//! `async fn(&DurableContext, &mut Tx, args…) -> Result<T>` so call sites skip
//! the `|tx| Box::pin(…)` scaffolding — see [`transaction`](macro@crate::transaction).

#[doc(no_inline)]
pub use crate::{IsolationLevel, Param, Row, TransactionOptions, Tx, TxBody};

#[allow(unused_imports)]
#[cfg(feature = "postgres")]
use crate::PostgresProvider;
#[allow(unused_imports)]
#[cfg(feature = "sqlite")]
use crate::SqliteProvider;
#[allow(unused_imports)]
use crate::{DurableContext, InMemoryProvider};
