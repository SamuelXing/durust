//! Transactional steps: run user SQL and the step's checkpoint in **one**
//! database transaction.
//!
//! A regular [`DurableContext::step`](crate::DurableContext::step) checkpoints
//! *after* its body runs, in a separate commit, so a crash between the two
//! re-runs the body (at-least-once). A transactional step instead commits the
//! user's SQL writes and the `operation_outputs` checkpoint together, so the
//! writes happen **exactly once** — on replay the recorded output is returned
//! without touching the database again.
//!
//! The user's body receives a [`Tx`], a thin dialect-agnostic handle that runs
//! on either Postgres or SQLite. SQL is written with `?` placeholders (they are
//! rewritten to `$1, $2, …` for Postgres); bind values go through [`Param`],
//! most easily via the [`params!`](crate::params) macro.

use crate::context::RetryPredicate;
use crate::error::{Error, Result};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// A bound parameter for transactional-step SQL. Build these with
/// [`params!`](crate::params) rather than by hand in the common case.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    /// SQL `NULL`.
    Null,
    /// A signed 64-bit integer.
    Int(i64),
    /// A 64-bit floating-point number.
    Float(f64),
    /// A UTF-8 text value.
    Text(String),
    /// A boolean.
    Bool(bool),
    /// A binary blob.
    Bytes(Vec<u8>),
}

impl From<i64> for Param {
    fn from(v: i64) -> Self {
        Param::Int(v)
    }
}
impl From<i32> for Param {
    fn from(v: i32) -> Self {
        Param::Int(v as i64)
    }
}
impl From<f64> for Param {
    fn from(v: f64) -> Self {
        Param::Float(v)
    }
}
impl From<bool> for Param {
    fn from(v: bool) -> Self {
        Param::Bool(v)
    }
}
impl From<&str> for Param {
    fn from(v: &str) -> Self {
        Param::Text(v.to_string())
    }
}
impl From<String> for Param {
    fn from(v: String) -> Self {
        Param::Text(v)
    }
}
impl From<Vec<u8>> for Param {
    fn from(v: Vec<u8>) -> Self {
        Param::Bytes(v)
    }
}
impl<T: Into<Param>> From<Option<T>> for Param {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => Param::Null,
        }
    }
}

/// Build a `Vec<Param>` for a transactional-step query: `params![amount, id]`.
#[macro_export]
macro_rules! params {
    () => { ::std::vec::Vec::<$crate::Param>::new() };
    ($($x:expr),+ $(,)?) => { ::std::vec![$($crate::Param::from($x)),+] };
}

/// A handle to the in-progress transaction, handed to a transactional step's
/// body. Runs against either Postgres or SQLite; the step's checkpoint commits
/// in this same transaction.
pub struct Tx<'c> {
    inner: TxInner<'c>,
}

enum TxInner<'c> {
    Postgres(&'c mut sqlx::PgConnection),
    Sqlite(&'c mut sqlx::SqliteConnection),
}

impl<'c> Tx<'c> {
    pub(crate) fn postgres(conn: &'c mut sqlx::PgConnection) -> Self {
        Tx {
            inner: TxInner::Postgres(conn),
        }
    }

    pub(crate) fn sqlite(conn: &'c mut sqlx::SqliteConnection) -> Self {
        Tx {
            inner: TxInner::Sqlite(conn),
        }
    }

    /// Run a statement (INSERT/UPDATE/DELETE/DDL); returns rows affected.
    pub async fn execute(&mut self, sql: &str, params: &[Param]) -> Result<u64> {
        match &mut self.inner {
            TxInner::Postgres(conn) => {
                let sql = to_pg_placeholders(sql);
                let q = bind_pg(sqlx::query(&sql), params);
                Ok(q.execute(&mut **conn).await?.rows_affected())
            }
            TxInner::Sqlite(conn) => {
                let q = bind_sqlite(sqlx::query(sql), params);
                Ok(q.execute(&mut **conn).await?.rows_affected())
            }
        }
    }

    /// Run a query and return every row.
    pub async fn query_all(&mut self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        match &mut self.inner {
            TxInner::Postgres(conn) => {
                let sql = to_pg_placeholders(sql);
                let q = bind_pg(sqlx::query(&sql), params);
                let rows = q.fetch_all(&mut **conn).await?;
                Ok(rows
                    .into_iter()
                    .map(|r| Row {
                        inner: RowInner::Postgres(r),
                    })
                    .collect())
            }
            TxInner::Sqlite(conn) => {
                let q = bind_sqlite(sqlx::query(sql), params);
                let rows = q.fetch_all(&mut **conn).await?;
                Ok(rows
                    .into_iter()
                    .map(|r| Row {
                        inner: RowInner::Sqlite(r),
                    })
                    .collect())
            }
        }
    }

    /// Run a query and return the first row, or `None` if it matched nothing.
    pub async fn query_opt(&mut self, sql: &str, params: &[Param]) -> Result<Option<Row>> {
        match &mut self.inner {
            TxInner::Postgres(conn) => {
                let sql = to_pg_placeholders(sql);
                let q = bind_pg(sqlx::query(&sql), params);
                Ok(q.fetch_optional(&mut **conn).await?.map(|r| Row {
                    inner: RowInner::Postgres(r),
                }))
            }
            TxInner::Sqlite(conn) => {
                let q = bind_sqlite(sqlx::query(sql), params);
                Ok(q.fetch_optional(&mut **conn).await?.map(|r| Row {
                    inner: RowInner::Sqlite(r),
                }))
            }
        }
    }

    /// Run a query expected to return exactly one row; errors if none matched.
    pub async fn query_one(&mut self, sql: &str, params: &[Param]) -> Result<Row> {
        self.query_opt(sql, params)
            .await?
            .ok_or_else(|| Error::app("query_one: no rows returned"))
    }
}

/// One row from a transactional-step query. Read columns by name with
/// [`Row::get`] / [`Row::try_get`].
pub struct Row {
    inner: RowInner,
}

enum RowInner {
    Postgres(sqlx::postgres::PgRow),
    Sqlite(sqlx::sqlite::SqliteRow),
}

impl Row {
    /// Read column `col` as `T`, panicking on a decode/missing-column error.
    /// Use [`Row::try_get`] to handle that error instead.
    pub fn get<'r, T>(&'r self, col: &str) -> T
    where
        T: sqlx::Decode<'r, sqlx::Postgres>
            + sqlx::Type<sqlx::Postgres>
            + sqlx::Decode<'r, sqlx::Sqlite>
            + sqlx::Type<sqlx::Sqlite>,
    {
        use sqlx::Row as _;
        match &self.inner {
            RowInner::Postgres(r) => r.get::<T, _>(col),
            RowInner::Sqlite(r) => r.get::<T, _>(col),
        }
    }

    /// Read column `col` as `T`, returning an error on a decode/missing-column
    /// failure.
    pub fn try_get<'r, T>(&'r self, col: &str) -> Result<T>
    where
        T: sqlx::Decode<'r, sqlx::Postgres>
            + sqlx::Type<sqlx::Postgres>
            + sqlx::Decode<'r, sqlx::Sqlite>
            + sqlx::Type<sqlx::Sqlite>,
    {
        use sqlx::Row as _;
        match &self.inner {
            RowInner::Postgres(r) => Ok(r.try_get::<T, _>(col)?),
            RowInner::Sqlite(r) => Ok(r.try_get::<T, _>(col)?),
        }
    }
}

/// The type-erased body a transactional step runs: it borrows the [`Tx`] and
/// resolves to the step's JSON output. Produced by
/// [`DurableContext::transaction`](crate::DurableContext::transaction); consumed
/// by the provider, which supplies the [`Tx`] and the surrounding transaction.
///
/// It is `Fn`, not `FnOnce`, because a transaction-level conflict
/// (serialization failure / deadlock) under a higher isolation level restarts
/// the whole transaction on a fresh one, re-running the body.
pub type TxBody<'a> = Box<
    dyn for<'t, 'c> Fn(&'t mut Tx<'c>) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + 't>>
        + Send
        + Sync
        + 'a,
>;

/// Isolation level for a transactional step. `ReadCommitted` is the default;
/// `RepeatableRead`/`Serializable` give stronger guarantees but can fail with a
/// serialization conflict, which the transactional step retries automatically.
/// SQLite runs every transaction serializably, so the level is advisory there.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    /// `READ COMMITTED` — the default.
    #[default]
    ReadCommitted,
    /// `REPEATABLE READ` — a stabler snapshot; may raise a serialization conflict.
    RepeatableRead,
    /// `SERIALIZABLE` — the strongest guarantee; may raise a serialization conflict.
    Serializable,
}

impl IsolationLevel {
    /// The `SET TRANSACTION ISOLATION LEVEL` clause for Postgres.
    pub(crate) fn pg_sql(self) -> &'static str {
        match self {
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// Options for a transactional step: its checkpoint `name`, isolation level,
/// whether the transaction is read-only, and the user-facing retry policy for
/// application errors raised by the body.
///
/// Two retry layers apply, matching the reference SDK. A transaction-level
/// **conflict** (serialization / deadlock / `SQLITE_BUSY`) is always retried on
/// a fresh transaction and never counts against the budget below. An
/// **application error** returned by the body is retried only if `max_retries`
/// allows it (and any [`retry_if`](Self::retry_if) predicate accepts it), with
/// exponential backoff; the whole body re-runs on a new transaction. Only after
/// the budget is exhausted is the failure checkpointed durably. With the default
/// `max_retries` of 0 an application error fails immediately, as before.
#[derive(Clone)]
pub struct TransactionOptions {
    /// Checkpoint name recorded for this transactional step.
    pub name: String,
    /// Isolation level the transaction runs at.
    pub isolation: IsolationLevel,
    /// Hint that the transaction performs no writes (`READ ONLY` on Postgres).
    pub read_only: bool,
    /// Additional attempts after the first failure (0 = run once, no retry).
    pub max_retries: u32,
    /// Exponential backoff multiplier between attempts.
    pub backoff_factor: f64,
    /// Delay before the first retry.
    pub base_interval: Duration,
    /// Upper bound on any single backoff delay.
    pub max_interval: Duration,
    /// Optional predicate deciding whether a body error is retryable. Returning
    /// `false` stops retries immediately even with attempts remaining, so a
    /// permanent error fails fast. `None` (the default) retries every error up to
    /// `max_retries`. A transaction conflict is retried regardless of this.
    pub retry_if: Option<RetryPredicate>,
}

impl TransactionOptions {
    /// Default options (`ReadCommitted`, read-write, no user-retry) for a step
    /// named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        TransactionOptions {
            name: name.into(),
            isolation: IsolationLevel::default(),
            read_only: false,
            max_retries: 0,
            backoff_factor: 2.0,
            base_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(5),
            retry_if: None,
        }
    }

    /// Set the isolation level.
    pub fn isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation = level;
        self
    }

    /// Mark the transaction read-only.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set the number of retries (attempts after the first) for body errors.
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Set the backoff multiplier between retries.
    pub fn backoff_factor(mut self, f: f64) -> Self {
        self.backoff_factor = f;
        self
    }

    /// Set the initial retry delay.
    pub fn base_interval(mut self, d: Duration) -> Self {
        self.base_interval = d;
        self
    }

    /// Set the maximum retry delay.
    pub fn max_interval(mut self, d: Duration) -> Self {
        self.max_interval = d;
        self
    }

    /// Set a predicate deciding whether a body error is retryable. It is
    /// consulted on every failure before backoff; returning `false` stops retries
    /// at once (the error propagates), so a permanent failure doesn't burn
    /// attempts:
    ///
    /// ```
    /// use durare::{Error, TransactionOptions};
    ///
    /// let opts = TransactionOptions::new("transfer")
    ///     .max_retries(5)
    ///     .retry_if(|e: &Error| e.is_retryable());
    /// ```
    pub fn retry_if<P>(mut self, predicate: P) -> Self
    where
        P: Fn(&Error) -> bool + Send + Sync + 'static,
    {
        self.retry_if = Some(std::sync::Arc::new(predicate));
        self
    }

    /// Exponential backoff delay before the `attempt`-th user-retry (0-based),
    /// matching [`StepOptions`](crate::StepOptions): `base * factor^attempt`,
    /// capped at `max_interval`.
    pub(crate) fn user_retry_backoff(&self, attempt: u32) -> Duration {
        let secs = self.base_interval.as_secs_f64() * self.backoff_factor.powi(attempt as i32);
        Duration::from_secs_f64(secs).min(self.max_interval)
    }

    /// Decide whether a body error should be retried on attempt `attempt`
    /// (0-based, the number of retries already performed). A transaction conflict
    /// is never retried here — it belongs to the inner transaction loop and does
    /// not count against this budget, so an exhausted conflict fails immediately
    /// rather than re-running the whole body.
    pub(crate) fn should_user_retry(&self, err: &Error, attempt: u32) -> bool {
        !err.is_tx_conflict()
            && attempt < self.max_retries
            && self.retry_if.as_ref().is_none_or(|p| p(err))
    }
}

/// Rewrite `?` placeholders to Postgres `$1, $2, …`, leaving any `?` inside a
/// single-quoted string literal untouched.
fn to_pg_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut n = 0u32;
    let mut in_str = false;
    for c in sql.chars() {
        match c {
            '\'' => {
                in_str = !in_str;
                out.push(c);
            }
            '?' if !in_str => {
                n += 1;
                out.push('$');
                out.push_str(&n.to_string());
            }
            _ => out.push(c),
        }
    }
    out
}

fn bind_pg<'q>(
    mut q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: &'q [Param],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for p in params {
        q = match p {
            Param::Null => q.bind(Option::<&str>::None),
            Param::Int(v) => q.bind(*v),
            Param::Float(v) => q.bind(*v),
            Param::Text(v) => q.bind(v.as_str()),
            Param::Bool(v) => q.bind(*v),
            Param::Bytes(v) => q.bind(v.as_slice()),
        };
    }
    q
}

fn bind_sqlite<'q>(
    mut q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    params: &'q [Param],
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for p in params {
        q = match p {
            Param::Null => q.bind(Option::<&str>::None),
            Param::Int(v) => q.bind(*v),
            Param::Float(v) => q.bind(*v),
            Param::Text(v) => q.bind(v.as_str()),
            Param::Bool(v) => q.bind(*v),
            Param::Bytes(v) => q.bind(v.as_slice()),
        };
    }
    q
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_rewritten_outside_string_literals() {
        assert_eq!(
            to_pg_placeholders("UPDATE a SET b = ? WHERE id = ?"),
            "UPDATE a SET b = $1 WHERE id = $2"
        );
        // A `?` inside a quoted literal is left alone.
        assert_eq!(
            to_pg_placeholders("SELECT '? literal' , ? FROM t"),
            "SELECT '? literal' , $1 FROM t"
        );
    }

    #[test]
    fn params_macro_builds_in_order() {
        let p = params![1_i64, "x", true];
        assert_eq!(
            p,
            vec![Param::Int(1), Param::Text("x".into()), Param::Bool(true)]
        );
        let empty = params![];
        assert!(empty.is_empty());
    }
}
