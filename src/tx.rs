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

use crate::error::{Error, Result};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;

/// A bound parameter for transactional-step SQL. Build these with
/// [`params!`](crate::params) rather than by hand in the common case.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
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
pub type TxBody<'a> = Box<
    dyn for<'t, 'c> FnOnce(
            &'t mut Tx<'c>,
        ) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + 't>>
        + Send
        + 'a,
>;

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
