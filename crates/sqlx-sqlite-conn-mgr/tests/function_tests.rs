//! A scalar function applies to every connection that either pool opens.
//!
//! Every test in this binary shares the process-global function registry, and each
//! database captures the registered set at its `connect`. `REGISTERED` therefore holds
//! every function this binary needs, under a name per test, and `open` forces it before
//! the first database connects.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use rstest::rstest;
use sqlx::sqlite::{Sqlite, SqliteRow};
use sqlx::{Executor, FromRow};
use sqlx_sqlite_conn_mgr::{
   FunctionError, InvocationScope, ScalarFunction, ScalarHandler, SqlValue, SqlValueRef,
   SqliteDatabase, SqliteDatabaseConfig, register_function, register_or_replace_function,
};
use tempfile::TempDir;

/// A handler that returns whichever value it received.
fn echo() -> ScalarHandler {
   Arc::new(|args| Ok(args[0].to_owned()))
}

fn register(name: &str, arity: i32, handler: ScalarHandler) {
   register_function(ScalarFunction {
      name: name.to_string(),
      arity,
      deterministic: true,
      invocation_scope: InvocationScope::DirectOnly,
      handler,
   })
   .expect("registration");
}

/// Every function this binary uses, one per test, registered before any database opens.
static REGISTERED: LazyLock<()> = LazyLock::new(|| {
   register(
      "fn_read_write",
      1,
      Arc::new(|args| match &args[0] {
         SqlValueRef::Text(text) => Ok(SqlValue::Text(format!("{text}!"))),
         _ => Err(FunctionError::new("expected text")),
      }),
   );
   register("fn_pool_growth", 1, echo());
   register("fn_after_idle", 1, echo());
   register(
      "fn_handler_error",
      1,
      Arc::new(|_args| Err(FunctionError::new("payload is not decodable"))),
   );
   register("fn_panics", 1, Arc::new(|_args| panic!("handler exploded")));
   register("fn_round_trip", 1, echo());
   register("fn_direct_only", 1, echo());
   register(
      "fn_no_args",
      0,
      Arc::new(|_args| Ok(SqlValue::Text("no arguments".into()))),
   );
   register(
      "fn_pair",
      2,
      Arc::new(|args| match (&args[0], &args[1]) {
         (SqlValueRef::Text(first), SqlValueRef::Text(second)) => {
            Ok(SqlValue::Text(format!("{first}|{second}")))
         }
         _ => Err(FunctionError::new("expected two text arguments")),
      }),
   );

   // One name at two arities. Each handler names its own arity, so a call proves which
   // registration SQLite resolved.
   register(
      "fn_over",
      1,
      Arc::new(|_args| Ok(SqlValue::Text("one argument".into()))),
   );
   register(
      "fn_over",
      2,
      Arc::new(|_args| Ok(SqlValue::Text("two arguments".into()))),
   );

   register_function(ScalarFunction {
      name: "fn_in_schema".into(),
      arity: 1,
      deterministic: true,
      invocation_scope: InvocationScope::Schema,
      handler: echo(),
   })
   .expect("registration");

   register_function(ScalarFunction {
      name: "fn_trusted".into(),
      arity: 1,
      deterministic: true,
      invocation_scope: InvocationScope::Schema,
      handler: echo(),
   })
   .expect("registration");

   register_function(ScalarFunction {
      name: "fn_innocuous".into(),
      arity: 1,
      deterministic: true,
      invocation_scope: InvocationScope::InnocuousSchema,
      handler: echo(),
   })
   .expect("registration");
});

async fn open(dir: &TempDir, config: Option<SqliteDatabaseConfig>) -> Arc<SqliteDatabase> {
   LazyLock::force(&REGISTERED);

   SqliteDatabase::connect(dir.path().join("test.db"), config)
      .await
      .expect("connect")
}

/// Runs `sql` on any executor and returns its one column, panicking with the SQL itself.
async fn scalar<'e, 'c: 'e, T, E>(executor: E, sql: &'static str) -> T
where
   T: Send + Unpin,
   (T,): Send + Unpin + for<'r> FromRow<'r, SqliteRow>,
   E: 'e + Executor<'c, Database = Sqlite>,
{
   sqlx::query_scalar(sql)
      .fetch_one(executor)
      .await
      .expect(sql)
}

/// Runs `sql` expecting it to fail, and returns the error it failed with.
async fn scalar_error<'e, 'c: 'e, E>(executor: E, sql: &'static str) -> sqlx::Error
where
   E: 'e + Executor<'c, Database = Sqlite>,
{
   sqlx::query_scalar::<_, String>(sql)
      .fetch_one(executor)
      .await
      .expect_err(sql)
}

#[tokio::test]
async fn resolves_on_a_read_connection_and_on_the_write_connection() {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;

   let from_reader: String = scalar(db.read_pool().unwrap(), "SELECT fn_read_write('read')").await;
   assert_eq!(from_reader, "read!");

   let mut writer = db.acquire_writer().await.unwrap();
   let from_writer: String = scalar(&mut *writer, "SELECT fn_read_write('write')").await;
   assert_eq!(from_writer, "write!");
}

#[tokio::test]
async fn resolves_on_every_connection_once_the_read_pool_grows() {
   let dir = TempDir::new().unwrap();
   let config = SqliteDatabaseConfig {
      max_read_connections: 3,
      ..Default::default()
   };
   let db = open(&dir, Some(config)).await;

   // The test holds each connection until the loop ends. The pool must therefore open a
   // new connection each iteration, rather than hand back one already open. Every query
   // below runs on a newly opened connection.
   let mut held = Vec::new();

   for _ in 0..3 {
      let mut conn = db.read_pool().unwrap().acquire().await.expect("acquire");

      let value: i64 = scalar(&mut *conn, "SELECT fn_pool_growth(7)").await;
      assert_eq!(value, 7);

      held.push(conn);
   }

   assert_eq!(db.read_pool().unwrap().size(), 3);
}

#[tokio::test]
async fn resolves_on_a_connection_that_replaces_an_idle_one() {
   let dir = TempDir::new().unwrap();
   let config = SqliteDatabaseConfig {
      idle_timeout_secs: 1,
      ..Default::default()
   };
   let db = open(&dir, Some(config)).await;
   let pool = db.read_pool().unwrap();

   // `connect_with` opens and tests one connection, so the pool starts with one.
   assert_eq!(pool.size(), 1);

   // Wait for sqlx's reaper to drop it. The reaper's period is the idle timeout, so this
   // asserts on the pool being empty rather than on elapsed time.
   let deadline = Instant::now() + Duration::from_secs(10);
   while pool.size() > 0 && Instant::now() < deadline {
      tokio::time::sleep(Duration::from_millis(100)).await;
   }
   assert_eq!(pool.size(), 0, "the idle connection was never reaped");

   let value: String = scalar(pool, "SELECT fn_after_idle('fresh')").await;
   assert_eq!(value, "fresh");
}

#[tokio::test]
async fn a_handler_error_fails_the_statement_and_carries_its_message() {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;

   let error = scalar_error(db.read_pool().unwrap(), "SELECT fn_handler_error('x')").await;

   assert!(
      error.to_string().contains("payload is not decodable"),
      "error did not include the handler's message: {error}"
   );
}

#[tokio::test]
async fn a_panicking_handler_produces_an_error_and_leaves_the_pool_usable() {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;
   let pool = db.read_pool().unwrap();

   // The panic hook still prints to stderr, so this test is noisy by design.
   let error = scalar_error(pool, "SELECT fn_panics('x')").await;

   assert!(
      error.to_string().contains("fn_panics: handler panicked"),
      "error did not name the panicking function: {error}"
   );

   // The pool serves queries after a handler panic, rather than holding a poisoned
   // connection.
   let survivor: i64 = scalar(pool, "SELECT 1").await;
   assert_eq!(survivor, 1);
}

/// SQLite's `quote()` renders the echoed value as an SQL literal, so one string pins both
/// the value and its storage class: text is quoted, a blob is `X'..'`, and `42` and `42.0`
/// differ.
#[rstest]
#[case::null("SELECT quote(fn_round_trip(NULL))", "NULL")]
#[case::integer("SELECT quote(fn_round_trip(42))", "42")]
#[case::real("SELECT quote(fn_round_trip(1.5))", "1.5")]
#[case::text("SELECT quote(fn_round_trip('abc'))", "'abc'")]
#[case::blob("SELECT quote(fn_round_trip(x'0102ff'))", "X'0102FF'")]
#[tokio::test]
async fn every_storage_class_round_trips_and_null_passes_through(
   #[case] sql: &'static str,
   #[case] expected: &str,
) {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;

   let literal: String = scalar(db.read_pool().unwrap(), sql).await;
   assert_eq!(literal, expected);
}

/// A zero-argument function drives `dispatch` with an empty argument vector, which no
/// other test in this binary reaches.
#[tokio::test]
async fn a_zero_argument_function_resolves() {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;

   let value: String = scalar(db.read_pool().unwrap(), "SELECT fn_no_args()").await;
   assert_eq!(value, "no arguments");
}

/// Two calls with the arguments swapped. Either call alone catches a reversed order,
/// since its two arguments differ. The `swapped` case also catches a dispatch that orders
/// the arguments by value rather than by position.
#[rstest]
#[case::in_order("SELECT fn_pair('a', 'b')", "a|b")]
#[case::swapped("SELECT fn_pair('b', 'a')", "b|a")]
#[tokio::test]
async fn a_two_argument_function_receives_its_arguments_in_order(
   #[case] sql: &'static str,
   #[case] expected: &str,
) {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;

   let value: String = scalar(db.read_pool().unwrap(), sql).await;
   assert_eq!(value, expected);
}

/// SQLite overloads a name by argument count, and the registry holds one entry per name
/// and arity pair. Each call reaches the handler registered for its own arity.
#[rstest]
#[case::one_argument("SELECT fn_over('x')", "one argument")]
#[case::two_arguments("SELECT fn_over('x', 'y')", "two arguments")]
#[tokio::test]
async fn one_name_registered_at_two_arities_resolves_at_both(
   #[case] sql: &'static str,
   #[case] expected: &str,
) {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;

   let value: String = scalar(db.read_pool().unwrap(), sql).await;
   assert_eq!(value, expected);
}

#[tokio::test]
async fn a_registered_function_runs_from_top_level_sql_but_not_from_a_schema_object() {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;
   let mut writer = db.acquire_writer().await.unwrap();

   let direct: String = scalar(&mut *writer, "SELECT fn_direct_only('x')").await;
   assert_eq!(direct, "x");

   sqlx::query("CREATE TABLE doc (title TEXT)")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("INSERT INTO doc (title) VALUES ('x')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("CREATE VIEW doc_view AS SELECT fn_direct_only(title) AS title FROM doc")
      .execute(&mut *writer)
      .await
      .unwrap();

   // A view in an attached file can name a registered function, so SQLite must refuse the
   // call rather than run a handler this application never meant to expose there.
   let from_view = scalar_error(&mut *writer, "SELECT title FROM doc_view").await;
   assert!(
      from_view
         .to_string()
         .contains("unsafe use of fn_direct_only"),
      "a view calling the function was not refused: {from_view}"
   );

   // An expression index would write the function name into the schema, leaving the table
   // unreadable by any process that opens the file without registering the function.
   let index = sqlx::query("CREATE INDEX doc_title ON doc (fn_direct_only(title))")
      .execute(&mut *writer)
      .await
      .expect_err("an expression index naming the function was accepted");
   assert!(
      index.to_string().contains("unsafe use of fn_direct_only"),
      "an expression index naming the function was not refused: {index}"
   );
}

/// A replacement follows the same rule a first registration does: it reaches the databases
/// that connect after it, and the database already open keeps the handler it captured.
#[tokio::test]
async fn a_replacement_reaches_the_next_database_and_leaves_an_open_one_alone() {
   let replaced = |result: &'static str| ScalarFunction {
      name: "fn_replaced".into(),
      arity: 1,
      deterministic: true,
      invocation_scope: InvocationScope::DirectOnly,
      handler: Arc::new(move |_args| Ok(SqlValue::Text(result.to_string()))),
   };

   register_function(replaced("first")).expect("registration");

   let earlier_dir = TempDir::new().unwrap();
   let earlier = SqliteDatabase::connect(earlier_dir.path().join("earlier.db"), None)
      .await
      .expect("connect");

   register_or_replace_function(replaced("second")).expect("replacement");

   let later_dir = TempDir::new().unwrap();
   let later = SqliteDatabase::connect(later_dir.path().join("later.db"), None)
      .await
      .expect("connect");

   let from_earlier: String = scalar(earlier.read_pool().unwrap(), "SELECT fn_replaced('x')").await;
   assert_eq!(from_earlier, "first");

   let from_later: String = scalar(later.read_pool().unwrap(), "SELECT fn_replaced('x')").await;
   assert_eq!(from_later, "second");
}

/// `InvocationScope::Schema` is what a consumer picks to build a view over a function.
#[tokio::test]
async fn a_schema_scoped_function_runs_from_inside_a_view() {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;
   let mut writer = db.acquire_writer().await.unwrap();

   sqlx::query("CREATE TABLE note (body TEXT)")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("INSERT INTO note (body) VALUES ('kept')")
      .execute(&mut *writer)
      .await
      .unwrap();
   sqlx::query("CREATE VIEW note_view AS SELECT fn_in_schema(body) AS body FROM note")
      .execute(&mut *writer)
      .await
      .unwrap();

   let through_view: String = scalar(&mut *writer, "SELECT body FROM note_view").await;
   assert_eq!(through_view, "kept");

   // The view belongs to the schema, so a read connection resolves the function too.
   let from_reader: String = scalar(db.read_pool().unwrap(), "SELECT body FROM note_view").await;
   assert_eq!(from_reader, "kept");
}

/// `SQLITE_INNOCUOUS` is what separates [`InvocationScope::InnocuousSchema`] from
/// [`InvocationScope::Schema`]. Both run from a view on a connection that trusts the
/// schema, so the test that tells them apart needs a connection that does not.
/// `fn_trusted` is the control: without the flag, SQLite refuses the same call.
#[tokio::test]
async fn only_an_innocuous_function_runs_from_a_view_when_the_schema_is_untrusted() {
   let dir = TempDir::new().unwrap();
   let db = open(&dir, None).await;

   // The views are created while the schema is still trusted, and the writer is released
   // before the reads below.
   {
      let mut writer = db.acquire_writer().await.unwrap();

      for sql in [
         "CREATE TABLE memo (body TEXT)",
         "INSERT INTO memo (body) VALUES ('held')",
         "CREATE VIEW trusted_view AS SELECT fn_trusted(body) AS body FROM memo",
         "CREATE VIEW innocuous_view AS SELECT fn_innocuous(body) AS body FROM memo",
      ] {
         sqlx::query(sql).execute(&mut *writer).await.expect(sql);
      }
   }

   // `PRAGMA trusted_schema` belongs to one connection, and the read pool hands out
   // whichever connection it has. So the test holds the connection it set the pragma on
   // for both reads.
   let mut conn = db.read_pool().unwrap().acquire().await.expect("acquire");
   sqlx::query("PRAGMA trusted_schema = OFF")
      .execute(&mut *conn)
      .await
      .expect("trusted_schema = OFF");

   let refused = scalar_error(&mut *conn, "SELECT body FROM trusted_view").await;
   assert!(
      refused.to_string().contains("unsafe use of fn_trusted"),
      "a schema-scoped function was not refused on an untrusted schema: {refused}"
   );

   let accepted: String = scalar(&mut *conn, "SELECT body FROM innocuous_view").await;
   assert_eq!(accepted, "held");
}

/// A database captures the registered set at its `connect`. A registration made after
/// that point applies to the next database to connect, and to no connection of the one
/// already open, however the pools grow or replace connections.
#[tokio::test]
async fn a_registration_after_a_connect_applies_to_the_next_database_only() {
   let dir = TempDir::new().unwrap();
   let earlier = open(&dir, None).await;

   register_function(ScalarFunction {
      name: "fn_after_connect".into(),
      arity: 1,
      deterministic: true,
      invocation_scope: InvocationScope::DirectOnly,
      handler: echo(),
   })
   .expect("registration");

   let unresolved =
      scalar_error(earlier.read_pool().unwrap(), "SELECT fn_after_connect('x')").await;
   assert!(
      unresolved
         .to_string()
         .contains("no such function: fn_after_connect"),
      "the earlier database must not resolve the function: {unresolved}"
   );

   let later_dir = TempDir::new().unwrap();
   let later = SqliteDatabase::connect(later_dir.path().join("later.db"), None)
      .await
      .expect("connect");

   let resolved: String = scalar(later.read_pool().unwrap(), "SELECT fn_after_connect('x')").await;
   assert_eq!(resolved, "x");
}
