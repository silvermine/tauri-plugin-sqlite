//! Schema introspection utilities for SQLite tables.
//!
//! Provides functions to query table schema information needed for
//! primary key extraction and WITHOUT ROWID detection.

use regex::Regex;
use sqlx::{Row, SqliteConnection};
use std::sync::OnceLock;

use crate::change::TableInfo;

/// Queries the schema information for a table.
///
/// Returns `TableInfo` containing primary key column indices and WITHOUT ROWID status.
/// Returns `None` if the table doesn't exist.
pub async fn query_table_info(
   conn: &mut SqliteConnection,
   table_name: &str,
) -> crate::Result<Option<TableInfo>> {
   // Check if table exists and get WITHOUT ROWID status
   let without_rowid = is_without_rowid(conn, table_name).await?;

   // Get primary key columns using pragma_table_info()
   let pk_columns = query_pk_columns(conn, table_name).await?;

   // Determine if table exists:
   // - If pk_columns is None, pragma_table_info returned no rows (table doesn't exist)
   // - If without_rowid is true, the table must exist (we found it in sqlite_master)
   // - A table with no explicit PK returns Some([]), not None
   if pk_columns.is_none() && !without_rowid {
      return Ok(None);
   }

   Ok(Some(TableInfo::new(
      pk_columns.unwrap_or_default(),
      without_rowid,
   )))
}

/// Checks if a table was created with WITHOUT ROWID.
///
/// Uses a regex anchored to the end of the CREATE TABLE statement to avoid
/// false positives from string literals or comments containing "WITHOUT ROWID".
async fn is_without_rowid(conn: &mut SqliteConnection, table_name: &str) -> crate::Result<bool> {
   let sql = r#"
        SELECT sql FROM sqlite_master
        WHERE type = 'table' AND name = ?1
    "#;

   let row: Option<(Option<String>,)> = sqlx::query_as(sql)
      .bind(table_name)
      .fetch_optional(&mut *conn)
      .await
      .map_err(crate::Error::Sqlx)?;

   match row {
      Some((Some(create_sql),)) => Ok(has_without_rowid_clause(&create_sql)),
      _ => Ok(false),
   }
}

/// Checks if a CREATE TABLE statement ends with WITHOUT ROWID.
///
/// The regex matches "WITHOUT ROWID" only when it appears at the end of the
/// statement (after the closing parenthesis), avoiding false matches in
/// string literals or comments.
fn has_without_rowid_clause(create_sql: &str) -> bool {
   static RE: OnceLock<Regex> = OnceLock::new();
   let re = RE.get_or_init(|| {
      // Match WITHOUT ROWID after ) with optional whitespace, case-insensitive
      Regex::new(r"(?i)\)\s*WITHOUT\s+ROWID\s*$").expect("invalid regex")
   });
   re.is_match(create_sql)
}

/// Queries the primary key column indices for a table.
///
/// Returns column indices in the order they appear in the PRIMARY KEY definition.
/// For composite primary keys, the `pk` column in PRAGMA table_info indicates
/// the position (1-indexed) within the PK.
///
/// Uses the `pragma_table_info()` table-valued function (available since SQLite
/// 3.16.0) so the table name can be bound as a parameter instead of interpolated
/// into the SQL string.
async fn query_pk_columns(
   conn: &mut SqliteConnection,
   table_name: &str,
) -> crate::Result<Option<Vec<usize>>> {
   // pragma_table_info returns: cid, name, type, notnull, dflt_value, pk
   // pk is 0 for non-PK columns, or 1-indexed position for PK columns
   let sql = "SELECT cid, name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?1)";

   let rows = sqlx::query(sql)
      .bind(table_name)
      .fetch_all(&mut *conn)
      .await
      .map_err(crate::Error::Sqlx)?;

   if rows.is_empty() {
      return Ok(None); // Table doesn't exist
   }

   // Collect (cid, pk_position) for columns that are part of the PK
   let mut pk_columns: Vec<(usize, i32)> = rows
      .iter()
      .filter_map(|row| {
         let cid: i32 = row.get("cid");
         let pk: i32 = row.get("pk");
         if pk > 0 {
            Some((cid as usize, pk))
         } else {
            None
         }
      })
      .collect();

   // Sort by pk position to get correct order for composite PKs
   pk_columns.sort_by_key(|(_, pk_pos)| *pk_pos);

   // Return just the column indices
   Ok(Some(pk_columns.into_iter().map(|(cid, _)| cid).collect()))
}

#[cfg(test)]
mod tests {
   use super::*;
   use rstest::rstest;

   #[rstest]
   #[case::trailing_clause("CREATE TABLE t (id TEXT PRIMARY KEY) WITHOUT ROWID")]
   #[case::trailing_space("CREATE TABLE t (id TEXT PRIMARY KEY) WITHOUT ROWID ")]
   #[case::extra_whitespace("CREATE TABLE t (id TEXT PRIMARY KEY)  WITHOUT  ROWID")]
   #[case::lowercase("CREATE TABLE t (id TEXT PRIMARY KEY) without rowid")]
   #[case::newline_before_clause("CREATE TABLE t (id TEXT PRIMARY KEY)\nWITHOUT ROWID")]
   fn test_has_without_rowid_clause_detects(#[case] sql: &str) {
      assert!(
         has_without_rowid_clause(sql),
         "{sql:?} should be detected as WITHOUT ROWID"
      );
   }

   #[rstest]
   #[case::normal_table("CREATE TABLE t (id INTEGER PRIMARY KEY)")]
   // False positive prevention
   #[case::in_string_literal("CREATE TABLE t (note TEXT DEFAULT 'see WITHOUT ROWID docs')")]
   #[case::in_sql_comment("CREATE TABLE t (id INT, note TEXT) -- WITHOUT ROWID comment")]
   fn test_has_without_rowid_clause_ignores(#[case] sql: &str) {
      assert!(
         !has_without_rowid_clause(sql),
         "{sql:?} should not be detected as WITHOUT ROWID"
      );
   }
}
