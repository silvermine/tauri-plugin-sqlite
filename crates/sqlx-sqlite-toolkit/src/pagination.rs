//! Keyset pagination types and SQL generation.
//!
//! Provides declarative, builder-friendly keyset pagination that avoids the
//! performance degradation of OFFSET-based pagination on large datasets.
//! Supports both forward (`.after()`) and backward (`.before()`) pagination.
//!
//! # How It Works
//!
//! Instead of skipping rows with OFFSET, keyset pagination uses indexed column
//! values from a boundary row to seek directly to the next or previous page.
//! This keeps query performance constant regardless of how deep you paginate.
//!
//! For backward pagination, all sort directions are reversed internally so the
//! database returns rows from the opposite end, then the rows are reversed to
//! restore the original order.
//!
//! # Example
//!
//! ```no_run
//! use sqlx_sqlite_toolkit::pagination::{KeysetColumn, SortDirection};
//!
//! let keyset = vec![
//!    KeysetColumn::asc("category"),
//!    KeysetColumn::desc("score"),
//!    KeysetColumn::asc("id"),
//! ];
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::Error;

/// Sort direction for a keyset column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SortDirection {
   /// Ascending order (smallest first)
   Asc,
   /// Descending order (largest first)
   Desc,
}

impl SortDirection {
   /// Return the opposite sort direction.
   pub fn reversed(self) -> Self {
      match self {
         SortDirection::Asc => SortDirection::Desc,
         SortDirection::Desc => SortDirection::Asc,
      }
   }
}

/// A column in the keyset used for cursor-based pagination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeysetColumn {
   /// Column name as it appears in the query result set
   pub name: String,
   /// Sort direction for this column
   pub direction: SortDirection,
}

impl KeysetColumn {
   /// Create a keyset column with ascending sort direction.
   pub fn asc(name: impl Into<String>) -> Self {
      Self {
         name: name.into(),
         direction: SortDirection::Asc,
      }
   }

   /// Create a keyset column with descending sort direction.
   pub fn desc(name: impl Into<String>) -> Self {
      Self {
         name: name.into(),
         direction: SortDirection::Desc,
      }
   }
}

/// Validate that a column name is safe for SQL interpolation.
///
/// Accepts names matching `[a-zA-Z_][a-zA-Z0-9_.]*`, which covers plain column
/// names, qualified names (e.g., `table.column`), and underscored identifiers.
pub(crate) fn validate_column_name(name: &str) -> Result<(), Error> {
   if name.is_empty() {
      return Err(Error::InvalidColumnName {
         name: name.to_string(),
      });
   }

   let mut chars = name.chars();
   let first = chars.next().unwrap();
   if !first.is_ascii_alphabetic() && first != '_' {
      return Err(Error::InvalidColumnName {
         name: name.to_string(),
      });
   }

   for ch in chars {
      if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.' {
         return Err(Error::InvalidColumnName {
            name: name.to_string(),
         });
      }
   }

   Ok(())
}

/// Quote a column name with double-quote identifiers for defense-in-depth.
///
/// Any embedded double quotes are doubled per SQL standard (`"` → `""`).
pub(crate) fn quote_identifier(name: &str) -> String {
   format!("\"{}\"", name.replace('"', "\"\""))
}

/// A page of results from keyset pagination.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeysetPage {
   /// The rows in this page
   pub rows: Vec<indexmap::IndexMap<String, JsonValue>>,
   /// Cursor values to continue pagination in the **same direction**,
   /// or `None` if there are no more pages.
   ///
   /// After `.after()`, pass to another `.after()` for the next page.
   /// After `.before()`, pass to another `.before()` to keep going backward.
   pub next_cursor: Option<Vec<JsonValue>>,
   /// Whether there are more rows in the current pagination direction
   pub has_more: bool,
}

/// Check whether `keyword` appears as a standalone keyword at position `i`
/// in the uppercased byte slice `bytes` (length `len`).
///
/// "Standalone" means the character before and after the keyword (if present)
/// is not an identifier character (`[A-Z0-9_]`).
fn is_keyword_at(bytes: &[u8], len: usize, i: usize, keyword: &[u8]) -> bool {
   let klen = keyword.len();
   if i + klen > len {
      return false;
   }
   if &bytes[i..i + klen] != keyword {
      return false;
   }
   let before_ok = i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
   let after_ok =
      i + klen >= len || (!bytes[i + klen].is_ascii_alphanumeric() && bytes[i + klen] != b'_');

   before_ok && after_ok
}

/// Advance the scanner index past a quoted literal or identifier.
///
/// `quote` is the opening quote character (`'` or `"`). The scanner handles
/// SQL-standard doubled-quote escaping (`''` or `""`).
fn skip_quoted(bytes: &[u8], len: usize, i: usize, quote: u8) -> usize {
   let mut j = i + 1;
   while j < len {
      if bytes[j] == quote {
         // Doubled quote is an escape — skip both and continue
         if j + 1 < len && bytes[j + 1] == quote {
            j += 2;
            continue;
         }
         // End of quoted section
         return j;
      }
      j += 1;
   }
   j // unterminated — return end
}

/// Advance the scanner index past a `--` line comment (until newline or end).
fn skip_line_comment(bytes: &[u8], len: usize, i: usize) -> usize {
   let mut j = i + 2; // skip the `--`
   while j < len && bytes[j] != b'\n' {
      j += 1;
   }
   j
}

/// Advance the scanner index past a `/* … */` block comment.
fn skip_block_comment(bytes: &[u8], len: usize, i: usize) -> usize {
   let mut j = i + 2; // skip the `/*`
   while j + 1 < len {
      if bytes[j] == b'*' && bytes[j + 1] == b'/' {
         return j + 1; // position of the closing `/`
      }
      j += 1;
   }
   len.saturating_sub(1) // unterminated — return end
}

/// Scan the uppercased query, calling `on_keyword` at each top-level position
/// (depth == 0, outside quotes and comments).
///
/// `on_keyword` receives `(uppercased_bytes, len, position)` and returns
/// `Some(T)` to short-circuit or `None` to keep scanning.
fn scan_top_level<T>(
   query: &str,
   mut on_keyword: impl FnMut(&[u8], usize, usize) -> Option<T>,
) -> Option<T> {
   let upper = query.to_uppercase();
   let bytes = upper.as_bytes();
   let len = bytes.len();
   let mut depth: i32 = 0;
   let mut i = 0;

   while i < len {
      match bytes[i] {
         b'(' => depth += 1,
         b')' => depth -= 1,
         // Single-quoted string literal (with '' escape handling)
         b'\'' => {
            i = skip_quoted(bytes, len, i, b'\'');
         }
         // Double-quoted identifier (with "" escape handling)
         b'"' => {
            i = skip_quoted(bytes, len, i, b'"');
         }
         // Line comment: --
         b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
            i = skip_line_comment(bytes, len, i);
         }
         // Block comment: /* ... */
         b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
            i = skip_block_comment(bytes, len, i);
         }
         _ if depth == 0 => {
            if let Some(result) = on_keyword(bytes, len, i) {
               return Some(result);
            }
         }
         _ => {}
      }
      i += 1;
   }

   None
}

/// Validate that a base query does not contain top-level ORDER BY or LIMIT.
///
/// These clauses conflict with the pagination logic, which appends its own
/// ORDER BY and LIMIT automatically. Clauses inside parenthesized
/// subexpressions (e.g., subqueries), comments, and string literals are
/// allowed.
pub(crate) fn validate_base_query(query: &str) -> Result<(), Error> {
   let found_forbidden = scan_top_level(query, |bytes, len, i| {
      if is_keyword_at(bytes, len, i, b"ORDER BY") {
         return Some(());
      }
      if is_keyword_at(bytes, len, i, b"LIMIT") {
         return Some(());
      }
      None
   });

   if found_forbidden.is_some() {
      return Err(Error::InvalidPaginationQuery);
   }

   Ok(())
}

/// Detect whether a base query has a WHERE clause at paren depth 0.
pub(crate) fn has_top_level_where(query: &str) -> bool {
   scan_top_level(query, |bytes, len, i| {
      if is_keyword_at(bytes, len, i, b"WHERE") {
         Some(())
      } else {
         None
      }
   })
   .is_some()
}

/// Build the cursor WHERE condition for seeking past the previous page.
///
/// `param_offset` is the number of user-supplied bind values that precede
/// the cursor values. Cursor placeholders are numbered `$N` starting from
/// `param_offset + 1` so they never collide with the user's `$1`, `$2`, …
/// placeholders (or positional `?` parameters).
///
/// Returns the SQL fragment and the bind values to use.
///
/// For uniform direction (all ASC or all DESC), uses row-value comparison:
/// `(col1, col2) > ($3, $4)` or `(col1, col2) < ($3, $4)`
///
/// For mixed directions, uses expanded OR form:
/// `(a > $3) OR (a = $4 AND b < $5) OR (a = $6 AND b = $7 AND c > $8)`
pub(crate) fn build_cursor_condition(
   keyset: &[KeysetColumn],
   cursor_values: &[JsonValue],
   param_offset: usize,
) -> (String, Vec<JsonValue>) {
   let n = keyset.len();
   let mut next_param = param_offset + 1;

   // Check if all directions are the same (uniform)
   let all_asc = keyset.iter().all(|k| k.direction == SortDirection::Asc);
   let all_desc = keyset.iter().all(|k| k.direction == SortDirection::Desc);

   if all_asc || all_desc {
      // Uniform direction: use row-value comparison
      let cols: Vec<String> = keyset.iter().map(|k| quote_identifier(&k.name)).collect();
      let placeholders: Vec<String> = (0..n).map(|i| format!("${}", next_param + i)).collect();
      let op = if all_asc { ">" } else { "<" };

      let sql = format!("({}) {} ({})", cols.join(", "), op, placeholders.join(", "));
      let values = cursor_values.to_vec();
      return (sql, values);
   }

   // Mixed directions: expanded OR form
   let mut clauses = Vec::new();
   let mut values = Vec::new();

   for level in 0..n {
      let mut parts = Vec::new();

      // Equality conditions for all columns before this level
      for eq_idx in 0..level {
         parts.push(format!(
            "{} = ${}",
            quote_identifier(&keyset[eq_idx].name),
            next_param
         ));
         next_param += 1;
         values.push(cursor_values[eq_idx].clone());
      }

      // Inequality condition for the column at this level
      let op = match keyset[level].direction {
         SortDirection::Asc => ">",
         SortDirection::Desc => "<",
      };
      parts.push(format!(
         "{} {} ${}",
         quote_identifier(&keyset[level].name),
         op,
         next_param
      ));
      next_param += 1;
      values.push(cursor_values[level].clone());

      clauses.push(format!("({})", parts.join(" AND ")));
   }

   let sql = clauses.join(" OR ");
   (sql, values)
}

/// Build the ORDER BY clause from the keyset definition.
pub(crate) fn build_order_by(keyset: &[KeysetColumn]) -> String {
   let parts: Vec<String> = keyset
      .iter()
      .map(|k| {
         let dir = match k.direction {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
         };
         format!("{} {}", quote_identifier(&k.name), dir)
      })
      .collect();

   format!("ORDER BY {}", parts.join(", "))
}

/// Create a keyset with all sort directions reversed.
fn reversed_keyset(keyset: &[KeysetColumn]) -> Vec<KeysetColumn> {
   keyset
      .iter()
      .map(|k| KeysetColumn {
         name: k.name.clone(),
         direction: k.direction.reversed(),
      })
      .collect()
}

/// Build the complete paginated query from a base query.
///
/// `user_param_count` is the number of bind values the caller supplies for
/// the base query (e.g., 2 when the query contains `$1` and `$2`). Cursor
/// placeholders are numbered starting from `user_param_count + 1` so they
/// never collide with user parameters.
///
/// When `backward` is true, all sort directions are reversed so the database
/// returns rows from the opposite end of the result set. The caller is
/// responsible for reversing the returned rows to restore the original order.
///
/// Returns the final SQL and all cursor bind values (which should be appended
/// after the user's own bind values).
pub(crate) fn build_paginated_query(
   base_query: &str,
   keyset: &[KeysetColumn],
   cursor: Option<&[JsonValue]>,
   page_size: usize,
   backward: bool,
   user_param_count: usize,
) -> Result<(String, Vec<JsonValue>), Error> {
   validate_base_query(base_query)?;

   // Validate all column names before interpolating into SQL
   for col in keyset {
      validate_column_name(&col.name)?;
   }

   let effective;
   let effective_keyset: &[KeysetColumn] = if backward {
      effective = reversed_keyset(keyset);
      &effective
   } else {
      keyset
   };

   let mut sql = base_query.trim_end().trim_end_matches(';').to_string();
   let mut cursor_bind_values = Vec::new();

   if let Some(cursor_vals) = cursor {
      let (condition, values) =
         build_cursor_condition(effective_keyset, cursor_vals, user_param_count);
      cursor_bind_values = values;

      if has_top_level_where(&sql) {
         sql = format!("{} AND ({})", sql, condition);
      } else {
         sql = format!("{} WHERE ({})", sql, condition);
      }
   }

   let order_by = build_order_by(effective_keyset);
   let limit = page_size.checked_add(1).ok_or(Error::InvalidPageSize)?;
   sql = format!("{} {} LIMIT {}", sql, order_by, limit);

   Ok((sql, cursor_bind_values))
}

#[cfg(test)]
mod tests {
   use super::*;
   use serde_json::json;

   // ─── validate_base_query ───

   #[test]
   fn validate_rejects_top_level_order_by() {
      let result = validate_base_query("SELECT * FROM posts ORDER BY id");
      assert!(result.is_err());
   }

   #[test]
   fn validate_rejects_top_level_limit() {
      let result = validate_base_query("SELECT * FROM posts LIMIT 10");
      assert!(result.is_err());
   }

   #[test]
   fn validate_accepts_clean_query() {
      let result = validate_base_query("SELECT * FROM posts WHERE category = ?");
      assert!(result.is_ok());
   }

   #[test]
   fn validate_allows_order_by_inside_subquery() {
      let result = validate_base_query("SELECT * FROM (SELECT * FROM posts ORDER BY id LIMIT 5)");
      assert!(result.is_ok());
   }

   #[test]
   fn validate_allows_limit_inside_subquery() {
      let result = validate_base_query("SELECT * FROM (SELECT * FROM posts LIMIT 5)");
      assert!(result.is_ok());
   }

   #[test]
   fn validate_rejects_order_by_after_subquery() {
      let result = validate_base_query("SELECT * FROM (SELECT * FROM posts LIMIT 5) ORDER BY id");
      assert!(result.is_err());
   }

   // ─── has_top_level_where ───

   #[test]
   fn detects_top_level_where() {
      assert!(has_top_level_where("SELECT * FROM posts WHERE id > 5"));
   }

   #[test]
   fn no_where_clause() {
      assert!(!has_top_level_where("SELECT * FROM posts"));
   }

   #[test]
   fn where_inside_subquery_only() {
      assert!(!has_top_level_where(
         "SELECT * FROM (SELECT * FROM posts WHERE id > 5)"
      ));
   }

   // ─── scanner: comments and quoted strings ───

   #[test]
   fn validate_ignores_order_by_in_line_comment() {
      let result = validate_base_query("SELECT * FROM posts -- ORDER BY id");
      assert!(result.is_ok());
   }

   #[test]
   fn validate_ignores_limit_in_block_comment() {
      let result = validate_base_query("SELECT * FROM posts /* LIMIT 10 */");
      assert!(result.is_ok());
   }

   #[test]
   fn validate_ignores_order_by_in_string_literal() {
      let result = validate_base_query("SELECT * FROM posts WHERE name = 'ORDER BY clause'");
      assert!(result.is_ok());
   }

   #[test]
   fn validate_ignores_keywords_in_escaped_single_quotes() {
      // SQLite escapes single quotes by doubling: 'order''s ORDER BY clause'
      let result = validate_base_query("SELECT * FROM t WHERE name = 'order''s ORDER BY clause'");
      assert!(result.is_ok());
   }

   #[test]
   fn validate_ignores_keywords_in_double_quoted_identifier() {
      let result = validate_base_query(r#"SELECT "ORDER BY" FROM posts"#);
      assert!(result.is_ok());
   }

   #[test]
   fn validate_detects_order_by_after_block_comment() {
      let result = validate_base_query("SELECT * FROM posts /* comment */ ORDER BY id");
      assert!(result.is_err());
   }

   #[test]
   fn validate_detects_limit_after_line_comment() {
      let result = validate_base_query("SELECT * FROM posts -- comment\nLIMIT 10");
      assert!(result.is_err());
   }

   #[test]
   fn where_ignores_where_in_line_comment() {
      assert!(!has_top_level_where("SELECT * FROM posts -- WHERE id > 5"));
   }

   #[test]
   fn where_ignores_where_in_string_with_escaped_quotes() {
      // The real WHERE is found; the WHERE inside the escaped-quote string is ignored
      assert!(has_top_level_where(
         "SELECT * FROM t WHERE name = 'it''s WHERE we go'"
      ));
      // No real WHERE — the word WHERE is entirely inside a string with escaped quotes
      assert!(!has_top_level_where("SELECT 'it''s WHERE we go' FROM t"));
   }

   // ─── validate_column_name ───

   #[test]
   fn column_name_valid_simple() {
      assert!(validate_column_name("id").is_ok());
      assert!(validate_column_name("category").is_ok());
      assert!(validate_column_name("_private").is_ok());
      assert!(validate_column_name("col_123").is_ok());
   }

   #[test]
   fn column_name_valid_qualified() {
      assert!(validate_column_name("posts.id").is_ok());
      assert!(validate_column_name("schema.table.column").is_ok());
   }

   #[test]
   fn column_name_rejects_empty() {
      assert!(validate_column_name("").is_err());
   }

   #[test]
   fn column_name_rejects_injection() {
      assert!(validate_column_name("id; DROP TABLE posts --").is_err());
      assert!(validate_column_name("id)--").is_err());
      assert!(validate_column_name("1bad").is_err());
      assert!(validate_column_name("col name").is_err());
   }

   // ─── build_cursor_condition ───

   #[test]
   fn cursor_condition_uniform_asc() {
      let keyset = vec![KeysetColumn::asc("a"), KeysetColumn::asc("b")];
      let cursor = vec![json!(1), json!(2)];

      let (sql, values) = build_cursor_condition(&keyset, &cursor, 0);

      assert_eq!(sql, r#"("a", "b") > ($1, $2)"#);
      assert_eq!(values, vec![json!(1), json!(2)]);
   }

   #[test]
   fn cursor_condition_uniform_asc_with_offset() {
      let keyset = vec![KeysetColumn::asc("a"), KeysetColumn::asc("b")];
      let cursor = vec![json!(1), json!(2)];

      // Simulate 2 user parameters ($1, $2) preceding the cursor
      let (sql, values) = build_cursor_condition(&keyset, &cursor, 2);

      assert_eq!(sql, r#"("a", "b") > ($3, $4)"#);
      assert_eq!(values, vec![json!(1), json!(2)]);
   }

   #[test]
   fn cursor_condition_uniform_desc() {
      let keyset = vec![KeysetColumn::desc("a"), KeysetColumn::desc("b")];
      let cursor = vec![json!(10), json!(20)];

      let (sql, values) = build_cursor_condition(&keyset, &cursor, 0);

      assert_eq!(sql, r#"("a", "b") < ($1, $2)"#);
      assert_eq!(values, vec![json!(10), json!(20)]);
   }

   #[test]
   fn cursor_condition_mixed_directions() {
      let keyset = vec![
         KeysetColumn::asc("a"),
         KeysetColumn::desc("b"),
         KeysetColumn::asc("c"),
      ];
      let cursor = vec![json!("va"), json!("vb"), json!("vc")];

      let (sql, values) = build_cursor_condition(&keyset, &cursor, 0);

      assert_eq!(
         sql,
         r#"("a" > $1) OR ("a" = $2 AND "b" < $3) OR ("a" = $4 AND "b" = $5 AND "c" > $6)"#
      );
      assert_eq!(
         values,
         vec![
            json!("va"),
            json!("va"),
            json!("vb"),
            json!("va"),
            json!("vb"),
            json!("vc"),
         ]
      );
   }

   #[test]
   fn cursor_condition_mixed_directions_with_offset() {
      let keyset = vec![
         KeysetColumn::asc("a"),
         KeysetColumn::desc("b"),
         KeysetColumn::asc("c"),
      ];
      let cursor = vec![json!("va"), json!("vb"), json!("vc")];

      // Simulate 1 user parameter ($1) preceding the cursor
      let (sql, values) = build_cursor_condition(&keyset, &cursor, 1);

      assert_eq!(
         sql,
         r#"("a" > $2) OR ("a" = $3 AND "b" < $4) OR ("a" = $5 AND "b" = $6 AND "c" > $7)"#
      );
      assert_eq!(
         values,
         vec![
            json!("va"),
            json!("va"),
            json!("vb"),
            json!("va"),
            json!("vb"),
            json!("vc"),
         ]
      );
   }

   #[test]
   fn cursor_condition_single_column_asc() {
      let keyset = vec![KeysetColumn::asc("id")];
      let cursor = vec![json!(42)];

      let (sql, values) = build_cursor_condition(&keyset, &cursor, 0);

      assert_eq!(sql, r#"("id") > ($1)"#);
      assert_eq!(values, vec![json!(42)]);
   }

   #[test]
   fn cursor_condition_single_column_desc() {
      let keyset = vec![KeysetColumn::desc("id")];
      let cursor = vec![json!(42)];

      let (sql, values) = build_cursor_condition(&keyset, &cursor, 0);

      assert_eq!(sql, r#"("id") < ($1)"#);
      assert_eq!(values, vec![json!(42)]);
   }

   // ─── build_order_by ───

   #[test]
   fn order_by_mixed_directions() {
      let keyset = vec![
         KeysetColumn::asc("category"),
         KeysetColumn::desc("score"),
         KeysetColumn::asc("id"),
      ];

      let sql = build_order_by(&keyset);

      assert_eq!(sql, r#"ORDER BY "category" ASC, "score" DESC, "id" ASC"#);
   }

   // ─── build_paginated_query ───

   #[test]
   fn paginated_query_first_page() {
      let keyset = vec![KeysetColumn::asc("id")];

      let (sql, values) =
         build_paginated_query("SELECT * FROM posts", &keyset, None, 20, false, 0).unwrap();

      assert_eq!(sql, r#"SELECT * FROM posts ORDER BY "id" ASC LIMIT 21"#);
      assert!(values.is_empty());
   }

   #[test]
   fn paginated_query_with_cursor() {
      let keyset = vec![KeysetColumn::asc("id")];
      let cursor = vec![json!(100)];

      let (sql, values) =
         build_paginated_query("SELECT * FROM posts", &keyset, Some(&cursor), 20, false, 0)
            .unwrap();

      assert_eq!(
         sql,
         r#"SELECT * FROM posts WHERE (("id") > ($1)) ORDER BY "id" ASC LIMIT 21"#
      );
      assert_eq!(values, vec![json!(100)]);
   }

   #[test]
   fn paginated_query_with_existing_where() {
      let keyset = vec![KeysetColumn::asc("id")];
      let cursor = vec![json!(100)];

      // 1 user param ($1 for category) → cursor starts at $2
      let (sql, values) = build_paginated_query(
         "SELECT * FROM posts WHERE category = $1",
         &keyset,
         Some(&cursor),
         20,
         false,
         1,
      )
      .unwrap();

      assert_eq!(
         sql,
         r#"SELECT * FROM posts WHERE category = $1 AND (("id") > ($2)) ORDER BY "id" ASC LIMIT 21"#
      );
      assert_eq!(values, vec![json!(100)]);
   }

   #[test]
   fn paginated_query_strips_trailing_semicolon() {
      let keyset = vec![KeysetColumn::asc("id")];

      let (sql, _) =
         build_paginated_query("SELECT * FROM posts;", &keyset, None, 10, false, 0).unwrap();

      assert_eq!(sql, r#"SELECT * FROM posts ORDER BY "id" ASC LIMIT 11"#);
   }

   #[test]
   fn paginated_query_rejects_order_by() {
      let keyset = vec![KeysetColumn::asc("id")];

      let result = build_paginated_query(
         "SELECT * FROM posts ORDER BY id",
         &keyset,
         None,
         10,
         false,
         0,
      );
      assert!(result.is_err());
   }

   #[test]
   fn paginated_query_mixed_keyset_with_cursor() {
      let keyset = vec![
         KeysetColumn::asc("category"),
         KeysetColumn::desc("score"),
         KeysetColumn::asc("id"),
      ];
      let cursor = vec![json!("tech"), json!(95), json!(42)];

      let (sql, values) =
         build_paginated_query("SELECT * FROM posts", &keyset, Some(&cursor), 25, false, 0)
            .unwrap();

      assert_eq!(
         sql,
         r#"SELECT * FROM posts WHERE (("category" > $1) OR ("category" = $2 AND "score" < $3) OR ("category" = $4 AND "score" = $5 AND "id" > $6)) ORDER BY "category" ASC, "score" DESC, "id" ASC LIMIT 26"#
      );
      assert_eq!(
         values,
         vec![
            json!("tech"),
            json!("tech"),
            json!(95),
            json!("tech"),
            json!(95),
            json!(42),
         ]
      );
   }

   // ─── SortDirection::reversed ───

   #[test]
   fn sort_direction_reversed() {
      assert_eq!(SortDirection::Asc.reversed(), SortDirection::Desc);
      assert_eq!(SortDirection::Desc.reversed(), SortDirection::Asc);
   }

   // ─── build_paginated_query backward ───

   #[test]
   fn paginated_query_backward_no_cursor() {
      let keyset = vec![KeysetColumn::asc("id")];

      let (sql, values) =
         build_paginated_query("SELECT * FROM posts", &keyset, None, 20, true, 0).unwrap();

      // Reversed: ASC becomes DESC
      assert_eq!(sql, r#"SELECT * FROM posts ORDER BY "id" DESC LIMIT 21"#);
      assert!(values.is_empty());
   }

   #[test]
   fn paginated_query_backward_uniform_asc() {
      let keyset = vec![KeysetColumn::asc("a"), KeysetColumn::asc("b")];
      let cursor = vec![json!(10), json!(20)];

      let (sql, values) =
         build_paginated_query("SELECT * FROM posts", &keyset, Some(&cursor), 20, true, 0).unwrap();

      // Reversed ASC→DESC: uses < operator
      assert_eq!(
         sql,
         r#"SELECT * FROM posts WHERE (("a", "b") < ($1, $2)) ORDER BY "a" DESC, "b" DESC LIMIT 21"#
      );
      assert_eq!(values, vec![json!(10), json!(20)]);
   }

   #[test]
   fn paginated_query_backward_uniform_desc() {
      let keyset = vec![KeysetColumn::desc("a"), KeysetColumn::desc("b")];
      let cursor = vec![json!(10), json!(20)];

      let (sql, values) =
         build_paginated_query("SELECT * FROM posts", &keyset, Some(&cursor), 20, true, 0).unwrap();

      // Reversed DESC→ASC: uses > operator
      assert_eq!(
         sql,
         r#"SELECT * FROM posts WHERE (("a", "b") > ($1, $2)) ORDER BY "a" ASC, "b" ASC LIMIT 21"#
      );
      assert_eq!(values, vec![json!(10), json!(20)]);
   }

   #[test]
   fn paginated_query_backward_mixed_with_cursor() {
      let keyset = vec![
         KeysetColumn::asc("a"),
         KeysetColumn::desc("b"),
         KeysetColumn::asc("c"),
      ];
      let cursor = vec![json!("va"), json!("vb"), json!("vc")];

      let (sql, values) =
         build_paginated_query("SELECT * FROM posts", &keyset, Some(&cursor), 25, true, 0).unwrap();

      // Reversed: ASC→DESC (uses <), DESC→ASC (uses >), ASC→DESC (uses <)
      assert_eq!(
         sql,
         r#"SELECT * FROM posts WHERE (("a" < $1) OR ("a" = $2 AND "b" > $3) OR ("a" = $4 AND "b" = $5 AND "c" < $6)) ORDER BY "a" DESC, "b" ASC, "c" DESC LIMIT 26"#
      );
      assert_eq!(
         values,
         vec![
            json!("va"),
            json!("va"),
            json!("vb"),
            json!("va"),
            json!("vb"),
            json!("vc"),
         ]
      );
   }

   #[test]
   fn paginated_query_backward_with_existing_where() {
      let keyset = vec![KeysetColumn::asc("id")];
      let cursor = vec![json!(100)];

      // 1 user param ($1 for category) → cursor starts at $2
      let (sql, values) = build_paginated_query(
         "SELECT * FROM posts WHERE category = $1",
         &keyset,
         Some(&cursor),
         20,
         true,
         1,
      )
      .unwrap();

      assert_eq!(
         sql,
         r#"SELECT * FROM posts WHERE category = $1 AND (("id") < ($2)) ORDER BY "id" DESC LIMIT 21"#
      );
      assert_eq!(values, vec![json!(100)]);
   }

   // ─── build_paginated_query: column validation ───

   #[test]
   fn paginated_query_rejects_invalid_column_name() {
      let keyset = vec![KeysetColumn::asc("id; DROP TABLE posts --")];

      let result = build_paginated_query("SELECT * FROM posts", &keyset, None, 10, false, 0);

      assert!(matches!(result, Err(Error::InvalidColumnName { .. })));
   }

   // ─── quote_identifier ───

   #[test]
   fn quote_identifier_simple() {
      assert_eq!(quote_identifier("id"), r#""id""#);
   }

   #[test]
   fn quote_identifier_with_dot() {
      assert_eq!(quote_identifier("t.id"), r#""t.id""#);
   }

   // ─── SortDirection serde ───

   #[test]
   fn sort_direction_serializes_to_camel_case() {
      assert_eq!(
         serde_json::to_string(&SortDirection::Asc).unwrap(),
         "\"asc\""
      );
      assert_eq!(
         serde_json::to_string(&SortDirection::Desc).unwrap(),
         "\"desc\""
      );
   }

   #[test]
   fn sort_direction_deserializes_from_camel_case() {
      let asc: SortDirection = serde_json::from_str("\"asc\"").unwrap();
      let desc: SortDirection = serde_json::from_str("\"desc\"").unwrap();
      assert_eq!(asc, SortDirection::Asc);
      assert_eq!(desc, SortDirection::Desc);
   }
}
