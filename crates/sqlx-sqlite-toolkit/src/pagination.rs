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

   // Reject structurally invalid dot usage: trailing dots, leading dots
   // (after first char), consecutive dots, or empty parts
   if name.split('.').any(|part| part.is_empty()) {
      return Err(Error::InvalidColumnName {
         name: name.to_string(),
      });
   }

   Ok(())
}

/// Quote a column name with double-quote identifiers for defense-in-depth.
///
/// Qualified names (e.g., `table.column`) are split on `.` and each part is
/// quoted individually: `"table"."column"`. Any embedded double quotes in a
/// part are doubled per SQL standard (`"` → `""`).
pub(crate) fn quote_identifier(name: &str) -> String {
   name
      .split('.')
      .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
      .collect::<Vec<_>>()
      .join(".")
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

/// Check whether `ORDER BY` starts at position `i`, allowing any amount of
/// whitespace (spaces, tabs, newlines) between `ORDER` and `BY`.
fn is_order_by_at(bytes: &[u8], len: usize, i: usize) -> bool {
   if !is_keyword_at(bytes, len, i, b"ORDER") {
      return false;
   }
   let mut j = i + 5; // skip "ORDER"
   while j < len && bytes[j].is_ascii_whitespace() {
      j += 1;
   }
   is_keyword_at(bytes, len, j, b"BY")
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
         // Clamp to zero so a stray `)` in malformed SQL doesn't push depth
         // negative, which would cause the scanner to miss subsequent
         // top-level keywords. SQLite will reject the unbalanced parens; we
         // just need to keep our own detection predictable.
         b')' => depth = (depth - 1).max(0),
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
      if is_order_by_at(bytes, len, i) {
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
   use rstest::rstest;
   use serde_json::json;

   // ─── validate_base_query ───

   #[rstest]
   #[case::top_level_order_by("SELECT * FROM posts ORDER BY id")]
   #[case::top_level_limit("SELECT * FROM posts LIMIT 10")]
   #[case::order_by_after_subquery("SELECT * FROM (SELECT * FROM posts LIMIT 5) ORDER BY id")]
   #[case::order_by_with_double_space("SELECT * FROM posts ORDER  BY id")]
   #[case::order_by_with_tab("SELECT * FROM posts ORDER\tBY id")]
   #[case::order_by_with_newline("SELECT * FROM posts ORDER\nBY id")]
   #[case::order_by_after_block_comment("SELECT * FROM posts /* comment */ ORDER BY id")]
   #[case::limit_after_line_comment("SELECT * FROM posts -- comment\nLIMIT 10")]
   fn validate_base_query_rejects(#[case] query: &str) {
      let result = validate_base_query(query);
      assert!(
         matches!(result, Err(Error::InvalidPaginationQuery)),
         "{query:?} should be rejected, got {result:?}"
      );
   }

   #[rstest]
   #[case::clean_query("SELECT * FROM posts WHERE category = ?")]
   #[case::order_by_inside_subquery("SELECT * FROM (SELECT * FROM posts ORDER BY id LIMIT 5)")]
   #[case::limit_inside_subquery("SELECT * FROM (SELECT * FROM posts LIMIT 5)")]
   #[case::order_by_in_line_comment("SELECT * FROM posts -- ORDER BY id")]
   #[case::limit_in_block_comment("SELECT * FROM posts /* LIMIT 10 */")]
   #[case::order_by_in_string_literal("SELECT * FROM posts WHERE name = 'ORDER BY clause'")]
   // SQLite escapes single quotes by doubling: 'order''s ORDER BY clause'
   #[case::keywords_in_escaped_single_quotes(
      "SELECT * FROM t WHERE name = 'order''s ORDER BY clause'"
   )]
   #[case::keywords_in_double_quoted_identifier(r#"SELECT "ORDER BY" FROM posts"#)]
   fn validate_base_query_accepts(#[case] query: &str) {
      let result = validate_base_query(query);
      assert!(
         result.is_ok(),
         "{query:?} should be accepted, got {result:?}"
      );
   }

   // ─── has_top_level_where ───

   #[rstest]
   #[case::top_level_where("SELECT * FROM posts WHERE id > 5")]
   // The real WHERE is found; the WHERE inside the escaped-quote string is ignored
   #[case::where_beside_escaped_quote_string("SELECT * FROM t WHERE name = 'it''s WHERE we go'")]
   fn has_top_level_where_detects(#[case] query: &str) {
      assert!(
         has_top_level_where(query),
         "{query:?} should have a top-level WHERE"
      );
   }

   #[rstest]
   #[case::no_where_clause("SELECT * FROM posts")]
   #[case::where_inside_subquery_only("SELECT * FROM (SELECT * FROM posts WHERE id > 5)")]
   #[case::where_in_line_comment("SELECT * FROM posts -- WHERE id > 5")]
   // No real WHERE — the word WHERE is entirely inside a string with escaped quotes
   #[case::where_only_in_escaped_quote_string("SELECT 'it''s WHERE we go' FROM t")]
   fn has_top_level_where_ignores(#[case] query: &str) {
      assert!(
         !has_top_level_where(query),
         "{query:?} should not have a top-level WHERE"
      );
   }

   // ─── validate_column_name ───

   #[rstest]
   #[case::simple_id("id")]
   #[case::simple_word("category")]
   #[case::leading_underscore("_private")]
   #[case::underscore_and_digits("col_123")]
   #[case::qualified_table_column("posts.id")]
   #[case::qualified_schema_table_column("schema.table.column")]
   fn validate_column_name_accepts(#[case] name: &str) {
      let result = validate_column_name(name);
      assert!(
         result.is_ok(),
         "{name:?} should be accepted, got {result:?}"
      );
   }

   #[rstest]
   #[case::empty("")]
   #[case::semicolon_injection("id; DROP TABLE posts --")]
   #[case::paren_comment_injection("id)--")]
   #[case::leading_digit("1bad")]
   #[case::contains_space("col name")]
   #[case::trailing_dot("table.")]
   #[case::consecutive_dots("table..column")]
   #[case::leading_dot(".column")]
   fn validate_column_name_rejects(#[case] name: &str) {
      let result = validate_column_name(name);
      assert!(
         matches!(&result, Err(Error::InvalidColumnName { name: rejected }) if rejected == name),
         "{name:?} should be rejected as InvalidColumnName naming it, got {result:?}"
      );
   }

   // ─── build_cursor_condition ───

   #[rstest]
   #[case::uniform_asc(
      vec![KeysetColumn::asc("a"), KeysetColumn::asc("b")],
      vec![json!(1), json!(2)],
      0,
      r#"("a", "b") > ($1, $2)"#,
      vec![json!(1), json!(2)]
   )]
   // Simulate 2 user parameters ($1, $2) preceding the cursor
   #[case::uniform_asc_with_offset(
      vec![KeysetColumn::asc("a"), KeysetColumn::asc("b")],
      vec![json!(1), json!(2)],
      2,
      r#"("a", "b") > ($3, $4)"#,
      vec![json!(1), json!(2)]
   )]
   #[case::uniform_desc(
      vec![KeysetColumn::desc("a"), KeysetColumn::desc("b")],
      vec![json!(10), json!(20)],
      0,
      r#"("a", "b") < ($1, $2)"#,
      vec![json!(10), json!(20)]
   )]
   #[case::mixed_directions(
      vec![
         KeysetColumn::asc("a"),
         KeysetColumn::desc("b"),
         KeysetColumn::asc("c"),
      ],
      vec![json!("va"), json!("vb"), json!("vc")],
      0,
      r#"("a" > $1) OR ("a" = $2 AND "b" < $3) OR ("a" = $4 AND "b" = $5 AND "c" > $6)"#,
      vec![
         json!("va"),
         json!("va"),
         json!("vb"),
         json!("va"),
         json!("vb"),
         json!("vc"),
      ]
   )]
   // Simulate 1 user parameter ($1) preceding the cursor
   #[case::mixed_directions_with_offset(
      vec![
         KeysetColumn::asc("a"),
         KeysetColumn::desc("b"),
         KeysetColumn::asc("c"),
      ],
      vec![json!("va"), json!("vb"), json!("vc")],
      1,
      r#"("a" > $2) OR ("a" = $3 AND "b" < $4) OR ("a" = $5 AND "b" = $6 AND "c" > $7)"#,
      vec![
         json!("va"),
         json!("va"),
         json!("vb"),
         json!("va"),
         json!("vb"),
         json!("vc"),
      ]
   )]
   #[case::single_column_asc(
      vec![KeysetColumn::asc("id")],
      vec![json!(42)],
      0,
      r#"("id") > ($1)"#,
      vec![json!(42)]
   )]
   #[case::single_column_desc(
      vec![KeysetColumn::desc("id")],
      vec![json!(42)],
      0,
      r#"("id") < ($1)"#,
      vec![json!(42)]
   )]
   fn cursor_condition_builds_sql_and_values(
      #[case] keyset: Vec<KeysetColumn>,
      #[case] cursor: Vec<JsonValue>,
      #[case] param_offset: usize,
      #[case] expected_sql: &str,
      #[case] expected_values: Vec<JsonValue>,
   ) {
      let (sql, values) = build_cursor_condition(&keyset, &cursor, param_offset);

      assert_eq!(sql, expected_sql);
      assert_eq!(values, expected_values);
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

      let (sql, values) =
         build_paginated_query("SELECT * FROM posts;", &keyset, None, 10, false, 0).unwrap();

      assert_eq!(sql, r#"SELECT * FROM posts ORDER BY "id" ASC LIMIT 11"#);
      assert!(values.is_empty());
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

   #[rstest]
   #[case::asc(SortDirection::Asc, SortDirection::Desc)]
   #[case::desc(SortDirection::Desc, SortDirection::Asc)]
   fn sort_direction_reversed(#[case] direction: SortDirection, #[case] expected: SortDirection) {
      assert_eq!(direction.reversed(), expected);
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

   #[rstest]
   #[case::simple("id", r#""id""#)]
   #[case::with_dot("t.id", r#""t"."id""#)]
   #[case::schema_table_column("schema.table.col", r#""schema"."table"."col""#)]
   fn quote_identifier_quotes_each_part(#[case] name: &str, #[case] expected: &str) {
      assert_eq!(quote_identifier(name), expected);
   }

   // ─── SortDirection serde ───

   #[rstest]
   #[case::asc(SortDirection::Asc, "\"asc\"")]
   #[case::desc(SortDirection::Desc, "\"desc\"")]
   fn sort_direction_serializes_to_camel_case(
      #[case] direction: SortDirection,
      #[case] expected: &str,
   ) {
      assert_eq!(serde_json::to_string(&direction).unwrap(), expected);
   }

   #[rstest]
   #[case::asc("\"asc\"", SortDirection::Asc)]
   #[case::desc("\"desc\"", SortDirection::Desc)]
   fn sort_direction_deserializes_from_camel_case(
      #[case] json: &str,
      #[case] expected: SortDirection,
   ) {
      let direction: SortDirection = serde_json::from_str(json).unwrap();
      assert_eq!(direction, expected);
   }
}
