use rstest::rstest;
use serde_json::{Value as JsonValue, json};
use sqlx_sqlite_toolkit::{DatabaseWrapper, Error, KeysetColumn, KeysetPage};
use tempfile::TempDir;

async fn create_test_db() -> (DatabaseWrapper, TempDir) {
   let temp_dir = TempDir::new().expect("Failed to create temp directory");
   let db_path = temp_dir.path().join("test.db");
   let wrapper = DatabaseWrapper::connect(&db_path, None)
      .await
      .expect("Failed to connect to test database");

   (wrapper, temp_dir)
}

/// Seed 7 posts across 3 categories with varying scores.
///
/// ```text
/// id | title  | category | score
/// ---|--------|----------|------
///  1 | Post 1 | science  | 95
///  2 | Post 2 | science  | 80
///  3 | Post 3 | tech     | 90
///  4 | Post 4 | tech     | 85
///  5 | Post 5 | tech     | 70
///  6 | Post 6 | art      | 88
///  7 | Post 7 | art      | 60
/// ```
async fn seed_posts_table(db: &DatabaseWrapper) {
   db.execute(
      "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT NOT NULL, category TEXT NOT NULL, score INTEGER NOT NULL)".into(),
      vec![],
   )
   .await
   .unwrap();

   let rows = [
      (1, "Post 1", "science", 95),
      (2, "Post 2", "science", 80),
      (3, "Post 3", "tech", 90),
      (4, "Post 4", "tech", 85),
      (5, "Post 5", "tech", 70),
      (6, "Post 6", "art", 88),
      (7, "Post 7", "art", 60),
   ];

   for (id, title, category, score) in rows {
      db.execute(
         "INSERT INTO posts (id, title, category, score) VALUES ($1, $2, $3, $4)".into(),
         vec![json!(id), json!(title), json!(category), json!(score)],
      )
      .await
      .unwrap();
   }
}

/// Extract the `id` column from each row for concise assertions.
fn row_ids(page: &KeysetPage) -> Vec<i64> {
   page
      .rows
      .iter()
      .map(|r| r["id"].as_i64().unwrap())
      .collect()
}

// ─── Core Forward Pagination ───

#[rstest]
// ── Page 1 (no cursor) ──
// Generated SQL:
//    SELECT id, title FROM posts ORDER BY id ASC LIMIT 4
//
// The builder appends ORDER BY from the keyset and LIMIT = page_size + 1.
// The extra row is a "sentinel" — if it comes back, there are more pages.
#[case::asc_single_column(vec![KeysetColumn::asc("id")], &[1, 2, 3], vec![json!(3)])]
// ── Page 1 (descending, no cursor) ──
// Generated SQL:
//    SELECT id, title FROM posts ORDER BY id DESC LIMIT 4
//
// Descending keyset means the highest IDs come first.
#[case::desc_single_column(vec![KeysetColumn::desc("id")], &[7, 6, 5], vec![json!(5)])]
#[tokio::test]
async fn first_page_no_cursor(
   #[case] keyset: Vec<KeysetColumn>,
   #[case] expected_ids: &[i64],
   #[case] next_cursor: Vec<JsonValue>,
) {
   let (db, _temp) = create_test_db().await;
   seed_posts_table(&db).await;

   let page = db
      .fetch_page("SELECT id, title FROM posts".into(), vec![], keyset, 3)
      .await
      .unwrap();

   assert_eq!(row_ids(&page), expected_ids);
   assert!(page.has_more);
   assert_eq!(page.next_cursor, Some(next_cursor));

   db.remove().await.unwrap();
}

#[tokio::test]
async fn forward_pagination_all_pages() {
   let (db, _temp) = create_test_db().await;
   seed_posts_table(&db).await;

   let keyset = vec![KeysetColumn::asc("id")];
   let query = "SELECT id, title FROM posts";

   // ── Page 1 (no cursor) ──
   // Generated SQL:
   //    SELECT id, title FROM posts ORDER BY id ASC LIMIT 4
   //
   // The builder appends ORDER BY from the keyset and LIMIT = page_size + 1.
   // The extra row is a "sentinel" — if it comes back, there are more pages.
   let page1 = db
      .fetch_page(query.into(), vec![], keyset.clone(), 3)
      .await
      .unwrap();

   assert_eq!(row_ids(&page1), vec![1, 2, 3]);
   assert!(page1.has_more);
   assert_eq!(page1.next_cursor, Some(vec![json!(3)]));

   // ── Page 2 (cursor = [3]) ──
   // Generated SQL:
   //    SELECT id, title FROM posts
   //       WHERE ((id) > (?))          -- cursor condition
   //       ORDER BY id ASC LIMIT 4
   //    bind: [3]
   //
   // For a single ASC column, the cursor condition is simply `col > ?`.
   // This seeks past all rows on page 1 without scanning them.
   let page2 = db
      .fetch_page(query.into(), vec![], keyset.clone(), 3)
      .after(page1.next_cursor.unwrap())
      .await
      .unwrap();

   assert_eq!(row_ids(&page2), vec![4, 5, 6]);
   assert!(page2.has_more);
   assert_eq!(page2.next_cursor, Some(vec![json!(6)]));

   // ── Page 3 (cursor = [6]) ──
   // Generated SQL:
   //    SELECT id, title FROM posts
   //       WHERE ((id) > (?))
   //       ORDER BY id ASC LIMIT 4
   //    bind: [6]
   //
   // Only 1 row remains (id=7), so the sentinel row is absent → has_more=false.
   let page3 = db
      .fetch_page(query.into(), vec![], keyset, 3)
      .after(page2.next_cursor.unwrap())
      .await
      .unwrap();

   assert_eq!(row_ids(&page3), vec![7]);
   assert!(!page3.has_more);
   assert_eq!(page3.next_cursor, None);

   db.remove().await.unwrap();
}

// ─── Backward Pagination ───

#[tokio::test]
async fn backward_returns_original_sort_order() {
   let (db, _temp) = create_test_db().await;
   seed_posts_table(&db).await;

   let keyset = vec![KeysetColumn::asc("id")];

   // ── Backward from cursor [4] ──
   // Generated SQL (sort directions reversed internally):
   //    SELECT id, title FROM posts
   //       WHERE ((id) < (?))         -- reversed: ASC→DESC flips > to <
   //       ORDER BY id DESC LIMIT 4   -- reversed: ASC→DESC
   //    bind: [4]
   //
   // The database returns rows [3, 2, 1] in DESC order.
   // The builder reverses them back to [1, 2, 3] (original ASC order).
   let page = db
      .fetch_page("SELECT id, title FROM posts".into(), vec![], keyset, 3)
      .before(vec![json!(4)])
      .await
      .unwrap();

   assert_eq!(row_ids(&page), vec![1, 2, 3]);
   assert!(!page.has_more);
   assert_eq!(page.next_cursor, None);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn backward_has_more_when_rows_remain() {
   let (db, _temp) = create_test_db().await;
   seed_posts_table(&db).await;

   let keyset = vec![KeysetColumn::asc("id")];

   // ── Backward from cursor [7], page_size=2 ──
   // Generated SQL (sort directions reversed internally):
   //    SELECT id, title FROM posts
   //       WHERE ((id) < (?))
   //       ORDER BY id DESC LIMIT 3   -- page_size + 1 = 3
   //    bind: [7]
   //
   // DB returns [6, 5, 4] (3 rows > page_size of 2 → sentinel present).
   // Truncated to [6, 5], then reversed to [5, 6] (original ASC order).
   // next_cursor comes from the first row after reversal = [5].
   let page = db
      .fetch_page("SELECT id, title FROM posts".into(), vec![], keyset, 2)
      .before(vec![json!(7)])
      .await
      .unwrap();

   assert_eq!(row_ids(&page), vec![5, 6]);
   assert!(page.has_more);
   assert_eq!(page.next_cursor, Some(vec![json!(5)]));

   db.remove().await.unwrap();
}

// ─── Mixed Sort Directions ───

#[tokio::test]
async fn mixed_sort_directions_forward() {
   let (db, _temp) = create_test_db().await;
   seed_posts_table(&db).await;

   let keyset = vec![
      KeysetColumn::asc("category"),
      KeysetColumn::desc("score"),
      KeysetColumn::asc("id"),
   ];
   let query = "SELECT * FROM posts";

   // ── Page 1 (no cursor) ──
   // Generated SQL:
   //    SELECT * FROM posts
   //       ORDER BY category ASC, score DESC, id ASC
   //       LIMIT 4
   //
   // Rows sorted: (art,88,6), (art,60,7), (science,95,1), (science,80,2),
   //              (tech,90,3), (tech,85,4), (tech,70,5)
   let page1 = db
      .fetch_page(query.into(), vec![], keyset.clone(), 3)
      .await
      .unwrap();

   assert_eq!(row_ids(&page1), vec![6, 7, 1]);
   assert!(page1.has_more);
   assert_eq!(
      page1.next_cursor,
      Some(vec![json!("science"), json!(95), json!(1)])
   );

   // ── Page 2 (cursor = ["science", 95, 1]) ──
   // Generated SQL:
   //    SELECT * FROM posts
   //       WHERE (
   //          (category > ?)
   //          OR (category = ? AND score < ?)
   //          OR (category = ? AND score = ? AND id > ?)
   //       )
   //       ORDER BY category ASC, score DESC, id ASC
   //       LIMIT 4
   //    bind: ["science", "science", 95, "science", 95, 1]
   //
   // Mixed directions can't use row-value comparison (col1, col2) > (?, ?),
   // so the builder expands to an OR of increasingly-specific AND clauses.
   // Each level uses > for ASC columns and < for DESC columns.
   let page2 = db
      .fetch_page(query.into(), vec![], keyset.clone(), 3)
      .after(page1.next_cursor.unwrap())
      .await
      .unwrap();

   assert_eq!(row_ids(&page2), vec![2, 3, 4]);
   assert!(page2.has_more);
   assert_eq!(
      page2.next_cursor,
      Some(vec![json!("tech"), json!(85), json!(4)])
   );

   // ── Page 3 (cursor = ["tech", 85, 4]) ──
   // Generated SQL:
   //    SELECT * FROM posts
   //       WHERE (
   //          (category > ?)
   //          OR (category = ? AND score < ?)
   //          OR (category = ? AND score = ? AND id > ?)
   //       )
   //       ORDER BY category ASC, score DESC, id ASC
   //       LIMIT 4
   //    bind: ["tech", "tech", 85, "tech", 85, 4]
   //
   // Only id=5 (tech, 70) remains → no sentinel → has_more=false.
   let page3 = db
      .fetch_page(query.into(), vec![], keyset, 3)
      .after(page2.next_cursor.unwrap())
      .await
      .unwrap();

   assert_eq!(row_ids(&page3), vec![5]);
   assert!(!page3.has_more);
   assert_eq!(page3.next_cursor, None);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn mixed_sort_directions_backward() {
   let (db, _temp) = create_test_db().await;
   seed_posts_table(&db).await;

   let keyset = vec![
      KeysetColumn::asc("category"),
      KeysetColumn::desc("score"),
      KeysetColumn::asc("id"),
   ];

   // ── Backward from cursor ["science", 80, 2] (first row of page 2) ──
   // Generated SQL (sort directions reversed internally):
   //    SELECT * FROM posts
   //       WHERE (
   //          (category < ?)
   //          OR (category = ? AND score > ?)
   //          OR (category = ? AND score = ? AND id < ?)
   //       )
   //       ORDER BY category DESC, score ASC, id DESC
   //       LIMIT 4
   //    bind: ["science", "science", 80, "science", 80, 2]
   //
   // Reversed keyset: category DESC, score ASC, id DESC.
   // Each OR level flips the comparison operator vs. the original keyset.
   // Matching rows: (art,88,6), (art,60,7), (science,95,1).
   // DB returns in reversed order, then the builder reverses back to
   // the original sort: [6, 7, 1] — which is exactly page 1.
   let page = db
      .fetch_page("SELECT * FROM posts".into(), vec![], keyset, 3)
      .before(vec![json!("science"), json!(80), json!(2)])
      .await
      .unwrap();

   assert_eq!(row_ids(&page), vec![6, 7, 1]);
   assert!(!page.has_more);
   assert_eq!(page.next_cursor, None);

   db.remove().await.unwrap();
}

// ─── Boundary Conditions ───

#[tokio::test]
async fn empty_table() {
   let (db, _temp) = create_test_db().await;

   db.execute(
      "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)".into(),
      vec![],
   )
   .await
   .unwrap();

   let keyset = vec![KeysetColumn::asc("id")];

   // ── Empty table ──
   // Generated SQL:
   //    SELECT id, title FROM posts ORDER BY id ASC LIMIT 4
   //
   // No rows exist, so we get an empty result set.
   let page = db
      .fetch_page("SELECT id, title FROM posts".into(), vec![], keyset, 3)
      .await
      .unwrap();

   assert!(page.rows.is_empty());
   assert!(!page.has_more);
   assert_eq!(page.next_cursor, None);

   db.remove().await.unwrap();
}

// fetch_page asks for page_size + 1 rows; the extra row is the sentinel that sets has_more.
#[rstest]
// 3 rows = page_size: no sentinel row, so has_more is false.
#[case::exactly_page_size_rows(3, 3, &[1, 2, 3], false, None)]
// 4 rows = page_size + 1: the sentinel sets has_more and is truncated away.
#[case::page_size_plus_one_rows(4, 3, &[1, 2, 3], true, Some(vec![json!(3)]))]
// 1 row, well under page_size (10): no sentinel, so has_more is false.
#[case::single_row(1, 10, &[1], false, None)]
#[tokio::test]
async fn has_more_only_when_rows_exceed_page_size(
   #[case] row_count: i64,
   #[case] page_size: usize,
   #[case] expected_ids: &[i64],
   #[case] has_more: bool,
   #[case] next_cursor: Option<Vec<JsonValue>>,
) {
   let (db, _temp) = create_test_db().await;

   db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)".into(), vec![])
      .await
      .unwrap();

   for id in 1..=row_count {
      db.execute("INSERT INTO t (id) VALUES ($1)".into(), vec![json!(id)])
         .await
         .unwrap();
   }

   let keyset = vec![KeysetColumn::asc("id")];

   let page = db
      .fetch_page("SELECT id FROM t".into(), vec![], keyset, page_size)
      .await
      .unwrap();

   assert_eq!(row_ids(&page), expected_ids);
   assert_eq!(page.has_more, has_more);
   assert_eq!(page.next_cursor, next_cursor);

   db.remove().await.unwrap();
}

// ─── WHERE Clause + Cursor ───

#[tokio::test]
async fn where_clause_combined_with_cursor() {
   let (db, _temp) = create_test_db().await;
   seed_posts_table(&db).await;

   let keyset = vec![KeysetColumn::asc("id")];

   // ── Page 1 with WHERE filter ──
   // Generated SQL:
   //    SELECT id, title, category FROM posts
   //       WHERE category = $1
   //       ORDER BY id ASC LIMIT 3
   //    bind: ["tech"]
   //
   // Only tech posts (ids 3, 4, 5) pass the filter.
   let page1 = db
      .fetch_page(
         "SELECT id, title, category FROM posts WHERE category = $1".into(),
         vec![json!("tech")],
         keyset.clone(),
         2,
      )
      .await
      .unwrap();

   assert_eq!(row_ids(&page1), vec![3, 4]);
   assert!(page1.has_more);
   assert_eq!(page1.next_cursor, Some(vec![json!(4)]));

   // ── Page 2 with WHERE + cursor ──
   // Generated SQL:
   //    SELECT id, title, category FROM posts
   //       WHERE category = $1 AND ((id) > ($2))
   //       ORDER BY id ASC LIMIT 3
   //    bind: ["tech", 4]
   //
   // The cursor condition uses $2 (numbered after the user's $1) so
   // parameter bindings never collide.
   // Only id=5 (tech, 70) matches both conditions.
   let page2 = db
      .fetch_page(
         "SELECT id, title, category FROM posts WHERE category = $1".into(),
         vec![json!("tech")],
         keyset,
         2,
      )
      .after(page1.next_cursor.unwrap())
      .await
      .unwrap();

   assert_eq!(row_ids(&page2), vec![5]);
   assert!(!page2.has_more);
   assert_eq!(page2.next_cursor, None);

   db.remove().await.unwrap();
}

#[tokio::test]
async fn where_clause_multiple_params_combined_with_cursor() {
   let (db, _temp) = create_test_db().await;
   seed_posts_table(&db).await;

   let keyset = vec![KeysetColumn::asc("id")];

   // ── Multiple user params ($1, $2) + cursor ──
   // Generated SQL:
   //    SELECT id, title, category, score FROM posts
   //       WHERE category = $1 AND score >= $2 AND ((id) > ($3))
   //       ORDER BY id ASC LIMIT 3
   //    bind: ["tech", 70, 3]
   //
   // Cursor starts at $3 because there are 2 user params.
   // Matching rows: id=4 (tech, 85) and id=5 (tech, 70).
   let page = db
      .fetch_page(
         "SELECT id, title, category, score FROM posts WHERE category = $1 AND score >= $2".into(),
         vec![json!("tech"), json!(70)],
         keyset,
         2,
      )
      .after(vec![json!(3)])
      .await
      .unwrap();

   assert_eq!(row_ids(&page), vec![4, 5]);
   assert!(!page.has_more);
   assert_eq!(page.next_cursor, None);

   db.remove().await.unwrap();
}

// ─── Error Cases ───

#[rstest]
#[case::empty_keyset("SELECT 1", vec![], 10, "EMPTY_KEYSET_COLUMNS")]
#[case::zero_page_size("SELECT 1", vec![KeysetColumn::asc("id")], 0, "INVALID_PAGE_SIZE")]
#[case::query_contains_order_by(
   "SELECT id FROM posts ORDER BY id",
   vec![KeysetColumn::asc("id")],
   10,
   "INVALID_PAGINATION_QUERY"
)]
#[case::query_contains_limit(
   "SELECT id FROM posts LIMIT 10",
   vec![KeysetColumn::asc("id")],
   10,
   "INVALID_PAGINATION_QUERY"
)]
#[tokio::test]
async fn error_invalid_page_request(
   #[case] query: &str,
   #[case] keyset: Vec<KeysetColumn>,
   #[case] page_size: usize,
   #[case] expected_code: &str,
) {
   let (db, _temp) = create_test_db().await;

   let err = db
      .fetch_page(query.into(), vec![], keyset, page_size)
      .await
      .unwrap_err();

   assert_eq!(err.error_code(), expected_code, "got {err:?}");

   db.remove().await.unwrap();
}

#[tokio::test]
async fn error_cursor_length_mismatch() {
   let (db, _temp) = create_test_db().await;

   // 2 cursor values but only 1 keyset column
   let err = db
      .fetch_page("SELECT 1".into(), vec![], vec![KeysetColumn::asc("id")], 10)
      .after(vec![json!(1), json!(2)])
      .await
      .unwrap_err();

   assert!(matches!(
      err,
      Error::CursorLengthMismatch {
         cursor_len: 2,
         keyset_len: 1,
      }
   ));

   db.remove().await.unwrap();
}
