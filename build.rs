fn main() {
   tauri_plugin::Builder::new(&[
      "load",
      "execute",
      "execute_transaction",
      "begin_interruptible_transaction",
      "transaction_continue",
      "transaction_read",
      "fetch_all",
      "fetch_one",
      "close",
      "close_all",
      "remove",
      "get_migration_events",
   ])
   .build();
}
