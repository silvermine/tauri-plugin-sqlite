fn main() {
   tauri_plugin::Builder::new(&[
      "load",
      "execute",
      "execute_transaction",
      "execute_interruptible_transaction",
      "transaction_continue",
      "transaction_read",
      "fetch_all",
      "fetch_one",
      "close",
      "close_all",
      "remove",
   ])
   .build();
}
