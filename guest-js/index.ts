import { invoke } from '@tauri-apps/api/core'

/**
 * Valid SQLite parameter binding value types.
 *
 * SQLite supports a limited set of types for parameter binding:
 * - `string` - TEXT, DATE, TIME, DATETIME
 * - `number` - INTEGER, REAL
 * - `boolean` - BOOLEAN
 * - `null` - NULL
 * - `Uint8Array` - BLOB (binary data)
 */
export type SqlValue = string | number | boolean | null | Uint8Array

/**
 * Result returned from write operations (INSERT, UPDATE, DELETE, etc.).
 */
export interface WriteQueryResult {
   /** The number of rows affected by the write operation. */
   rowsAffected: number
   /**
    * The last inserted row ID (SQLite ROWID).
    * Only set for INSERT operations on tables with a ROWID.
    * Tables created with WITHOUT ROWID will not set this value (returns 0).
    */
   lastInsertId: number
}

/**
 * Structured error returned from SQLite operations.
 *
 * All errors thrown by the plugin will have this structure.
 */
export interface SqliteError {
   /** Machine-readable error code (e.g., "SQLITE_CONSTRAINT", "DATABASE_NOT_LOADED") */
   code: string
   /** Human-readable error message */
   message: string
}

/**
 * **InterruptibleTransaction**
 *
 * Represents an active interruptible transaction that can be continued, committed, or rolled back.
 * Provides methods to read uncommitted data and execute additional statements.
 */
export class InterruptibleTransaction {
   constructor(
      private readonly dbPath: string,
      private readonly transactionId: string
   ) {}

   /**
    * **read**
    *
    * Read data from the database within this transaction context.
    * This allows you to see uncommitted writes from the current transaction.
    *
    * The query executes on the same connection as the transaction, so you can
    * read data that hasn't been committed yet.
    *
    * @param query - SELECT query to execute
    * @param bindValues - Optional parameter values
    * @returns Promise that resolves with query results
    *
    * @example
    * ```ts
    * const tx = await db.executeInterruptibleTransaction([
    *    ['INSERT INTO users (name) VALUES ($1)', ['Alice']]
    * ]);
    *
    * const users = await tx.read<User[]>(
    *    'SELECT * FROM users WHERE name = $1',
    *    ['Alice']
    * );
    * ```
    */
   async read<T>(query: string, bindValues?: SqlValue[]): Promise<T> {
      return await invoke<T>('plugin:sqlite|transaction_read', {
         token: { dbPath: this.dbPath, transactionId: this.transactionId },
         query,
         values: bindValues ?? []
      })
   }

   /**
    * **continue**
    *
    * Execute additional statements within this transaction and return a new transaction handle.
    *
    * @param statements - Array of [query, values?] tuples to execute
    * @returns Promise that resolves with a new transaction handle
    *
    * @example
    * ```ts
    * const tx = await db.executeInterruptibleTransaction([...]);
    * const tx2 = await tx.continue([
    *    ['INSERT INTO users (name) VALUES ($1)', ['Bob']]
    * ]);
    * await tx2.commit();
    * ```
    */
   async continue(statements: Array<[string, SqlValue[]?]>): Promise<InterruptibleTransaction> {
      const token = await invoke<{ dbPath: string; transactionId: string }>(
         'plugin:sqlite|transaction_continue',
         {
            token: { dbPath: this.dbPath, transactionId: this.transactionId },
            action: {
               type: 'Continue',
               statements: statements.map(([query, values]) => ({
                  query,
                  values: values ?? []
               }))
            }
         }
      )
      return new InterruptibleTransaction(token.dbPath, token.transactionId)
   }

   /**
    * **commit**
    *
    * Commit this transaction and release the write lock.
    *
    * @example
    * ```ts
    * const tx = await db.executeInterruptibleTransaction([...]);
    * await tx.commit();
    * ```
    */
   async commit(): Promise<void> {
      await invoke<void>('plugin:sqlite|transaction_continue', {
         token: { dbPath: this.dbPath, transactionId: this.transactionId },
         action: { type: 'Commit' }
      })
   }

   /**
    * **rollback**
    *
    * Rollback this transaction and release the write lock.
    *
    * @example
    * ```ts
    * const tx = await db.executeInterruptibleTransaction([...]);
    * await tx.rollback();
    * ```
    */
   async rollback(): Promise<void> {
      await invoke<void>('plugin:sqlite|transaction_continue', {
         token: { dbPath: this.dbPath, transactionId: this.transactionId },
         action: { type: 'Rollback' }
      })
   }
}

/**
 * Custom configuration for SQLite database connection
 */
export interface CustomConfig {
   /** Maximum number of concurrent read connections. Default: 6 */
   maxReadConnections?: number
   /** Idle timeout in seconds for connections. Default: 30 */
   idleTimeoutSecs?: number
}

/**
 * Event payload emitted during database migration operations.
 *
 * Listen for these events to track migration progress:
 *
 * @example
 * ```ts
 * import { listen } from '@tauri-apps/api/event'
 * import type { MigrationEvent } from '@silvermine/tauri-plugin-sqlite'
 *
 * await listen<MigrationEvent>('sqlite:migration', (event) => {
 *    const { dbPath, status, migrationCount, error } = event.payload
 *
 *    switch (status) {
 *       case 'running':
 *          console.log(`Running migrations for ${dbPath}`)
 *          break
 *       case 'completed':
 *          console.log(`Completed ${migrationCount} migrations for ${dbPath}`)
 *          break
 *       case 'failed':
 *          console.error(`Migration failed for ${dbPath}: ${error}`)
 *          break
 *    }
 * })
 * ```
 */
export interface MigrationEvent {
   /** Database path (relative, as registered with the plugin) */
   dbPath: string
   /** Status: "running", "completed", "failed" */
   status: 'running' | 'completed' | 'failed'
   /** Total number of migrations in the migrator (on "completed"), not just newly applied */
   migrationCount?: number
   /** Error message (on "failed") */
   error?: string
}

/**
 * **Database**
 *
 * The `Database` class serves as the primary interface for
 * communicating with SQLite databases through the plugin.
 */
export default class Database {
   path: string
   constructor(path: string) {
      this.path = path
   }

   /**
    * **load**
    *
    * A static initializer which connects to the underlying SQLite database and
    * returns a `Database` instance once a connection is established.
    *
    * The path is relative to `tauri::path::BaseDirectory::AppConfig`.
    *
    * @param path - Database file path (relative to AppConfig directory)
    * @param customConfig - Optional custom configuration for connection pools
    *
    * @example
    * ```ts
    * // Use default configuration
    * const db = await Database.load("test.db");
    *
    * // Use custom configuration
    * const db = await Database.load("test.db", {
    *   maxReadConnections: 10,
    *   idleTimeoutSecs: 60
    * });
    * ```
    */
   static async load(path: string, customConfig?: CustomConfig): Promise<Database> {
      const _path = await invoke<string>('plugin:sqlite|load', {
         db: path,
         customConfig
      })

      return new Database(_path)
   }

   /**
    * **get**
    *
    * A static initializer which synchronously returns an instance of
    * the Database class while deferring the actual database connection
    * until the first invocation or selection on the database.
    *
    * The path is relative to `tauri::path::BaseDirectory::AppConfig`.
    *
    * @example
    * ```ts
    * const db = Database.get("test.db");
    * ```
    */
   static get(path: string): Database {
      return new Database(path)
   }

   /**
    * **execute**
    *
    * Executes a write query against the database (INSERT, UPDATE, DELETE, etc.).
    * This method is for mutations that modify data.
    *
    * For SELECT queries, use `fetchAll()` or `fetchOne()` instead.
    *
    * SQLite uses `$1`, `$2`, etc. for parameter binding.
    *
    * @example
    * ```ts
    * // INSERT example
    * const result = await db.execute(
    *    "INSERT INTO todos (id, title, status) VALUES ($1, $2, $3)",
    *    [todos.id, todos.title, todos.status]
    * );
    * console.log(`Inserted ${result.rowsAffected} rows`);
    * console.log(`Last insert ID: ${result.lastInsertId}`);
    *
    * // UPDATE example
    * const result = await db.execute(
    *    "UPDATE todos SET title = $1, status = $2 WHERE id = $3",
    *    [todos.title, todos.status, todos.id]
    * );
    * ```
    */
   async execute(query: string, bindValues?: SqlValue[]): Promise<WriteQueryResult> {
      const [rowsAffected, lastInsertId] = await invoke<[number, number]>(
         'plugin:sqlite|execute',
         {
            db: this.path,
            query,
            values: bindValues ?? []
         }
      )
      return {
         lastInsertId,
         rowsAffected
      }
   }

   /**
    * **executeTransaction**
    *
    * Executes multiple write statements atomically within a transaction.
    * All statements either succeed together or fail together.
    *
    * **Use this method** when you have a batch of writes to execute and don't need to
    * read data mid-transaction. For transactions that require reading uncommitted data
    * to decide how to proceed, use `executeInterruptibleTransaction()` instead.
    *
    * The function automatically:
    * - Begins a transaction (BEGIN)
    * - Executes all statements in order
    * - Commits on success (COMMIT)
    * - Rolls back on any error (ROLLBACK)
    *
    * @param statements - Array of [query, values?] tuples to execute
    * @returns Promise that resolves with results for each statement when all complete successfully
    * @throws SqliteError if any statement fails (after rollback)
    *
    * @example
    * ```ts
    * // Execute multiple inserts atomically
    * const results = await db.executeTransaction([
    *    ['INSERT INTO users (name, email) VALUES ($1, $2)', ['Alice', 'alice@example.com']],
    *    ['INSERT INTO audit_log (action, user) VALUES ($1, $2)', ['user_created', 'Alice']]
    * ]);
    * console.log(`User ID: ${results[0].lastInsertId}`);
    * console.log(`Log rows affected: ${results[1].rowsAffected}`);
    *
    * // Mixed operations
    * const results = await db.executeTransaction([
    *    ['UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, 1]],
    *    ['UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, 2]],
    *    ['INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)', [1, 2, 100]]
    * ]);
    * ```
    */
   async executeTransaction(statements: Array<[string, SqlValue[]?]>): Promise<WriteQueryResult[]> {
      return await invoke<WriteQueryResult[]>('plugin:sqlite|execute_transaction', {
         db: this.path,
         statements: statements.map(([query, values]) => ({
            query,
            values: values ?? []
         }))
      })
   }

   /**
    * **fetchAll**
    *
    * Passes in a SELECT query to the database for execution.
    * Returns all matching rows as an array.
    *
    * SQLite uses `$1`, `$2`, etc. for parameter binding.
    *
    * @example
    * ```ts
    * const todos = await db.fetchAll<Todo[]>(
    *    "SELECT * FROM todos WHERE id = $1",
    *    [id]
    * );
    *
    * // Multiple parameters
    * const result = await db.fetchAll(
    *    "SELECT * FROM todos WHERE status = $1 AND priority > $2",
    *    ["active", 5]
    * );
    * ```
    */
   async fetchAll<T>(query: string, bindValues?: SqlValue[]): Promise<T> {
      const result = await invoke<T>('plugin:sqlite|fetch_all', {
         db: this.path,
         query,
         values: bindValues ?? []
      })

      return result
   }

   /**
    * **fetchOne**
    *
    * Passes in a SELECT query expecting zero or one result.
    * Returns `undefined` if no rows match the query.
    *
    * SQLite uses `$1`, `$2`, etc. for parameter binding.
    *
    * @example
    * ```ts
    * const todo = await db.fetchOne<Todo>(
    *    "SELECT * FROM todos WHERE id = $1",
    *    [id]
    * );
    *
    * if (todo) {
    *    console.log(todo.title);
    * } else {
    *    console.log("Todo not found");
    * }
    * ```
    */
   async fetchOne<T>(query: string, bindValues?: SqlValue[]): Promise<T | undefined> {
      const result = await invoke<T | undefined>('plugin:sqlite|fetch_one', {
         db: this.path,
         query,
         values: bindValues ?? []
      })

      return result
   }

   /**
    * **close**
    *
    * Closes the database connection pool(s) for this specific database.
    *
    * @returns `true` if the database was loaded and successfully closed,
    *          `false` if the database was not loaded (nothing to close)
    *
    * @example
    * ```ts
    * const wasClosed = await db.close()
    * if (wasClosed) {
    *    console.log('Database closed successfully')
    * } else {
    *    console.log('Database was not loaded')
    * }
    * ```
    */
   async close(): Promise<boolean> {
      const success = await invoke<boolean>('plugin:sqlite|close', {
         db: this.path
      })
      return success
   }

   /**
    * **closeAll**
    *
    * Closes connection pools for all databases.
    *
    * @example
    * ```ts
    * await Database.closeAll()
    * ```
    */
   static async closeAll(): Promise<void> {
      await invoke<void>('plugin:sqlite|close_all')
   }

   /**
    * **remove**
    *
    * Closes the database connection pool and deletes all database files
    * (including the main database file, and any WAL/SHM files).
    *
    * **Warning:** This permanently deletes the database files from disk. Use with caution!
    *
    * @returns `true` if the database was loaded and successfully removed,
    *          `false` if the database was not loaded (nothing to remove)
    *
    * @example
    * ```ts
    * const wasRemoved = await db.remove()
    * if (wasRemoved) {
    *    console.log('Database deleted successfully')
    * } else {
    *    console.log('Database was not loaded')
    * }
    * ```
    */
   async remove(): Promise<boolean> {
      const success = await invoke<boolean>('plugin:sqlite|remove', {
         db: this.path
      })
      return success
   }

   /**
    * **executeInterruptibleTransaction**
    *
    * Begins an interruptible transaction for cases where you need to **read data mid-transaction
    * to decide how to proceed**. For example, inserting a record and then reading its
    * generated ID or computed values before continuing with related writes.
    *
    * The transaction remains open, holding a write lock on the database, until you
    * call `commit()` or `rollback()` on the returned transaction handle.
    *
    * **Use this method when:**
    * - You need to read back generated IDs (e.g., AUTOINCREMENT columns)
    * - You need to see computed values (e.g., triggers, default values)
    * - Your next writes depend on data from earlier writes in the same transaction
    *
    * **Use `executeTransaction()` instead when:**
    * - You just need to execute a batch of writes atomically
    * - You know all the data upfront and don't need to read mid-transaction
    *
    * **Important:** Only one transaction can be active per database at a time. The
    * writer connection is held for the entire duration - keep transactions short.
    *
    * @param initialStatements - Array of [query, values?] tuples to execute initially
    * @returns Promise that resolves with an InterruptibleTransaction handle
    *
    * @example
    * ```ts
    * // Insert an order and read back its ID
    * const tx = await db.executeInterruptibleTransaction([
    *    ['INSERT INTO orders (user_id, total) VALUES ($1, $2)', [userId, 0]]
    * ]);
    *
    * // Read the generated order ID
    * const orders = await tx.read<Array<{ id: number }>>(
    *    'SELECT id FROM orders WHERE user_id = $1 ORDER BY id DESC LIMIT 1',
    *    [userId]
    * );
    * const orderId = orders[0].id;
    *
    * // Use the ID in subsequent writes
    * const tx2 = await tx.continue([
    *    ['INSERT INTO order_items (order_id, product_id) VALUES ($1, $2)', [orderId, productId]]
    * ]);
    *
    * await tx2.commit();
    * ```
    */
   async executeInterruptibleTransaction(
      initialStatements: Array<[string, SqlValue[]?]>
   ): Promise<InterruptibleTransaction> {
      const token = await invoke<{ dbPath: string; transactionId: string }>(
         'plugin:sqlite|execute_interruptible_transaction',
         {
            db: this.path,
            initialStatements: initialStatements.map(([query, values]) => ({
               query,
               values: values ?? []
            }))
         }
      )
      return new InterruptibleTransaction(token.dbPath, token.transactionId)
   }

   /**
    * **getMigrationEvents**
    *
    * Retrieves all cached migration events for this database.
    *
    * This method solves the race condition where migrations complete before the
    * frontend can register an event listener. Events are cached on the backend
    * and can be retrieved at any time.
    *
    * @returns Array of all migration events that have occurred for this database
    *
    * @example
    * ```ts
    * const db = await Database.load('mydb.db')
    *
    * // Get all migration events (including ones that happened before we could listen)
    * const events = await db.getMigrationEvents()
    * for (const event of events) {
    *    console.log(`${event.status}: ${event.dbPath}`)
    *    if (event.status === 'failed') {
    *       console.error(`Migration error: ${event.error}`)
    *    }
    * }
    * ```
    */
   async getMigrationEvents(): Promise<MigrationEvent[]> {
      return await invoke<MigrationEvent[]>('plugin:sqlite|get_migration_events', {
         db: this.path
      })
   }
}
