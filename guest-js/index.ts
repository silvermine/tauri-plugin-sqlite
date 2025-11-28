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
 * Custom configuration for SQLite database connection
 */
export interface CustomConfig {
   /** Maximum number of concurrent read connections. Default: 6 */
   maxReadConnections?: number
   /** Idle timeout in seconds for connections. Default: 30 */
   idleTimeoutSecs?: number
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
    * @example
    * ```ts
    * const success = await db.close()
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
    * const success = await Database.closeAll()
    * ```
    */
   static async closeAll(): Promise<boolean> {
      const success = await invoke<boolean>('plugin:sqlite|close_all')
      return success
   }

   /**
    * **remove**
    *
    * Closes the database connection pool and deletes all database files
    * (including the main database file, and any WAL/SHM files).
    *
    * **Warning:** This permanently deletes the database files from disk. Use with caution!
    *
    * @example
    * ```ts
    * const success = await db.remove()
    * ```
    */
   async remove(): Promise<boolean> {
      const success = await invoke<boolean>('plugin:sqlite|remove', {
         db: this.path
      })
      return success
   }
}
