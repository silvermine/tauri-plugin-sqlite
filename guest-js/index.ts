import { invoke } from '@tauri-apps/api/core';

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
export type SqlValue = string | number | boolean | null | Uint8Array;

/**
 * Access mode for attached database
 */
export type AttachedDatabaseMode = 'readOnly' | 'readWrite';

/**
 * Specification for attaching a database to a query
 */
export interface AttachedDatabaseSpec {

   /**
    * Path to the database to attach (must be loaded via `Database.load()` first)
    */
   databasePath: string;

   /**
    * Schema name to use for the attached database in queries
    * (e.g., "orders" to query as "SELECT * FROM orders.table_name")
    */
   schemaName: string;

   /**
    * Access mode: "readOnly" or "readWrite"
    */
   mode: AttachedDatabaseMode;
}

/**
 * Result returned from write operations (INSERT, UPDATE, DELETE, etc.).
 */
export interface WriteQueryResult {

   /** The number of rows affected by the write operation. */
   rowsAffected: number;

   /**
    * The last inserted row ID (SQLite ROWID).
    * Only set for INSERT operations on tables with a ROWID.
    * Tables created with WITHOUT ROWID will not set this value (returns 0).
    */
   lastInsertId: number;
}

/**
 * Structured error returned from SQLite operations.
 *
 * All errors thrown by the plugin will have this structure.
 */
export interface SqliteError {

   /** Machine-readable error code (e.g., "SQLITE_CONSTRAINT", "DATABASE_NOT_LOADED") */
   code: string;

   /** Human-readable error message */
   message: string;
}

/**
 * **InterruptibleTransaction**
 *
 * Represents an active interruptible transaction that can be continued,
 * committed, or rolled back.
 * Provides methods to read uncommitted data and execute additional statements.
 */
export class InterruptibleTransaction {
   private readonly _dbPath: string;
   private readonly _transactionId: string;

   public constructor(dbPath: string, transactionId: string) {
      this._dbPath = dbPath;
      this._transactionId = transactionId;
   }

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
    * let tx = await db.executeInterruptibleTransaction([
    *    ['INSERT INTO users (name) VALUES ($1)', ['Alice']]
    * ]);
    *
    * const users = await tx.read<User[]>(
    *    'SELECT * FROM users WHERE name = $1',
    *    ['Alice']
    * );
    * ```
    */
   public async read<T>(query: string, bindValues?: SqlValue[]): Promise<T> {
      return await invoke<T>('plugin:sqlite|transaction_read', {
         token: { dbPath: this._dbPath, transactionId: this._transactionId },
         query,
         values: bindValues ?? [],
      });
   }

   /**
    * **continue**
    *
    * Execute additional statements within this transaction and return a new
    * transaction handle.
    *
    * @param statements - Array of [query, values?] tuples to execute
    * @returns Promise that resolves with a new transaction handle
    *
    * @example
    * ```ts
    * let tx = await db.executeInterruptibleTransaction([...]);
    * tx = await tx.continue([
    *    ['INSERT INTO users (name) VALUES ($1)', ['Bob']]
    * ]);
    * await tx.commit();
    * ```
    */
   public async continue(statements: Array<[string, SqlValue[]?]>): Promise<InterruptibleTransaction> {
      const token = await invoke<{ dbPath: string; transactionId: string }>(
         'plugin:sqlite|transaction_continue',
         {
            token: { dbPath: this._dbPath, transactionId: this._transactionId },
            action: {
               type: 'Continue',
               statements: statements.map(([ query, values ]) => {
                  return {
                     query,
                     values: values ?? [],
                  };
               }),
            },
         }
      );

      return new InterruptibleTransaction(token.dbPath, token.transactionId);
   }

   /**
    * **commit**
    *
    * Commit this transaction and release the write lock.
    *
    * @example
    * ```ts
    * let tx = await db.executeInterruptibleTransaction([...]);
    * [...]
    * await tx.commit();
    * ```
    */
   public async commit(): Promise<void> {
      await invoke<void>('plugin:sqlite|transaction_continue', {
         token: { dbPath: this._dbPath, transactionId: this._transactionId },
         action: { type: 'Commit' },
      });
   }

   /**
    * **rollback**
    *
    * Rollback this transaction and release the write lock.
    *
    * @example
    * ```ts
    * let tx = await db.executeInterruptibleTransaction([...]);
    * [...]
    * await tx.rollback();
    * ```
    */
   public async rollback(): Promise<void> {
      await invoke<void>('plugin:sqlite|transaction_continue', {
         token: { dbPath: this._dbPath, transactionId: this._transactionId },
         action: { type: 'Rollback' },
      });
   }
}

/**
 * Custom configuration for SQLite database connection
 */
export interface CustomConfig {

   /** Maximum number of concurrent read connections. Default: 6 */
   maxReadConnections?: number;

   /** Idle timeout in seconds for connections. Default: 30 */
   idleTimeoutSecs?: number;
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
 * // Get all migration events (including ones emitted before registering listener)
 * const events = await db.getMigrationEvents()
 * for (const event of events) {
 *    console.log(`${event.status}: ${event.dbPath}`)
 *    if (event.status === 'failed') {
 *       console.error(`Migration error: ${event.error}`)
 *    }
 * }
 *
 * // Or...
 *
 * // Listen for real-time events (may miss early events)
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
   dbPath: string;

   /** Status: "running", "completed", "failed" */
   status: 'running' | 'completed' | 'failed';

   /**
    * Total number of migrations in the migrator (on "completed"),
    * not just newly applied
    */
   migrationCount?: number;

   /** Error message (on "failed") */
   error?: string;
}

/**
 * Builder for SELECT queries returning multiple rows
 */
class FetchAllBuilder<T> implements PromiseLike<T> {
   private readonly _db: Database;
   private readonly _query: string;
   private readonly _bindValues: SqlValue[];
   private _attached: AttachedDatabaseSpec[];

   public constructor(
      db: Database,
      query: string,
      bindValues: SqlValue[],
      attached: AttachedDatabaseSpec[] = []
   ) {
      this._db = db;
      this._query = query;
      this._bindValues = bindValues;
      this._attached = attached;
   }

   /**
    * Attach databases for cross-database queries
    */
   public attach(specs: AttachedDatabaseSpec[]): this {
      this._attached = specs;
      return this;
   }

   /**
    * Make the builder directly awaitable
    */
   public then<TResult1 = T, TResult2 = never>(
      onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
      onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
   ): PromiseLike<TResult1 | TResult2> {
      return this._execute().then(onfulfilled, onrejected);
   }

   private async _execute(): Promise<T> {
      return await invoke<T>('plugin:sqlite|fetch_all', {
         db: this._db.path,
         query: this._query,
         values: this._bindValues,
         attached: this._attached.length > 0 ? this._attached : null,
      });
   }
}

/**
 * Builder for SELECT queries returning zero or one row
 */
class FetchOneBuilder<T> implements PromiseLike<T | undefined> {
   private readonly _db: Database;
   private readonly _query: string;
   private readonly _bindValues: SqlValue[];
   private _attached: AttachedDatabaseSpec[];

   public constructor(
      db: Database,
      query: string,
      bindValues: SqlValue[],
      attached: AttachedDatabaseSpec[] = []
   ) {
      this._db = db;
      this._query = query;
      this._bindValues = bindValues;
      this._attached = attached;
   }

   /**
    * Attach databases for cross-database queries
    */
   public attach(specs: AttachedDatabaseSpec[]): this {
      this._attached = specs;
      return this;
   }

   /**
    * Make the builder directly awaitable
    */
   public then<TResult1 = T | undefined, TResult2 = never>(
      onfulfilled?: ((value: T | undefined) => TResult1 | PromiseLike<TResult1>) | null,
      onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
   ): PromiseLike<TResult1 | TResult2> {
      return this._execute().then(onfulfilled, onrejected);
   }

   private async _execute(): Promise<T | undefined> {
      return await invoke<T | undefined>('plugin:sqlite|fetch_one', {
         db: this._db.path,
         query: this._query,
         values: this._bindValues,
         attached: this._attached.length > 0 ? this._attached : null,
      });
   }
}

/**
 * Builder for write queries (INSERT, UPDATE, DELETE)
 */
class ExecuteBuilder implements PromiseLike<WriteQueryResult> {
   private readonly _db: Database;
   private readonly _query: string;
   private readonly _bindValues: SqlValue[];
   private _attached: AttachedDatabaseSpec[];

   public constructor(
      db: Database,
      query: string,
      bindValues: SqlValue[],
      attached: AttachedDatabaseSpec[] = []
   ) {
      this._db = db;
      this._query = query;
      this._bindValues = bindValues;
      this._attached = attached;
   }

   /**
    * Attach databases for cross-database writes
    */
   public attach(specs: AttachedDatabaseSpec[]): this {
      this._attached = specs;
      return this;
   }

   /**
    * Make the builder directly awaitable
    */
   public then<TResult1 = WriteQueryResult, TResult2 = never>(
      onfulfilled?: ((value: WriteQueryResult) => TResult1 | PromiseLike<TResult1>) | null,
      onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
   ): PromiseLike<TResult1 | TResult2> {
      return this._execute().then(onfulfilled, onrejected);
   }

   private async _execute(): Promise<WriteQueryResult> {
      const [ rowsAffected, lastInsertId ] = await invoke<[number, number]>(
         'plugin:sqlite|execute',
         {
            db: this._db.path,
            query: this._query,
            values: this._bindValues,
            attached: this._attached.length > 0 ? this._attached : null,
         }
      );

      return {
         lastInsertId,
         rowsAffected,
      };
   }
}

/**
 * Builder for transaction operations
 */
class TransactionBuilder implements PromiseLike<WriteQueryResult[]> {
   private readonly _db: Database;
   private readonly _statements: Array<[string, SqlValue[]?]>;
   private _attached: AttachedDatabaseSpec[];

   public constructor(
      db: Database,
      statements: Array<[string, SqlValue[]?]>,
      attached: AttachedDatabaseSpec[] = []
   ) {
      this._db = db;
      this._statements = statements;
      this._attached = attached;
   }

   /**
    * Attach databases for cross-database transactions
    */
   public attach(specs: AttachedDatabaseSpec[]): this {
      this._attached = specs;
      return this;
   }

   /**
    * Make the builder directly awaitable
    */
   public then<TResult1 = WriteQueryResult[], TResult2 = never>(
      onfulfilled?: ((value: WriteQueryResult[]) => TResult1 | PromiseLike<TResult1>) | null,
      onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
   ): PromiseLike<TResult1 | TResult2> {
      return this._execute().then(onfulfilled, onrejected);
   }

   private async _execute(): Promise<WriteQueryResult[]> {
      return await invoke<WriteQueryResult[]>('plugin:sqlite|execute_transaction', {
         db: this._db.path,
         statements: this._statements.map(([ query, values ]) => {
            return {
               query,
               values: values ?? [],
            };
         }),
         attached: this._attached.length > 0 ? this._attached : null,
      });
   }
}

/**
 * **Database**
 *
 * The `Database` class serves as the primary interface for
 * communicating with SQLite databases through the plugin.
 */
export default class Database {
   public path: string;

   public constructor(path: string) {
      this.path = path;
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
   public static async load(
      path: string,
      customConfig?: CustomConfig
   ): Promise<Database> {
      const resolvedPath = await invoke<string>('plugin:sqlite|load', {
         db: path,
         customConfig,
      });

      return new Database(resolvedPath);
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
   public static get(path: string): Database {
      return new Database(path);
   }

   /**
    * **close_all**
    *
    * Closes connection pools for all databases.
    *
    * @example
    * ```ts
    * await Database.close_all()
    * ```
    */
   public static async close_all(): Promise<void> {
      await invoke<void>('plugin:sqlite|close_all');
   }

   /**
    * **execute**
    *
    * Creates a builder for write queries (INSERT, UPDATE, DELETE, etc.).
    * Returns a builder that can optionally attach databases before executing.
    *
    * For SELECT queries, use `fetchAll()` or `fetchOne()` instead.
    *
    * SQLite uses `$1`, `$2`, etc. for parameter binding.
    *
    * @param query - SQL query to execute
    * @param bindValues - Optional parameter values
    *
    * @example
    * ```ts
    * // Simple INSERT - directly awaitable
    * const result = await db.execute(
    *    "INSERT INTO todos (id, title, status) VALUES ($1, $2, $3)",
    *    [ todos.id, todos.title, todos.status ]
    * );
    * console.log(`Inserted ${result.rowsAffected} rows`);
    *
    * // Cross-database UPDATE with attached database
    * const result = await db.execute(
    *    "UPDATE todos SET status = $1 WHERE id IN " +
    *    "(SELECT todo_id FROM archive.completed)",
    *    [ "archived" ]
    * ).attach([{
    *    databasePath: "archive.db",
    *    schemaName: "archive",
    *    mode: "readOnly"
    * }]);
    * ```
    */
   public execute(query: string, bindValues?: SqlValue[]): ExecuteBuilder {
      return new ExecuteBuilder(this, query, bindValues ?? []);
   }

   /**
    * **executeTransaction**
    *
    * Creates a builder for executing multiple write statements atomically
    * within a transaction.
    * All statements either succeed together or fail together.
    *
    * **Use this method** when you have a batch of writes to execute and
    * don't need to read data mid-transaction. For transactions that
    * require reading uncommitted data to decide how to proceed, use
    * `executeInterruptibleTransaction()` instead.
    *
    * The function automatically:
    * - Begins a transaction (BEGIN IMMEDIATE)
    * - Executes all statements in order
    * - Commits on success (COMMIT)
    * - Rolls back on any error (ROLLBACK)
    *
    * @param statements - Array of [query, values?] tuples to execute
    * @returns Builder that can attach databases and execute the transaction
    * @throws SqliteError if any statement fails (after rollback)
    *
    * @example
    * ```ts
    * // Execute multiple inserts atomically - directly awaitable
    * const results = await db.executeTransaction([
    *    [
    *       'INSERT INTO users (name, email) VALUES ($1, $2)',
    *       [ 'Alice', 'alice@example.com' ],
    *    ],
    *    [
    *       'INSERT INTO audit_log (action, user) VALUES ($1, $2)',
    *       [ 'user_created', 'Alice' ],
    *    ]
    * ]);
    * console.log(`User ID: ${results[0].lastInsertId}`);
    *
    * // Cross-database transaction with attached database
    * const results = await db.executeTransaction([
    *    ['INSERT INTO main.orders (user_id, total) VALUES ($1, $2)', [userId, total]],
    *    ['UPDATE archive.stats SET order_count = order_count + 1', []]
    * ]).attach([{
    *    databasePath: "archive.db",
    *    schemaName: "archive",
    *    mode: "readWrite"
    * }]);
    * ```
    */
   public executeTransaction(statements: Array<[string, SqlValue[]?]>): TransactionBuilder {
      return new TransactionBuilder(this, statements);
   }

   /**
    * **fetchAll**
    *
    * Creates a builder for SELECT queries returning multiple rows.
    * Returns a builder that can optionally attach databases before executing.
    *
    * SQLite uses `$1`, `$2`, etc. for parameter binding.
    *
    * @param query - SQL SELECT query
    * @param bindValues - Optional parameter values
    *
    * @example
    * ```ts
    * // Simple query - directly awaitable
    * const todos = await db.fetchAll<Todo[]>(
    *    "SELECT * FROM todos WHERE id = $1",
    *    [id]
    * );
    *
    * // Cross-database query with attached database
    * const results = await db.fetchAll(
    *    "SELECT u.name, o.total FROM users u JOIN orders.orders o ON u.id = o.user_id",
    *    []
    * ).attach([{
    *    databasePath: "orders.db",
    *    schemaName: "orders",
    *    mode: "readOnly"
    * }]);
    * ```
    */
   public fetchAll<T>(query: string, bindValues?: SqlValue[]): FetchAllBuilder<T> {
      return new FetchAllBuilder<T>(this, query, bindValues ?? []);
   }

   /**
    * **fetchOne**
    *
    * Creates a builder for SELECT queries expecting zero or one result.
    * Returns `undefined` if no rows match the query.
    * Returns a builder that can optionally attach databases before executing.
    *
    * SQLite uses `$1`, `$2`, etc. for parameter binding.
    *
    * @param query - SQL SELECT query
    * @param bindValues - Optional parameter values
    *
    * @example
    * ```ts
    * // Simple query - directly awaitable
    * const todo = await db.fetchOne<Todo>(
    *    "SELECT * FROM todos WHERE id = $1",
    *    [id]
    * );
    *
    * // Cross-database query with attached database
    * const summary = await db.fetchOne(
    *    "SELECT COUNT(*) as total FROM users u JOIN orders.orders o ON u.id = o.user_id",
    *    []
    * ).attach([{
    *    databasePath: "orders.db",
    *    schemaName: "orders",
    *    mode: "readOnly"
    * }]);
    * ```
    */
   public fetchOne<T>(query: string, bindValues?: SqlValue[]): FetchOneBuilder<T> {
      return new FetchOneBuilder<T>(this, query, bindValues ?? []);
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
   public async close(): Promise<boolean> {
      const success = await invoke<boolean>('plugin:sqlite|close', {
         db: this.path,
      });

      return success;
   }

   /**
    * **remove**
    *
    * Closes the database connection pool and deletes all database files
    * (including the main database file, and any WAL/SHM files).
    *
    * **Warning:** This permanently deletes the database files from disk.
    * Use with caution!
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
   public async remove(): Promise<boolean> {
      const success = await invoke<boolean>('plugin:sqlite|remove', {
         db: this.path,
      });

      return success;
   }

   /**
    * **executeInterruptibleTransaction**
    *
    * Begins an interruptible transaction for cases where you need to
    * **read data mid-transaction to decide how to proceed**. For example,
    * inserting a record and then reading its generated ID or computed
    * values before continuing with related writes.
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
    * let tx = await db.executeInterruptibleTransaction([
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
    * tx = await tx.continue([
    *    [
    *       'INSERT INTO order_items (order_id, product_id) VALUES ($1, $2)',
    *       [ orderId, productId ],
    *    ]
    * ]);
    *
    * await tx.commit();
    * ```
    */
   public async executeInterruptibleTransaction(
      initialStatements: Array<[string, SqlValue[]?]>
   ): Promise<InterruptibleTransaction> {
      const token = await invoke<{ dbPath: string; transactionId: string }>(
         'plugin:sqlite|execute_interruptible_transaction',
         {
            db: this.path,
            initialStatements: initialStatements.map(([ query, values ]) => {
               return {
                  query,
                  values: values ?? [],
               };
            }),
         }
      );

      return new InterruptibleTransaction(token.dbPath, token.transactionId);
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
   public async getMigrationEvents(): Promise<MigrationEvent[]> {
      return await invoke<MigrationEvent[]>('plugin:sqlite|get_migration_events', {
         db: this.path,
      });
   }
}
