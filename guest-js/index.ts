import { Channel, invoke } from '@tauri-apps/api/core';

async function ensureDatabaseLoaded(db: Database): Promise<void> {
   await db.load();
}

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
    * Database key for identifying the database to attach
    */
   databaseKey: string;

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
   private readonly _dbKey: string;
   private readonly _transactionId: string;

   public constructor(dbKey: string, transactionId: string) {
      this._dbKey = dbKey;
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
    * let tx = await db.beginInterruptibleTransaction([
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
         token: { dbKey: this._dbKey, transactionId: this._transactionId },
         query,
         values: bindValues ?? [],
      });
   }

   /**
    * **continueWith**
    *
    * Execute additional statements within this transaction and return a new
    * transaction handle.
    *
    * @param statements - Array of [query, values?] tuples to execute
    * @returns Promise that resolves with a new transaction handle
    *
    * @example
    * ```ts
    * let tx = await db.beginInterruptibleTransaction([...]);
    * tx = await tx.continueWith([
    *    ['INSERT INTO users (name) VALUES ($1)', ['Bob']]
    * ]);
    * await tx.commit();
    * ```
    */
   public async continueWith(statements: Array<[string, SqlValue[]?]>): Promise<InterruptibleTransaction> {
      const token = await invoke<{ dbKey: string; transactionId: string }>(
         'plugin:sqlite|transaction_continue',
         {
            token: { dbKey: this._dbKey, transactionId: this._transactionId },
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

      return new InterruptibleTransaction(token.dbKey, token.transactionId);
   }

   /**
    * **commit**
    *
    * Commit this transaction and release the write lock.
    *
    * @example
    * ```ts
    * let tx = await db.beginInterruptibleTransaction([...]);
    * [...]
    * await tx.commit();
    * ```
    */
   public async commit(): Promise<void> {
      await invoke<void>('plugin:sqlite|transaction_continue', {
         token: { dbKey: this._dbKey, transactionId: this._transactionId },
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
    * let tx = await db.beginInterruptibleTransaction([...]);
    * [...]
    * await tx.rollback();
    * ```
    */
   public async rollback(): Promise<void> {
      await invoke<void>('plugin:sqlite|transaction_continue', {
         token: { dbKey: this._dbKey, transactionId: this._transactionId },
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
 *    console.log(`${event.status}: ${event.dbKey}`)
 *    if (event.status === 'failed') {
 *       console.error(`Migration error: ${event.error}`)
 *    }
 * }
 *
 * // Or...
 *
 * // Listen for real-time events (may miss early events)
 * await listen<MigrationEvent>('sqlite:migration', (event) => {
 *    const { dbKey, dbPath, status, migrationCount, error } = event.payload
 *
 *    switch (status) {
 *       case 'running':
 *          console.log(`Running migrations for ${dbKey}`)
 *          break
 *       case 'completed':
 *          console.log(`Completed ${migrationCount} migrations for ${dbKey}`)
 *          break
 *       case 'failed':
 *          console.error(`Migration failed for ${dbKey}: ${error}`)
 *          break
 *    }
 * })
 * ```
 */
export interface MigrationEvent {

   /** Database registration key (such as `MAIN`) */
   dbKey: string;

   /** Database path, the absolute path to the database file, such as
    * `/var/lib/myapp/main.db`.
    */
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

// ─── Pagination Types ───

/**
 * Sort direction for a keyset column.
 */
export type SortDirection = 'asc' | 'desc';

/**
 * A column in the keyset used for cursor-based pagination.
 */
export interface KeysetColumn {

   /** Column name as it appears in the query result set */
   name: string;

   /** Sort direction for this column */
   direction: SortDirection;
}

/**
 * A page of results from keyset pagination.
 *
 * @typeParam T - Row type, defaults to `Record<string, SqlValue>`
 */
export interface KeysetPage<T = Record<string, SqlValue>> {

   /** The rows in this page */
   rows: T[];

   /** Cursor values to continue pagination, or null if there are no more pages */
   nextCursor: SqlValue[] | null;

   /** Whether there are more rows in the current pagination direction */
   hasMore: boolean;
}

// ─── Observer Types ───

/**
 * Configuration for the database observer.
 */
export interface ObserverConfig {

   /**
    * Capacity of the broadcast channel for change notifications.
    *
    * Set this to at least your largest expected transaction size (number of
    * INSERT/UPDATE/DELETE statements in a single transaction). Default: 256.
    */
   channelCapacity?: number;

   /**
    * Whether to capture column values in change notifications.
    *
    * When `true` (default), `TableChange` includes `oldValues` and `newValues`
    * with actual column data. When `false`, these fields are undefined,
    * reducing memory usage per notification.
    */
   captureValues?: boolean;
}

/**
 * The type of change operation that occurred.
 */
export type ChangeOperation = 'insert' | 'update' | 'delete';

/**
 * A typed column value from SQLite.
 *
 * Tagged union representing a single column's value with its native SQLite type.
 */
export type ColumnValue =
   | { type: 'null' }
   | { type: 'integer'; value: number }
   | { type: 'real'; value: number }
   | { type: 'text'; value: string }
   | { type: 'blob'; value: string }; // base64-encoded

/**
 * Notification of a change to a database table.
 */
export interface TableChange {

   /** Name of the table that was changed */
   table: string;

   /** The type of change operation (insert, update, delete) */
   operation?: ChangeOperation;

   /** The SQLite internal rowid (undefined for WITHOUT ROWID tables) */
   rowid?: number;

   /** Primary key value(s) for the affected row */
   primaryKey: ColumnValue[];

   /** Column values before the change (for update and delete) */
   oldValues?: ColumnValue[];

   /** Column values after the change (for insert and update) */
   newValues?: ColumnValue[];
}

/**
 * Event yielded by a table change subscription.
 *
 * Most events are `change` variants containing actual table change data.
 * A `lagged` event indicates the consumer fell behind and missed some
 * notifications.
 */
export type TableChangeEvent =
   | { event: 'change'; data: TableChange }
   | { event: 'lagged'; data: { count: number } };

/**
 * Represents an active subscription to table change notifications.
 *
 * Use `unsubscribe()` to stop receiving notifications.
 */
export class Subscription {
   private readonly _subscriptionId: string;

   public constructor(subscriptionId: string) {
      this._subscriptionId = subscriptionId;
   }

   /**
    * Get the subscription ID.
    */
   public get id(): string {
      return this._subscriptionId;
   }

   /**
    * Stop receiving change notifications for this subscription.
    *
    * @returns `true` if the subscription was active and removed
    */
   public async unsubscribe(): Promise<boolean> {
      return await invoke<boolean>('plugin:sqlite|unsubscribe', {
         subscriptionId: this._subscriptionId,
      });
   }
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
      await ensureDatabaseLoaded(this._db);

      return await invoke<T>('plugin:sqlite|fetch_all', {
         dbKey: this._db.key,
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
      await ensureDatabaseLoaded(this._db);

      return await invoke<T | undefined>('plugin:sqlite|fetch_one', {
         dbKey: this._db.key,
         query: this._query,
         values: this._bindValues,
         attached: this._attached.length > 0 ? this._attached : null,
      });
   }
}

/**
 * Builder for paginated SELECT queries using keyset (cursor-based) pagination
 */
class FetchPageBuilder<T> implements PromiseLike<KeysetPage<T>> {
   private readonly _db: Database;
   private readonly _query: string;
   private readonly _bindValues: SqlValue[];
   private readonly _keyset: KeysetColumn[];
   private readonly _pageSize: number;
   private _after: SqlValue[] | null;
   private _before: SqlValue[] | null;
   private _attached: AttachedDatabaseSpec[];

   public constructor(
      db: Database,
      query: string,
      bindValues: SqlValue[],
      keyset: KeysetColumn[],
      pageSize: number
   ) {
      this._db = db;
      this._query = query;
      this._bindValues = bindValues;
      this._keyset = keyset;
      this._pageSize = pageSize;
      this._after = null;
      this._before = null;
      this._attached = [];
   }

   /**
    * Set the cursor for fetching the next page (forward pagination).
    *
    * Pass the `nextCursor` from a previous `KeysetPage` to fetch the page
    * that follows it in the original sort order.
    */
   public after(cursor: SqlValue[]): this {
      this._after = cursor;
      return this;
   }

   /**
    * Set the cursor for fetching the previous page (backward pagination).
    *
    * Pass a cursor to fetch the page that precedes it in the original sort
    * order. Rows are returned in the original sort order (not reversed).
    */
   public before(cursor: SqlValue[]): this {
      this._before = cursor;
      return this;
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
   public then<TResult1 = KeysetPage<T>, TResult2 = never>(
      onfulfilled?: ((value: KeysetPage<T>) => TResult1 | PromiseLike<TResult1>) | null,
      onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
   ): PromiseLike<TResult1 | TResult2> {
      return this._execute().then(onfulfilled, onrejected);
   }

   private async _execute(): Promise<KeysetPage<T>> {
      await ensureDatabaseLoaded(this._db);

      return await invoke<KeysetPage<T>>('plugin:sqlite|fetch_page', {
         dbKey: this._db.key,
         query: this._query,
         values: this._bindValues,
         keyset: this._keyset,
         pageSize: this._pageSize,
         after: this._after,
         before: this._before,
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
      await ensureDatabaseLoaded(this._db);

      const [ rowsAffected, lastInsertId ] = await invoke<[number, number]>(
         'plugin:sqlite|execute',
         {
            dbKey: this._db.key,
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
 * Builder for interruptible transaction operations
 */
class InterruptibleTransactionBuilder implements PromiseLike<InterruptibleTransaction> {
   private readonly _db: Database;
   private readonly _initialStatements: Array<[string, SqlValue[]?]>;
   private _attached: AttachedDatabaseSpec[];

   public constructor(
      db: Database,
      initialStatements: Array<[string, SqlValue[]?]>,
      attached: AttachedDatabaseSpec[] = []
   ) {
      this._db = db;
      this._initialStatements = initialStatements;
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
   public then<TResult1 = InterruptibleTransaction, TResult2 = never>(
      onfulfilled?: ((value: InterruptibleTransaction) => TResult1 | PromiseLike<TResult1>) | null,
      onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
   ): PromiseLike<TResult1 | TResult2> {
      return this._execute().then(onfulfilled, onrejected);
   }

   private async _execute(): Promise<InterruptibleTransaction> {
      await ensureDatabaseLoaded(this._db);

      const token = await invoke<{ dbKey: string; transactionId: string }>(
         'plugin:sqlite|begin_interruptible_transaction',
         {
            dbKey: this._db.key,
            initialStatements: this._initialStatements.map(([ query, values ]) => {
               return {
                  query,
                  values: values ?? [],
               };
            }),
            attached: this._attached.length > 0 ? this._attached : null,
         }
      );

      return new InterruptibleTransaction(token.dbKey, token.transactionId);
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
      await ensureDatabaseLoaded(this._db);

      return await invoke<WriteQueryResult[]>('plugin:sqlite|execute_transaction', {
         dbKey: this._db.key,
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
 *
 * @remarks
 * Database instances are shared across all webviews/windows within the same Tauri
 * application. A database loaded in one window is accessible from any other window
 * without calling `load()` again. This means writes from one window are immediately
 * visible to reads in another, and closing a database affects all windows.
 */
export default class Database {

   /** Database key for identifying the database */
   public readonly key: string;

   /** Database path, the absolute path to the database file, such as
    * `/var/lib/myapp/main.db`.
    *
    * This will only be set after the database has been loaded, and cannot be publicly
    * set.
    */
   private _path: string | null = null;

   private _loadPromise: Promise<void> | null = null;

   public constructor(key: string) {
      this.key = key;
   }

   /**
    * **load**
    *
    * An initializer which connects to the underlying SQLite database and
    * returns a `Database` instance once a connection is established.
    *
    * The key must be a key that the Rust side has registered with the plugin
    * (via `Builder::register_database` / `SetupRegistrar::register_database`),
    * or an in-memory database such as `:memory:`.
    *
    * @param key - The key of the database to load
    * @param customConfig - Optional custom configuration for connection pools
    *
    * @example
    * ```ts
    * const dbKey = 'MAIN';
    *
    * // Use default configuration
    * const db = await Database.load(dbKey);
    *
    * // Use custom configuration
    * const db2 = await Database.load(dbKey, {
    *   maxReadConnections: 10,
    *   idleTimeoutSecs: 60
    * });
    * ```
    */
   public static async load(
      dbKey: string,
      customConfig?: CustomConfig
   ): Promise<Database> {
      const database = new Database(dbKey);

      await database.load(customConfig);
      return database;
   }

   /**
    * **get**
    *
    * Synchronously returns a `Database` handle for a registered key while deferring
    * the actual connection until the first query or other operation that requires a
    * loaded connection. Use {@link Database.load} when you need to pass
    * `customConfig` or connect eagerly.
    *
    * The key must be registered with the plugin on the Rust side
    * (via `Builder::register_database` / `SetupRegistrar::register_database`).
    *
    * @example
    * ```ts
    * const db = Database.get('MAIN');
    * await db.fetchAll('SELECT 1');
    * ```
    */
   public static get(dbKey: string): Database {
      return new Database(dbKey);
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
    * **path**
    *
    * The absolute path to the database file, such as
    * `/var/lib/myapp/main.db`.
    *
    * This will only be set after the database has been loaded, and cannot be publicly
    * set.
    */
   public getPath(): string | null {
      return this._path;
   }

   /**
    * **load**
    *
    * An initializer which connects to the underlying SQLite database and
    * returns a `Database` instance once a connection is established.
    *
    * The key must be a key that the Rust side has registered with the plugin
    * (via `Builder::register_database` / `SetupRegistrar::register_database`),
    * or an in-memory database such as `:memory:`.
    *
    * @param key - The key of the database to load
    * @param customConfig - Optional custom configuration for connection pools
    *
    * @example
    * ```ts
    * const dbKey = 'MAIN';
    *
    * // Use default configuration
    * const db = await Database.load(dbKey);
    *
    * // Use custom configuration
    * const db2 = await Database.load(dbKey, {
    *   maxReadConnections: 10,
    *   idleTimeoutSecs: 60
    * });
    * ```
    */
   public async load(
      customConfig?: CustomConfig
   ): Promise<void> {
      if (this._path !== null) {
         return;
      }

      if (this._loadPromise) {
         return this._loadPromise;
      }

      this._loadPromise = (async () => {
         try {
            const resolvedPath = await invoke<string>('plugin:sqlite|load', {
               dbKey: this.key,
               customConfig,
            });

            this._path = resolvedPath;
         } catch(err) {
            this._loadPromise = null;
            throw err;
         }
      })();

      await this._loadPromise;
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
    *    databaseKey: "ARCHIVE",
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
    * `beginInterruptibleTransaction()` instead.
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
    *    databaseKey: "ARCHIVE",
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
    * @remarks
    * This method returns the entire result set in a single response. For large or
    * unbounded queries, prefer {@link fetchPage} with keyset pagination to keep memory
    * usage bounded on both the Rust and TypeScript sides.
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
    *    databaseKey: "ORDERS",
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
    *    databaseKey: "ORDERS",
    *    schemaName: "orders",
    *    mode: "readOnly"
    * }]);
    * ```
    */
   public fetchOne<T>(query: string, bindValues?: SqlValue[]): FetchOneBuilder<T> {
      return new FetchOneBuilder<T>(this, query, bindValues ?? []);
   }

   /**
    * **fetchPage**
    *
    * Creates a builder for paginated SELECT queries using keyset (cursor-based)
    * pagination. Keyset pagination avoids the performance degradation of
    * OFFSET-based pagination on large datasets by using indexed column values
    * to seek directly to the next or previous page.
    *
    * The base query must not contain ORDER BY or LIMIT clauses — the builder
    * appends these automatically based on the keyset definition.
    *
    * SQLite uses `$1`, `$2`, etc. for parameter binding.
    *
    * @param query - SQL SELECT query (without ORDER BY or LIMIT)
    * @param bindValues - Parameter values for the query
    * @param keyset - Columns defining the sort order and cursor
    * @param pageSize - Number of rows per page
    *
    * @example
    * ```ts
    * const keyset: KeysetColumn[] = [
    *    { name: 'category', direction: 'asc' },
    *    { name: 'score', direction: 'desc' },
    *    { name: 'id', direction: 'asc' },
    * ];
    *
    * // First page
    * const page = await db.fetchPage<Post>(
    *    'SELECT * FROM posts',
    *    [],
    *    keyset,
    *    25,
    * );
    *
    * // Next page (forward) — pass the cursor from the previous page
    * if (page.nextCursor) {
    *    const nextPage = await db.fetchPage<Post>(
    *       'SELECT * FROM posts',
    *       [],
    *       keyset,
    *       25,
    *    ).after(page.nextCursor);
    *
    *    // Previous page (backward) — rows are returned in original sort order
    *    const prevPage = await db.fetchPage<Post>(
    *       'SELECT * FROM posts',
    *       [],
    *       keyset,
    *       25,
    *    ).before(page.nextCursor);
    * }
    *
    * // With attached database
    * const page = await db.fetchPage(
    *    'SELECT * FROM posts',
    *    [],
    *    keyset,
    *    25,
    * ).attach([{
    *    databaseKey: 'ARCHIVE',
    *    schemaName: 'archive',
    *    mode: 'readOnly',
    * }]);
    * ```
    */
   public fetchPage<T>(
      query: string,
      bindValues: SqlValue[],
      keyset: KeysetColumn[],
      pageSize: number
   ): FetchPageBuilder<T> {
      return new FetchPageBuilder<T>(this, query, bindValues, keyset, pageSize);
   }

   // ─── Observer Methods ───

   /**
    * **observe**
    *
    * Enable change observation for the specified tables.
    *
    * Must be called before `subscribe()`. This configures the database to track
    * changes via SQLite hooks. Changes are only published after transactions commit.
    *
    * If observation is already enabled, calling this again will abort all existing
    * subscriptions for this database, tear down the previous observer, and create
    * a new one with the provided configuration. You must re-subscribe after
    * re-calling `observe()`.
    *
    * @param tables - Table names to observe for changes
    * @param config - Optional observer configuration
    *
    * @example
    * ```ts
    * await db.observe(['users', 'posts']);
    *
    * // With custom config
    * await db.observe(['users'], {
    *    channelCapacity: 512,
    *    captureValues: false,
    * });
    * ```
    */
   public async observe(tables: string[], config?: ObserverConfig): Promise<void> {
      await ensureDatabaseLoaded(this);

      await invoke<void>('plugin:sqlite|observe', {
         dbKey: this.key,
         tables,
         config: config ?? null,
      });
   }

   /**
    * **subscribe**
    *
    * Subscribe to change notifications for specific tables.
    *
    * Returns a `Subscription` that can be used to unsubscribe later. Change events
    * are streamed to the provided callback function.
    *
    * Requires `observe()` to have been called first.
    *
    * @param tables - Table names to receive notifications for
    * @param onEvent - Callback invoked for each change event
    * @returns A Subscription that can be used to stop receiving notifications
    *
    * @example
    * ```ts
    * await db.observe(['users']);
    *
    * const subscription = await db.subscribe(['users'], (event) => {
    *    if (event.event === 'change') {
    *       console.log(`${event.data.operation} on ${event.data.table}`);
    *       console.log('Primary key:', event.data.primaryKey);
    *    } else {
    *       console.warn(`Missed ${event.data.count} notifications`);
    *    }
    * });
    *
    * // Later, stop receiving notifications
    * await subscription.unsubscribe();
    * ```
    */
   public async subscribe(
      tables: string[],
      onEvent: (event: TableChangeEvent) => void
   ): Promise<Subscription> {
      await ensureDatabaseLoaded(this);

      const channel = new Channel<TableChangeEvent>();

      channel.onmessage = onEvent;

      const subscriptionId = await invoke<string>('plugin:sqlite|subscribe', {
         dbKey: this.key,
         tables,
         onEvent: channel,
      });

      return new Subscription(subscriptionId);
   }

   /**
    * **unobserve**
    *
    * Disable change observation for this database.
    *
    * Stops tracking changes and aborts all active subscriptions for this database.
    *
    * @example
    * ```ts
    * await db.unobserve();
    * ```
    */
   public async unobserve(): Promise<void> {
      await ensureDatabaseLoaded(this);

      await invoke<void>('plugin:sqlite|unobserve', {
         dbKey: this.key,
      });
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
         dbKey: this.key,
      });

      if (success) {
         this._resetLoadState();
      }

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
         dbKey: this.key,
      });

      if (success) {
         this._resetLoadState();
      }

      return success;
   }

   /**
    * **beginInterruptibleTransaction**
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
    * @returns Builder for setting up the transaction with optional attached databases
    *
    * @example
    * ```ts
    * // Insert an order and read back its ID
    * let tx = await db.beginInterruptibleTransaction([
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
    * tx = await tx.continueWith([
    *    [
    *       'INSERT INTO order_items (order_id, product_id) VALUES ($1, $2)',
    *       [ orderId, productId ],
    *    ]
    * ]);
    *
    * await tx.commit();
    * ```
    *
    * @example
    * ```ts
    * // Transaction with attached database
    * let tx = await db.beginInterruptibleTransaction([
    *    ['DELETE FROM users WHERE archived = 1']
    * ]).attach([{
    *    databaseKey: 'ARCHIVE',
    *    schemaName: 'archive',
    *    mode: 'readWrite'
    * }]);
    *
    * tx = await tx.continueWith([
    *    ['INSERT INTO archive.users SELECT * FROM users WHERE archived = 1']
    * ]);
    *
    * await tx.commit();
    * ```
    */
   public beginInterruptibleTransaction(
      initialStatements: Array<[string, SqlValue[]?]>
   ): InterruptibleTransactionBuilder {
      return new InterruptibleTransactionBuilder(this, initialStatements);
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
    *    console.log(`${event.status}: ${event.dbKey}`)
    *    if (event.status === 'failed') {
    *       console.error(`Migration error: ${event.error}`)
    *    }
    * }
    * ```
    */
   public async getMigrationEvents(): Promise<MigrationEvent[]> {
      return await invoke<MigrationEvent[]>('plugin:sqlite|get_migration_events', {
         dbKey: this.key,
      });
   }

   private _resetLoadState(): void {
      this._path = null;
      this._loadPromise = null;
   }
}
