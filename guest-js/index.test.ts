/**
 * Sanity checks to test the bridge between TypeScript and the Tauri commands.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockIPC, clearMocks } from '@tauri-apps/api/mocks';
import Database, {
   MigrationEvent,
   Subscription,
   ObserverConfig,
   ColumnValue,
   TableChange,
   TableChangeEvent,
   KeysetColumn,
} from './index';

let lastCmd = '',
    lastArgs: Record<string, unknown> = {};

const DATABASE_KEYS = {
   T: 'T',
   TEST: 'TEST',
   ARCHIVE: 'ARCHIVE',
   STATS: 'STATS',
   ORDERS: 'ORDERS',
   MAIN: 'MAIN',
};

function getDatabasePath(key: string): string {
   if (key === DATABASE_KEYS.ARCHIVE) {
      return 'archive.db';
   }

   if (key === DATABASE_KEYS.STATS) {
      return 'stats.db';
   }

   if (key === DATABASE_KEYS.ORDERS) {
      return 'orders.db';
   }

   if (key === DATABASE_KEYS.T) {
      return 't.db';
   }

   if (key === DATABASE_KEYS.TEST) {
      return 'test.db';
   }

   if (key === DATABASE_KEYS.MAIN) {
      return 'main.db';
   }

   throw new Error(`Unknown database key: ${key}`);
}

beforeEach(() => {
   mockIPC((cmd, args) => {
      lastCmd = cmd;
      lastArgs = args as Record<string, unknown>;
      if (cmd === 'plugin:sqlite|load') {
         return getDatabasePath((args as { dbKey: string }).dbKey);
      }
      if (cmd === 'plugin:sqlite|execute') {
         return [ 1, 1 ];
      }
      if (cmd === 'plugin:sqlite|execute_transaction') {
         return [];
      }
      if (cmd === 'plugin:sqlite|begin_interruptible_transaction') {
         return { dbKey: (args as { dbKey: string }).dbKey, transactionId: 'test-tx-id' };
      }
      if (cmd === 'plugin:sqlite|transaction_continue') {
         const action = (args as { action: { type: string } }).action;

         if (action.type === 'Continue') {
            return { dbKey: DATABASE_KEYS.TEST, transactionId: 'test-tx-id' };
         }
         return undefined;
      }
      if (cmd === 'plugin:sqlite|transaction_read') {
         return [];
      }
      if (cmd === 'plugin:sqlite|fetch_all') {
         return [];
      }
      if (cmd === 'plugin:sqlite|fetch_page') {
         return { rows: [], nextCursor: null, hasMore: false };
      }
      if (cmd === 'plugin:sqlite|fetch_one') {
         return null;
      }
      if (cmd === 'plugin:sqlite|close') {
         return true;
      }
      if (cmd === 'plugin:sqlite|close_all') {
         return undefined;
      }
      if (cmd === 'plugin:sqlite|remove') {
         return true;
      }
      if (cmd === 'plugin:sqlite|get_migration_events') {
         return [];
      }
      if (cmd === 'plugin:sqlite|observe') {
         return undefined;
      }
      if (cmd === 'plugin:sqlite|subscribe') {
         return 'sub-123';
      }
      if (cmd === 'plugin:sqlite|unsubscribe') {
         return true;
      }
      if (cmd === 'plugin:sqlite|unobserve') {
         return undefined;
      }
      return undefined;
   });
});

afterEach(() => { return clearMocks(); });

describe('Database commands', () => {
   it('load', async () => {
      await Database.load(DATABASE_KEYS.TEST);
      expect(lastCmd).toBe('plugin:sqlite|load');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.TEST);
   });

   it('get is lazy until first operation', async () => {
      const db = Database.get(DATABASE_KEYS.TEST);

      expect(db.getPath()).toBeNull();

      await db.fetchAll('SELECT 1');
      expect(db.getPath()).toBe(getDatabasePath(DATABASE_KEYS.TEST));
   });

   it('get retries load after a transient failure', async () => {
      let loadAttempts = 0;

      mockIPC((cmd, args) => {
         lastCmd = cmd;
         lastArgs = args as Record<string, unknown>;
         if (cmd === 'plugin:sqlite|load') {
            loadAttempts += 1;
            if (loadAttempts === 1) {
               throw new Error('PATH_NOT_REGISTERED');
            }
            return getDatabasePath((args as { dbKey: string }).dbKey);
         }
         if (cmd === 'plugin:sqlite|fetch_all') {
            return [];
         }
         return undefined;
      });

      const db = Database.get(DATABASE_KEYS.TEST);

      await expect(db.fetchAll('SELECT 1')).rejects.toThrow('PATH_NOT_REGISTERED');
      expect(loadAttempts).toBe(1);
      expect(db.getPath()).toBeNull();

      await db.fetchAll('SELECT 1');
      expect(loadAttempts).toBe(2);
      expect(db.getPath()).toBe(getDatabasePath(DATABASE_KEYS.TEST));
      expect(lastCmd).toBe('plugin:sqlite|fetch_all');
   });

   it('load is idempotent', async () => {
      let loadAttempts = 0;

      mockIPC((cmd, args) => {
         lastCmd = cmd;
         lastArgs = args as Record<string, unknown>;
         if (cmd === 'plugin:sqlite|load') {
            loadAttempts += 1;
            return getDatabasePath((args as { dbKey: string }).dbKey);
         }
         return undefined;
      });

      const db = Database.get(DATABASE_KEYS.TEST);

      await db.load();
      await db.load();

      expect(loadAttempts).toBe(1);
      expect(db.getPath()).toBe(getDatabasePath(DATABASE_KEYS.TEST));
   });

   it('load coalesces concurrent calls', async () => {
      let loadAttempts = 0,
          releaseLoad!: () => void;

      const loadBlocked = new Promise<void>((resolve) => {
         releaseLoad = resolve;
      });

      mockIPC((cmd, args) => {
         lastCmd = cmd;
         lastArgs = args as Record<string, unknown>;
         if (cmd === 'plugin:sqlite|load') {
            loadAttempts += 1;
            return loadBlocked.then(() => {
               return getDatabasePath((args as { dbKey: string }).dbKey);
            });
         }
         return undefined;
      });

      const db = Database.get(DATABASE_KEYS.TEST);

      const firstLoad = db.load(),
            secondLoad = db.load();

      await Promise.resolve();
      expect(loadAttempts).toBe(1);

      releaseLoad();
      await Promise.all([ firstLoad, secondLoad ]);

      expect(loadAttempts).toBe(1);
      expect(db.getPath()).toBe(getDatabasePath(DATABASE_KEYS.TEST));
   });

   it('close resets load state and allows reload', async () => {
      let loadAttempts = 0;

      mockIPC((cmd, args) => {
         lastCmd = cmd;
         lastArgs = args as Record<string, unknown>;
         if (cmd === 'plugin:sqlite|load') {
            loadAttempts += 1;
            return getDatabasePath((args as { dbKey: string }).dbKey);
         }
         if (cmd === 'plugin:sqlite|close') {
            return true;
         }
         return undefined;
      });

      const db = Database.get(DATABASE_KEYS.TEST);

      await db.load();
      expect(loadAttempts).toBe(1);
      expect(db.getPath()).toBe(getDatabasePath(DATABASE_KEYS.TEST));

      const wasClosed = await db.close();

      expect(wasClosed).toBe(true);
      expect(lastCmd).toBe('plugin:sqlite|close');
      expect(db.getPath()).toBeNull();

      await db.load();
      expect(loadAttempts).toBe(2);
      expect(db.getPath()).toBe(getDatabasePath(DATABASE_KEYS.TEST));
   });

   it('remove resets load state and allows reload', async () => {
      let loadAttempts = 0;

      mockIPC((cmd, args) => {
         lastCmd = cmd;
         lastArgs = args as Record<string, unknown>;
         if (cmd === 'plugin:sqlite|load') {
            loadAttempts += 1;
            return getDatabasePath((args as { dbKey: string }).dbKey);
         }
         if (cmd === 'plugin:sqlite|remove') {
            return true;
         }
         return undefined;
      });

      const db = Database.get(DATABASE_KEYS.TEST);

      await db.load();
      expect(loadAttempts).toBe(1);

      const wasRemoved = await db.remove();

      expect(wasRemoved).toBe(true);
      expect(lastCmd).toBe('plugin:sqlite|remove');
      expect(db.getPath()).toBeNull();

      await db.load();
      expect(loadAttempts).toBe(2);
      expect(db.getPath()).toBe(getDatabasePath(DATABASE_KEYS.TEST));
   });

   it('execute', async () => {
      await Database.get(DATABASE_KEYS.T).execute('INSERT INTO t VALUES ($1)', [ 1 ]);
      expect(lastCmd).toBe('plugin:sqlite|execute');
      expect(lastArgs).toMatchObject({ dbKey: DATABASE_KEYS.T, query: 'INSERT INTO t VALUES ($1)', values: [ 1 ], attached: null });
   });

   it('execute with attached databases', async () => {
      await Database.get(DATABASE_KEYS.MAIN)
         .execute('UPDATE todos SET status = $1 WHERE id IN (SELECT todo_id FROM archive.completed)', [ 'archived' ])
         .attach([
            {
               databaseKey: DATABASE_KEYS.ARCHIVE,
               schemaName: 'archive',
               mode: 'readOnly',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|execute');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.MAIN);
      expect(lastArgs.attached).toEqual([
         {
            databaseKey: 'ARCHIVE',
            schemaName: 'archive',
            mode: 'readOnly',
         },
      ]);
   });

   it('execute_transaction', async () => {
      await Database.get(DATABASE_KEYS.T).executeTransaction([ [ 'DELETE FROM t' ] ]);
      expect(lastCmd).toBe('plugin:sqlite|execute_transaction');
      expect(lastArgs.statements).toEqual([ { query: 'DELETE FROM t', values: [] } ]);
      expect(lastArgs.attached).toBe(null);
   });

   it('execute_transaction with attached databases', async () => {
      await Database.get(DATABASE_KEYS.MAIN)
         .executeTransaction([
            [ 'INSERT INTO orders (user_id, total) VALUES ($1, $2)', [ 1, 99.99 ] ],
            [ 'UPDATE stats.order_stats SET order_count = order_count + 1', [] ],
         ])
         .attach([
            {
               databaseKey: DATABASE_KEYS.STATS,
               schemaName: 'stats',
               mode: 'readWrite',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|execute_transaction');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.MAIN);
      expect(lastArgs.attached).toEqual([
         {
            databaseKey: DATABASE_KEYS.STATS,
            schemaName: 'stats',
            mode: 'readWrite',
         },
      ]);
   });

   it('fetch_all', async () => {
      await Database.get(DATABASE_KEYS.T).fetchAll('SELECT * FROM t');
      expect(lastCmd).toBe('plugin:sqlite|fetch_all');
      expect(lastArgs).toMatchObject({ dbKey: DATABASE_KEYS.T, query: 'SELECT * FROM t', attached: null });
   });

   it('fetch_all with attached databases', async () => {
      await Database.get(DATABASE_KEYS.MAIN)
         .fetchAll('SELECT u.name, o.total FROM users u JOIN orders.orders o ON u.id = o.user_id', [])
         .attach([
            {
               databaseKey: DATABASE_KEYS.ORDERS,
               schemaName: 'orders',
               mode: 'readOnly',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|fetch_all');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.MAIN);
      expect(lastArgs.attached).toEqual([
         {
            databaseKey: DATABASE_KEYS.ORDERS,
            schemaName: 'orders',
            mode: 'readOnly',
         },
      ]);
   });

   it('fetch_one', async () => {
      await Database.get(DATABASE_KEYS.T).fetchOne('SELECT * FROM t WHERE id = $1', [ 1 ]);
      expect(lastCmd).toBe('plugin:sqlite|fetch_one');
      expect(lastArgs).toMatchObject({ dbKey: DATABASE_KEYS.T, values: [ 1 ], attached: null });
   });

   it('fetch_one with attached databases', async () => {
      await Database.get(DATABASE_KEYS.MAIN)
         .fetchOne('SELECT COUNT(*) as total FROM users u JOIN orders.orders o ON u.id = o.user_id', [])
         .attach([
            {
               databaseKey: DATABASE_KEYS.ORDERS,
               schemaName: 'orders',
               mode: 'readOnly',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|fetch_one');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.MAIN);
      expect(lastArgs.attached).toEqual([
         {
            databaseKey: DATABASE_KEYS.ORDERS,
            schemaName: 'orders',
            mode: 'readOnly',
         },
      ]);
   });

   it('fetch_page first page (no cursor)', async () => {
      const keyset: KeysetColumn[] = [
         { name: 'id', direction: 'asc' },
      ];

      await Database.get(DATABASE_KEYS.T).fetchPage('SELECT * FROM posts', [], keyset, 25);
      expect(lastCmd).toBe('plugin:sqlite|fetch_page');
      expect(lastArgs).toMatchObject({
         dbKey: DATABASE_KEYS.T,
         query: 'SELECT * FROM posts',
         values: [],
         keyset: [ { name: 'id', direction: 'asc' } ],
         pageSize: 25,
         after: null,
         before: null,
         attached: null,
      });
   });

   it('fetch_page with cursor via .after()', async () => {
      const keyset: KeysetColumn[] = [
         { name: 'id', direction: 'asc' },
      ];

      await Database.get(DATABASE_KEYS.T)
         .fetchPage('SELECT * FROM posts', [], keyset, 25)
         .after([ 100 ]);

      expect(lastCmd).toBe('plugin:sqlite|fetch_page');
      expect(lastArgs.after).toEqual([ 100 ]);
      expect(lastArgs.before).toBeNull();
   });

   it('fetch_page with cursor via .before()', async () => {
      const keyset: KeysetColumn[] = [
         { name: 'id', direction: 'asc' },
      ];

      await Database.get(DATABASE_KEYS.T)
         .fetchPage('SELECT * FROM posts', [], keyset, 25)
         .before([ 50 ]);

      expect(lastCmd).toBe('plugin:sqlite|fetch_page');
      expect(lastArgs.before).toEqual([ 50 ]);
      expect(lastArgs.after).toBeNull();
   });

   it('fetch_page with attached databases', async () => {
      const keyset: KeysetColumn[] = [
         { name: 'id', direction: 'asc' },
      ];

      await Database.get(DATABASE_KEYS.MAIN)
         .fetchPage('SELECT * FROM posts', [], keyset, 25)
         .attach([
            {
               databaseKey: DATABASE_KEYS.ARCHIVE,
               schemaName: 'archive',
               mode: 'readOnly',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|fetch_page');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.MAIN);
      expect(lastArgs.after).toBeNull();
      expect(lastArgs.before).toBeNull();
      expect(lastArgs.attached).toEqual([
         {
            databaseKey: DATABASE_KEYS.ARCHIVE,
            schemaName: 'archive',
            mode: 'readOnly',
         },
      ]);
   });

   it('fetch_page multi-column keyset with .after()', async () => {
      const keyset: KeysetColumn[] = [
         { name: 'category', direction: 'asc' },
         { name: 'score', direction: 'desc' },
         { name: 'id', direction: 'asc' },
      ];

      await Database.get(DATABASE_KEYS.T)
         .fetchPage('SELECT * FROM posts WHERE active = $1', [ true ], keyset, 50)
         .after([ 'tech', 95, 42 ]);

      expect(lastCmd).toBe('plugin:sqlite|fetch_page');
      expect(lastArgs.keyset).toEqual([
         { name: 'category', direction: 'asc' },
         { name: 'score', direction: 'desc' },
         { name: 'id', direction: 'asc' },
      ]);
      expect(lastArgs.values).toEqual([ true ]);
      expect(lastArgs.after).toEqual([ 'tech', 95, 42 ]);
      expect(lastArgs.before).toBeNull();
      expect(lastArgs.pageSize).toBe(50);
   });

   it('fetch_page multi-column keyset with .before()', async () => {
      const keyset: KeysetColumn[] = [
         { name: 'category', direction: 'asc' },
         { name: 'score', direction: 'desc' },
         { name: 'id', direction: 'asc' },
      ];

      await Database.get(DATABASE_KEYS.T)
         .fetchPage('SELECT * FROM posts WHERE active = $1', [ true ], keyset, 50)
         .before([ 'tech', 95, 42 ]);

      expect(lastCmd).toBe('plugin:sqlite|fetch_page');
      expect(lastArgs.keyset).toEqual([
         { name: 'category', direction: 'asc' },
         { name: 'score', direction: 'desc' },
         { name: 'id', direction: 'asc' },
      ]);
      expect(lastArgs.values).toEqual([ true ]);
      expect(lastArgs.before).toEqual([ 'tech', 95, 42 ]);
      expect(lastArgs.after).toBeNull();
      expect(lastArgs.pageSize).toBe(50);
   });

   it('close', async () => {
      await Database.get(DATABASE_KEYS.T).close();
      expect(lastCmd).toBe('plugin:sqlite|close');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.T);
   });

   it('close_all', async () => {
      await Database.close_all();
      expect(lastCmd).toBe('plugin:sqlite|close_all');
   });

   it('remove', async () => {
      await Database.get(DATABASE_KEYS.T).remove();
      expect(lastCmd).toBe('plugin:sqlite|remove');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.T);
   });

   it('getMigrationEvents', async () => {
      const mockEvents: MigrationEvent[] = [
         { dbKey: DATABASE_KEYS.T, dbPath: 't.db', status: 'running' },
         { dbKey: DATABASE_KEYS.T, dbPath: 't.db', status: 'completed', migrationCount: 5 },
      ];

      mockIPC((cmd, args) => {
         lastCmd = cmd;
         lastArgs = args as Record<string, unknown>;
         if (cmd === 'plugin:sqlite|get_migration_events') {
            return mockEvents;
         }
         return undefined;
      });

      const events = await Database.get(DATABASE_KEYS.T).getMigrationEvents();

      expect(lastCmd).toBe('plugin:sqlite|get_migration_events');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.T);
      expect(events).toEqual(mockEvents);
   });

   it('getMigrationEvents - empty array', async () => {
      const events = await Database.get(DATABASE_KEYS.TEST).getMigrationEvents();

      expect(lastCmd).toBe('plugin:sqlite|get_migration_events');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.TEST);
      expect(events).toEqual([]);
   });

   it('beginInterruptibleTransaction', async () => {
      const tx = await Database.get(DATABASE_KEYS.T).beginInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      expect(lastCmd).toBe('plugin:sqlite|begin_interruptible_transaction');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.T);
      expect(lastArgs.initialStatements).toEqual([
         { query: 'INSERT INTO users (name) VALUES ($1)', values: [ 'Alice' ] },
      ]);
      expect(lastArgs.attached).toBe(null);
      expect(tx).toBeInstanceOf(Object);
   });

   it('beginInterruptibleTransaction with attached databases', async () => {
      const tx = await Database.get(DATABASE_KEYS.MAIN)
         .beginInterruptibleTransaction([
            [ 'DELETE FROM users WHERE id IN (SELECT user_id FROM archive.archived_users)' ],
         ])
         .attach([
            {
               databaseKey: DATABASE_KEYS.ARCHIVE,
               schemaName: 'archive',
               mode: 'readOnly',
            },
         ]);

      expect(lastCmd).toBe('plugin:sqlite|begin_interruptible_transaction');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.MAIN);
      expect(lastArgs.initialStatements).toEqual([
         { query: 'DELETE FROM users WHERE id IN (SELECT user_id FROM archive.archived_users)', values: [] },
      ]);
      expect(lastArgs.attached).toEqual([
         {
            databaseKey: DATABASE_KEYS.ARCHIVE,
            schemaName: 'archive',
            mode: 'readOnly',
         },
      ]);
      expect(tx).toBeInstanceOf(Object);
   });

   it('InterruptibleTransaction.continueWith()', async () => {
      const tx = await Database.get(DATABASE_KEYS.TEST).beginInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      const tx2 = await tx.continueWith([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Bob' ] ],
      ]);

      expect(lastCmd).toBe('plugin:sqlite|transaction_continue');
      expect(lastArgs.token).toEqual({ dbKey: DATABASE_KEYS.TEST, transactionId: 'test-tx-id' });
      expect((lastArgs.action as { type: string }).type).toBe('Continue');
      expect(tx2).toBeInstanceOf(Object);
   });

   it('InterruptibleTransaction.commit()', async () => {
      const tx = await Database.get(DATABASE_KEYS.TEST).beginInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      await tx.commit();
      expect(lastCmd).toBe('plugin:sqlite|transaction_continue');
      expect(lastArgs.token).toEqual({ dbKey: DATABASE_KEYS.TEST, transactionId: 'test-tx-id' });
      expect((lastArgs.action as { type: string }).type).toBe('Commit');
   });

   it('InterruptibleTransaction.rollback()', async () => {
      const tx = await Database.get(DATABASE_KEYS.TEST).beginInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      await tx.rollback();
      expect(lastCmd).toBe('plugin:sqlite|transaction_continue');
      expect(lastArgs.token).toEqual({ dbKey: DATABASE_KEYS.TEST, transactionId: 'test-tx-id' });
      expect((lastArgs.action as { type: string }).type).toBe('Rollback');
   });

   it('InterruptibleTransaction.read()', async () => {
      const tx = await Database.get(DATABASE_KEYS.TEST).beginInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      await tx.read('SELECT * FROM users WHERE name = $1', [ 'Alice' ]);
      expect(lastCmd).toBe('plugin:sqlite|transaction_read');
      expect(lastArgs.token).toEqual({ dbKey: DATABASE_KEYS.TEST, transactionId: 'test-tx-id' });
      expect(lastArgs.query).toBe('SELECT * FROM users WHERE name = $1');
      expect(lastArgs.values).toEqual([ 'Alice' ]);
   });

   it('handles errors from backend', async () => {
      const db = new Database(DATABASE_KEYS.T);

      mockIPC(() => {
         throw new Error('Database error');
      });

      await expect(db.execute('SELECT 1', [])).rejects.toThrow('Database error');
   });
});

describe('Database.load with customConfig', () => {
   it('passes customConfig to backend', async () => {
      await Database.load(DATABASE_KEYS.TEST, { maxReadConnections: 10, idleTimeoutSecs: 60 });
      expect(lastCmd).toBe('plugin:sqlite|load');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.TEST);
      expect(lastArgs.customConfig).toEqual({ maxReadConnections: 10, idleTimeoutSecs: 60 });
   });
});

describe('Observer commands', () => {
   it('observe', async () => {
      await Database.get(DATABASE_KEYS.T).observe([ 'users', 'posts' ]);
      expect(lastCmd).toBe('plugin:sqlite|observe');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.T);
      expect(lastArgs.tables).toEqual([ 'users', 'posts' ]);
      expect(lastArgs.config).toBe(null);
   });

   it('observe with config', async () => {
      const config: ObserverConfig = { channelCapacity: 512, captureValues: false };

      await Database.get(DATABASE_KEYS.T).observe([ 'users' ], config);
      expect(lastCmd).toBe('plugin:sqlite|observe');
      expect(lastArgs.tables).toEqual([ 'users' ]);
      expect(lastArgs.config).toEqual({ channelCapacity: 512, captureValues: false });
   });

   it('subscribe', async () => {
      const events: TableChangeEvent[] = [];

      const sub = await Database.get(DATABASE_KEYS.T).subscribe([ 'users' ], (e) => { events.push(e); });

      expect(lastCmd).toBe('plugin:sqlite|subscribe');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.T);
      expect(lastArgs.tables).toEqual([ 'users' ]);
      expect(lastArgs.onEvent).toBeDefined();
      expect(sub).toBeInstanceOf(Subscription);
      expect(sub.id).toBe('sub-123');
   });

   it('unsubscribe', async () => {
      const sub = new Subscription('sub-456');

      expect(sub.id).toBe('sub-456');

      const result = await sub.unsubscribe();

      expect(lastCmd).toBe('plugin:sqlite|unsubscribe');
      expect(lastArgs.subscriptionId).toBe('sub-456');
      expect(result).toBe(true);
   });

   it('unobserve', async () => {
      await Database.get(DATABASE_KEYS.T).unobserve();
      expect(lastCmd).toBe('plugin:sqlite|unobserve');
      expect(lastArgs.dbKey).toBe(DATABASE_KEYS.T);
   });
});

describe('MigrationEvent type', () => {
   it('accepts running status', () => {
      const event: MigrationEvent = {
         dbKey: 'TEST',
         dbPath: 'test.db',
         status: 'running',
      };

      expect(event.status).toBe('running');
      expect(event.migrationCount).toBeUndefined();
      expect(event.error).toBeUndefined();
   });

   it('accepts completed status with migrationCount', () => {
      const event: MigrationEvent = {
         dbKey: 'TEST',
         dbPath: 'test.db',
         status: 'completed',
         migrationCount: 3,
      };

      expect(event.status).toBe('completed');
      expect(event.migrationCount).toBe(3);
   });

   it('accepts failed status with error', () => {
      const event: MigrationEvent = {
         dbKey: 'TEST',
         dbPath: 'test.db',
         status: 'failed',
         error: 'Migration failed: syntax error',
      };

      expect(event.status).toBe('failed');
      expect(event.error).toBe('Migration failed: syntax error');
   });
});

describe('Observer types', () => {
   it('ColumnValue variants', () => {
      const values: ColumnValue[] = [
         { type: 'null' },
         { type: 'integer', value: 42 },
         { type: 'real', value: 3.14 },
         { type: 'text', value: 'hello' },
         { type: 'blob', value: 'SGVsbG8=' },
      ];

      expect(values).toHaveLength(5);
      expect(values[0].type).toBe('null');
      expect(values[1].type).toBe('integer');
      expect((values[1] as { type: 'integer'; value: number }).value).toBe(42);
      expect(values[4].type).toBe('blob');
   });

   it('TableChange structure', () => {
      const change: TableChange = {
         schema: 'main',
         table: 'users',
         operation: 'insert',
         rowid: 1,
         primaryKey: [ { type: 'integer', value: 1 } ],
         newValues: [
            { type: 'integer', value: 1 },
            { type: 'text', value: 'Alice' },
         ],
      };

      expect(change.table).toBe('users');
      expect(change.operation).toBe('insert');
      expect(change.rowid).toBe(1);
      expect(change.primaryKey).toHaveLength(1);
      expect(change.oldValues).toBeUndefined();
      expect(change.newValues).toHaveLength(2);
   });

   it('TableChange without rowid', () => {
      const change: TableChange = {
         schema: 'main',
         table: 'kv_store',
         operation: 'update',
         primaryKey: [ { type: 'text', value: 'my-key' } ],
      };

      expect(change.rowid).toBeUndefined();
      expect(change.primaryKey[0]).toEqual({ type: 'text', value: 'my-key' });
   });

   it('TableChangeEvent change variant', () => {
      const event: TableChangeEvent = {
         event: 'change',
         data: {
            // Not 'main' here on purpose - schema is whatever alias the
            // write happened under, not always the primary database, so a
            // fixture that only ever uses 'main' would document the field as
            // if it were a constant.
            schema: 'archive',
            table: 'users',
            operation: 'delete',
            primaryKey: [ { type: 'integer', value: 5 } ],
         },
      };

      expect(event.event).toBe('change');
      expect(event.data).toHaveProperty('table', 'users');
   });

   it('TableChangeEvent lagged variant', () => {
      const event: TableChangeEvent = {
         event: 'lagged',
         data: { count: 42 },
      };

      expect(event.event).toBe('lagged');
      expect(event.data).toEqual({ count: 42 });
   });
});
