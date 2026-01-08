/**
 * Sanity checks to test the bridge between TypeScript and the Tauri commands.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockIPC, clearMocks } from '@tauri-apps/api/mocks';
import Database, { MigrationEvent } from './index';

let lastCmd = '',
    lastArgs: Record<string, unknown> = {};

beforeEach(() => {
   mockIPC((cmd, args) => {
      lastCmd = cmd;
      lastArgs = args as Record<string, unknown>;
      if (cmd === 'plugin:sqlite|load') {
         return (args as { db: string }).db;
      }
      if (cmd === 'plugin:sqlite|execute') {
         return [ 1, 1 ];
      }
      if (cmd === 'plugin:sqlite|execute_transaction') {
         return [];
      }
      if (cmd === 'plugin:sqlite|execute_interruptible_transaction') {
         return { dbPath: (args as { db: string }).db, transactionId: 'test-tx-id' };
      }
      if (cmd === 'plugin:sqlite|transaction_continue') {
         const action = (args as { action: { type: string } }).action;

         if (action.type === 'Continue') {
            return { dbPath: 'test.db', transactionId: 'test-tx-id' };
         }
         return undefined;
      }
      if (cmd === 'plugin:sqlite|transaction_read') {
         return [];
      }
      if (cmd === 'plugin:sqlite|fetch_all') {
         return [];
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
      return undefined;
   });
});

afterEach(() => { return clearMocks(); });

describe('Database commands', () => {
   it('load', async () => {
      await Database.load('test.db');
      expect(lastCmd).toBe('plugin:sqlite|load');
      expect(lastArgs.db).toBe('test.db');
   });

   it('execute', async () => {
      await Database.get('t.db').execute('INSERT INTO t VALUES ($1)', [ 1 ]);
      expect(lastCmd).toBe('plugin:sqlite|execute');
      expect(lastArgs).toMatchObject({ db: 't.db', query: 'INSERT INTO t VALUES ($1)', values: [ 1 ], attached: null });
   });

   it('execute with attached databases', async () => {
      await Database.get('main.db')
         .execute('UPDATE todos SET status = $1 WHERE id IN (SELECT todo_id FROM archive.completed)', [ 'archived' ])
         .attach([
            {
               databasePath: 'archive.db',
               schemaName: 'archive',
               mode: 'readOnly',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|execute');
      expect(lastArgs.db).toBe('main.db');
      expect(lastArgs.attached).toEqual([
         {
            databasePath: 'archive.db',
            schemaName: 'archive',
            mode: 'readOnly',
         },
      ]);
   });

   it('execute_transaction', async () => {
      await Database.get('t.db').executeTransaction([ [ 'DELETE FROM t' ] ]);
      expect(lastCmd).toBe('plugin:sqlite|execute_transaction');
      expect(lastArgs.statements).toEqual([ { query: 'DELETE FROM t', values: [] } ]);
      expect(lastArgs.attached).toBe(null);
   });

   it('execute_transaction with attached databases', async () => {
      await Database.get('main.db')
         .executeTransaction([
            [ 'INSERT INTO orders (user_id, total) VALUES ($1, $2)', [ 1, 99.99 ] ],
            [ 'UPDATE stats.order_stats SET order_count = order_count + 1', [] ],
         ])
         .attach([
            {
               databasePath: 'stats.db',
               schemaName: 'stats',
               mode: 'readWrite',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|execute_transaction');
      expect(lastArgs.db).toBe('main.db');
      expect(lastArgs.attached).toEqual([
         {
            databasePath: 'stats.db',
            schemaName: 'stats',
            mode: 'readWrite',
         },
      ]);
   });

   it('fetch_all', async () => {
      await Database.get('t.db').fetchAll('SELECT * FROM t');
      expect(lastCmd).toBe('plugin:sqlite|fetch_all');
      expect(lastArgs).toMatchObject({ db: 't.db', query: 'SELECT * FROM t', attached: null });
   });

   it('fetch_all with attached databases', async () => {
      await Database.get('main.db')
         .fetchAll('SELECT u.name, o.total FROM users u JOIN orders.orders o ON u.id = o.user_id', [])
         .attach([
            {
               databasePath: 'orders.db',
               schemaName: 'orders',
               mode: 'readOnly',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|fetch_all');
      expect(lastArgs.db).toBe('main.db');
      expect(lastArgs.attached).toEqual([
         {
            databasePath: 'orders.db',
            schemaName: 'orders',
            mode: 'readOnly',
         },
      ]);
   });

   it('fetch_one', async () => {
      await Database.get('t.db').fetchOne('SELECT * FROM t WHERE id = $1', [ 1 ]);
      expect(lastCmd).toBe('plugin:sqlite|fetch_one');
      expect(lastArgs).toMatchObject({ db: 't.db', values: [ 1 ], attached: null });
   });

   it('fetch_one with attached databases', async () => {
      await Database.get('main.db')
         .fetchOne('SELECT COUNT(*) as total FROM users u JOIN orders.orders o ON u.id = o.user_id', [])
         .attach([
            {
               databasePath: 'orders.db',
               schemaName: 'orders',
               mode: 'readOnly',
            },
         ]);
      expect(lastCmd).toBe('plugin:sqlite|fetch_one');
      expect(lastArgs.db).toBe('main.db');
      expect(lastArgs.attached).toEqual([
         {
            databasePath: 'orders.db',
            schemaName: 'orders',
            mode: 'readOnly',
         },
      ]);
   });

   it('close', async () => {
      await Database.get('t.db').close();
      expect(lastCmd).toBe('plugin:sqlite|close');
      expect(lastArgs.db).toBe('t.db');
   });

   it('close_all', async () => {
      await Database.close_all();
      expect(lastCmd).toBe('plugin:sqlite|close_all');
   });

   it('remove', async () => {
      await Database.get('t.db').remove();
      expect(lastCmd).toBe('plugin:sqlite|remove');
      expect(lastArgs.db).toBe('t.db');
   });

   it('getMigrationEvents', async () => {
      const mockEvents: MigrationEvent[] = [
         { dbPath: 't.db', status: 'running' },
         { dbPath: 't.db', status: 'completed', migrationCount: 5 },
      ];

      mockIPC((cmd, args) => {
         lastCmd = cmd;
         lastArgs = args as Record<string, unknown>;
         if (cmd === 'plugin:sqlite|get_migration_events') {
            return mockEvents;
         }
         return undefined;
      });

      const events = await Database.get('t.db').getMigrationEvents();

      expect(lastCmd).toBe('plugin:sqlite|get_migration_events');
      expect(lastArgs.db).toBe('t.db');
      expect(events).toEqual(mockEvents);
   });

   it('getMigrationEvents - empty array', async () => {
      const events = await Database.get('test.db').getMigrationEvents();

      expect(lastCmd).toBe('plugin:sqlite|get_migration_events');
      expect(lastArgs.db).toBe('test.db');
      expect(events).toEqual([]);
   });

   it('executeInterruptibleTransaction', async () => {
      const tx = await Database.get('t.db').executeInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      expect(lastCmd).toBe('plugin:sqlite|execute_interruptible_transaction');
      expect(lastArgs.db).toBe('t.db');
      expect(lastArgs.initialStatements).toEqual([
         { query: 'INSERT INTO users (name) VALUES ($1)', values: [ 'Alice' ] },
      ]);
      expect(tx).toBeInstanceOf(Object);
   });

   it('InterruptibleTransaction.continue()', async () => {
      const tx = await Database.get('test.db').executeInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      const tx2 = await tx.continue([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Bob' ] ],
      ]);

      expect(lastCmd).toBe('plugin:sqlite|transaction_continue');
      expect(lastArgs.token).toEqual({ dbPath: 'test.db', transactionId: 'test-tx-id' });
      expect((lastArgs.action as { type: string }).type).toBe('Continue');
      expect(tx2).toBeInstanceOf(Object);
   });

   it('InterruptibleTransaction.commit()', async () => {
      const tx = await Database.get('test.db').executeInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      await tx.commit();
      expect(lastCmd).toBe('plugin:sqlite|transaction_continue');
      expect(lastArgs.token).toEqual({ dbPath: 'test.db', transactionId: 'test-tx-id' });
      expect((lastArgs.action as { type: string }).type).toBe('Commit');
   });

   it('InterruptibleTransaction.rollback()', async () => {
      const tx = await Database.get('test.db').executeInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      await tx.rollback();
      expect(lastCmd).toBe('plugin:sqlite|transaction_continue');
      expect(lastArgs.token).toEqual({ dbPath: 'test.db', transactionId: 'test-tx-id' });
      expect((lastArgs.action as { type: string }).type).toBe('Rollback');
   });

   it('InterruptibleTransaction.read()', async () => {
      const tx = await Database.get('test.db').executeInterruptibleTransaction([
         [ 'INSERT INTO users (name) VALUES ($1)', [ 'Alice' ] ],
      ]);

      await tx.read('SELECT * FROM users WHERE name = $1', [ 'Alice' ]);
      expect(lastCmd).toBe('plugin:sqlite|transaction_read');
      expect(lastArgs.token).toEqual({ dbPath: 'test.db', transactionId: 'test-tx-id' });
      expect(lastArgs.query).toBe('SELECT * FROM users WHERE name = $1');
      expect(lastArgs.values).toEqual([ 'Alice' ]);
   });

   it('handles errors from backend', async () => {
      mockIPC(() => {
         throw new Error('Database error');
      });
      await expect(Database.get('t.db').execute('SELECT 1', [])).rejects.toThrow('Database error');
   });
});

describe('MigrationEvent type', () => {
   it('accepts running status', () => {
      const event: MigrationEvent = {
         dbPath: 'test.db',
         status: 'running',
      };

      expect(event.status).toBe('running');
      expect(event.migrationCount).toBeUndefined();
      expect(event.error).toBeUndefined();
   });

   it('accepts completed status with migrationCount', () => {
      const event: MigrationEvent = {
         dbPath: 'test.db',
         status: 'completed',
         migrationCount: 3,
      };

      expect(event.status).toBe('completed');
      expect(event.migrationCount).toBe(3);
   });

   it('accepts failed status with error', () => {
      const event: MigrationEvent = {
         dbPath: 'test.db',
         status: 'failed',
         error: 'Migration failed: syntax error',
      };

      expect(event.status).toBe('failed');
      expect(event.error).toBe('Migration failed: syntax error');
   });
});
