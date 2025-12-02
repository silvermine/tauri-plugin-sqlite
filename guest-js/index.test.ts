/**
 * Sanity check to test the bridge between TypeScript and the Tauri commands.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mockIPC, clearMocks } from '@tauri-apps/api/mocks'
import Database from './index'

let lastCmd = ''
let lastArgs: Record<string, unknown> = {}

beforeEach(() => {
   mockIPC((cmd, args) => {
      lastCmd = cmd
      lastArgs = args as Record<string, unknown>
      if (cmd === 'plugin:sqlite|load') return (args as { db: string }).db
      if (cmd === 'plugin:sqlite|execute') return [1, 1]
      if (cmd === 'plugin:sqlite|execute_transaction') return []
      if (cmd === 'plugin:sqlite|fetch_all') return []
      if (cmd === 'plugin:sqlite|fetch_one') return null
      if (cmd === 'plugin:sqlite|close') return true
      if (cmd === 'plugin:sqlite|close_all') return undefined
      if (cmd === 'plugin:sqlite|remove') return true
      return undefined
   })
})

afterEach(() => clearMocks())

describe('Database commands', () => {
   it('load', async () => {
      await Database.load('test.db')
      expect(lastCmd).toBe('plugin:sqlite|load')
      expect(lastArgs.db).toBe('test.db')
   })

   it('execute', async () => {
      await Database.get('t.db').execute('INSERT INTO t VALUES ($1)', [1])
      expect(lastCmd).toBe('plugin:sqlite|execute')
      expect(lastArgs).toMatchObject({ db: 't.db', query: 'INSERT INTO t VALUES ($1)', values: [1] })
   })

   it('execute_transaction', async () => {
      await Database.get('t.db').executeTransaction([['DELETE FROM t']])
      expect(lastCmd).toBe('plugin:sqlite|execute_transaction')
      expect(lastArgs.statements).toEqual([{ query: 'DELETE FROM t', values: [] }])
   })

   it('fetch_all', async () => {
      await Database.get('t.db').fetchAll('SELECT * FROM t')
      expect(lastCmd).toBe('plugin:sqlite|fetch_all')
      expect(lastArgs).toMatchObject({ db: 't.db', query: 'SELECT * FROM t' })
   })

   it('fetch_one', async () => {
      await Database.get('t.db').fetchOne('SELECT * FROM t WHERE id = $1', [1])
      expect(lastCmd).toBe('plugin:sqlite|fetch_one')
      expect(lastArgs).toMatchObject({ db: 't.db', values: [1] })
   })

   it('close', async () => {
      await Database.get('t.db').close()
      expect(lastCmd).toBe('plugin:sqlite|close')
      expect(lastArgs.db).toBe('t.db')
   })

   it('close_all', async () => {
      await Database.closeAll()
      expect(lastCmd).toBe('plugin:sqlite|close_all')
   })

   it('remove', async () => {
      await Database.get('t.db').remove()
      expect(lastCmd).toBe('plugin:sqlite|remove')
      expect(lastArgs.db).toBe('t.db')
   })
})
