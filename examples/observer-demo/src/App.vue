<script setup lang="ts">
import { onMounted, ref } from 'vue';
import Database from '@silvermine/tauri-plugin-sqlite';
import type { TableChangeEvent, ColumnValue } from '@silvermine/tauri-plugin-sqlite';
import './style.css';
import MessageList from './components/MessageList.vue';
import ChangeFeed from './components/ChangeFeed.vue';
import ActionBar from './components/ActionBar.vue';

export interface Message {
   id: number;
   content: string;
   createdAt: string;
}

export interface ChangeEntry {
   id: number;
   timestamp: string;
   operation: string;
   table: string;
   primaryKey: string;
   oldValues: string;
   newValues: string;
}

const DB_PATH = 'observer-demo.db',
      messages = ref<Message[]>([]),
      changeEvents = ref<ChangeEntry[]>([]),
      ready = ref(false),
      error = ref('');

let db: Database | undefined,
    nextChangeID = 1,
    insertCounter = 0;

// Extract a primitive JS value from a typed ColumnValue
const unwrap = (val: ColumnValue): string | number | null => {
   if (val.type === 'null') { return null; }
   return val.value;
};

const formatColumnValue = (val: ColumnValue): string => {
   if (val.type === 'null') { return 'NULL'; }
   return String(val.value);
};

const formatColumnValues = (values: ColumnValue[] | undefined): string => {
   if (!values || values.length === 0) { return '-'; }
   return values.map(formatColumnValue).join(', ');
};

// Build a Message from observer column values (column order: id, content,
// created_at — matching the table definition)
const messageFromValues = (values: ColumnValue[]): Message => {
   return {
      id: unwrap(values[0]) as number,
      content: unwrap(values[1]) as string,
      createdAt: unwrap(values[2]) as string,
   };
};

const applyChange = (event: TableChangeEvent): void => {
   if (event.event !== 'change') { return; }

   const { data } = event;

   if (data.operation === 'insert' && data.newValues) {
      const msg = messageFromValues(data.newValues);

      messages.value = [...messages.value, msg];
   } else if (data.operation === 'update' && data.newValues) {
      const updated = messageFromValues(data.newValues);

      messages.value = messages.value.map((m) => {
         return m.id === updated.id ? updated : m;
      });
   } else if (data.operation === 'delete' && data.oldValues) {
      const deleted = messageFromValues(data.oldValues);

      messages.value = messages.value.filter((m) => {
         return m.id !== deleted.id;
      });
   }
};

const handleChangeEvent = (event: TableChangeEvent): void => {
   const now = new Date(),
         timestamp = now.toLocaleTimeString(undefined, { hour12: false, fractionalSecondDigits: 3 });

   // Update the messages list directly from the event data — no re-querying
   applyChange(event);

   if (event.event === 'change') {
      const { data } = event;

      changeEvents.value = [{
         id: nextChangeID++,
         timestamp,
         operation: data.operation ?? 'unknown',
         table: data.table,
         primaryKey: data.primaryKey.map(formatColumnValue).join(', '),
         oldValues: formatColumnValues(data.oldValues),
         newValues: formatColumnValues(data.newValues),
      }, ...changeEvents.value];
   } else {
      changeEvents.value = [{
         id: nextChangeID++,
         timestamp,
         operation: 'lagged',
         table: '-',
         primaryKey: '-',
         oldValues: '-',
         newValues: `Missed ${event.data.count} notification(s)`,
      }, ...changeEvents.value];
   }
};

const handleInsert = async (): Promise<void> => {
   if (!db) { return; }
   insertCounter += 1;

   await db.execute(
      'INSERT INTO messages (content) VALUES ($1)',
      [`Message #${insertCounter}`]
   );
};

const handleUpdateLast = async (): Promise<void> => {
   if (!db || messages.value.length === 0) { return; }
   const last = messages.value[messages.value.length - 1];

   await db.execute(
      'UPDATE messages SET content = $1 WHERE id = $2',
      [`${last.content} (edited)`, last.id]
   );
};

const handleDeleteLast = async (): Promise<void> => {
   if (!db || messages.value.length === 0) { return; }
   const last = messages.value[messages.value.length - 1];

   await db.execute('DELETE FROM messages WHERE id = $1', [last.id]);
};

onMounted(async () => {
   try {
      db = await Database.load(DB_PATH);

      await db.execute(`
         CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
         )
      `);

      // Load existing rows once at startup so the table isn't empty on restart.
      // After this, all updates come exclusively from observer events.
      const existing = await db.fetchAll<Array<{ id: number; content: string; created_at: string }>>(
         'SELECT id, content, created_at FROM messages ORDER BY id ASC'
      );

      messages.value = existing.map((row) => {
         return { id: row.id, content: row.content, createdAt: row.created_at };
      });
      insertCounter = messages.value.length;

      // Enable observer and subscribe to changes
      await db.observe(['messages'], { captureValues: true });
      await db.subscribe(['messages'], handleChangeEvent);

      ready.value = true;
   } catch (err) {
      error.value = String(err);
   }
});

// No explicit cleanup needed — the Tauri plugin tears down observers and
// connections when the process exits. Attempting async IPC calls during
// window shutdown can deadlock because the runtime may already be gone.
</script>

<template>
   <div class="app">
      <header class="app-header">
         <h1>SQLite Observer Demo</h1>
         <p class="app-header-subtitle">
            Real-time change notifications powered by
            <code>@silvermine/tauri-plugin-sqlite</code>
         </p>
      </header>

      <div v-if="error" class="app-error">{{ error }}</div>

      <template v-if="ready">
         <ActionBar :message-count="messages.length"
            @insert="handleInsert"
            @update-last="handleUpdateLast"
            @delete-last="handleDeleteLast">
         </ActionBar>

         <div class="app-panels">
            <MessageList :messages="messages"></MessageList>
            <ChangeFeed :change-events="changeEvents"></ChangeFeed>
         </div>
      </template>

      <div v-else-if="!error" class="app-loading">Connecting to database...</div>
   </div>
</template>

<style scoped>
.app {
   max-width: 1000px;
   margin: 0 auto;
   padding: 24px;
}

.app-header {
   margin-block-end: 24px;
}

.app-header h1 {
   font-size: 22px;
   font-weight: 700;
   margin-block-end: 4px;
}

.app-header-subtitle {
   font-size: 13px;
   color: var(--color-text-secondary);
}

.app-header-subtitle code {
   font-family: var(--font-mono);
   font-size: 12px;
   background-color: var(--color-bg-secondary);
   padding: 1px 5px;
   border-radius: var(--radius-sm);
}

.app-error {
   padding: 12px 16px;
   background-color: var(--color-delete-bg);
   color: var(--color-delete);
   border-radius: var(--radius);
   margin-block-end: 16px;
   font-family: var(--font-mono);
   font-size: 13px;
}

.app-panels {
   display: grid;
   grid-template-columns: 1fr 1fr;
   gap: 16px;
}

.app-loading {
   color: var(--color-text-secondary);
   text-align: center;
   padding: 48px 0;
}

@media (max-width: 700px) {
   .app {
      padding: 12px;
   }

   .app-panels {
      grid-template-columns: 1fr;
   }
}
</style>
