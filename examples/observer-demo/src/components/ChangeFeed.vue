<script setup lang="ts">
import type { ChangeEntry } from '../App.vue';

defineProps({
   changeEvents: {
      type: Array as () => ChangeEntry[],
      required: true,
   },
});
</script>

<template>
   <section class="changeFeed">
      <h2 class="changeFeed-title">Change Feed</h2>
      <div class="changeFeed-body">
         <ul v-if="changeEvents.length > 0" class="changeFeed-list">
            <li v-for="entry in changeEvents" :key="entry.id"
               class="changeFeed-item">
               <div class="changeFeed-item-header">
                  <span class="changeFeed-item-time">{{ entry.timestamp }}</span>
                  <span class="changeFeed-item-badge"
                     :class="`changeFeed-item-badge--${entry.operation}`">
                     {{ entry.operation }}
                  </span>
                  <span class="changeFeed-item-table">{{ entry.table }}</span>
                  <span class="changeFeed-item-pk">PK: {{ entry.primaryKey }}</span>
               </div>
               <div class="changeFeed-item-values">
                  <template v-if="entry.operation === 'insert'">
                     <span class="changeFeed-item-label">new:</span>
                     <code>{{ entry.newValues }}</code>
                  </template>
                  <template v-else-if="entry.operation === 'delete'">
                     <span class="changeFeed-item-label">old:</span>
                     <code>{{ entry.oldValues }}</code>
                  </template>
                  <template v-else-if="entry.operation === 'update'">
                     <span class="changeFeed-item-label">old:</span>
                     <code>{{ entry.oldValues }}</code>
                     <br />
                     <span class="changeFeed-item-label">new:</span>
                     <code>{{ entry.newValues }}</code>
                  </template>
                  <template v-else>
                     <code>{{ entry.newValues }}</code>
                  </template>
               </div>
            </li>
         </ul>
         <p v-else class="changeFeed-empty">
            Waiting for change notifications...
         </p>
      </div>
   </section>
</template>

<style scoped>
.changeFeed {
   border: 1px solid var(--color-border);
   border-radius: var(--radius);
   overflow: hidden;
}

.changeFeed-title {
   font-size: 14px;
   font-weight: 600;
   padding: 10px 14px;
   background-color: var(--color-bg-secondary);
   border-block-end: 1px solid var(--color-border);
}

.changeFeed-body {
   max-height: 400px;
   overflow-y: auto;
}

.changeFeed-list {
   list-style: none;
}

.changeFeed-item {
   padding: 10px 14px;
   border-block-end: 1px solid var(--color-border);
}

.changeFeed-item:last-child {
   border-block-end: none;
}

.changeFeed-item-header {
   display: flex;
   flex-wrap: wrap;
   align-items: center;
   gap: 6px 8px;
   margin-block-end: 4px;
}

.changeFeed-item-time {
   font-family: var(--font-mono);
   font-size: 11px;
   color: var(--color-text-secondary);
}

.changeFeed-item-badge {
   font-size: 11px;
   font-weight: 600;
   text-transform: uppercase;
   padding: 1px 6px;
   border-radius: var(--radius-sm);
}

.changeFeed-item-badge--insert {
   color: var(--color-insert);
   background-color: var(--color-insert-bg);
}

.changeFeed-item-badge--update {
   color: var(--color-update);
   background-color: var(--color-update-bg);
}

.changeFeed-item-badge--delete {
   color: var(--color-delete);
   background-color: var(--color-delete-bg);
}

.changeFeed-item-badge--lagged {
   color: var(--color-lagged);
   background-color: var(--color-lagged-bg);
}

.changeFeed-item-table {
   font-weight: 500;
   font-size: 13px;
}

.changeFeed-item-pk {
   font-family: var(--font-mono);
   font-size: 11px;
   color: var(--color-text-secondary);
   margin-inline-start: auto;
}

.changeFeed-item-values {
   font-size: 12px;
   color: var(--color-text-secondary);
   line-height: 1.6;
}

.changeFeed-item-values code {
   font-family: var(--font-mono);
   font-size: 11px;
   background-color: var(--color-bg-secondary);
   padding: 1px 4px;
   border-radius: 2px;
}

.changeFeed-item-label {
   font-size: 11px;
   font-weight: 500;
   display: inline-block;
   min-width: 28px;
}

.changeFeed-empty {
   padding: 32px 14px;
   text-align: center;
   color: var(--color-text-secondary);
   font-size: 13px;
}
</style>
