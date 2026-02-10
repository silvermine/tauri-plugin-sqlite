<script setup lang="ts">
import type { Message } from '../App.vue';

defineProps({
   messages: {
      type: Array as () => Message[],
      required: true,
   },
});
</script>

<template>
   <section class="messageList">
      <h2 class="messageList-title">Messages</h2>
      <div class="messageList-body">
         <table v-if="messages.length > 0" class="messageList-table">
            <thead>
               <tr>
                  <th>ID</th>
                  <th>Content</th>
                  <th>Created</th>
               </tr>
            </thead>
            <tbody>
               <tr v-for="msg in messages" :key="msg.id">
                  <td class="messageList-table-id">{{ msg.id }}</td>
                  <td>{{ msg.content }}</td>
                  <td class="messageList-table-date">{{ msg.createdAt }}</td>
               </tr>
            </tbody>
         </table>
         <p v-else class="messageList-empty">
            No messages yet. Click <strong>Insert Message</strong> to add one.
         </p>
      </div>
   </section>
</template>

<style scoped>
.messageList {
   border: 1px solid var(--color-border);
   border-radius: var(--radius);
   overflow: hidden;
}

.messageList-title {
   font-size: 14px;
   font-weight: 600;
   padding: 10px 14px;
   background-color: var(--color-bg-secondary);
   border-block-end: 1px solid var(--color-border);
}

.messageList-body {
   max-height: 400px;
   overflow-y: auto;
   overflow-x: auto;
}

.messageList-table {
   width: 100%;
   border-collapse: collapse;
}

.messageList-table th,
.messageList-table td {
   padding: 8px 14px;
   text-align: start;
   border-block-end: 1px solid var(--color-border);
}

.messageList-table th {
   font-size: 11px;
   font-weight: 600;
   text-transform: uppercase;
   letter-spacing: 0.5px;
   color: var(--color-text-secondary);
   background-color: var(--color-bg-secondary);
}

.messageList-table-id {
   font-family: var(--font-mono);
   font-size: 12px;
   color: var(--color-text-secondary);
}

.messageList-table-date {
   font-family: var(--font-mono);
   font-size: 12px;
   color: var(--color-text-secondary);
   white-space: nowrap;
}

.messageList-empty {
   padding: 32px 14px;
   text-align: center;
   color: var(--color-text-secondary);
   font-size: 13px;
}
</style>
