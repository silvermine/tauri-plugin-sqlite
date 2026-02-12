<script setup lang="ts">
defineProps({
   messageCount: {
      type: Number,
      required: true,
   },
});

const emit = defineEmits<{
   insert: [];
   'update-last': [];
   'delete-last': [];
}>();
</script>

<template>
   <div class="actionBar">
      <button class="actionBar-btn actionBar-btn--insert"
         @click="emit('insert')">
         Insert Message
      </button>
      <button class="actionBar-btn actionBar-btn--update"
         :disabled="messageCount === 0"
         @click="emit('update-last')">
         Update Last
      </button>
      <button class="actionBar-btn actionBar-btn--delete"
         :disabled="messageCount === 0"
         @click="emit('delete-last')">
         Delete Last
      </button>
   </div>
</template>

<style scoped>
.actionBar {
   display: flex;
   flex-wrap: wrap;
   gap: 8px;
   margin-block-end: 16px;
}

.actionBar-btn--insert {
   background-color: var(--color-insert);
   border-color: var(--color-insert);
   color: #fff;
}

.actionBar-btn--insert:hover:not(:disabled),
.actionBar-btn--insert:focus:not(:disabled),
.actionBar-btn--insert:active:not(:disabled) {
   background-color: var(--color-insert);
   opacity: 0.9;
}

.actionBar-btn--update {
   background-color: var(--color-update);
   border-color: var(--color-update);
   color: #fff;
}

.actionBar-btn--update:hover:not(:disabled),
.actionBar-btn--update:focus:not(:disabled),
.actionBar-btn--update:active:not(:disabled) {
   background-color: var(--color-update);
   opacity: 0.9;
}

.actionBar-btn--delete {
   background-color: var(--color-delete);
   border-color: var(--color-delete);
   color: #fff;
}

.actionBar-btn--delete:hover:not(:disabled),
.actionBar-btn--delete:focus:not(:disabled),
.actionBar-btn--delete:active:not(:disabled) {
   background-color: var(--color-delete);
   opacity: 0.9;
}
</style>
