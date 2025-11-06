import { invoke } from '@tauri-apps/api/core';

/**
 * Says hello from the SQLite plugin
 * @param name - The name to greet
 * @returns A greeting message
 */
export async function hello(name: string): Promise<string> {
   return await invoke<string>('plugin:sqlite|hello', { name });
}
