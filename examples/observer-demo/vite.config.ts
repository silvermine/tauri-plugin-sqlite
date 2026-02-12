import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

export default defineConfig({
   plugins: [vue()],
   clearScreen: false,
   server: {
      host: process.env.TAURI_DEV_HOST || false,
      port: 1420,
      strictPort: true,
   },
   envPrefix: ['VITE_', 'TAURI_'],
   build: {
      target: ['es2021', 'chrome100', 'safari14'],
      minify: !process.env.TAURI_DEBUG ? 'esbuild' : false,
      sourcemap: !!process.env.TAURI_DEBUG,
   },
});
