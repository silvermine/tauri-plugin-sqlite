import { readFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import typescript from '@rollup/plugin-typescript';
import terser from '@rollup/plugin-terser';

const pkg = JSON.parse(readFileSync('./package.json', 'utf8'));

// Convert package name to camelCase for IIFE variable
// @silvermine/tauri-plugin-sqlite -> sqlite
const pluginJsName = pkg.name
   .replace('@silvermine/tauri-plugin-', '')
   .replace(/-./g, (x) => { return x[1].toUpperCase(); });

// IIFE variable name: __TAURI_PLUGIN_SQLITE__
const iifeVarName = `__TAURI_PLUGIN_${pkg.name
   .replace('@silvermine/tauri-plugin-', '')
   .replace('-', '_')
   .toUpperCase()}__`;

export default [
   // ESM and CJS builds
   {
      input: 'guest-js/index.ts',
      output: [
         {
            file: pkg.exports.import,
            format: 'esm',
            exports: 'named',
         },
         {
            file: pkg.exports.require,
            format: 'cjs',
            exports: 'named',
         },
      ],
      plugins: [
         typescript({
            tsconfig: './guest-js/tsconfig.json',
            declaration: true,
            declarationDir: dirname(pkg.exports.import),
         }),
      ],
      external: [
         /^@tauri-apps\/api/,
         ...Object.keys(pkg.dependencies || {}),
         ...Object.keys(pkg.peerDependencies || {}),
      ],
      onwarn: (warning) => {
         throw Object.assign(new Error(), warning);
      },
   },

   // IIFE build for direct browser usage
   {
      input: 'guest-js/index.ts',
      output: {
         format: 'iife',
         name: iifeVarName,
         // IIFE is in the format `var ${iifeVarName} = (() => {})()`
         // we check if __TAURI__ exists and inject the API object
         banner: 'if (\'__TAURI__\' in window) {',
         // the last `}` closes the if in the banner
         footer: `Object.defineProperty(window.__TAURI__, '${pluginJsName}', { value: ${iifeVarName} }) }`,
         file: 'api-iife.js',
         exports: 'named',
      },
      plugins: [
         typescript({
            tsconfig: './guest-js/tsconfig.json',
            declaration: false,
            outDir: '.',
         }),
         terser(),
         nodeResolve(),
      ],
      onwarn: (warning) => {
         throw Object.assign(new Error(), warning);
      },
   },
];
