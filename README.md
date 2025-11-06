# Tauri SQLite Plugin

[![CI][ci-badge]][ci-url]

A Tauri plugin for SQLite database access with connection management. This plugin
depends on [SQLx](https://github.com/launchbadge/sqlx) and enforces pragmatic policies
for connection management.

[ci-badge]: https://github.com/silvermine/tauri-plugin-sqlite/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/silvermine/tauri-plugin-sqlite/actions/workflows/ci.yml

## Project Structure

This project is organized as a Cargo workspace with the following structure:

```text
tauri-plugin-sqlite/
├── crates/
│   └── sqlx-sqlite-conn-mgr/   # SQLx SQLite connection pool manager
│       ├── src/
│       │   └── lib.rs
│       └── Cargo.toml
├── src/                        # Tauri plugin implementation
│   ├── commands.rs             # Plugin commands
│   ├── error.rs                # Error types
│   ├── lib.rs                  # Main plugin code
│   └── models.rs               # Data models
├── guest-js/                   # JavaScript/TypeScript bindings
│   ├── index.ts
│   └── tsconfig.json
├── permissions/                # Permission definitions (mostly generated)
├── dist-js/                    # Compiled JS (generated)
├── Cargo.toml                  # Workspace configuration
├── package.json                # NPM package configuration
└── build.rs                    # Build script
```

## Crates

### sqlx-sqlite-conn-mgr

A pure Rust module with no dependencies on Tauri or its plugin architecture. It
provides connection management for SQLite databases using SQLx. It's designed to be
published as a standalone crate in the future with minimal changes.

See [`crates/sqlx-sqlite-conn-mgr/README.md`](crates/sqlx-sqlite-conn-mgr/README.md)
for more details.

### Tauri Plugin

The main plugin provides a Tauri integration layer that exposes SQLite functionality
to Tauri applications. It uses the `sqlx-sqlite-conn-mgr` module internally.

## Getting Started

### Installation

1. Install NPM dependencies:

   ```bash
   npm install
   ```

2. Build the TypeScript bindings:

   ```bash
   npm run build
   ```

3. Build the Rust plugin:

   ```bash
   cargo build
   ```

### Tests

Run Rust tests:

```bash
cargo test
```

### Linting and standards checks

```bash
npm run standards
```

## Usage

### In a Tauri Application

Add the plugin to your Tauri application's `Cargo.toml`:

```toml
[dependencies]
tauri-plugin-sqlite = { path = "../path/to/tauri-plugin-sqlite" }
```

Initialize the plugin in your Tauri app:

```rust
fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_sqlite::init())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

### JavaScript/TypeScript API

Install the JavaScript package in your frontend:

```bash
npm install @silvermine/tauri-plugin-sqlite
```

Use the plugin from JavaScript:

```typescript
// TODO: Add real examples once we have decided on the plugin API
import { hello } from '@silvermine/tauri-plugin-sqlite';

// Call the hello command
const greeting = await hello('World');
console.log(greeting); // "Hello, World! This is the SQLite plugin."
```

## Development Standards

This project follows the
[Silvermine standardization](https://github.com/silvermine/standardization)
guidelines. Key standards include:

   * **EditorConfig**: Consistent editor settings across the team
   * **Markdownlint**: Markdown linting for documentation
   * **Commitlint**: Conventional commit message format
   * **Code Style**: 3-space indentation, LF line endings

### Running Standards Checks

```bash
npm run standards
```

## License

MIT

## Contributing

Contributions are welcome! Please follow the established coding standards and commit
message conventions.
