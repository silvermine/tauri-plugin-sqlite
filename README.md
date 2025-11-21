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

### Linting and Standards Checks

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

### Tracing and Logging

This plugin and its connection manager crate use the
[`tracing`](https://crates.io/crates/tracing) ecosystem for internal logging. They are
configured with the `release_max_level_off` feature so that **all log statements are
compiled out of release builds**. This guarantees that logging from this plugin will never
reach production binaries unless you explicitly change that configuration.

To see logs during development, initialize a `tracing-subscriber` in your Tauri
application crate and keep it behind a `debug_assertions` guard, for example:

```toml
[dependencies]
tracing = { version = "0.1.41", default-features = false, features = ["std", "release_max_level_off"] }
tracing-subscriber = { version = "0.3.20", features = ["fmt", "env-filter"] }
```

```rust
#[cfg(debug_assertions)]
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("trace"));

    fmt().with_env_filter(filter).compact().init();
}

#[cfg(not(debug_assertions))]
fn init_tracing() {}

fn main() {
    init_tracing();

    tauri::Builder::default()
        .plugin(tauri_plugin_sqlite::init())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

With this setup, `tauri dev` shows all plugin and app logs, while `tauri build` produces
a release binary that contains no logging from this plugin or your app-level `tracing`
calls.

### JavaScript/TypeScript API

Install the JavaScript package in your frontend:

```bash
npm install @silvermine/tauri-plugin-sqlite
```

Use the plugin from JavaScript:

Add the plugin permission to your capabilities file `src-tauri/capabilities/default.json`:

```json
{
    "permissions": [
        "core:default",
        "sqlite:default"
    ]
}
```

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
