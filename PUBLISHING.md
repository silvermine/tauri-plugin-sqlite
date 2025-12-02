# Publishing Guide

This guide explains how to publish the `tauri-plugin-sqlite` to both
crates.io (Rust) and npm (JavaScript/TypeScript).

## Overview

This Tauri plugin has two primary artifacts that need to be published:

1. **Rust crate** (`tauri-plugin-sqlite`) - Published to
   [crates.io](https://crates.io)
2. **NPM package** (`@silvermine/tauri-plugin-sqlite`) - Published to
   [npmjs.com](https://www.npmjs.com)

## Prerequisites

### For Rust (crates.io)

   * A [crates.io](https://crates.io) account
   * Authentication token configured: `cargo login <your-token>`
   * Write permissions to the `tauri-plugin-sqlite` crate (for updates)

### For NPM

   * An [npm](https://www.npmjs.com) account
   * Authentication configured: `npm login` or `npm adduser`
   * Member of the `@silvermine` organization (or owner)
   * Write permissions to the `@silvermine/tauri-plugin-sqlite` package
     (for updates)

## Pre-Publishing Checklist

Before publishing, ensure:

   * [ ] All tests pass: `cargo test`
   * [ ] Code passes linting: `npm run standards`
   * [ ] JavaScript is built: `npm run build`
   * [ ] Version numbers are updated in both `Cargo.toml` and `package.json`
   * [ ] CHANGELOG is updated with release notes
   * [ ] README is up to date
   * [ ] All changes are committed to git

## Publishing Steps

### 1. Update Version Numbers

Update the version in both files to match (e.g., `0.2.0`):

#### Cargo.toml

```toml
[package]
version = "0.2.0"
```

#### package.json

```json
{
  "version": "0.2.0"
}
```

### 2. Build JavaScript Artifacts

```bash
npm run build
```

This creates the distribution files in `dist-js/`:

   * `index.js` (ESM)
   * `index.cjs` (CommonJS)
   * `index.d.ts` (TypeScript declarations)

And the IIFE bundle:

   * `api-iife.js`

### 3. Verify Package Contents

#### Rust Package

```bash
cargo package --list --allow-dirty
```

This should include:

   * `src/` - Rust source code
   * `permissions/` - Tauri permission definitions
   * `build.rs` - Build script
   * `Cargo.toml` - Package manifest
   * `LICENSE` - MIT license
   * `README.md` - Documentation

#### NPM Package

```bash
npm pack --dry-run
```

This should include:

   * `dist-js/` - Built JavaScript files
   * `permissions/` - Tauri permission definitions
   * `LICENSE` - MIT license
   * `README.md` - Documentation
   * `package.json` - Package manifest

### 4. Test the Packages Locally

#### Test Rust Package

```bash
# Create a test package (doesn't publish)
cargo package --allow-dirty

# The package will be in target/package/tauri-plugin-sqlite-X.Y.Z.crate
# You can test it in another project with:
# cargo add --path /path/to/target/package/tauri-plugin-sqlite-X.Y.Z
```

#### Test NPM Package

```bash
# Create a tarball
npm pack

# This creates: silvermine-tauri-plugin-sqlite-X.Y.Z.tgz
# You can test it in another project with:
# npm install /path/to/silvermine-tauri-plugin-sqlite-X.Y.Z.tgz
```

### 5. Publish to crates.io

```bash
# Publish the Rust crate
cargo publish
```

If you need to do a dry run first:

```bash
cargo publish --dry-run
```

### 6. Publish to npm

```bash
# Publish the NPM package
npm publish --access public
```

**Note**: The `--access public` flag is required for scoped packages (@silvermine).

If you need to do a dry run first:

```bash
npm publish --dry-run
```

### 7. Create a Git Tag

```bash
git tag -a v0.2.0 -m "Release version 0.2.0"
git push origin v0.2.0
```

### 8. Create a GitHub Release

1. Go to the [Releases page](https://github.com/silvermine/tauri-plugin-sqlite/releases)
2. Click "Draft a new release"
3. Select the tag you just created
4. Add release notes (copy from CHANGELOG)
5. Publish the release

## Post-Publishing Verification

After publishing, verify the packages are available:

### Crates.io

Visit: <https://crates.io/crates/tauri-plugin-sqlite>

Or test installation in a new project:

```bash
cargo add tauri-plugin-sqlite
```

### NPM

Visit: <https://www.npmjs.com/package/@silvermine/tauri-plugin-sqlite>

Or test installation in a new project:

```bash
npm install @silvermine/tauri-plugin-sqlite
```

## Troubleshooting

### "crate already exists" Error

If you see this error from `cargo publish`, the version you're trying to
publish already exists. You cannot overwrite a published version on crates.io.
You need to increment the version number.

### "cannot publish package with no version" Error

Make sure the version is set in both `Cargo.toml` and `package.json`.

### "You do not have permission to publish" Error

For crates.io:

   * You need to be added as an owner:
     `cargo owner --add <username> tauri-plugin-sqlite`

For npm:

   * You need to be a member of the `@silvermine` organization
   * Contact the organization admin

### Package Size Too Large

If you get warnings about package size:

For Rust: Check the `include` field in `Cargo.toml` to ensure you're not
including unnecessary files.

For npm: Check the `files` field in `package.json`.

## Version Compatibility

When publishing, consider version compatibility:

   * **Rust crate**: Follow [Semantic Versioning](https://semver.org/)
   * **NPM package**: Also follows Semantic Versioning
   * Keep both versions in sync (same version number)

## Dependencies

Both packages have dependencies that should be kept up to date:

### Rust Dependencies

   * `tauri` - Should match the Tauri version your plugin supports
   * `sqlx` - Database library version

### NPM Dependencies

   * `@tauri-apps/api` - Must be a peer dependency (not bundled)

## Additional Resources

   * [Cargo Publishing Guide](https://doc.rust-lang.org/cargo/reference/publishing.html)
   * [NPM Publishing Guide](https://docs.npmjs.com/packages-and-modules/contributing-packages-to-the-registry) <!-- markdownlint-disable-line MD013 -->
   * [Tauri Plugin Documentation](https://tauri.app/develop/plugins/)
