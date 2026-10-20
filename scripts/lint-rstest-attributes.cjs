/**
 * Checks the attribute order of #[rstest] functions in the Rust sources.
 *
 * On rstest 0.28, a test attribute placed before a #[case] silently drops cases, and
 * clippy does not warn: `#[rstest] #[test] #[case] #[case]` generates only the first
 * case. This script flags any test attribute (#[test], #[tokio::test] or another
 * #[<path>::test]) above the last #[case]. The forms this repo uses are:
 *
 *    sync:   #[rstest], the #[case] lines, then the fn (rstest adds #[test] itself)
 *    async:  #[rstest], the #[case] lines, #[tokio::test], then the fn
 *
 * Usage: node scripts/lint-rstest-attributes.cjs [dir ...]   (default: crates src)
 */

'use strict';

const fs = require('fs/promises'),
      path = require('path');

const REPO_ROOT = path.resolve(__dirname, '..'),
      DEFAULT_ROOTS = [ 'crates', 'src' ],
      SKIP_DIRS = new Set([ 'target', 'node_modules' ]),
      RSTEST_ATTR = /^#\[(?:(?:::)?rstest::)?rstest\s*[\](]/,
      CASE_ATTR = /^#\[case\b/,
      PLAIN_TEST_ATTR = /^#\[test\s*\]/,
      // #[tokio::test], #[tokio::test(flavor = "multi_thread")] or any #[<path>::test]
      PATH_TEST_ATTR = /^#\[((?:::)?(?:\w+::)+test)\s*[\](]/,
      FN_LINE = /^(?:(?:pub(?:\([^)]*\))?|const|async|unsafe)\s+)*fn\s/;

async function findRustFiles(dir) {
   const entries = await fs.readdir(dir, { withFileTypes: true });

   const nested = await Promise.all(entries.map((entry) => {
      const entryPath = path.join(dir, entry.name);

      if (entry.isDirectory()) {
         return SKIP_DIRS.has(entry.name) ? [] : findRustFiles(entryPath);
      }
      return entry.isFile() && entry.name.endsWith('.rs') ? [ entryPath ] : [];
   }));

   return nested.flat();
}

function checkAttributes(file, attrs) {
   const lastCase = attrs.findLastIndex((attr) => { return CASE_ATTR.test(attr.text); });

   return attrs.flatMap((attr, index) => {
      const pathTest = PATH_TEST_ATTR.exec(attr.text),
            at = `${file}:${attr.line}:`;

      if (PLAIN_TEST_ATTR.test(attr.text) && index < lastCase) {
         return [ `${at} #[test] before the last #[case] silently drops cases; remove it (rstest adds the test attribute itself)` ];
      }
      if (pathTest && index < lastCase) {
         return [ `${at} #[${pathTest[1]}] before the last #[case] silently drops cases; move it after the last #[case]` ];
      }
      return [];
   });
}

/**
 * A block starts at an #[rstest] attribute and runs until its fn line. Inside it, only a
 * line starting with #[ begins an attribute: other lines are comments, blank lines or
 * the rest of a multi-line attribute.
 */
function checkSource(file, source) {
   const problems = [];

   let attrs = null,
       rstestLine = 0,
       functionCount = 0;

   source.split(/\r?\n/).forEach((rawLine, index) => {
      const line = rawLine.trim();

      if (attrs === null) {
         if (RSTEST_ATTR.test(line)) {
            attrs = [];
            rstestLine = index + 1;
         }
         return;
      }

      if (FN_LINE.test(line)) {
         problems.push(...checkAttributes(file, attrs));
         functionCount += 1;
         attrs = null;
      } else if (line.startsWith('#[')) {
         attrs.push({ line: index + 1, text: line });
      }
   });

   if (attrs !== null) {
      problems.push(`${file}:${rstestLine}: #[rstest] has no fn line after it; check its attributes`);
   }

   return { problems, functionCount };
}

function defaultRoots() {
   return DEFAULT_ROOTS.map((dir) => {
      return path.relative(process.cwd(), path.join(REPO_ROOT, dir));
   });
}

async function main() {
   const args = process.argv.slice(2),
         roots = args.length > 0 ? args : defaultRoots(),
         files = (await Promise.all(roots.map(findRustFiles))).flat().sort();

   const results = await Promise.all(files.map(async (file) => {
      return checkSource(file, await fs.readFile(file, 'utf8'));
   }));

   const problems = results.flatMap((result) => { return result.problems; }),
         functionCount = results.reduce((sum, result) => { return sum + result.functionCount; }, 0);

   if (problems.length > 0) {
      process.stderr.write(`${problems.join('\n')}\n${problems.length} rstest attribute problem(s) found\n`);
      process.exitCode = 1;
      return;
   }

   // Finding nothing means the roots or RSTEST_ATTR are wrong, not that every
   // function is fine: passing here would turn this check off without a trace.
   if (functionCount === 0) {
      process.stderr.write(`no #[rstest] functions found in ${files.length} files; check the roots and RSTEST_ATTR\n`);
      process.exitCode = 1;
      return;
   }

   process.stdout.write(`rstest attributes OK: checked ${functionCount} #[rstest] functions in ${files.length} files\n`);
}

main().catch((err) => {
   process.stderr.write(`${err.stack || err}\n`);
   process.exitCode = 1;
});
