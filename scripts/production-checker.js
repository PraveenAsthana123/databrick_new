#!/usr/bin/env node

/**
 * Production Readiness Checker
 *
 * Scans codebase for common AI-generated code issues that fail in production.
 * Run: node scripts/production-checker.js
 */

const fs = require('fs');
const path = require('path');

const SRC_DIR = path.join(__dirname, '..', 'src');
const RESULTS = { errors: [], warnings: [], passed: [] };

function scan(dir, extensions) {
  const files = [];
  function walk(d) {
    if (!fs.existsSync(d)) return;
    fs.readdirSync(d).forEach((f) => {
      const full = path.join(d, f);
      if (fs.statSync(full).isDirectory()) {
        if (!f.startsWith('.') && f !== 'node_modules' && f !== '__tests__') walk(full);
      } else if (extensions.some((ext) => f.endsWith(ext))) {
        files.push(full);
      }
    });
  }
  walk(dir);
  return files;
}

function check(type, name, condition, detail) {
  if (condition) {
    RESULTS.passed.push({ name, detail: detail || 'OK' });
  } else {
    RESULTS[type].push({ name, detail });
  }
}

// ─── Scan all JS/JSX files ────────────────────
const jsFiles = scan(SRC_DIR, ['.js', '.jsx']);
const allCode = jsFiles.map((f) => ({ path: f, content: fs.readFileSync(f, 'utf8') }));

// ─── 1. Hardcoded URLs ───────────────────────
const hardcodedUrls = allCode.filter((f) => {
  if (f.path.includes('test') || f.path.includes('spec')) return false;
  // Check lines outside of template literals and code example strings
  const lines = f.content.split('\n');
  return lines.some(
    (line) =>
      line.includes('http://localhost') &&
      !line.trim().startsWith('//') &&
      !line.trim().startsWith('#') &&
      !line.trim().startsWith('*') &&
      !line.includes('process.env') &&
      !line.includes("'http://localhost") && // inside string constant (code examples)
      !line.includes('"http://localhost') // inside string constant (code examples)
  );
});
check(
  'errors',
  'No hardcoded localhost URLs',
  hardcodedUrls.length === 0,
  `Found in: ${hardcodedUrls.map((f) => path.relative(SRC_DIR, f.path)).join(', ')}`
);

// ─── 2. Console.log in production code ────────
const consoleLogs = allCode.filter(
  (f) =>
    f.content.includes('console.log(') && !f.path.includes('test') && !f.path.includes('__tests__')
);
check(
  'warnings',
  'No console.log in production code',
  consoleLogs.length === 0,
  `Found in: ${consoleLogs.map((f) => path.relative(SRC_DIR, f.path)).join(', ')}`
);

// ─── 3. Missing error handling on fetch ───────
const fetchWithoutCatch = allCode.filter((f) => {
  const hasFetch = f.content.includes('fetch(') || f.content.includes('api.get(');
  const hasCatch = f.content.includes('.catch') || f.content.includes('try');
  return hasFetch && !hasCatch && !f.path.includes('test');
});
check(
  'errors',
  'All fetch calls have error handling',
  fetchWithoutCatch.length === 0,
  `Missing in: ${fetchWithoutCatch.map((f) => path.relative(SRC_DIR, f.path)).join(', ')}`
);

// ─── 4. useEffect without cleanup ─────────────
const effectWithoutCleanup = allCode.filter((f) => {
  const effects = (f.content.match(/useEffect\(/g) || []).length;
  const cleanups = (f.content.match(/return\s*\(\)\s*=>/g) || []).length;
  const cancelledPattern = (f.content.match(/cancelled\s*=\s*true/g) || []).length;
  return effects > 0 && cleanups === 0 && cancelledPattern === 0 && !f.path.includes('test');
});
check(
  'warnings',
  'useEffect hooks have cleanup functions',
  effectWithoutCleanup.length === 0,
  `Missing cleanup in: ${effectWithoutCleanup.map((f) => path.relative(SRC_DIR, f.path)).join(', ')}`
);

// ─── 5. TODO/FIXME left in code ───────────────
const todos = allCode.filter(
  (f) =>
    (f.content.includes('TODO') || f.content.includes('FIXME') || f.content.includes('HACK')) &&
    !f.path.includes('test')
);
check(
  'warnings',
  'No TODO/FIXME/HACK in production code',
  todos.length === 0,
  `Found in: ${todos.map((f) => path.relative(SRC_DIR, f.path)).join(', ')}`
);

// ─── 6. Hardcoded API keys/secrets ────────────
const secretPatterns = [
  /['"]sk-[a-zA-Z0-9]{20,}['"]/,
  /['"]api[_-]?key['"]\s*[:=]\s*['"][^'"]+['"]/i,
  /password\s*[:=]\s*['"][^'"]+['"]/i,
];
const hardcodedSecrets = allCode.filter((f) =>
  secretPatterns.some((pattern) => pattern.test(f.content))
);
check(
  'errors',
  'No hardcoded secrets/API keys',
  hardcodedSecrets.length === 0,
  `Found in: ${hardcodedSecrets.map((f) => path.relative(SRC_DIR, f.path)).join(', ')}`
);

// ─── 7. Unsafe HTML injection (XSS risk) ──────
const unsafeHtml = allCode.filter(
  (f) => f.content.includes('.innerHTML') && !f.path.includes('test')
);
check(
  'warnings',
  'No direct innerHTML usage (XSS risk)',
  unsafeHtml.length === 0,
  `Found in: ${unsafeHtml.map((f) => path.relative(SRC_DIR, f.path)).join(', ')}`
);

// ─── 8. .env.template exists ──────────────────
const envTemplate = fs.existsSync(path.join(__dirname, '..', '.env.template'));
check('errors', '.env.template file exists', envTemplate, 'Missing .env.template');

// ─── 9. Error boundary exists ─────────────────
const hasErrorBoundary = allCode.some(
  (f) => f.content.includes('ErrorBoundary') || f.content.includes('getDerivedStateFromError')
);
check('errors', 'ErrorBoundary component exists', hasErrorBoundary, 'No ErrorBoundary found');

// ─── 10. Package-lock.json exists ─────────────
const hasLockfile = fs.existsSync(path.join(__dirname, '..', 'package-lock.json'));
check('errors', 'package-lock.json exists (reproducible builds)', hasLockfile, 'Missing lockfile');

// ─── 11. .gitignore covers secrets ────────────
const gitignore = fs.existsSync(path.join(__dirname, '..', '.gitignore'))
  ? fs.readFileSync(path.join(__dirname, '..', '.gitignore'), 'utf8')
  : '';
const gitignoreCovers =
  gitignore.includes('.env') && gitignore.includes('*.key') && gitignore.includes('node_modules');
check(
  'errors',
  '.gitignore covers .env, *.key, node_modules',
  gitignoreCovers,
  'Missing entries in .gitignore'
);

// ─── 12. Tests exist ──────────────────────────
// Scan including __tests__ directories for test file count
function scanAll(dir, extensions) {
  const found = [];
  function walkAll(d) {
    if (!fs.existsSync(d)) return;
    fs.readdirSync(d).forEach((f) => {
      const full = path.join(d, f);
      if (fs.statSync(full).isDirectory()) {
        if (!f.startsWith('.') && f !== 'node_modules') walkAll(full);
      } else if (extensions.some((ext) => f.endsWith(ext))) {
        found.push(full);
      }
    });
  }
  walkAll(dir);
  return found;
}
const testFiles = scanAll(SRC_DIR, ['.test.js', '.test.jsx', '.spec.js']);
check('errors', 'Unit tests exist (min 3 files)', testFiles.length >= 3, `Only ${testFiles.length} test files`);

// ─── 13. CI pipeline exists ───────────────────
const hasCi = fs.existsSync(path.join(__dirname, '..', '.github', 'workflows', 'ci.yml'));
check('errors', 'CI pipeline exists (.github/workflows/ci.yml)', hasCi, 'No CI pipeline');

// ─── 14. README exists ────────────────────────
const hasReadme = fs.existsSync(path.join(__dirname, '..', 'README.md'));
check('errors', 'README.md exists', hasReadme, 'Missing README.md');

// ─── 15. ErrorTracker exists ──────────────────
const hasErrorTracker = allCode.some((f) => f.content.includes('errorTracker'));
check('warnings', 'ErrorTracker initialized', hasErrorTracker, 'No errorTracker found in code');

// ─── Output Report ────────────────────────────
const timestamp = new Date().toISOString().replace('T', ' ').slice(0, 19);

process.stdout.write('\n');
process.stdout.write('='.repeat(55) + '\n');
process.stdout.write(`  PRODUCTION READINESS CHECK — ${timestamp}\n`);
process.stdout.write('='.repeat(55) + '\n\n');

if (RESULTS.errors.length > 0) {
  process.stdout.write(`  ERRORS (${RESULTS.errors.length}) — Must fix before deploy:\n`);
  RESULTS.errors.forEach((e) => process.stdout.write(`    [FAIL] ${e.name}\n           ${e.detail}\n`));
  process.stdout.write('\n');
}

if (RESULTS.warnings.length > 0) {
  process.stdout.write(`  WARNINGS (${RESULTS.warnings.length}) — Should fix:\n`);
  RESULTS.warnings.forEach((w) =>
    process.stdout.write(`    [WARN] ${w.name}\n           ${w.detail}\n`)
  );
  process.stdout.write('\n');
}

process.stdout.write(`  PASSED (${RESULTS.passed.length}):\n`);
RESULTS.passed.forEach((p) => process.stdout.write(`    [PASS] ${p.name}\n`));

process.stdout.write('\n');
process.stdout.write('-'.repeat(55) + '\n');
const total = RESULTS.passed.length + RESULTS.errors.length + RESULTS.warnings.length;
const score = Math.round((RESULTS.passed.length / total) * 100);
process.stdout.write(
  `  Score: ${score}% (${RESULTS.passed.length} passed, ${RESULTS.errors.length} errors, ${RESULTS.warnings.length} warnings)\n`
);
process.stdout.write(
  score >= 80 ? '  Status: READY for production\n' : '  Status: NOT READY — fix errors first\n'
);
process.stdout.write('='.repeat(55) + '\n\n');

process.exit(RESULTS.errors.length > 0 ? 1 : 0);
