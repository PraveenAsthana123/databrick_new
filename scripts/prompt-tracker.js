#!/usr/bin/env node

/**
 * Prompt Tracker — SQLite + MD File
 * Tracks every prompt/input given to the system.
 * Persists to both SQLite DB and markdown file for crash recovery.
 *
 * Usage:
 *   node scripts/prompt-tracker.js --save "prompt text here"
 *   node scripts/prompt-tracker.js --list
 *   node scripts/prompt-tracker.js --export
 *   node scripts/prompt-tracker.js --search "keyword"
 */

const fs = require('fs');
const path = require('path');

// ─── Config ───────────────────────────────────
const DB_PATH = path.join(__dirname, '..', 'data', 'prompts.db');
const MD_PATH = path.join(__dirname, '..', 'data', 'prompt-history.md');
const CRASH_LOG = path.join(__dirname, '..', 'data', 'crash-recovery.md');

// Ensure data directory exists
const dataDir = path.join(__dirname, '..', 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

// ─── SQLite Setup (using better-sqlite3 if available, fallback to JSON) ───
let db = null;
let useJsonFallback = false;
const JSON_DB_PATH = path.join(dataDir, 'prompts.json');

try {
  const Database = require('better-sqlite3');
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');

  // Create tables
  db.prepare(`
    CREATE TABLE IF NOT EXISTS prompts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TEXT NOT NULL DEFAULT (datetime('now')),
      session_id TEXT,
      prompt_text TEXT NOT NULL,
      category TEXT DEFAULT 'general',
      source TEXT DEFAULT 'user',
      status TEXT DEFAULT 'recorded',
      response_summary TEXT,
      metadata TEXT
    )
  `).run();

  db.prepare(`
    CREATE TABLE IF NOT EXISTS crash_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TEXT NOT NULL DEFAULT (datetime('now')),
      event_type TEXT NOT NULL,
      details TEXT,
      prompt_id INTEGER,
      recovery_status TEXT DEFAULT 'pending',
      FOREIGN KEY (prompt_id) REFERENCES prompts(id)
    )
  `).run();

  db.prepare(`
    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      started_at TEXT NOT NULL DEFAULT (datetime('now')),
      ended_at TEXT,
      total_prompts INTEGER DEFAULT 0,
      status TEXT DEFAULT 'active'
    )
  `).run();

  // Create indexes safely
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_prompts_timestamp ON prompts(timestamp)`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_prompts_category ON prompts(category)`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_prompts_session ON prompts(session_id)`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_crash_log_timestamp ON crash_log(timestamp)`).run();
} catch {
  useJsonFallback = true;
  if (!fs.existsSync(JSON_DB_PATH)) {
    fs.writeFileSync(
      JSON_DB_PATH,
      JSON.stringify({ prompts: [], crash_log: [], sessions: [] }, null, 2)
    );
  }
}

// ─── Helper Functions ─────────────────────────

function generateSessionId() {
  return `session_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function formatTimestamp() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

function savePrompt(text, category, source, sessionId) {
  const cat = category || 'general';
  const src = source || 'user';
  const timestamp = formatTimestamp();
  const sid = sessionId || generateSessionId();

  if (!useJsonFallback && db) {
    const stmt = db.prepare(`
      INSERT INTO prompts (timestamp, session_id, prompt_text, category, source)
      VALUES (?, ?, ?, ?, ?)
    `);
    const result = stmt.run(timestamp, sid, text, cat, src);

    // Also write to MD file for crash recovery
    appendToMd(timestamp, text, cat, result.lastInsertRowid);

    return { id: result.lastInsertRowid, timestamp, sessionId: sid };
  }

  // JSON fallback
  const data = JSON.parse(fs.readFileSync(JSON_DB_PATH, 'utf8'));
  const id = data.prompts.length + 1;
  data.prompts.push({ id, timestamp, session_id: sid, prompt_text: text, category: cat, source: src });
  fs.writeFileSync(JSON_DB_PATH, JSON.stringify(data, null, 2));

  appendToMd(timestamp, text, cat, id);
  return { id, timestamp, sessionId: sid };
}

function appendToMd(timestamp, text, category, id) {
  const entry = [
    '',
    `## Prompt #${id} — ${timestamp}`,
    `- **Category**: ${category}`,
    `- **Text**: ${text}`,
    '---',
    '',
  ].join('\n');

  if (!fs.existsSync(MD_PATH)) {
    fs.writeFileSync(
      MD_PATH,
      '# Prompt History\n\n> Auto-generated. Each prompt is tracked here and in SQLite.\n\n---\n'
    );
  }

  fs.appendFileSync(MD_PATH, entry);
}

function logCrashEvent(eventType, details, promptId) {
  const timestamp = formatTimestamp();

  if (!useJsonFallback && db) {
    db.prepare(`
      INSERT INTO crash_log (timestamp, event_type, details, prompt_id)
      VALUES (?, ?, ?, ?)
    `).run(timestamp, eventType, details, promptId || null);
  }

  // Always write to crash recovery MD
  const entry = [
    '',
    `## ${eventType} — ${timestamp}`,
    `- **Details**: ${details}`,
    `- **Prompt ID**: ${promptId || 'N/A'}`,
    '- **Recovery**: pending',
    '---',
    '',
  ].join('\n');

  if (!fs.existsSync(CRASH_LOG)) {
    fs.writeFileSync(
      CRASH_LOG,
      '# Crash Recovery Log\n\n> System crash/error events tracked here.\n\n---\n'
    );
  }

  fs.appendFileSync(CRASH_LOG, entry);
}

function listPrompts(limit) {
  const count = limit || 20;
  if (!useJsonFallback && db) {
    return db.prepare('SELECT * FROM prompts ORDER BY id DESC LIMIT ?').all(count);
  }
  const data = JSON.parse(fs.readFileSync(JSON_DB_PATH, 'utf8'));
  return data.prompts.slice(-count).reverse();
}

function searchPrompts(keyword) {
  if (!useJsonFallback && db) {
    return db
      .prepare('SELECT * FROM prompts WHERE prompt_text LIKE ? ORDER BY id DESC')
      .all(`%${keyword}%`);
  }
  const data = JSON.parse(fs.readFileSync(JSON_DB_PATH, 'utf8'));
  return data.prompts.filter((p) => p.prompt_text.includes(keyword)).reverse();
}

function exportAll() {
  const prompts = listPrompts(1000);
  const exportPath = path.join(dataDir, `prompt-export-${Date.now()}.json`);
  fs.writeFileSync(exportPath, JSON.stringify(prompts, null, 2));
  return exportPath;
}

// ─── CLI Interface ────────────────────────────
const args = process.argv.slice(2);

if (args[0] === '--save' && args[1]) {
  const result = savePrompt(args.slice(1).join(' '));
  process.stdout.write(JSON.stringify(result));
} else if (args[0] === '--list') {
  const prompts = listPrompts(parseInt(args[1], 10) || 20);
  process.stdout.write(JSON.stringify(prompts, null, 2));
} else if (args[0] === '--search' && args[1]) {
  const results = searchPrompts(args[1]);
  process.stdout.write(JSON.stringify(results, null, 2));
} else if (args[0] === '--export') {
  const exportPath = exportAll();
  process.stdout.write(`Exported to: ${exportPath}`);
} else if (args[0] === '--crash') {
  logCrashEvent(args[1] || 'UNKNOWN', args.slice(2).join(' ') || 'No details');
  process.stdout.write('Crash event logged');
} else {
  process.stdout.write(
    [
      'Prompt Tracker — Usage:',
      '  --save "prompt text"     Save a prompt',
      '  --list [count]           List recent prompts (default: 20)',
      '  --search "keyword"       Search prompts',
      '  --export                 Export all prompts to JSON',
      '  --crash "type" "details" Log a crash event',
    ].join('\n')
  );
}

module.exports = { savePrompt, listPrompts, searchPrompts, logCrashEvent, exportAll };
