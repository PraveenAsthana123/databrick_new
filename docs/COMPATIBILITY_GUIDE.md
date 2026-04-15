# Backward & Forward Compatibility Guide — Mandatory for Every Project

> Never break existing users. Never block future upgrades.

---

## 1. What They Mean

```
BACKWARD COMPATIBILITY (BC)
  New code works with OLD data, APIs, configs, browsers
  "Upgrade without breaking existing users"

  v2.0 API ──► still accepts v1.0 request format
  New component ──► still renders with old props
  New DB schema ──► still reads old data

FORWARD COMPATIBILITY (FC)
  Old code works with NEW data, APIs, configs
  "Old version handles unknown future changes gracefully"

  v1.0 API ──► ignores unknown fields from v2.0 client
  Old component ──► renders safely with extra/unknown props
  Old DB reader ──► skips unknown columns
```

---

## 2. Compatibility Matrix

```
                    OLD CLIENT          NEW CLIENT
                ┌───────────────┬───────────────┐
  OLD SERVER    │   Works       │  Forward      │
                │   (baseline)  │  Compatible?  │
                ├───────────────┼───────────────┤
  NEW SERVER    │   Backward    │   Works       │
                │   Compatible? │   (target)    │
                └───────────────┴───────────────┘
```

---

## 3. API Compatibility

### 3.1 Backward Compatible API Changes (SAFE)

| Change | Example | Why Safe |
|--------|---------|----------|
| Add new optional field | `{ name, email, phone? }` | Old clients don't send phone, still works |
| Add new endpoint | `GET /api/v1/reports` | Old clients never call it |
| Add new enum value | `status: "active" | "paused" | "archived"` | Old clients only send known values |
| Widen input validation | Accept `string` where `number` was required | Old inputs still valid |
| Add default value | `limit = 50` if not provided | Old clients get default behavior |
| Add new HTTP method | `PATCH /api/v1/users/:id` | Old clients use PUT, still works |

### 3.2 Breaking API Changes (DANGEROUS)

| Change | Example | Impact | Fix |
|--------|---------|--------|-----|
| Remove field | Remove `user.avatar` | Old clients crash on `undefined` | Deprecate first, remove later |
| Rename field | `userName` to `username` | Old clients send wrong key | Support both during transition |
| Change type | `id: number` to `id: string` | Old clients send wrong type | Accept both, coerce |
| Remove endpoint | Delete `GET /api/v1/legacy` | Old clients get 404 | Version the API |
| Narrow validation | Require field that was optional | Old requests rejected | Keep optional, validate new |
| Change response shape | `{ data: [...] }` to `[...]` | Old clients can't parse | Version the API |

### 3.3 API Versioning Pattern

```javascript
// CORRECT — version in URL
app.use('/api/v1/users', usersV1Router);
app.use('/api/v2/users', usersV2Router);

// v1 handler — keep forever (or deprecate with warning)
router.get('/api/v1/users', (req, res) => {
  const users = getUsers();
  res.json({ data: users, total: users.length }); // v1 shape
});

// v2 handler — new shape
router.get('/api/v2/users', (req, res) => {
  const users = getUsers();
  res.json({ items: users, meta: { total: users.length, page: 1 } }); // v2 shape
});
```

### 3.4 Deprecation Pattern

```javascript
// Step 1: Add deprecation header (warn for 2 releases)
router.get('/api/v1/old-endpoint', (req, res) => {
  res.set('Deprecation', 'true');
  res.set('Sunset', 'Sat, 01 Jan 2027 00:00:00 GMT');
  res.set('Link', '</api/v2/new-endpoint>; rel="successor-version"');
  // ... still works, but clients are warned
});

// Step 2: Return 301 redirect (next release)
router.get('/api/v1/old-endpoint', (req, res) => {
  res.redirect(301, '/api/v2/new-endpoint');
});

// Step 3: Return 410 Gone (final removal)
router.get('/api/v1/old-endpoint', (req, res) => {
  res.status(410).json({ detail: 'This endpoint has been removed. Use /api/v2/new-endpoint' });
});
```

---

## 4. Database Compatibility

### 4.1 Safe Schema Changes (Backward Compatible)

| Change | SQL | Why Safe |
|--------|-----|----------|
| Add nullable column | `ALTER TABLE users ADD COLUMN phone TEXT` | Old rows get NULL |
| Add column with default | `ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT true` | Old rows get default |
| Add new table | `CREATE TABLE reports (...)` | Old code never queries it |
| Add index | `CREATE INDEX idx_users_email ON users(email)` | Transparent to code |
| Widen column | `ALTER TABLE users ALTER COLUMN name TYPE TEXT` | Old data still fits |

### 4.2 Breaking Schema Changes (Need Migration Strategy)

| Change | Impact | Safe Migration |
|--------|--------|----------------|
| Remove column | Old code reads it | 1) Stop reading 2) Deploy 3) Drop column |
| Rename column | Old code uses old name | 1) Add new 2) Copy data 3) Update code 4) Drop old |
| Change type | Old data may not convert | 1) Add new column 2) Migrate data 3) Switch code 4) Drop old |
| Add NOT NULL | Old rows may have NULL | 1) Backfill data 2) Then add constraint |
| Remove table | Old code queries it | 1) Remove all references 2) Deploy 3) Drop table |

### 4.3 Migration Strategy

```
EXPAND → MIGRATE → CONTRACT

Step 1: EXPAND (backward compatible)
  - Add new column/table
  - Keep old structure intact
  - Deploy code that writes to BOTH old and new

Step 2: MIGRATE (data)
  - Backfill new column from old data
  - Verify data integrity
  - Deploy code that reads from NEW

Step 3: CONTRACT (cleanup)
  - Remove old column/table
  - Deploy code that only uses new structure
```

```sql
-- Migration 005: Rename user_name to username (3-step)

-- Step 1: EXPAND
ALTER TABLE users ADD COLUMN username TEXT;
UPDATE users SET username = user_name;

-- Step 2: Code deploys reading from username, writing to both
-- (wait for all old code to drain)

-- Step 3: CONTRACT (separate migration, later)
ALTER TABLE users DROP COLUMN user_name;
```

---

## 5. React Component Compatibility

### 5.1 Props Backward Compatibility

```jsx
// GOOD — backward compatible prop change
// Old usage: <Button label="Click" />
// New usage: <Button label="Click" variant="primary" />

function Button({ label, onClick, variant = 'secondary', size = 'md', ...rest }) {
  // Default values keep old usage working
  // ...rest passes unknown props through (forward compatible)
  return (
    <button
      className={`btn btn-${variant} btn-${size}`}
      onClick={onClick}
      {...rest}
    >
      {label}
    </button>
  );
}

// BAD — breaking change
// Renaming `label` to `text` breaks all existing usage
function Button({ text, onClick }) { ... }
```

### 5.2 Props Deprecation Pattern

```jsx
function Button({ label, text, onClick, ...rest }) {
  // Support both old and new prop names
  const displayText = text || label;

  if (label && process.env.NODE_ENV === 'development') {
    // Warn developers to migrate
    // eslint-disable-next-line no-console
    console.warn('Button: "label" prop is deprecated. Use "text" instead.');
  }

  return <button onClick={onClick} {...rest}>{displayText}</button>;
}
```

### 5.3 Children/Render Props Compatibility

```jsx
// GOOD — supports both old and new patterns
function Card({ children, title, renderHeader }) {
  return (
    <div className="card">
      {/* New way: custom header renderer */}
      {renderHeader ? renderHeader(title) : (
        /* Old way: simple title */
        <div className="card-header"><h3>{title}</h3></div>
      )}
      {children}
    </div>
  );
}
```

---

## 6. State & Data Compatibility

### 6.1 localStorage / sessionStorage

```javascript
// GOOD — versioned storage with migration
const STORAGE_VERSION = 2;

function loadState() {
  const raw = localStorage.getItem('appState');
  if (!raw) return getDefaultState();

  const saved = JSON.parse(raw);

  // Migrate old versions
  if (!saved._version || saved._version < 2) {
    // v1 had `userName`, v2 uses `user.name`
    return {
      ...getDefaultState(),
      user: { name: saved.userName || '' },
      _version: STORAGE_VERSION,
    };
  }

  return saved;
}

function saveState(state) {
  localStorage.setItem('appState', JSON.stringify({
    ...state,
    _version: STORAGE_VERSION,
  }));
}
```

### 6.2 JSON Response Handling (Forward Compatible)

```javascript
// GOOD — ignore unknown fields, use defaults for missing
function parseUserResponse(data) {
  return {
    id: data.id,
    name: data.name || data.userName || 'Unknown', // fallback chain
    email: data.email || '',
    // Ignore any extra fields server sends (forward compatible)
    // Don't destructure with exact shape — use property access
  };
}

// BAD — breaks if server adds/removes fields
const { id, name, email } = await response.json();
// If server adds `phone`, no problem (ignored)
// If server removes `name`, this silently returns undefined
```

---

## 7. CSS Compatibility

### 7.1 Browser Compatibility

```css
/* GOOD — progressive enhancement */
.card {
  display: flex;                    /* Works in all modern browsers */
  gap: 1rem;                       /* Chrome 84+, Firefox 63+, Safari 14.1+ */
}

/* Fallback for older browsers */
@supports not (gap: 1rem) {
  .card > * + * {
    margin-left: 1rem;
  }
}

/* GOOD — CSS variable with fallback */
.text {
  color: var(--text-primary, #1e1e1e);  /* Fallback if variable undefined */
}
```

### 7.2 Responsive Compatibility

```css
/* Mobile-first (backward compatible with all screens) */
.grid {
  display: grid;
  grid-template-columns: 1fr;        /* Mobile default */
}

@media (min-width: 768px) {
  .grid { grid-template-columns: 1fr 1fr; }      /* Tablet */
}

@media (min-width: 1024px) {
  .grid { grid-template-columns: 1fr 1fr 1fr; }  /* Desktop */
}
```

---

## 8. Deployment Compatibility

### 8.1 Blue-Green Deployment

```
             Load Balancer
              /        \
     ┌───────────┐  ┌───────────┐
     │  BLUE     │  │  GREEN    │
     │  (v1.0)   │  │  (v2.0)   │
     │  CURRENT  │  │  NEW      │
     └───────────┘  └───────────┘

Step 1: Deploy v2.0 to GREEN (not serving traffic)
Step 2: Run smoke tests on GREEN
Step 3: Switch load balancer to GREEN
Step 4: Monitor for errors
Step 5: If errors → switch back to BLUE (instant rollback)
Step 6: If stable → decommission BLUE
```

### 8.2 Rolling Deployment

```
Instance 1: v1.0 ──► v2.0 ──► serving
Instance 2: v1.0 ──────────── v2.0 ──► serving
Instance 3: v1.0 ──────────────────── v2.0 ──► serving

During rollout: BOTH v1.0 and v2.0 serve traffic simultaneously
→ API must be backward compatible!
→ DB schema must work with both versions!
```

### 8.3 Feature Flags

```javascript
// GOOD — deploy code but don't activate until ready
const FEATURES = {
  NEW_DASHBOARD: process.env.REACT_APP_FEATURE_DASHBOARD === 'true',
  V2_API: process.env.REACT_APP_FEATURE_V2_API === 'true',
};

function Dashboard() {
  if (FEATURES.NEW_DASHBOARD) {
    return <NewDashboard />;
  }
  return <LegacyDashboard />; // Old version stays until flag is on
}
```

---

## 9. Package/Dependency Compatibility

### 9.1 Semantic Versioning Rules

```
MAJOR.MINOR.PATCH
  3  .  2  .  1

MAJOR: Breaking changes      → consumers MUST update code
MINOR: New features          → backward compatible
PATCH: Bug fixes             → backward compatible

package.json ranges:
  "react": "^19.2.5"    → accepts 19.x.x (minor + patch)
  "react": "~19.2.5"    → accepts 19.2.x (patch only)
  "react": "19.2.5"     → exact version (most safe)
```

### 9.2 Safe Dependency Updates

```bash
# Check for outdated packages
npm outdated

# Update patch versions only (safest)
npm update

# Check for vulnerabilities
npm audit

# Lock exact versions for production
npm ci  # Uses package-lock.json exactly
```

---

## 10. Compatibility Checklist (Run Before Every Release)

### Code Changes
- [ ] New API fields have default values (old clients work)
- [ ] Removed fields have deprecation period (not instant delete)
- [ ] Renamed fields support both names during transition
- [ ] New component props have defaults (old usage works)
- [ ] Deprecated props show console.warn in dev mode

### Database Changes
- [ ] New columns are nullable OR have defaults
- [ ] No column removals without code-first removal
- [ ] Migrations use EXPAND → MIGRATE → CONTRACT pattern
- [ ] Rollback migration exists for every forward migration

### API Changes
- [ ] Version bumped if breaking change (v1 → v2)
- [ ] Old version still works (sunset date set)
- [ ] New fields are optional with defaults
- [ ] Response shape additions only (no removals)
- [ ] Error codes stable (no renaming)

### Frontend Changes
- [ ] CSS uses variables with fallbacks: `var(--x, fallback)`
- [ ] Responsive: mobile-first with progressive enhancement
- [ ] localStorage has version + migration logic
- [ ] Feature flags for risky changes (deploy dark, activate later)

### Deployment
- [ ] Both old and new code can run simultaneously (rolling deploy)
- [ ] Database schema works with both old and new code
- [ ] Rollback plan documented and tested
- [ ] Health check endpoint exists (`/api/health`)
- [ ] Feature flags can disable new features without redeploy

---

## 11. Anti-Patterns to Avoid

| Anti-Pattern | Problem | Correct Approach |
|-------------|---------|-----------------|
| Big bang migration | All-or-nothing, no rollback | Incremental EXPAND/MIGRATE/CONTRACT |
| Removing API fields | Breaks all clients instantly | Deprecate → sunset → remove |
| Changing response types | `number` to `string` breaks parsers | Version the API (v1/v2) |
| Force-updating localStorage | Loses user data | Version + migrate |
| Requiring new props | Old component usage breaks | Default values on all new props |
| Dropping DB columns first | Old code crashes | Remove code first, then column |
| Changing env var names | Deployment fails | Support both names during transition |
| Renaming CSS classes | All templates break | Add new class, deprecate old |
