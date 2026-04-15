# Debugging Plugins & Tools — Mandatory for Every Project

> Complete toolkit for F12 console, DOM, CSS, HTML, routing, port, network, and performance debugging

---

## 1. Installed Libraries (npm)

| Library | Version | Purpose | Category |
|---------|---------|---------|----------|
| `react-error-boundary` | 6.x | Declarative error boundaries | Render errors |
| `react-router-dom` | 7.x | Client-side routing | Routing |
| `@axe-core/react` | (dev) | Accessibility audit in console | A11y/DOM |
| `source-map-explorer` | (dev) | Bundle size analysis | Performance |
| `web-vitals` | 2.x | Core Web Vitals (LCP, FID, CLS) | Performance |
| `@playwright/test` | 1.59 | E2E browser testing | Testing |

---

## 2. Built-in ErrorTracker (src/utils/errorTracker.js)

### What It Tracks Automatically

| Category | What | How |
|----------|------|-----|
| **Console errors** | All `console.error()` calls | Monkey-patches console.error |
| **Console warnings** | All `console.warn()` calls | Monkey-patches console.warn |
| **Unhandled errors** | Uncaught exceptions | `window.addEventListener('error')` |
| **Promise rejections** | Unhandled async errors | `window.addEventListener('unhandledrejection')` |
| **Network errors** | Failed fetch calls (4xx, 5xx) | Monkey-patches window.fetch |
| **Port errors** | Connection refused (ECONNREFUSED) | Detects in fetch wrapper |
| **Timeout errors** | AbortError from AbortController | Detects in fetch wrapper |
| **DOM issues** | Empty links, missing alt, duplicate IDs, deep nesting | `querySelectorAll` checks |
| **CSS issues** | Missing variables, inline styles, overflow, layout shift | `getComputedStyle` checks |
| **HTML issues** | Missing viewport, charset, deprecated elements, unlabeled inputs | DOM inspection |
| **Accessibility** | Same color text/bg, non-semantic clickables | Color + element checks |
| **Performance** | Long tasks (>100ms), layout shifts (CLS > 0.1) | PerformanceObserver |
| **Routing** | 404s, route load errors | Manual `trackRouteChange()` |

### How to Use in Browser Console (F12)

```javascript
// Open F12 → Console tab, then:

// Get summary of all issues
window.__errors.getSummary()
// → { errors: 2, warnings: 5, console: 1, dom: 2, css: 1, network: 0, ... }

// Get full error report
window.__errors.getReport()
// → { timestamp, totalErrors, totalWarnings, errors: [...], warnings: [...], byType: {...} }

// Get just errors
window.__errors.getErrors()

// Get just warnings
window.__errors.getWarnings()

// Clear all tracked errors
window.__errors.clear()

// Track a routing error manually
window.__errors.trackRouteChange('/invalid-page', 'not_found')
```

---

## 3. Chrome Extensions to Install

### Essential (Install These)

| Extension | Purpose | Install |
|-----------|---------|---------|
| **React Developer Tools** | Inspect components, props, state, profiling | Chrome Web Store |
| **Redux DevTools** | State debugging (if using Redux) | Chrome Web Store |
| **axe DevTools** | Accessibility testing (WCAG compliance) | Chrome Web Store |
| **Lighthouse** | Performance, a11y, SEO audit | Built into Chrome DevTools |
| **CSS Peeper** | Inspect CSS styles visually | Chrome Web Store |
| **Pesticide** | Outline all CSS boxes (debug layout) | Chrome Web Store |
| **WAVE** | Web accessibility evaluation | Chrome Web Store |

### Optional but Useful

| Extension | Purpose |
|-----------|---------|
| **Wappalyzer** | Detect tech stack of any website |
| **JSON Viewer** | Pretty-print JSON responses |
| **ColorZilla** | Pick colors from any webpage |
| **Responsive Viewer** | View multiple screen sizes at once |
| **Performance Monitor** | Real-time CPU, memory, FPS overlay |
| **Web Vitals** | Show CWV metrics in real-time |

---

## 4. F12 Debugging Cheat Sheet

### Console Tab

```javascript
// ─── Quick Checks ─────────────────────
// Check for errors on page
window.__errors.getSummary()

// Find all event listeners on an element
getEventListeners(document.querySelector('.sidebar'))

// Monitor all events on an element
monitorEvents(document.querySelector('.btn-primary'), 'click')

// Copy any object to clipboard
copy(window.__errors.getReport())

// Time a function
console.time('render'); /* code */ console.timeEnd('render');

// Group related logs
console.group('API calls'); /* logs */ console.groupEnd();

// Table display for arrays
console.table(window.__errors.getErrors());
```

### Elements Tab

```
Right-click element → Inspect
  → Styles panel: See applied CSS, overrides (strikethrough)
  → Computed panel: Final computed values
  → Layout panel: Box model (margin, padding, border)
  → Event Listeners: All attached handlers
  → Break on: subtree, attribute, node removal
```

### Network Tab

```
Filter buttons: XHR, JS, CSS, Img, Media, Font, Doc, WS
  → Red = failed request (check status code)
  → Slow = check Timing tab for bottleneck
  → Headers tab: request/response headers
  → Response tab: raw response body
  → Right-click → Copy as cURL (reproduce in terminal)
```

### Sources Tab

```
Breakpoints:
  → Click line number to add breakpoint
  → Conditional: right-click → "Add conditional breakpoint"
  → Logpoint: right-click → "Add logpoint" (non-blocking console.log)
  → XHR breakpoint: pause on specific URL pattern
  → Event listener breakpoint: pause on click, submit, etc.

Call Stack:
  → Shows function chain that led to current point
  → Click to jump to any frame

Scope:
  → Local, closure, and global variables at current breakpoint
```

### Performance Tab

```
1. Click Record
2. Interact with app (navigate, click, scroll)
3. Click Stop
4. Analyze:
   → Flame chart: tall bars = expensive functions
   → Summary: scripting vs rendering vs painting
   → Bottom-up: most expensive functions first
   → Call tree: top-down execution flow
```

### Memory Tab

```
Heap Snapshot:
  1. Take snapshot
  2. Interact with app
  3. Take another snapshot
  4. Compare: select "Objects allocated between Snapshot 1 and Snapshot 2"
  5. Look for: Detached DOM trees, growing arrays, event listeners

Allocation Timeline:
  1. Start recording
  2. Interact with app
  3. Stop
  4. Blue bars = allocated, gray = freed
  5. Persistent blue = potential memory leak
```

---

## 5. Issue-Specific Debugging

### Port Issues

```bash
# Check what's running on a port
lsof -i :3000
netstat -tulnp | grep 3000

# Kill process on port
kill -9 $(lsof -t -i:3000)

# Use different port
PORT=3001 npm start

# In ErrorTracker: port errors auto-detected via fetch wrapper
```

### CSS Issues

```javascript
// In F12 Console:

// Check if CSS variable is defined
getComputedStyle(document.documentElement).getPropertyValue('--primary')

// Find all elements with overflow
[...document.querySelectorAll('*')].filter(el =>
  el.scrollWidth > el.clientWidth || el.scrollHeight > el.clientHeight
)

// Check z-index stack
[...document.querySelectorAll('*')].map(el => ({
  tag: el.tagName,
  class: el.className,
  zIndex: getComputedStyle(el).zIndex
})).filter(x => x.zIndex !== 'auto').sort((a,b) => b.zIndex - a.zIndex)

// Find elements causing horizontal scroll
[...document.querySelectorAll('*')].filter(el =>
  el.getBoundingClientRect().right > document.documentElement.clientWidth
)
```

### HTML/DOM Issues

```javascript
// Find duplicate IDs
[...document.querySelectorAll('[id]')].map(e => e.id)
  .filter((id, i, arr) => arr.indexOf(id) !== i)

// Find images without alt
document.querySelectorAll('img:not([alt])')

// Find empty links
document.querySelectorAll('a:not([href]), a[href=""], a[href="#"]')

// Find deeply nested elements
function findDeep(el, depth=0, max=15) {
  if (depth > max) return [{el, depth}];
  return [...el.children].flatMap(c => findDeep(c, depth+1, max));
}
findDeep(document.body)
```

### Routing Issues

```javascript
// Check current route (if using React Router)
window.location.pathname

// Track 404 manually
window.__errors.trackRouteChange('/bad-page', 'not_found')

// Check all registered routes (React Router v7)
// In React DevTools → find Router component → inspect routes prop
```

### Network/API Issues

```javascript
// Test API endpoint
fetch('/api/health').then(r => r.json()).then(console.log).catch(console.error)

// Check CORS
fetch('http://other-domain.com/api').catch(e => console.error('CORS:', e))

// Monitor all network requests in console
// Network tab → check "Preserve log" → filter by XHR
```

---

## 6. npm Scripts for Debugging

```json
{
  "lint": "eslint src/ --max-warnings=0",
  "lint:fix": "eslint src/ --fix",
  "format:check": "prettier --check \"src/**/*.{js,jsx,css,json,md}\"",
  "test": "react-scripts test",
  "test:coverage": "react-scripts test --watchAll=false --coverage",
  "test:report": "react-scripts test --watchAll=false --verbose 2>&1 | tee data/test-report-$(date).txt",
  "test:e2e": "npx playwright test",
  "analyze": "source-map-explorer 'build/static/js/*.js'",
  "validate": "npm run lint && npm run format:check && npm test -- --watchAll=false"
}
```

---

## 7. Error Categories Quick Reference

| Error Type | Where to Check | Tool |
|-----------|---------------|------|
| **Console error** | F12 → Console (red) | ErrorTracker auto-captures |
| **Render crash** | White screen | ErrorBoundary shows fallback |
| **CSS broken** | F12 → Elements → Styles | ErrorTracker checks variables |
| **DOM issue** | F12 → Elements | ErrorTracker checks IDs, nesting |
| **HTML invalid** | F12 → Console (warnings) | ErrorTracker checks meta, forms |
| **Route 404** | URL bar + console | ErrorTracker.trackRouteChange() |
| **Port conflict** | Terminal + F12 → Console | ErrorTracker detects ECONNREFUSED |
| **API failure** | F12 → Network (red) | ErrorTracker captures status |
| **Timeout** | F12 → Network (pending) | AbortController in api.js |
| **Memory leak** | F12 → Memory tab | Heap snapshots comparison |
| **Performance** | F12 → Performance tab | ErrorTracker tracks long tasks |
| **Accessibility** | axe DevTools extension | ErrorTracker basic a11y check |
| **Layout shift** | Lighthouse → CLS | PerformanceObserver in ErrorTracker |
| **Bundle size** | `npm run analyze` | source-map-explorer |
