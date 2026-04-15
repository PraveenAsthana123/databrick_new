# Error Handling Guide — Mandatory for All Projects

---

## 1. Port Errors

### Common Port Issues & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `EADDRINUSE: port 3000` | Port already in use | Kill process or use different port |
| `EACCES: permission denied` | Port < 1024 needs root | Use port > 1024 |
| `ECONNREFUSED` | Server not running | Start the server first |
| `ERR_CONNECTION_RESET` | Server crashed | Check logs, restart |

### Fix: Port Already in Use
```bash
# Find what's using the port
lsof -i :3000
# or
netstat -tulnp | grep 3000

# Kill the process
kill -9 <PID>

# Or use a different port
PORT=3001 npm start
```

### Fix: Configure Default Port
```env
# .env file
PORT=3000
REACT_APP_API_URL=http://localhost:8000
```

### React Error Boundary for Network Errors
```jsx
// src/components/common/ErrorBoundary.js
import React from 'react';

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    // Log to monitoring service (not console.log in production)
    if (process.env.NODE_ENV === 'development') {
      console.error('ErrorBoundary caught:', error, errorInfo);
    }
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="error-boundary">
          <h2>Something went wrong</h2>
          <p>{this.state.error?.message}</p>
          <button onClick={() => this.setState({ hasError: false })}>
            Try Again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

export default ErrorBoundary;
```

---

## 2. CSS Errors

### Common CSS Issues & Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| Styles not applying | Specificity conflict | Use more specific selector or CSS modules |
| Layout broken on mobile | No responsive design | Add media queries |
| Flickering / FOUC | CSS loading order | Import CSS in correct order |
| Z-index war | Arbitrary z-index values | Use z-index scale |
| Overflow hidden cuts content | Container too small | Use `overflow: auto` or fix sizing |
| CSS variables not working | Browser support or typo | Check `var(--name, fallback)` |

### CSS Best Practices (Enforced)

```css
/* USE: CSS Variables for consistency */
:root {
  /* Colors */
  --color-primary: #1a73e8;
  --color-secondary: #5f6368;
  --color-error: #d93025;
  --color-success: #1e8e3e;
  --color-warning: #f9ab00;
  --color-bg: #ffffff;
  --color-surface: #f8f9fa;
  --color-text: #202124;
  --color-text-secondary: #5f6368;
  --color-border: #dadce0;

  /* Spacing (4px scale) */
  --space-xs: 4px;
  --space-sm: 8px;
  --space-md: 16px;
  --space-lg: 24px;
  --space-xl: 32px;
  --space-2xl: 48px;

  /* Typography */
  --font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  --font-size-sm: 0.875rem;
  --font-size-md: 1rem;
  --font-size-lg: 1.25rem;
  --font-size-xl: 1.5rem;

  /* Borders */
  --radius-sm: 4px;
  --radius-md: 8px;
  --radius-lg: 12px;

  /* Shadows */
  --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.1);
  --shadow-md: 0 2px 8px rgba(0, 0, 0, 0.15);
  --shadow-lg: 0 4px 16px rgba(0, 0, 0, 0.2);

  /* Z-index scale (never use arbitrary values) */
  --z-dropdown: 100;
  --z-sticky: 200;
  --z-modal-backdrop: 300;
  --z-modal: 400;
  --z-toast: 500;
  --z-tooltip: 600;

  /* Transitions */
  --transition-fast: 150ms ease;
  --transition-normal: 250ms ease;
}

/* Responsive breakpoints */
/* Mobile: default (< 768px) */
/* Tablet: @media (min-width: 768px) */
/* Desktop: @media (min-width: 1024px) */
/* Wide: @media (min-width: 1440px) */
```

### CSS Reset (Include in index.css)
```css
*,
*::before,
*::after {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

html {
  font-size: 16px;
  -webkit-font-smoothing: antialiased;
}

body {
  font-family: var(--font-family);
  color: var(--color-text);
  background: var(--color-bg);
  line-height: 1.5;
}

img, svg {
  max-width: 100%;
  display: block;
}

a {
  color: var(--color-primary);
  text-decoration: none;
}

button {
  cursor: pointer;
  font: inherit;
}
```

---

## 3. Runtime Error Handling

### React Error Patterns

| Pattern | When to Use |
|---------|------------|
| ErrorBoundary | Catch render errors in component tree |
| try/catch in async | API calls, data processing |
| .catch() on promises | Unhandled promise rejections |
| window.onerror | Global error handler |

### API Error Handling Pattern
```jsx
// src/services/api.js
const API_BASE = process.env.REACT_APP_API_URL || 'http://localhost:8000';

class ApiError extends Error {
  constructor(message, status, code) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

async function request(endpoint, options = {}) {
  const url = `${API_BASE}${endpoint}`;
  const timeout = options.timeout || 10000;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new ApiError(
        body.detail || `HTTP ${response.status}`,
        response.status,
        body.error_code || 'UNKNOWN'
      );
    }

    return response.json();
  } catch (error) {
    clearTimeout(timeoutId);

    if (error.name === 'AbortError') {
      throw new ApiError('Request timed out', 408, 'TIMEOUT');
    }
    if (error instanceof ApiError) throw error;
    throw new ApiError('Network error', 0, 'NETWORK');
  }
}

export { request, ApiError };
```

### Loading + Error State Pattern
```jsx
// Pattern: Every data-fetching component MUST handle 3 states
function DataComponent() {
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);

  React.useEffect(() => {
    let cancelled = false;

    async function fetchData() {
      try {
        setLoading(true);
        setError(null);
        const result = await request('/api/data');
        if (!cancelled) setData(result);
      } catch (err) {
        if (!cancelled) setError(err.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    fetchData();
    return () => { cancelled = true; };
  }, []);

  if (loading) return <LoadingSpinner />;
  if (error) return <ErrorMessage message={error} />;
  if (!data) return <EmptyState />;
  return <DataView data={data} />;
}
```

---

## 4. Build Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `Module not found` | Missing import/dependency | `npm install <package>` |
| `SyntaxError: Unexpected token` | Invalid JSX/JS | Check brackets, semicolons |
| `ReferenceError: X is not defined` | Undefined variable | Import or declare it |
| `TypeError: Cannot read property of undefined` | Null reference | Add optional chaining `?.` |
| `FATAL ERROR: heap out of memory` | Large build | `NODE_OPTIONS=--max_old_space_size=4096 npm run build` |
| `Invalid hook call` | Hook outside component | Only call hooks at top level of components |

---

## 5. Environment Errors

### .env Template (Always Include)
```env
# .env.template — copy to .env and fill values
# NEVER commit .env to git

# App
PORT=3000
REACT_APP_ENV=development

# API
REACT_APP_API_URL=http://localhost:8000

# Feature Flags
REACT_APP_ENABLE_AI=false
REACT_APP_ENABLE_DEBUG=true
```

### .gitignore Must Cover
```
.env
.env.local
.env.production
*.key
*.pem
credentials.*
secrets.*
```
