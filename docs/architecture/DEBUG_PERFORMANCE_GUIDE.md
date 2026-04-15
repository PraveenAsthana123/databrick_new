# Debug & Performance Guide — Tech Lead Toolkit

---

## Debug Workflow (Using Installed Plugins)

### Quick Reference
| Problem | Plugin/Command | What It Does |
|---------|---------------|-------------|
| Bug in code | `/superpowers:systematic-debugging` | Root cause analysis workflow |
| UI bug | `/chrome-devtools-mcp:chrome-devtools` | Inspect DOM, network, console |
| Slow page | `/chrome-devtools-mcp:debug-optimize-lcp` | LCP performance debugging |
| Code quality | `/sonarqube:analyze src/App.js` | Static analysis |
| Code review | `/coderabbit:review` | AI code review |
| Simplify code | `/simplify` | Reduce complexity |

### Systematic Debug Checklist
```
1. REPRODUCE    - Can you reproduce consistently?
2. ISOLATE      - What's the smallest failing case?
3. HYPOTHESIZE  - What could cause this? (list 3 theories)
4. VERIFY       - Test each hypothesis
5. FIX          - Implement smallest fix
6. VALIDATE     - Does the fix work? Any side effects?
7. PREVENT      - Add test to prevent regression
```

---

## Performance Optimization

### React Performance Checklist

#### Rendering Performance
- [ ] No unnecessary re-renders (use React DevTools Profiler)
- [ ] `React.memo()` on expensive pure components
- [ ] `useMemo()` for expensive calculations
- [ ] `useCallback()` for callbacks passed to children
- [ ] Virtual scrolling for lists > 100 items
- [ ] Lazy loading for routes and heavy components

#### Bundle Size
- [ ] Code splitting with `React.lazy()` + `Suspense`
- [ ] Tree shaking (no barrel imports like `import { x } from 'lodash'`)
- [ ] Dynamic imports for rarely used features
- [ ] Image optimization (WebP, lazy loading, srcset)
- [ ] No duplicate dependencies

#### Network Performance
- [ ] API response caching (SWR / React Query)
- [ ] Debounced search inputs
- [ ] Pagination for large datasets
- [ ] Prefetch critical data
- [ ] Compressed assets (gzip/brotli)

#### Measurement
```bash
# Bundle analysis
npx react-scripts build
npx source-map-explorer 'build/static/js/*.js'

# Lighthouse audit (in Chrome DevTools)
# Or use Chrome DevTools MCP plugin:
# "Run a Lighthouse audit on localhost:3000"
```

### Performance Metrics to Track
| Metric | Target | Tool |
|--------|--------|------|
| LCP (Largest Contentful Paint) | < 2.5s | Chrome DevTools / Lighthouse |
| FID (First Input Delay) | < 100ms | Chrome DevTools |
| CLS (Cumulative Layout Shift) | < 0.1 | Chrome DevTools |
| Bundle size (gzipped) | < 200KB | source-map-explorer |
| Time to Interactive | < 3.5s | Lighthouse |
| API response time (p95) | < 500ms | Network tab |

---

## Performance Anti-Patterns in React

| Anti-Pattern | Problem | Fix |
|-------------|---------|-----|
| Inline objects in JSX | Creates new ref every render | Extract to variable or useMemo |
| Inline functions in JSX | New function every render | useCallback |
| Index as key in dynamic lists | Incorrect reconciliation | Use unique ID |
| Fetching in useEffect without cleanup | Race conditions, memory leaks | AbortController or React Query |
| Large context providers | All consumers re-render | Split contexts by update frequency |
| Importing entire libraries | Large bundle | Import specific modules |

---

## How to Use with Claude Code

| What You Want | What to Say |
|---------------|-------------|
| Debug a bug | "Debug why [describe the issue]" |
| Performance audit | "Analyze performance of this React app" |
| LCP optimization | "Help me optimize LCP for this page" |
| Bundle analysis | "Analyze my bundle size and suggest optimizations" |
| Memory leak | "Help me find memory leaks in this component" |
| Re-render issues | "Why is [component] re-rendering too often?" |
