# Testing Guide — Mandatory for All Projects

---

## Testing Stack

| Layer | Tool | Config File | Command |
|-------|------|-------------|---------|
| Unit Tests | Jest + React Testing Library | package.json | `npm test` |
| E2E Tests | Playwright | playwright.config.js | `npm run test:e2e` |
| AI Code Review | CodeRabbit | (Claude plugin) | `/coderabbit:review` |
| Static Analysis | SonarQube | (Claude plugin) | `/sonarqube:analyze` |
| Lint | ESLint | package.json (eslintConfig) | `npm run lint` |
| Format | Prettier | .prettierrc | `npm run format:check` |

---

## Test Categories (ALL Required)

### P0 — Must Have Before Merge

| Category | What to Test | Example |
|----------|-------------|---------|
| Smoke | App renders without crash | `render(<App />)` |
| Component | Each component renders correctly | Props, states, events |
| User Interaction | Click, type, submit behaviors | `userEvent.click()` |
| Routing | All pages accessible | Navigate + assert content |
| Error States | Error boundaries catch errors | Throw in child + assert fallback |

### P1 — Must Have Before Release

| Category | What to Test | Example |
|----------|-------------|---------|
| E2E Happy Path | Complete user journeys | Login -> Dashboard -> Action |
| Responsive | Mobile/tablet/desktop layouts | Viewport resize tests |
| Accessibility | ARIA, keyboard navigation | `toHaveAccessibleName()` |
| API Integration | Data fetch, loading, error | Mock API + assert states |
| Edge Cases | Empty data, long text, null values | Boundary conditions |

### P2 — Nice to Have

| Category | What to Test |
|----------|-------------|
| Performance | Render time, bundle size |
| Visual Regression | Screenshot comparison |
| Cross-browser | Playwright multi-browser |
| Load Testing | Concurrent users |

---

## Writing Tests — Patterns

### Unit Test (Jest + RTL)
```jsx
// src/components/__tests__/Button.test.js
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import Button from '../Button';

describe('Button', () => {
  test('renders with label', () => {
    render(<Button label="Click me" />);
    expect(screen.getByText('Click me')).toBeInTheDocument();
  });

  test('calls onClick when clicked', async () => {
    const handleClick = jest.fn();
    render(<Button label="Click" onClick={handleClick} />);
    await userEvent.click(screen.getByText('Click'));
    expect(handleClick).toHaveBeenCalledTimes(1);
  });

  test('is disabled when disabled prop is true', () => {
    render(<Button label="Click" disabled />);
    expect(screen.getByText('Click')).toBeDisabled();
  });
});
```

### E2E Test (Playwright)
```js
// e2e/navigation.spec.js
const { test, expect } = require('@playwright/test');

test('can navigate to all main pages', async ({ page }) => {
  await page.goto('/');
  
  // Check sidebar links
  const links = page.locator('nav a');
  const count = await links.count();
  expect(count).toBeGreaterThan(0);
  
  // Click first link and verify navigation
  await links.first().click();
  await expect(page).not.toHaveURL('/');
});

test('no console errors on any page', async ({ page }) => {
  const errors = [];
  page.on('console', msg => {
    if (msg.type() === 'error') errors.push(msg.text());
  });
  await page.goto('/');
  expect(errors).toHaveLength(0);
});
```

---

## Test File Naming Convention

```
src/
  components/
    Button.js
    __tests__/
      Button.test.js        # Unit tests next to component
  pages/
    Dashboard.js
    __tests__/
      Dashboard.test.js
  utils/
    helpers.js
    __tests__/
      helpers.test.js
e2e/
  app.spec.js               # E2E smoke test
  navigation.spec.js        # E2E navigation
  [feature].spec.js         # E2E per feature
```

---

## Coverage Requirements

| Metric | Minimum | Target |
|--------|---------|--------|
| Statements | 60% | 80% |
| Branches | 50% | 70% |
| Functions | 60% | 80% |
| Lines | 60% | 80% |

Run coverage: `npm test -- --coverage --watchAll=false`
