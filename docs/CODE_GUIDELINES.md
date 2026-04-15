# Code Guidelines — databrick_new

## Style Rules (Enforced Automatically)

| Rule | Tool | Enforcement |
|------|------|-------------|
| Consistent formatting | Prettier | Pre-commit hook |
| No lint errors | ESLint | Pre-commit hook |
| No `console.log` | ESLint `no-console` | Warning, block on merge |
| Use `===` not `==` | ESLint `eqeqeq` | Error |
| Use `const` over `let` | ESLint `prefer-const` | Warning |
| No unsafe code execution | ESLint `no-eval` | Error |
| React keys in lists | ESLint `react/jsx-key` | Error |

## Code Review Process

### Before Submitting a PR
```bash
npm run lint          # Check for lint errors
npm run format:check  # Check formatting
npm test -- --watchAll=false   # Run unit tests
npm run build         # Verify build succeeds
npm run test:e2e      # Run E2E tests (optional but recommended)
```

Or use the single command:
```bash
npm run pre-merge     # Runs lint + format check + test + build
```

### Review Workflow

```
1. Create branch    ->  feature/my-feature or fix/bug-name
2. Write code       ->  Follow style rules (auto-enforced)
3. Pre-commit       ->  Husky runs lint-staged automatically
4. Push and open PR ->  Use PR template checklist
5. AI Review        ->  Ask Claude: "review my code" (CodeRabbit)
6. Human Review     ->  At least 1 approval required
7. CI Passes        ->  All checks green
8. Merge            ->  Squash merge to main
```

### Branch Naming Convention
| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feature/<name>` | `feature/login-page` |
| Bug fix | `fix/<name>` | `fix/header-overflow` |
| Refactor | `refactor/<name>` | `refactor/extract-api-client` |
| Docs | `docs/<name>` | `docs/update-readme` |

### Commit Message Format
```
<type>: <short description>

Types: feat, fix, refactor, style, test, docs, chore
```
Examples:
- `feat: add user login page`
- `fix: prevent form submit on empty fields`
- `refactor: extract Header into separate component`
- `test: add E2E tests for dashboard`

## Build and Deploy Checklist

### Before Deploy
- [ ] `npm run pre-merge` passes
- [ ] No warnings in build output
- [ ] Bundle size checked (no unexpected growth)
- [ ] Environment variables documented

### Approval Criteria
A PR can be merged when:
1. All automated checks pass (lint, test, build)
2. At least 1 code review approval
3. No open review comments
4. Branch is rebased on latest main
5. PR description is filled out completely

## Using Claude Code for Reviews

| Command | What It Does |
|---------|-------------|
| "review my code" | Triggers CodeRabbit AI review |
| `/sonarqube:analyze` | Deep code quality scan |
| `/simplify` | Suggest code simplifications |
| `/coderabbit:review` | Detailed AI code review |
| "debug this" | Systematic debugging workflow |

## File Organization

```
src/
├── components/     # Reusable UI components
│   ├── common/     # Buttons, inputs, modals
│   └── layout/     # Header, Footer, Sidebar
├── pages/          # Route-level page components
├── hooks/          # Custom React hooks
├── utils/          # Helper functions
├── services/       # API calls
├── constants/      # App-wide constants
└── assets/         # Images, fonts, icons
```

## Do's and Don'ts

### Do
- Use functional components with hooks
- Extract reusable logic into custom hooks
- Handle loading, error, and empty states
- Use meaningful variable/function names
- Write tests for business logic

### Don't
- Don't use `var` — use `const` or `let`
- Don't use `==` — use `===`
- Don't leave `console.log` in code
- Don't hardcode URLs or API keys
- Don't ignore ESLint warnings
- Don't skip the PR template
