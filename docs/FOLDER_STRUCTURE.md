# Standard Folder Structure — All Projects

---

## React Frontend Project

```
project/
├── .github/
│   ├── workflows/
│   │   └── ci.yml                  # Lint -> Test -> Build -> E2E
│   └── pull_request_template.md    # PR checklist
├── .husky/
│   └── pre-commit                  # Lint-staged hook
├── docs/
│   ├── architecture/
│   │   ├── ARCHITECTURE_TEMPLATES.md  # HLD, LLD, SAD, C4 templates
│   │   ├── DEBUG_PERFORMANCE_GUIDE.md # Debug & performance
│   │   ├── AI_GOVERNANCE_GUIDE.md     # XAI, RAI, Interpretable AI
│   │   └── TECH_LEAD_WORKFLOW.md      # Command reference
│   ├── CODE_GUIDELINES.md          # Style rules & review process
│   ├── ERROR_HANDLING_GUIDE.md     # Port, CSS, runtime errors
│   ├── INTEGRATION_GUIDE.md        # Frontend + Backend + ORM
│   ├── TESTING_GUIDE.md            # Testing strategy
│   ├── TECHSTACK.md                # Technology inventory
│   └── FOLDER_STRUCTURE.md         # This file
├── e2e/
│   ├── app.spec.js                 # Smoke tests
│   └── [feature].spec.js           # Feature E2E tests
├── src/
│   ├── assets/                     # Images, fonts, icons
│   │   ├── images/
│   │   ├── fonts/
│   │   └── icons/
│   ├── components/
│   │   ├── common/                 # Reusable: Button, Input, Modal, Card
│   │   │   ├── Button.js
│   │   │   ├── ErrorBoundary.js
│   │   │   ├── LoadingSpinner.js
│   │   │   └── EmptyState.js
│   │   ├── layout/                 # Header, Footer, Sidebar, Layout
│   │   │   └── Sidebar.js
│   │   └── __tests__/              # Component tests
│   ├── hooks/                      # Custom React hooks
│   │   ├── useApi.js               # Data fetching hook
│   │   └── useLocalStorage.js      # Persistent state hook
│   ├── pages/                      # Route-level components
│   │   ├── Dashboard.js
│   │   ├── [Feature].js
│   │   └── __tests__/
│   ├── services/                   # API client, external services
│   │   └── api.js                  # Centralized API client
│   ├── constants/                  # App-wide constants
│   │   └── index.js
│   ├── utils/                      # Helper functions
│   │   └── formatters.js
│   ├── App.js                      # Root component + routing
│   ├── App.css                     # Global styles + CSS variables
│   ├── App.test.js                 # Root smoke test
│   ├── index.js                    # Entry point
│   └── index.css                   # CSS reset + base styles
├── .env.template                   # Environment variables template
├── .eslintignore
├── .gitignore
├── .prettierrc                     # Prettier config
├── .prettierignore
├── CLAUDE.md                       # AI assistant instructions
├── README.md                       # Project documentation
├── package.json                    # Dependencies + scripts + ESLint + lint-staged
├── playwright.config.js            # E2E test config
└── requirements.txt                # Python deps (if backend needed)
```

## Fullstack Project (React + Backend)

```
project/
├── [all frontend files above]
├── backend/
│   ├── core/
│   │   ├── config.py               # Pydantic BaseSettings
│   │   ├── exceptions.py           # AppError hierarchy
│   │   ├── error_handlers.py       # Exception -> HTTP mapping
│   │   ├── middleware.py            # CORS, security headers, rate limit
│   │   ├── auth.py                 # API key middleware
│   │   ├── logging_config.py       # JSON structured logging
│   │   └── dependencies.py         # DI factories
│   ├── models/                     # ORM models (SQLAlchemy/Prisma)
│   │   └── item.py
│   ├── repositories/               # Data access (all SQL here)
│   │   ├── base.py
│   │   └── item_repo.py
│   ├── schemas/                    # Pydantic request/response
│   │   └── item.py
│   ├── services/                   # Business logic
│   │   └── item_service.py
│   ├── routers/                    # HTTP endpoints (thin)
│   │   └── items.py
│   ├── migrations/                 # Database migrations
│   │   └── 001_initial.sql
│   ├── tests/
│   │   ├── conftest.py
│   │   └── test_items.py
│   ├── database.py                 # DB connection + migration runner
│   └── main.py                     # App entry point
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## Rules for Folder Structure

1. **Components**: Split into `common/` (reusable) and `layout/` (structural)
2. **Tests**: Co-locate in `__tests__/` folders next to source
3. **Services**: One file per external integration
4. **Pages**: One file per route
5. **Hooks**: One file per custom hook
6. **No barrel exports**: Import from specific files, not index.js re-exports
7. **Max 10 files per folder**: Split further if exceeded
8. **No nested components**: If a component gets complex, extract sub-components
