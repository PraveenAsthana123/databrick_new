# Architecture Templates — Tech Lead Toolkit

> Use these templates with Claude Code. Just say: "Generate HLD for [feature]" or "Create C4 diagram for [system]"

---

## 1. High-Level Design (HLD) Template

```markdown
# HLD — [System/Feature Name]

## 1. Overview
- **Purpose**: What problem does this solve?
- **Scope**: What's in/out of scope?
- **Stakeholders**: Who are the users and teams involved?

## 2. Architecture Diagram (C4 Context Level)
[Mermaid diagram here — system context]

## 3. Key Components
| Component | Responsibility | Technology |
|-----------|---------------|------------|
| Frontend  | User interface | React      |
| API Layer | Business logic | Node/FastAPI |
| Database  | Data storage   | PostgreSQL |

## 4. Data Flow
[Sequence diagram — main user journey]

## 5. Non-Functional Requirements
| NFR | Target | Measurement |
|-----|--------|-------------|
| Availability | 99.9% | Uptime monitoring |
| Latency | <200ms p95 | APM tool |
| Throughput | 1000 RPS | Load test |
| Security | OWASP Top 10 | Security scan |

## 6. Technology Choices
| Decision | Choice | Alternatives Considered | Why |
|----------|--------|------------------------|-----|

## 7. Risks & Mitigations
| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|

## 8. Dependencies
- External APIs
- Third-party services
- Shared libraries
```

---

## 2. Low-Level Design (LLD) Template

```markdown
# LLD — [Component/Module Name]

## 1. Component Overview
- **Parent HLD**: Link to HLD document
- **Scope**: What this component handles

## 2. Class/Module Diagram
[Mermaid class diagram]

## 3. API Contracts
### Endpoint: POST /api/v1/resource
**Request**:
- Body: { field: type, ... }
- Headers: Authorization, Content-Type

**Response**:
- 200: { data: {...} }
- 400: { detail: "...", error_code: "VALIDATION" }
- 500: { detail: "...", error_code: "INTERNAL" }

## 4. Database Schema
[ERD diagram or table definitions]

## 5. State Machine (if applicable)
[State diagram for complex workflows]

## 6. Error Handling
| Error Scenario | Handler | Response | Recovery |
|---------------|---------|----------|----------|

## 7. Security Considerations
- Input validation rules
- Authentication/authorization
- Data encryption requirements

## 8. Testing Strategy
| Test Type | Coverage Target | Key Scenarios |
|-----------|----------------|---------------|
| Unit      | 80%            | Business logic |
| Integration | Key paths    | API contracts  |
| E2E       | Happy paths    | User journeys  |
```

---

## 3. Software Architecture Document (SAD) Template

```markdown
# SAD — [Project Name]

## 1. Executive Summary
One paragraph: what, why, for whom.

## 2. Architectural Goals & Constraints
### Goals
- Scalability, maintainability, security, performance

### Constraints
- Budget, timeline, team size, technology mandates

## 3. System Context (C4 Level 1)
[C4 Context diagram — system + external actors]

## 4. Container View (C4 Level 2)
[C4 Container diagram — apps, databases, message queues]

## 5. Component View (C4 Level 3)
[C4 Component diagram — internal structure of each container]

## 6. Deployment View
[Deployment diagram — servers, cloud services, networks]

## 7. Cross-Cutting Concerns
| Concern | Approach |
|---------|----------|
| Logging | Structured JSON, correlation IDs |
| Auth | JWT / API Keys |
| Monitoring | Prometheus + Grafana |
| CI/CD | GitHub Actions |
| Error Handling | Domain exceptions + error envelope |

## 8. Architecture Decision Records (ADR)
### ADR-001: [Decision Title]
- **Status**: Accepted
- **Context**: Why this decision was needed
- **Decision**: What was decided
- **Consequences**: Trade-offs accepted

## 9. Quality Attributes
[Table of NFRs with measurable targets]

## 10. Glossary
[Domain terms and definitions]
```

---

## 4. C4 Model Diagrams (Mermaid)

### Level 1 — System Context
```mermaid
C4Context
  title System Context Diagram
  Person(user, "User", "End user of the application")
  System(app, "Application", "Main application system")
  System_Ext(api, "External API", "Third-party service")
  System_Ext(db, "Database", "Data storage")
  Rel(user, app, "Uses", "HTTPS")
  Rel(app, api, "Calls", "REST/JSON")
  Rel(app, db, "Reads/Writes", "SQL")
```

### Level 2 — Container
```mermaid
C4Container
  title Container Diagram
  Person(user, "User")
  Container(spa, "SPA", "React", "Single page application")
  Container(api, "API", "Node.js/FastAPI", "Business logic")
  ContainerDb(db, "Database", "PostgreSQL", "Stores data")
  Rel(user, spa, "Uses", "HTTPS")
  Rel(spa, api, "API calls", "REST/JSON")
  Rel(api, db, "Reads/Writes", "SQL")
```

### Level 3 — Component
```mermaid
C4Component
  title Component Diagram - API
  Component(router, "Router", "Express/FastAPI", "HTTP routing")
  Component(service, "Service", "Business Logic", "Domain rules")
  Component(repo, "Repository", "Data Access", "SQL queries")
  ComponentDb(db, "Database", "PostgreSQL")
  Rel(router, service, "Calls")
  Rel(service, repo, "Uses")
  Rel(repo, db, "Queries")
```

---

## 5. Flowchart Templates

### User Journey
```mermaid
flowchart TD
  A[User visits page] --> B{Authenticated?}
  B -->|Yes| C[Show dashboard]
  B -->|No| D[Show login]
  D --> E[Enter credentials]
  E --> F{Valid?}
  F -->|Yes| C
  F -->|No| G[Show error]
  G --> D
```

### Data Pipeline
```mermaid
flowchart LR
  A[Source] --> B[Ingest]
  B --> C[Transform]
  C --> D[Validate]
  D --> E{Quality OK?}
  E -->|Yes| F[Load]
  E -->|No| G[Quarantine]
  F --> H[Serve]
```

---

## 6. Sequence Diagram Templates

### API Request Flow
```mermaid
sequenceDiagram
  actor User
  participant UI as React App
  participant API as API Server
  participant DB as Database

  User->>UI: Click action
  UI->>API: POST /api/resource
  API->>API: Validate input
  API->>DB: INSERT INTO table
  DB-->>API: Success
  API-->>UI: 201 Created
  UI-->>User: Show confirmation
```

### Error Handling Flow
```mermaid
sequenceDiagram
  actor User
  participant UI as React App
  participant API as API Server
  participant Log as Logger

  User->>UI: Submit form
  UI->>API: POST /api/data
  API->>API: Validate
  Note over API: Validation fails
  API->>Log: Log warning + correlation_id
  API-->>UI: 400 {detail, error_code, correlation_id}
  UI-->>User: Show error message
```

---

## 7. How to Use with Claude Code

| What You Want | What to Say |
|---------------|-------------|
| Full HLD | "Generate an HLD for [feature/system]" |
| Low-level design | "Create an LLD for the [component] module" |
| SAD document | "Write a Software Architecture Document for this project" |
| C4 Context diagram | "Create a C4 context diagram for this system" |
| C4 Container diagram | "Create a C4 container diagram showing all services" |
| Sequence diagram | "Draw a sequence diagram for the [user action] flow" |
| Flowchart | "Create a flowchart for the [process/workflow]" |
| Architecture review | "Review this architecture from a tech lead perspective" |
| ADR | "Write an ADR for [decision]" |
