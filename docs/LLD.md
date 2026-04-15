# Low Level Design (LLD)

## Databricks PySpark Data Automation Platform

### 1. React Application Design

#### 1.1 App.js - Main Router
```
App.js
  - useState: activePage, sidebarCollapsed
  - Lazy imports: 22 page components
  - Suspense wrapper for loading state
  - Topbar: brand, cluster status, user
  - Sidebar: navigation with sections
  - Main content: renderPage() switch
```

#### 1.2 Sidebar Component
```
Sidebar.js
  Props: activePage, onNavigate, collapsed
  Sections: [
    Overview (3 items),
    Scenarios (6 items),
    Pipelines & AI (3 items),
    Governance (2 items),
    Infrastructure (4 items),
    Data & Tools (3 items),
    System (1 item)
  ]
  Total: 22 navigation items
```

#### 1.3 Scenario Page Pattern (reusable across all scenario pages)
```
ScenarioPage.js
  State:
    selectedCategory: 'All'
    expandedId: null
    searchTerm: ''
    viewMode: 'auto' | 'manual' (optional)

  Data:
    scenarios: Array<{
      id: number,
      category: string,
      title: string,
      desc: string,
      code: string,      // PySpark/SQL automation code
      manual?: string     // Manual process steps (optional)
    }>
    categories: derived unique categories

  Computed:
    filtered: scenarios filtered by category + search

  Render:
    1. Page header (title, count)
    2. Filter bar (search input, category select, count label)
    3. Scenario list (expandable cards with code blocks)
```

### 2. CSS Design System

#### 2.1 CSS Variables
```css
--sidebar-width: 240px
--sidebar-collapsed: 60px
--topbar-height: 52px
--primary: #ff3621 (Databricks red)
--bg-dark: #1b1b32
--bg-sidebar: #16162a
--bg-page: #f5f5f7
--bg-card: #ffffff
--text-primary: #1e1e1e
--border: #e5e7eb
--success: #22c55e
--warning: #f59e0b
--error: #ef4444
--info: #3b82f6
```

#### 2.2 Component Classes
```
Layout:    .app, .app-body, .main-content, .sidebar
Topbar:    .topbar, .topbar-brand, .topbar-actions
Cards:     .card, .card-header, .stat-card, .stat-icon
Tables:    .table-wrapper, thead th, tbody td
Forms:     .form-group, .form-input
Buttons:   .btn, .btn-primary, .btn-secondary, .btn-sm
Badges:    .badge.running, .badge.stopped, .badge.completed, .badge.failed
Code:      .code-block (dark theme with syntax colors)
Grid:      .grid-2, .grid-3, .stats-grid
Tabs:      .tabs, .tab, .tab.active
Progress:  .progress-bar, .progress-fill
```

### 3. Data Model Design (Databricks)

#### 3.1 Bronze Layer Tables
```sql
-- catalog.bronze.raw_orders
CREATE TABLE catalog.bronze.raw_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    amount DOUBLE,
    order_date STRING,
    status STRING,
    _ingest_timestamp TIMESTAMP,  -- metadata
    _source_file STRING           -- metadata
) USING DELTA

-- catalog.bronze.quarantine
CREATE TABLE catalog.bronze.quarantine (
    _corrupt_record STRING,
    _source_file STRING,
    _ingest_timestamp TIMESTAMP
) USING DELTA
```

#### 3.2 Silver Layer Tables
```sql
-- catalog.silver.orders (deduplicated, typed)
CREATE TABLE catalog.silver.orders (
    order_id INT NOT NULL,
    customer_id INT,
    product_id INT,
    quantity INT CHECK (quantity > 0),
    amount DECIMAL(10,2) CHECK (amount > 0),
    order_date DATE,
    status STRING,
    _processed_at TIMESTAMP
) USING DELTA

-- catalog.silver.customers_scd2 (SCD Type 2)
CREATE TABLE catalog.silver.customers_scd2 (
    customer_id INT,
    name STRING,
    email STRING,
    phone STRING,
    address STRING,
    is_current BOOLEAN,
    start_date TIMESTAMP,
    end_date TIMESTAMP
) USING DELTA
```

#### 3.3 Gold Layer Tables
```sql
-- catalog.gold.fact_sales (Star Schema)
CREATE TABLE catalog.gold.fact_sales (
    order_id INT,
    date_key INT,
    customer_key INT,
    product_key INT,
    quantity INT,
    total_amount DECIMAL(10,2),
    profit DECIMAL(10,2)
) USING DELTA
PARTITIONED BY (order_date)
CLUSTER BY (date_key, customer_key)

-- catalog.gold.daily_kpis
CREATE TABLE catalog.gold.daily_kpis (
    report_date DATE,
    total_revenue DECIMAL(15,2),
    active_customers INT,
    avg_order_value DECIMAL(10,2),
    profit_margin_pct DECIMAL(5,2)
) USING DELTA
```

### 4. Pipeline Specifications

#### 4.1 Pipeline Stages
```
Each pipeline consists of:
1. EXTRACT: Read from source (file/DB/stream/API)
2. ENRICH: Add metadata (_ingest_timestamp, _source)
3. VALIDATE: Schema checks, data quality
4. TRANSFORM: Clean, deduplicate, type cast
5. MERGE: Upsert to target (MERGE INTO)
6. AUDIT: Log operation to audit table
7. ALERT: Notify on failure
```

#### 4.2 Error Handling Flow
```
Try pipeline step:
  Success -> Log to audit table -> Continue
  Failure ->
    - Log error to audit table
    - Move bad records to quarantine
    - Send alert (email/Slack)
    - Mark job as FAILED
    - Do NOT silently skip
```

### 5. Security Implementation

#### 5.1 Access Control Matrix
```
Role             | Catalog | Bronze | Silver | Gold  | ML Models
-----------------+---------+--------+--------+-------+----------
Data Engineer    | USAGE   | ALL    | ALL    | MODIFY| USAGE
Data Scientist   | USAGE   | SELECT | SELECT | ALL   | ALL
Data Analyst     | USAGE   | -      | SELECT | SELECT| USAGE
Admin            | ALL     | ALL    | ALL    | ALL   | ALL
Service Principal| USAGE   | MODIFY | MODIFY | MODIFY| MODIFY
```

#### 5.2 PII Protection
```
Column Masking:
  email  -> mask_email()  -> "ab***@***.com"
  phone  -> mask_phone()  -> "***-***-1234"
  ssn    -> mask_ssn()    -> "***-**-5678"

Row-Level Security:
  region_filter(region) -> IS_MEMBER check -> per-team access
```

### 6. File Organization

```
src/
  components/
    Sidebar.js           # 110 lines - Navigation component
  pages/
    Dashboard.js         # 100 lines - Overview
    Ingestion.js         # 800+ lines - 55 scenarios
    Modeling.js          # 850+ lines - 55 scenarios
    UnityCatalog.js      # 800+ lines - 55 scenarios
    Visualization.js     # 800+ lines - 55 scenarios
    ELTOperations.js     # 2500+ lines - 48 scenarios (5 tabs)
    DataTesting.js       # 2500+ lines - 55 scenarios
    SecurityGovernance.js# 2500+ lines - 55 scenarios (3 tabs)
    XAI.js               # 2200+ lines - 46 scenarios (3 tabs)
    RAGIntegration.js    # 2400+ lines - 36 scenarios (4 tabs)
    TerraformAzure.js    # 2100+ lines - 34 scenarios (4 tabs)
    PipelineBuilder.js   # 550+ lines - 20 pipelines
    Medallion.js         # 230+ lines - 15 examples (3 tabs)
    LandingZone.js       # 700+ lines - 10 patterns
    DataStorage.js       # 600+ lines - 8 examples
    SimulationTools.js   # 400+ lines - 10 simulations
    DownloadData.js      # 250+ lines - 6 generators
    Clusters.js          # 120 lines
    Notebooks.js         # 80 lines
    Jobs.js              # 70 lines
    SparkUI.js           # 90 lines
    Settings.js          # 80 lines
  App.js                 # 70 lines - Router
  App.css                # 450 lines - Design system
```
