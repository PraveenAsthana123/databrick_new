/**
 * Sample Datasets — Before/After processing for each pipeline category
 * Each dataset has: before (raw/source), after (processed/target), stats
 */

const SAMPLE_DATA = {
  // ─── Ingestion Scenarios ────────────────────
  ingestion_csv: {
    label: 'CSV Ingestion',
    before: {
      title: 'Raw CSV (Landing Zone)',
      path: '/mnt/landing/csv/orders_2024.csv',
      format: 'CSV',
      rows: [
        {
          id: '1',
          name: ' Alice ',
          email: 'ALICE@TEST.COM',
          amount: '150.50',
          date: '2024-01-15',
          status: 'active',
        },
        {
          id: '2',
          name: 'Bob',
          email: 'bob@test.com',
          amount: '200',
          date: '01/20/2024',
          status: 'active',
        },
        {
          id: '',
          name: 'Charlie',
          email: 'charlie@test.com',
          amount: '-50',
          date: '2024-02-01',
          status: '',
        },
        {
          id: '2',
          name: 'Bob',
          email: 'bob@test.com',
          amount: '200',
          date: '01/20/2024',
          status: 'active',
        },
        {
          id: '4',
          name: null,
          email: 'diana@test.com',
          amount: '320.25',
          date: '2024-03-10',
          status: 'paused',
        },
        {
          id: '5',
          name: 'Eve  ',
          email: 'eve@test',
          amount: 'abc',
          date: 'invalid',
          status: 'active',
        },
      ],
      stats: { rows: 10243, columns: 6, nulls: 312, duplicates: 145, size: '2.4 MB' },
    },
    after: {
      title: 'Clean Silver Table',
      path: 'catalog.silver.clean_orders',
      format: 'Delta',
      rows: [
        {
          id: 1,
          name: 'Alice',
          email: 'alice@test.com',
          amount: 150.5,
          date: '2024-01-15',
          status: 'active',
          _ingest_ts: '2024-01-16T00:00:00Z',
        },
        {
          id: 2,
          name: 'Bob',
          email: 'bob@test.com',
          amount: 200.0,
          date: '2024-01-20',
          status: 'active',
          _ingest_ts: '2024-01-16T00:00:00Z',
        },
        {
          id: 4,
          name: 'UNKNOWN',
          email: 'diana@test.com',
          amount: 320.25,
          date: '2024-03-10',
          status: 'paused',
          _ingest_ts: '2024-01-16T00:00:00Z',
        },
      ],
      stats: { rows: 9756, columns: 7, nulls: 0, duplicates: 0, size: '1.8 MB' },
    },
    changes: [
      { type: 'removed', desc: 'Removed 145 duplicate rows (id=2 duplicate)' },
      { type: 'removed', desc: 'Removed 342 rows with empty id' },
      { type: 'fixed', desc: 'Trimmed whitespace in name column' },
      { type: 'fixed', desc: 'Lowercased all email addresses' },
      { type: 'fixed', desc: 'Standardized date format to ISO-8601' },
      { type: 'fixed', desc: 'Filled null names with "UNKNOWN"' },
      { type: 'removed', desc: 'Filtered invalid amounts (non-numeric, negative)' },
      { type: 'added', desc: 'Added _ingest_ts metadata column' },
    ],
  },

  ingestion_api: {
    label: 'API Ingestion',
    before: {
      title: 'Raw API Response (JSON)',
      path: 'https://api.example.com/v1/orders',
      format: 'JSON',
      rows: [
        {
          order_id: 'ORD-001',
          customer: 'Acme Corp',
          amount: 5000,
          currency: 'USD',
          created: '2024-01-15T10:30:00Z',
          items: '[{...}]',
        },
        {
          order_id: 'ORD-002',
          customer: 'Beta Inc',
          amount: 0,
          currency: 'USD',
          created: '2024-01-16T14:20:00Z',
          items: '[]',
        },
        {
          order_id: 'ORD-003',
          customer: null,
          amount: 1500,
          currency: 'EUR',
          created: '2024-01-17',
          items: '[{...}]',
        },
        {
          order_id: 'ORD-001',
          customer: 'Acme Corp',
          amount: 5000,
          currency: 'USD',
          created: '2024-01-15T10:30:00Z',
          items: '[{...}]',
        },
      ],
      stats: { rows: 5432, columns: 6, nulls: 89, duplicates: 67, size: '3.1 MB' },
    },
    after: {
      title: 'Gold Aggregated Orders',
      path: 'catalog.gold.daily_orders',
      format: 'Delta',
      rows: [
        {
          order_date: '2024-01-15',
          order_count: 142,
          revenue: 425000.0,
          avg_order: 2992.96,
          currency: 'USD',
        },
        {
          order_date: '2024-01-16',
          order_count: 98,
          revenue: 310500.0,
          avg_order: 3168.37,
          currency: 'USD',
        },
        {
          order_date: '2024-01-17',
          order_count: 156,
          revenue: 502000.0,
          avg_order: 3217.95,
          currency: 'USD',
        },
      ],
      stats: { rows: 365, columns: 5, nulls: 0, duplicates: 0, size: '0.1 MB' },
    },
    changes: [
      { type: 'removed', desc: 'Removed 67 duplicate orders (order_id dedup)' },
      { type: 'removed', desc: 'Filtered 23 zero-amount orders' },
      { type: 'fixed', desc: 'Filled null customer with "Unknown"' },
      { type: 'transformed', desc: 'Aggregated by order_date: COUNT, SUM, AVG' },
      { type: 'transformed', desc: 'Flattened from 5432 rows to 365 daily summaries' },
    ],
  },

  ingestion_kafka: {
    label: 'Kafka Streaming',
    before: {
      title: 'Raw Kafka Messages',
      path: 'kafka://broker:9092/events',
      format: 'Avro/JSON',
      rows: [
        {
          offset: 1001,
          key: 'user_123',
          value: '{"event":"click","page":"/home","ts":1705334400}',
          partition: 0,
          timestamp: '2024-01-15T12:00:00Z',
        },
        {
          offset: 1002,
          key: 'user_456',
          value: '{"event":"purchase","page":"/cart","ts":1705334460}',
          partition: 1,
          timestamp: '2024-01-15T12:01:00Z',
        },
        {
          offset: 1003,
          key: null,
          value: '{"event":"view","page":"/products"}',
          partition: 0,
          timestamp: '2024-01-15T12:01:30Z',
        },
        {
          offset: 1004,
          key: 'user_123',
          value: 'MALFORMED_JSON{',
          partition: 0,
          timestamp: '2024-01-15T12:02:00Z',
        },
      ],
      stats: { rows: 1250000, columns: 5, nulls: 12000, duplicates: 0, size: 'Streaming' },
    },
    after: {
      title: 'Parsed Event Stream (Silver)',
      path: 'catalog.silver.user_events',
      format: 'Delta (Streaming)',
      rows: [
        {
          user_id: 'user_123',
          event_type: 'click',
          page: '/home',
          event_ts: '2024-01-15T12:00:00Z',
          _processed_ts: '2024-01-15T12:00:05Z',
        },
        {
          user_id: 'user_456',
          event_type: 'purchase',
          page: '/cart',
          event_ts: '2024-01-15T12:01:00Z',
          _processed_ts: '2024-01-15T12:01:03Z',
        },
      ],
      stats: { rows: 1235000, columns: 5, nulls: 0, duplicates: 0, size: '890 MB' },
    },
    changes: [
      { type: 'removed', desc: 'Dropped 12,000 messages with null keys' },
      { type: 'removed', desc: 'Dropped 3,000 malformed JSON messages' },
      { type: 'transformed', desc: 'Parsed JSON value into structured columns' },
      { type: 'added', desc: 'Added _processed_ts watermark column' },
    ],
  },

  // ─── ELT Scenarios ──────────────────────────
  elt_scd2: {
    label: 'SCD Type 2',
    before: {
      title: 'Source Customer Table',
      path: 'jdbc://postgres/customers',
      format: 'JDBC',
      rows: [
        {
          customer_id: 101,
          name: 'Alice Johnson',
          email: 'alice@new.com',
          city: 'San Francisco',
          tier: 'Gold',
        },
        {
          customer_id: 102,
          name: 'Bob Smith',
          email: 'bob@test.com',
          city: 'Chicago',
          tier: 'Silver',
        },
        {
          customer_id: 103,
          name: 'New Customer',
          email: 'new@test.com',
          city: 'Austin',
          tier: 'Bronze',
        },
      ],
      stats: { rows: 50000, columns: 5, nulls: 0, duplicates: 0, size: '12 MB' },
    },
    after: {
      title: 'SCD Type 2 History Table',
      path: 'catalog.silver.customers_history',
      format: 'Delta',
      rows: [
        {
          customer_id: 101,
          name: 'Alice Johnson',
          email: 'alice@old.com',
          city: 'New York',
          tier: 'Silver',
          is_current: false,
          valid_from: '2023-01-01',
          valid_to: '2024-01-15',
        },
        {
          customer_id: 101,
          name: 'Alice Johnson',
          email: 'alice@new.com',
          city: 'San Francisco',
          tier: 'Gold',
          is_current: true,
          valid_from: '2024-01-15',
          valid_to: '9999-12-31',
        },
        {
          customer_id: 102,
          name: 'Bob Smith',
          email: 'bob@test.com',
          city: 'Chicago',
          tier: 'Silver',
          is_current: true,
          valid_from: '2023-06-01',
          valid_to: '9999-12-31',
        },
        {
          customer_id: 103,
          name: 'New Customer',
          email: 'new@test.com',
          city: 'Austin',
          tier: 'Bronze',
          is_current: true,
          valid_from: '2024-01-16',
          valid_to: '9999-12-31',
        },
      ],
      stats: { rows: 85000, columns: 8, nulls: 0, duplicates: 0, size: '28 MB' },
    },
    changes: [
      { type: 'transformed', desc: 'Customer 101: closed old record (valid_to = 2024-01-15)' },
      {
        type: 'transformed',
        desc: 'Customer 101: inserted new record with updated email, city, tier',
      },
      { type: 'unchanged', desc: 'Customer 102: no changes detected' },
      { type: 'added', desc: 'Customer 103: new record inserted' },
      {
        type: 'added',
        desc: 'Added is_current, valid_from, valid_to columns for history tracking',
      },
    ],
  },

  // ─── ML Scenarios ───────────────────────────
  ml_training: {
    label: 'Model Training',
    before: {
      title: 'Training Features',
      path: 'catalog.gold.ml_features',
      format: 'Delta',
      rows: [
        {
          customer_id: 1,
          total_orders: 15,
          avg_amount: 250.0,
          days_since_last: 5,
          is_premium: 1,
          churn_label: 0,
        },
        {
          customer_id: 2,
          total_orders: 2,
          avg_amount: 50.0,
          days_since_last: 90,
          is_premium: 0,
          churn_label: 1,
        },
        {
          customer_id: 3,
          total_orders: 8,
          avg_amount: 180.0,
          days_since_last: 15,
          is_premium: 1,
          churn_label: 0,
        },
        {
          customer_id: 4,
          total_orders: 1,
          avg_amount: 30.0,
          days_since_last: 120,
          is_premium: 0,
          churn_label: 1,
        },
      ],
      stats: { rows: 50000, columns: 6, nulls: 0, duplicates: 0, size: '8 MB' },
    },
    after: {
      title: 'Scored Predictions',
      path: 'catalog.gold.churn_predictions',
      format: 'Delta',
      rows: [
        {
          customer_id: 1,
          churn_probability: 0.05,
          churn_prediction: 0,
          risk_tier: 'Low',
          model_version: 'v2.1',
        },
        {
          customer_id: 2,
          churn_probability: 0.92,
          churn_prediction: 1,
          risk_tier: 'Critical',
          model_version: 'v2.1',
        },
        {
          customer_id: 3,
          churn_probability: 0.15,
          churn_prediction: 0,
          risk_tier: 'Low',
          model_version: 'v2.1',
        },
        {
          customer_id: 4,
          churn_probability: 0.87,
          churn_prediction: 1,
          risk_tier: 'High',
          model_version: 'v2.1',
        },
      ],
      stats: {
        rows: 50000,
        columns: 5,
        nulls: 0,
        duplicates: 0,
        size: '5 MB',
        accuracy: '94.2%',
        auc: 0.97,
      },
    },
    changes: [
      { type: 'transformed', desc: 'Features scaled with StandardScaler' },
      { type: 'transformed', desc: 'Model: XGBoost (accuracy: 94.2%, AUC: 0.97)' },
      { type: 'added', desc: 'churn_probability: model confidence score' },
      {
        type: 'added',
        desc: 'risk_tier: Low (<0.3), Medium (0.3-0.7), High (0.7-0.9), Critical (>0.9)',
      },
      { type: 'added', desc: 'model_version: MLflow registered model v2.1' },
    ],
  },

  // ─── Governance Scenarios ───────────────────
  governance_quality: {
    label: 'Data Quality',
    before: {
      title: 'Unchecked Silver Table',
      path: 'catalog.silver.raw_customers',
      format: 'Delta',
      rows: [
        { id: 1, name: 'Alice', email: 'alice@test.com', phone: '555-0101', score: 95 },
        { id: 2, name: '', email: 'invalid-email', phone: null, score: -5 },
        { id: 3, name: 'Charlie', email: 'charlie@test.com', phone: '555-0103', score: 150 },
        { id: 2, name: '', email: 'invalid-email', phone: null, score: -5 },
      ],
      stats: { rows: 25000, columns: 5, nulls: 1250, duplicates: 340, size: '4 MB' },
    },
    after: {
      title: 'Quality Report',
      path: 'catalog.audit.data_quality_log',
      format: 'Delta',
      rows: [
        {
          check: 'Null Check',
          table: 'customers',
          column: 'phone',
          null_count: 1250,
          null_pct: '5.0%',
          status: 'WARN',
        },
        {
          check: 'Duplicate Check',
          table: 'customers',
          column: 'id',
          dup_count: 340,
          dup_pct: '1.4%',
          status: 'FAIL',
        },
        {
          check: 'Range Check',
          table: 'customers',
          column: 'score',
          out_of_range: 890,
          threshold: '0-100',
          status: 'FAIL',
        },
        {
          check: 'Format Check',
          table: 'customers',
          column: 'email',
          invalid: 567,
          pattern: '*@*.*',
          status: 'FAIL',
        },
        {
          check: 'Empty String',
          table: 'customers',
          column: 'name',
          empty: 230,
          pct: '0.9%',
          status: 'WARN',
        },
      ],
      stats: { total_checks: 15, passed: 10, failed: 3, warnings: 2, score: '67%' },
    },
    changes: [
      { type: 'detected', desc: '1,250 null phone numbers (5.0%)' },
      { type: 'detected', desc: '340 duplicate records on id column' },
      { type: 'detected', desc: '890 scores outside valid range (0-100)' },
      { type: 'detected', desc: '567 invalid email formats' },
      { type: 'detected', desc: '230 empty name strings' },
    ],
  },
};

// Map pipeline IDs to sample data keys
const PIPELINE_DATA_MAP = {
  1: 'ingestion_csv',
  2: 'ingestion_api',
  3: 'ingestion_kafka',
  4: 'ingestion_csv', // JDBC uses similar pattern
  5: 'ingestion_csv', // S3 uses similar pattern
  6: 'elt_scd2',
  7: 'ingestion_kafka', // CDC uses streaming pattern
  8: 'ml_training',
  9: 'ml_training',
  10: 'ml_training',
  11: 'ml_training',
  12: 'governance_quality',
  13: 'governance_quality',
  14: 'ingestion_api', // Report uses API pattern
  15: 'ingestion_csv', // Snowflake sync
  16: 'ingestion_csv', // Image pipeline
  17: 'ingestion_csv', // Log pipeline
  18: 'ingestion_csv', // Text pipeline
  19: 'elt_scd2',
  20: 'governance_quality',
};

export function getSampleData(pipelineId) {
  const key = PIPELINE_DATA_MAP[pipelineId] || 'ingestion_csv';
  return SAMPLE_DATA[key] || SAMPLE_DATA.ingestion_csv;
}

export { SAMPLE_DATA, PIPELINE_DATA_MAP };
export default SAMPLE_DATA;
