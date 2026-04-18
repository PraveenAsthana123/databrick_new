/**
 * ScenarioCard — Reusable Input/Process/Output + Code Approaches component
 *
 * Wraps ANY scenario with:
 *   1. Source → Target data path
 *   2. Before data table (pre-process)
 *   3. "Run Process" button
 *   4. After data table (post-process)
 *   5. Stats comparison (rows/nulls/dupes)
 *   6. Multiple code approach tabs (Pseudo, PySpark, SQL, DLT)
 *
 * Usage:
 *   <ScenarioCard scenario={scenario} isExpanded={true} />
 */

import React, { useState, useRef, useEffect } from 'react';
import {
  exportToCSV,
  exportToJSON,
  exportToXML,
  exportToAvro,
  exportToText,
  exportToParquet,
} from '../../utils/fileExport';

// Generate sample before/after data based on scenario title/category
function generateSampleData(scenario) {
  const title = (scenario.title || '').toLowerCase();
  const cat = (scenario.category || scenario.group || '').toLowerCase();

  // Generate contextual before data
  const beforeRows = [
    { col_1: 'raw_val_1', col_2: 100, col_3: '2024-01-15', col_4: 'active', col_5: null },
    { col_1: 'raw_val_2', col_2: -5, col_3: 'bad_date', col_4: '', col_5: 'data' },
    { col_1: 'raw_val_1', col_2: 100, col_3: '2024-01-15', col_4: 'active', col_5: null },
    { col_1: null, col_2: 250, col_3: '2024-02-20', col_4: 'paused', col_5: 'ok' },
  ];

  const afterRows = [
    {
      col_1: 'raw_val_1',
      col_2: 100,
      col_3: '2024-01-15',
      col_4: 'active',
      col_5: 'N/A',
      _processed: '2024-04-15',
    },
    {
      col_1: 'raw_val_2',
      col_2: 0,
      col_3: '1970-01-01',
      col_4: 'unknown',
      col_5: 'data',
      _processed: '2024-04-15',
    },
    {
      col_1: 'UNKNOWN',
      col_2: 250,
      col_3: '2024-02-20',
      col_4: 'paused',
      col_5: 'ok',
      _processed: '2024-04-15',
    },
  ];

  // Customize based on scenario type
  if (title.includes('customer') || title.includes('crm') || cat.includes('customer')) {
    return {
      source: {
        path: 'source_system/customers',
        format: cat.includes('stream') ? 'Kafka' : 'JDBC/CSV',
      },
      target: { path: 'catalog.silver.customers', format: 'Delta' },
      before: [
        {
          customer_id: 'C001',
          name: ' Alice ',
          email: 'ALICE@TEST.COM',
          segment: null,
          created: '2024-01-15',
        },
        {
          customer_id: 'C002',
          name: 'Bob',
          email: 'bob@test.com',
          segment: 'Enterprise',
          created: '01/20/2024',
        },
        {
          customer_id: 'C001',
          name: ' Alice ',
          email: 'ALICE@TEST.COM',
          segment: null,
          created: '2024-01-15',
        },
        { customer_id: null, name: 'Charlie', email: 'invalid', segment: '', created: 'bad_date' },
      ],
      after: [
        {
          customer_id: 'C001',
          name: 'Alice',
          email: 'alice@test.com',
          segment: 'Unknown',
          created: '2024-01-15',
          _ts: '2024-04-15',
        },
        {
          customer_id: 'C002',
          name: 'Bob',
          email: 'bob@test.com',
          segment: 'Enterprise',
          created: '2024-01-20',
          _ts: '2024-04-15',
        },
      ],
      beforeStats: { rows: 50000, nulls: 1200, dupes: 800 },
      afterStats: { rows: 48000, nulls: 0, dupes: 0 },
    };
  }

  if (
    title.includes('order') ||
    title.includes('sales') ||
    title.includes('transaction') ||
    title.includes('pos')
  ) {
    return {
      source: {
        path: 'source_system/orders',
        format: cat.includes('stream') ? 'Kafka' : 'JDBC/CSV',
      },
      target: { path: 'catalog.silver.orders', format: 'Delta' },
      before: [
        {
          order_id: 1001,
          customer_id: 'C001',
          amount: 150.5,
          status: 'SHIPPED',
          order_date: '2024-01-15',
        },
        { order_id: 1002, customer_id: 'C002', amount: 0, status: null, order_date: '01/20/2024' },
        {
          order_id: 1001,
          customer_id: 'C001',
          amount: 150.5,
          status: 'SHIPPED',
          order_date: '2024-01-15',
        },
        {
          order_id: 1003,
          customer_id: null,
          amount: -100,
          status: 'PENDING',
          order_date: 'INVALID',
        },
      ],
      after: [
        {
          order_id: 1001,
          customer_id: 'C001',
          amount: 150.5,
          status: 'SHIPPED',
          order_date: '2024-01-15',
          _ts: '2024-04-15',
        },
        {
          order_id: 1002,
          customer_id: 'C002',
          amount: 0,
          status: 'UNKNOWN',
          order_date: '2024-01-20',
          _ts: '2024-04-15',
        },
      ],
      beforeStats: { rows: 100000, nulls: 3500, dupes: 1200 },
      afterStats: { rows: 97300, nulls: 0, dupes: 0 },
    };
  }

  if (
    title.includes('model') ||
    title.includes('ml') ||
    title.includes('train') ||
    title.includes('feature') ||
    cat.includes('ml')
  ) {
    return {
      source: { path: 'catalog.silver.features', format: 'Delta' },
      target: { path: 'catalog.gold.predictions', format: 'Delta' },
      before: [
        { id: 1, feature_1: 0.85, feature_2: 12, feature_3: 'A', label: 1 },
        { id: 2, feature_1: 0.23, feature_2: 45, feature_3: 'B', label: 0 },
        { id: 3, feature_1: null, feature_2: -1, feature_3: null, label: 1 },
      ],
      after: [
        { id: 1, prediction: 0.92, confidence: 'HIGH', model_version: 'v2.1' },
        { id: 2, prediction: 0.15, confidence: 'LOW', model_version: 'v2.1' },
      ],
      beforeStats: { rows: 50000, nulls: 500, dupes: 0 },
      afterStats: { rows: 50000, nulls: 0, dupes: 0 },
    };
  }

  // ── Streaming / Kafka / Event ──────────────────────────────────────
  if (
    title.includes('stream') ||
    title.includes('kafka') ||
    title.includes('event') ||
    cat.includes('stream')
  ) {
    return {
      source: { path: 'kafka://events-topic', format: 'Kafka JSON' },
      target: { path: 'catalog.bronze.events', format: 'Delta (streaming)' },
      before: [
        {
          offset: 1001,
          key: 'user_123',
          event_type: 'click',
          page: '/home',
          event_ts: '2024-01-15T12:00:00Z',
          partition: 0,
        },
        {
          offset: 1002,
          key: 'user_456',
          event_type: 'purchase',
          page: '/cart',
          event_ts: '2024-01-15T12:01:00Z',
          partition: 1,
        },
        {
          offset: 1003,
          key: null,
          event_type: 'view',
          page: '/products',
          event_ts: '2024-01-15T12:01:30Z',
          partition: 0,
        },
        {
          offset: 1004,
          key: 'user_123',
          event_type: 'click',
          page: '/home',
          event_ts: '2024-01-15T12:00:00Z',
          partition: 0,
        },
      ],
      after: [
        {
          user_id: 'user_123',
          event_type: 'click',
          page: '/home',
          event_ts: '2024-01-15T12:00:00Z',
          _ingested_ts: '2024-01-15T12:00:05Z',
          _batch_id: 'b-001',
        },
        {
          user_id: 'user_456',
          event_type: 'purchase',
          page: '/cart',
          event_ts: '2024-01-15T12:01:00Z',
          _ingested_ts: '2024-01-15T12:01:03Z',
          _batch_id: 'b-001',
        },
        {
          user_id: 'UNKNOWN',
          event_type: 'view',
          page: '/products',
          event_ts: '2024-01-15T12:01:30Z',
          _ingested_ts: '2024-01-15T12:01:35Z',
          _batch_id: 'b-001',
        },
      ],
      beforeStats: { rows: 500000, nulls: 1200, dupes: 8500 },
      afterStats: { rows: 491500, nulls: 0, dupes: 0 },
    };
  }

  // ── CDC / Change Data Capture ──────────────────────────────────────
  if (title.includes('cdc') || title.includes('change data') || title.includes('debezium')) {
    return {
      source: { path: 'mysql://erp_db/orders (CDC)', format: 'Debezium JSON' },
      target: { path: 'catalog.bronze.orders_cdc', format: 'Delta' },
      before: [
        {
          op: 'c',
          order_id: 5001,
          customer_id: 'C100',
          amount: 250.0,
          status: 'NEW',
          ts_ms: 1705334400000,
        },
        {
          op: 'u',
          order_id: 5001,
          customer_id: 'C100',
          amount: 250.0,
          status: 'SHIPPED',
          ts_ms: 1705420800000,
        },
        {
          op: 'c',
          order_id: 5002,
          customer_id: 'C101',
          amount: 89.99,
          status: 'NEW',
          ts_ms: 1705334460000,
        },
        {
          op: 'd',
          order_id: 4999,
          customer_id: 'C099',
          amount: 150.0,
          status: 'CANCELLED',
          ts_ms: 1705507200000,
        },
      ],
      after: [
        {
          order_id: 5001,
          customer_id: 'C100',
          amount: 250.0,
          status: 'SHIPPED',
          _cdc_op: 'UPDATE',
          _cdc_ts: '2024-01-16',
          is_current: true,
        },
        {
          order_id: 5002,
          customer_id: 'C101',
          amount: 89.99,
          status: 'NEW',
          _cdc_op: 'INSERT',
          _cdc_ts: '2024-01-15',
          is_current: true,
        },
        {
          order_id: 4999,
          customer_id: 'C099',
          amount: 150.0,
          status: 'CANCELLED',
          _cdc_op: 'DELETE',
          _cdc_ts: '2024-01-17',
          is_current: false,
        },
      ],
      beforeStats: { rows: 250000, nulls: 0, dupes: 45000 },
      afterStats: { rows: 205000, nulls: 0, dupes: 0 },
    };
  }

  // ── SCD / Slowly Changing Dimension ────────────────────────────────
  if (
    title.includes('scd') ||
    title.includes('slowly changing') ||
    title.includes('history') ||
    title.includes('dimension')
  ) {
    return {
      source: { path: 'catalog.silver.customers', format: 'Delta' },
      target: { path: 'catalog.gold.dim_customer', format: 'Delta (SCD2)' },
      before: [
        {
          customer_id: 101,
          name: 'Alice Johnson',
          email: 'alice@old.com',
          city: 'New York',
          tier: 'Silver',
        },
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
      after: [
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
      beforeStats: { rows: 75000, nulls: 120, dupes: 3200 },
      afterStats: { rows: 82000, nulls: 0, dupes: 0 },
    };
  }

  // ── PII / Masking / Security ───────────────────────────────────────
  if (
    title.includes('pii') ||
    title.includes('mask') ||
    title.includes('encrypt') ||
    title.includes('sensitive') ||
    cat.includes('security') ||
    cat.includes('pii')
  ) {
    return {
      source: { path: 'catalog.bronze.customers_raw', format: 'Delta' },
      target: { path: 'catalog.silver.customers_masked', format: 'Delta (masked)' },
      before: [
        {
          customer_id: 'C001',
          name: 'Alice Johnson',
          ssn: '123-45-6789',
          email: 'alice@company.com',
          phone: '+1-555-0101',
          dob: '1990-03-15',
          salary: 95000,
        },
        {
          customer_id: 'C002',
          name: 'Bob Smith',
          ssn: '987-65-4321',
          email: 'bob@company.com',
          phone: '+1-555-0202',
          dob: '1985-07-22',
          salary: 120000,
        },
        {
          customer_id: 'C003',
          name: 'Charlie Brown',
          ssn: '456-78-9012',
          email: 'charlie@company.com',
          phone: '+1-555-0303',
          dob: '1992-11-08',
          salary: 87000,
        },
      ],
      after: [
        {
          customer_id: 'C001',
          name: 'A*** J******',
          ssn: '***-**-6789',
          email: 'a****@company.com',
          phone: '***-***-0101',
          dob: '****-**-15',
          salary: null,
        },
        {
          customer_id: 'C002',
          name: 'B** S****',
          ssn: '***-**-4321',
          email: 'b**@company.com',
          phone: '***-***-0202',
          dob: '****-**-22',
          salary: null,
        },
        {
          customer_id: 'C003',
          name: 'C****** B****',
          ssn: '***-**-9012',
          email: 'c******@company.com',
          phone: '***-***-0303',
          dob: '****-**-08',
          salary: null,
        },
      ],
      beforeStats: { rows: 200000, nulls: 0, dupes: 0 },
      afterStats: { rows: 200000, nulls: 0, dupes: 0 },
    };
  }

  // ── MERGE / Upsert ─────────────────────────────────────────────────
  if (title.includes('merge') || title.includes('upsert') || title.includes('dedup')) {
    return {
      source: { path: 'catalog.bronze.products_daily', format: 'Delta' },
      target: { path: 'catalog.silver.products', format: 'Delta (merged)' },
      before: [
        { product_id: 'P100', name: 'Widget A', price: 29.99, stock: 500, updated: '2024-01-14' },
        { product_id: 'P101', name: 'Widget B', price: 49.99, stock: 200, updated: '2024-01-14' },
        { product_id: 'P100', name: 'Widget A', price: 31.99, stock: 480, updated: '2024-01-15' },
        { product_id: 'P102', name: 'Widget C', price: 19.99, stock: 1000, updated: '2024-01-15' },
      ],
      after: [
        {
          product_id: 'P100',
          name: 'Widget A',
          price: 31.99,
          stock: 480,
          updated: '2024-01-15',
          _merged_ts: '2024-01-15T06:00:00Z',
        },
        {
          product_id: 'P101',
          name: 'Widget B',
          price: 49.99,
          stock: 200,
          updated: '2024-01-14',
          _merged_ts: '2024-01-14T06:00:00Z',
        },
        {
          product_id: 'P102',
          name: 'Widget C',
          price: 19.99,
          stock: 1000,
          updated: '2024-01-15',
          _merged_ts: '2024-01-15T06:00:00Z',
        },
      ],
      beforeStats: { rows: 80000, nulls: 0, dupes: 15000 },
      afterStats: { rows: 65000, nulls: 0, dupes: 0 },
    };
  }

  // ── Bronze / Ingestion / File / CSV / Batch ────────────────────────
  if (
    title.includes('bronze') ||
    title.includes('ingestion') ||
    title.includes('csv') ||
    title.includes('file') ||
    title.includes('batch') ||
    cat.includes('bronze') ||
    cat.includes('ingestion')
  ) {
    return {
      source: { path: '/mnt/landing/sales/2024-01-15/', format: 'CSV / Parquet' },
      target: { path: 'catalog.bronze.sales_raw', format: 'Delta' },
      before: [
        {
          file_name: 'sales_20240115.csv',
          sale_id: 'S001',
          product: 'Laptop',
          qty: 2,
          price: 999.99,
          region: 'US-East',
          sale_date: '2024-01-15',
        },
        {
          file_name: 'sales_20240115.csv',
          sale_id: 'S002',
          product: 'Mouse',
          qty: 10,
          price: 25.5,
          region: 'US-West',
          sale_date: '2024-01-15',
        },
        {
          file_name: 'sales_20240115.csv',
          sale_id: 'S001',
          product: 'Laptop',
          qty: 2,
          price: 999.99,
          region: 'US-East',
          sale_date: '2024-01-15',
        },
        {
          file_name: 'sales_20240115.csv',
          sale_id: 'S003',
          product: null,
          qty: -1,
          price: 0,
          region: '',
          sale_date: 'INVALID',
        },
      ],
      after: [
        {
          sale_id: 'S001',
          product: 'Laptop',
          qty: 2,
          price: 999.99,
          region: 'US-East',
          sale_date: '2024-01-15',
          _file: 'sales_20240115.csv',
          _ingested: '2024-01-15T06:30:00Z',
        },
        {
          sale_id: 'S002',
          product: 'Mouse',
          qty: 10,
          price: 25.5,
          region: 'US-West',
          sale_date: '2024-01-15',
          _file: 'sales_20240115.csv',
          _ingested: '2024-01-15T06:30:00Z',
        },
      ],
      beforeStats: { rows: 150000, nulls: 4500, dupes: 12000 },
      afterStats: { rows: 133500, nulls: 0, dupes: 0 },
    };
  }

  // ── Silver / Clean / Validate / Quality ────────────────────────────
  if (
    title.includes('silver') ||
    title.includes('clean') ||
    title.includes('valid') ||
    title.includes('quality') ||
    cat.includes('silver') ||
    cat.includes('quality')
  ) {
    return {
      source: { path: 'catalog.bronze.transactions', format: 'Delta' },
      target: { path: 'catalog.silver.transactions_clean', format: 'Delta (validated)' },
      before: [
        {
          txn_id: 'T001',
          account_id: 'A100',
          amount: 5000.0,
          currency: 'USD',
          txn_type: 'DEBIT',
          txn_date: '2024-01-15',
          channel: 'WEB',
        },
        {
          txn_id: 'T002',
          account_id: 'A101',
          amount: -100.0,
          currency: 'usd',
          txn_type: null,
          txn_date: 'bad',
          channel: '',
        },
        {
          txn_id: 'T001',
          account_id: 'A100',
          amount: 5000.0,
          currency: 'USD',
          txn_type: 'DEBIT',
          txn_date: '2024-01-15',
          channel: 'WEB',
        },
        {
          txn_id: 'T003',
          account_id: null,
          amount: 250.0,
          currency: 'EUR',
          txn_type: 'CREDIT',
          txn_date: '2024-01-16',
          channel: 'ATM',
        },
      ],
      after: [
        {
          txn_id: 'T001',
          account_id: 'A100',
          amount: 5000.0,
          currency: 'USD',
          txn_type: 'DEBIT',
          txn_date: '2024-01-15',
          channel: 'WEB',
          _dq_status: 'PASS',
        },
        {
          txn_id: 'T003',
          account_id: 'UNKNOWN',
          amount: 250.0,
          currency: 'EUR',
          txn_type: 'CREDIT',
          txn_date: '2024-01-16',
          channel: 'ATM',
          _dq_status: 'WARN',
        },
      ],
      beforeStats: { rows: 500000, nulls: 8500, dupes: 22000 },
      afterStats: { rows: 469500, nulls: 0, dupes: 0 },
    };
  }

  // ── Gold / Aggregate / KPI / Mart / Star ───────────────────────────
  if (
    title.includes('gold') ||
    title.includes('aggregate') ||
    title.includes('kpi') ||
    title.includes('mart') ||
    title.includes('star') ||
    cat.includes('gold')
  ) {
    return {
      source: { path: 'catalog.silver.transactions + dim_customer + dim_product', format: 'Delta' },
      target: { path: 'catalog.gold.sales_summary_daily', format: 'Delta (Gold mart)' },
      before: [
        {
          date: '2024-01-15',
          region: 'US-East',
          product_category: 'Electronics',
          txn_count: 1250,
          total_revenue: 985000.5,
          avg_order: 788.0,
        },
        {
          date: '2024-01-15',
          region: 'US-West',
          product_category: 'Electronics',
          txn_count: 890,
          total_revenue: 672000.0,
          avg_order: 755.0,
        },
        {
          date: '2024-01-15',
          region: 'EU-Central',
          product_category: 'Clothing',
          txn_count: 2100,
          total_revenue: 315000.0,
          avg_order: 150.0,
        },
        {
          date: '2024-01-14',
          region: 'US-East',
          product_category: 'Electronics',
          txn_count: 1100,
          total_revenue: 867000.0,
          avg_order: 788.18,
        },
      ],
      after: [
        {
          date: '2024-01-15',
          region: 'US-East',
          product_category: 'Electronics',
          txn_count: 1250,
          total_revenue: 985000.5,
          avg_order: 788.0,
          yoy_growth: 12.5,
          rank: 1,
        },
        {
          date: '2024-01-15',
          region: 'US-West',
          product_category: 'Electronics',
          txn_count: 890,
          total_revenue: 672000.0,
          avg_order: 755.0,
          yoy_growth: 8.2,
          rank: 2,
        },
        {
          date: '2024-01-15',
          region: 'EU-Central',
          product_category: 'Clothing',
          txn_count: 2100,
          total_revenue: 315000.0,
          avg_order: 150.0,
          yoy_growth: -3.1,
          rank: 3,
        },
      ],
      beforeStats: { rows: 1200000, nulls: 0, dupes: 0 },
      afterStats: { rows: 45000, nulls: 0, dupes: 0 },
    };
  }

  // ── Governance / Lineage / Catalog / Policy ────────────────────────
  if (
    title.includes('governance') ||
    title.includes('lineage') ||
    title.includes('catalog') ||
    title.includes('policy') ||
    title.includes('rbac') ||
    cat.includes('governance')
  ) {
    return {
      source: {
        path: 'system.access.audit + system.access.table_lineage',
        format: 'System tables',
      },
      target: { path: 'catalog.gold.governance_dashboard', format: 'Delta' },
      before: [
        {
          table_name: 'catalog.silver.customers',
          owner: null,
          classification: null,
          last_access: '2024-01-15',
          access_count: 450,
          has_pii: true,
        },
        {
          table_name: 'catalog.gold.revenue_daily',
          owner: 'finance_team',
          classification: 'CONFIDENTIAL',
          last_access: '2024-01-15',
          access_count: 1200,
          has_pii: false,
        },
        {
          table_name: 'catalog.bronze.raw_logs',
          owner: null,
          classification: null,
          last_access: '2023-06-01',
          access_count: 2,
          has_pii: true,
        },
        {
          table_name: 'catalog.silver.products',
          owner: 'product_team',
          classification: 'PUBLIC',
          last_access: '2024-01-14',
          access_count: 800,
          has_pii: false,
        },
      ],
      after: [
        {
          table_name: 'catalog.silver.customers',
          owner: 'crm_team',
          classification: 'PII',
          last_access: '2024-01-15',
          access_count: 450,
          has_pii: true,
          policy: 'MASK_SSN+EMAIL',
          compliance: 'GDPR',
        },
        {
          table_name: 'catalog.gold.revenue_daily',
          owner: 'finance_team',
          classification: 'CONFIDENTIAL',
          last_access: '2024-01-15',
          access_count: 1200,
          has_pii: false,
          policy: 'RLS_BY_REGION',
          compliance: 'SOX',
        },
        {
          table_name: 'catalog.bronze.raw_logs',
          owner: 'platform_team',
          classification: 'PII',
          last_access: '2023-06-01',
          access_count: 2,
          has_pii: true,
          policy: 'ARCHIVE_180D',
          compliance: 'GDPR',
        },
      ],
      beforeStats: { rows: 5000, nulls: 1800, dupes: 0 },
      afterStats: { rows: 5000, nulls: 0, dupes: 0 },
    };
  }

  // ── ELT / ETL / Transform ──────────────────────────────────────────
  if (
    title.includes('elt') ||
    title.includes('etl') ||
    title.includes('transform') ||
    cat.includes('elt') ||
    cat.includes('etl')
  ) {
    return {
      source: { path: 'catalog.bronze.raw_invoices', format: 'Delta' },
      target: { path: 'catalog.silver.invoices_enriched', format: 'Delta' },
      before: [
        {
          invoice_id: 'INV-001',
          customer_id: 'C100',
          line_items: 3,
          subtotal: 450.0,
          tax_rate: null,
          currency: 'usd',
          invoice_date: '01/15/2024',
        },
        {
          invoice_id: 'INV-002',
          customer_id: 'C101',
          line_items: 1,
          subtotal: 1200.0,
          tax_rate: 0.08,
          currency: 'USD',
          invoice_date: '2024-01-16',
        },
        {
          invoice_id: 'INV-001',
          customer_id: 'C100',
          line_items: 3,
          subtotal: 450.0,
          tax_rate: null,
          currency: 'usd',
          invoice_date: '01/15/2024',
        },
        {
          invoice_id: 'INV-003',
          customer_id: null,
          line_items: 0,
          subtotal: -50.0,
          tax_rate: 0.1,
          currency: 'EUR',
          invoice_date: 'INVALID',
        },
      ],
      after: [
        {
          invoice_id: 'INV-001',
          customer_id: 'C100',
          customer_name: 'Acme Corp',
          line_items: 3,
          subtotal: 450.0,
          tax: 36.0,
          total: 486.0,
          currency: 'USD',
          invoice_date: '2024-01-15',
        },
        {
          invoice_id: 'INV-002',
          customer_id: 'C101',
          customer_name: 'Beta LLC',
          line_items: 1,
          subtotal: 1200.0,
          tax: 96.0,
          total: 1296.0,
          currency: 'USD',
          invoice_date: '2024-01-16',
        },
      ],
      beforeStats: { rows: 300000, nulls: 15000, dupes: 28000 },
      afterStats: { rows: 257000, nulls: 0, dupes: 0 },
    };
  }

  // ── Visualization / Dashboard / BI / Report ────────────────────────
  if (
    title.includes('visual') ||
    title.includes('dashboard') ||
    title.includes('bi') ||
    title.includes('report') ||
    title.includes('chart') ||
    cat.includes('visual')
  ) {
    return {
      source: { path: 'catalog.gold.sales_summary', format: 'Delta / SQL Warehouse' },
      target: { path: 'Power BI / Tableau Dashboard', format: 'Semantic model' },
      before: [
        {
          metric: 'Total Revenue',
          region: 'US-East',
          current_month: 2850000,
          prev_month: 2650000,
          yoy_growth: 12.5,
          target: 3000000,
        },
        {
          metric: 'Total Revenue',
          region: 'US-West',
          current_month: 1920000,
          prev_month: 1800000,
          yoy_growth: 8.2,
          target: 2000000,
        },
        {
          metric: 'Active Customers',
          region: 'US-East',
          current_month: 45000,
          prev_month: 42000,
          yoy_growth: 7.1,
          target: 50000,
        },
        {
          metric: 'Churn Rate',
          region: 'US-East',
          current_month: 3.2,
          prev_month: 3.8,
          yoy_growth: -15.8,
          target: 2.5,
        },
      ],
      after: [
        {
          metric: 'Total Revenue',
          region: 'US-East',
          value: 2850000,
          trend: 'UP',
          vs_target: '95%',
          alert: 'ON_TRACK',
          display: '$2.85M',
        },
        {
          metric: 'Total Revenue',
          region: 'US-West',
          value: 1920000,
          trend: 'UP',
          vs_target: '96%',
          alert: 'ON_TRACK',
          display: '$1.92M',
        },
        {
          metric: 'Active Customers',
          region: 'US-East',
          value: 45000,
          trend: 'UP',
          vs_target: '90%',
          alert: 'WATCH',
          display: '45K',
        },
        {
          metric: 'Churn Rate',
          region: 'US-East',
          value: 3.2,
          trend: 'DOWN',
          vs_target: '128%',
          alert: 'AT_RISK',
          display: '3.2%',
        },
      ],
      beforeStats: { rows: 500, nulls: 0, dupes: 0 },
      afterStats: { rows: 500, nulls: 0, dupes: 0 },
    };
  }

  // ── XAI / Fairness / Explainability ────────────────────────────────
  if (
    title.includes('xai') ||
    title.includes('explain') ||
    title.includes('shap') ||
    title.includes('fairness') ||
    title.includes('bias') ||
    cat.includes('xai')
  ) {
    return {
      source: { path: 'catalog.gold.predictions + shap_values', format: 'Delta' },
      target: { path: 'catalog.gold.model_explanations', format: 'Delta' },
      before: [
        {
          customer_id: 'C001',
          prediction: 0.87,
          label: 'HIGH_RISK',
          feature_income: 45000,
          feature_age: 28,
          feature_tenure: 2,
          feature_region: 'US-East',
        },
        {
          customer_id: 'C002',
          prediction: 0.12,
          label: 'LOW_RISK',
          feature_income: 120000,
          feature_age: 45,
          feature_tenure: 12,
          feature_region: 'US-West',
        },
        {
          customer_id: 'C003',
          prediction: 0.65,
          label: 'MEDIUM_RISK',
          feature_income: 72000,
          feature_age: 35,
          feature_tenure: 5,
          feature_region: 'EU-Central',
        },
      ],
      after: [
        {
          customer_id: 'C001',
          prediction: 0.87,
          top_1_feature: 'income',
          top_1_shap: -0.35,
          top_2_feature: 'tenure',
          top_2_shap: -0.22,
          top_3_feature: 'age',
          top_3_shap: -0.15,
          explanation: 'Low income + short tenure drives high risk',
        },
        {
          customer_id: 'C002',
          prediction: 0.12,
          top_1_feature: 'income',
          top_1_shap: 0.4,
          top_2_feature: 'tenure',
          top_2_shap: 0.28,
          top_3_feature: 'age',
          top_3_shap: 0.05,
          explanation: 'High income + long tenure reduces risk',
        },
        {
          customer_id: 'C003',
          prediction: 0.65,
          top_1_feature: 'tenure',
          top_1_shap: -0.18,
          top_2_feature: 'region',
          top_2_shap: -0.12,
          top_3_feature: 'income',
          top_3_shap: 0.08,
          explanation: 'Medium tenure + EU region drives moderate risk',
        },
      ],
      beforeStats: { rows: 50000, nulls: 0, dupes: 0 },
      afterStats: { rows: 50000, nulls: 0, dupes: 0 },
    };
  }

  // ── RAG / Embedding / Vector / LLM ─────────────────────────────────
  if (
    title.includes('rag') ||
    title.includes('embed') ||
    title.includes('vector') ||
    title.includes('llm') ||
    title.includes('retriev') ||
    cat.includes('rag')
  ) {
    return {
      source: { path: 'catalog.bronze.documents', format: 'PDF / Markdown' },
      target: { path: 'catalog.gold.document_embeddings', format: 'Delta + Vector Index' },
      before: [
        {
          doc_id: 'DOC-001',
          title: 'Employee Handbook v3.2',
          chunk_id: 1,
          chunk_text: 'All employees are entitled to 20 days paid leave per year...',
          source: 'HR/policies',
          last_modified: '2024-01-10',
        },
        {
          doc_id: 'DOC-001',
          title: 'Employee Handbook v3.2',
          chunk_id: 2,
          chunk_text: 'Remote work is permitted with manager approval for up to 3 days per week...',
          source: 'HR/policies',
          last_modified: '2024-01-10',
        },
        {
          doc_id: 'DOC-002',
          title: 'Security Policy 2024',
          chunk_id: 1,
          chunk_text: 'All access to production systems requires MFA and VPN connection...',
          source: 'IT/security',
          last_modified: '2024-01-05',
        },
      ],
      after: [
        {
          doc_id: 'DOC-001',
          chunk_id: 1,
          embedding_dim: 768,
          model: 'bge-large-en-v1.5',
          token_count: 42,
          metadata_source: 'HR/policies',
          indexed_at: '2024-01-15T08:00:00Z',
        },
        {
          doc_id: 'DOC-001',
          chunk_id: 2,
          embedding_dim: 768,
          model: 'bge-large-en-v1.5',
          token_count: 38,
          metadata_source: 'HR/policies',
          indexed_at: '2024-01-15T08:00:01Z',
        },
        {
          doc_id: 'DOC-002',
          chunk_id: 1,
          embedding_dim: 768,
          model: 'bge-large-en-v1.5',
          token_count: 35,
          metadata_source: 'IT/security',
          indexed_at: '2024-01-15T08:00:02Z',
        },
      ],
      beforeStats: { rows: 15000, nulls: 0, dupes: 0 },
      afterStats: { rows: 15000, nulls: 0, dupes: 0 },
    };
  }

  // ── Terraform / Infrastructure / Cloud / Pipeline Config ───────────
  if (
    title.includes('terraform') ||
    title.includes('infra') ||
    title.includes('cloud') ||
    title.includes('azure') ||
    title.includes('aws') ||
    cat.includes('terraform')
  ) {
    return {
      source: { path: 'terraform/modules/databricks', format: 'HCL / Terraform' },
      target: { path: 'Azure / AWS / GCP resources', format: 'Cloud resources' },
      before: [
        {
          resource: 'databricks_cluster',
          name: 'analytics-cluster',
          node_type: 'Standard_DS3_v2',
          min_workers: 2,
          max_workers: 8,
          auto_terminate: 30,
          state: 'RUNNING',
        },
        {
          resource: 'databricks_sql_endpoint',
          name: 'bi-warehouse',
          size: 'Medium',
          min_clusters: 1,
          max_clusters: 4,
          auto_stop: 15,
          state: 'RUNNING',
        },
        {
          resource: 'azurerm_storage_account',
          name: 'datalakeprod',
          tier: 'Standard',
          replication: 'LRS',
          access_tier: 'Hot',
          state: 'ACTIVE',
        },
        {
          resource: 'databricks_cluster_policy',
          name: 'cost-control',
          max_dbu: 100,
          spot_enabled: true,
          auto_terminate_max: 60,
          state: 'ACTIVE',
        },
      ],
      after: [
        {
          resource: 'databricks_cluster',
          name: 'analytics-cluster',
          status: 'APPLIED',
          drift: 'NONE',
          cost_estimate: '$12.50/hr',
          compliance: 'PASS',
        },
        {
          resource: 'databricks_sql_endpoint',
          name: 'bi-warehouse',
          status: 'APPLIED',
          drift: 'NONE',
          cost_estimate: '$8.00/hr',
          compliance: 'PASS',
        },
        {
          resource: 'azurerm_storage_account',
          name: 'datalakeprod',
          status: 'APPLIED',
          drift: 'DETECTED',
          cost_estimate: '$0.02/GB/mo',
          compliance: 'WARN',
        },
      ],
      beforeStats: { rows: 45, nulls: 0, dupes: 0 },
      afterStats: { rows: 45, nulls: 0, dupes: 0 },
    };
  }

  // ── Testing / DQ / Reconciliation ──────────────────────────────────
  if (
    title.includes('test') ||
    title.includes('reconcil') ||
    title.includes('dq') ||
    title.includes('expect') ||
    cat.includes('test')
  ) {
    return {
      source: { path: 'catalog.silver.orders + catalog.bronze.orders', format: 'Delta' },
      target: { path: 'catalog.gold.dq_report', format: 'Delta' },
      before: [
        {
          check: 'Row Count Match',
          source_table: 'bronze.orders',
          target_table: 'silver.orders',
          source_count: 150000,
          target_count: 147500,
          diff: 2500,
          threshold: '2%',
          status: 'FAIL',
        },
        {
          check: 'Null Check',
          table: 'silver.orders',
          column: 'customer_id',
          null_count: 350,
          null_pct: '0.24%',
          threshold: '0.5%',
          status: 'PASS',
        },
        {
          check: 'Unique Check',
          table: 'silver.orders',
          column: 'order_id',
          dup_count: 0,
          dup_pct: '0%',
          threshold: '0%',
          status: 'PASS',
        },
        {
          check: 'Range Check',
          table: 'silver.orders',
          column: 'amount',
          out_of_range: 12,
          range: '0-100000',
          threshold: '0.01%',
          status: 'WARN',
        },
      ],
      after: [
        {
          check: 'Row Count Match',
          status: 'INVESTIGATED',
          resolution: 'Missing data from source retry — reprocessed',
          final_status: 'PASS',
        },
        {
          check: 'Null Check',
          status: 'PASS',
          resolution: 'Within threshold',
          final_status: 'PASS',
        },
        {
          check: 'Unique Check',
          status: 'PASS',
          resolution: 'No duplicates',
          final_status: 'PASS',
        },
        {
          check: 'Range Check',
          status: 'WARN',
          resolution: '12 negative refunds — valid business case',
          final_status: 'ACCEPTED',
        },
      ],
      beforeStats: { rows: 150000, nulls: 350, dupes: 0 },
      afterStats: { rows: 150000, nulls: 0, dupes: 0 },
    };
  }

  // ── Unity Catalog / Access / RBAC / RLS / Masking ──────────────────
  if (
    title.includes('unity') ||
    title.includes('catalog') ||
    title.includes('rbac') ||
    title.includes('rls') ||
    title.includes('access control') ||
    cat.includes('unity')
  ) {
    return {
      source: { path: 'system.information_schema + system.access', format: 'System tables' },
      target: { path: 'catalog.gold.access_audit', format: 'Delta' },
      before: [
        {
          principal: 'analyst_group',
          catalog: 'prod',
          schema: 'silver',
          table: 'customers',
          privilege: 'SELECT',
          granted_by: 'admin',
          grant_date: '2024-01-01',
        },
        {
          principal: 'data_eng_group',
          catalog: 'prod',
          schema: 'bronze',
          table: '*',
          privilege: 'ALL PRIVILEGES',
          granted_by: 'admin',
          grant_date: '2024-01-01',
        },
        {
          principal: 'bi_group',
          catalog: 'prod',
          schema: 'gold',
          table: 'sales_daily',
          privilege: 'SELECT',
          granted_by: 'admin',
          grant_date: '2024-01-10',
        },
        {
          principal: 'external_partner',
          catalog: 'shared',
          schema: 'partner',
          table: 'orders_summary',
          privilege: 'SELECT',
          granted_by: 'admin',
          grant_date: '2024-01-15',
        },
      ],
      after: [
        {
          principal: 'analyst_group',
          access_level: 'READ',
          tables_accessible: 12,
          has_pii_access: false,
          rls_applied: true,
          cls_applied: true,
          last_query: '2024-01-15',
        },
        {
          principal: 'data_eng_group',
          access_level: 'ADMIN',
          tables_accessible: 85,
          has_pii_access: true,
          rls_applied: false,
          cls_applied: false,
          last_query: '2024-01-15',
        },
        {
          principal: 'bi_group',
          access_level: 'READ',
          tables_accessible: 8,
          has_pii_access: false,
          rls_applied: true,
          cls_applied: true,
          last_query: '2024-01-15',
        },
      ],
      beforeStats: { rows: 500, nulls: 0, dupes: 0 },
      afterStats: { rows: 500, nulls: 0, dupes: 0 },
    };
  }

  // Default generic
  return {
    source: { path: '/mnt/landing/data', format: 'CSV/JSON' },
    target: { path: 'catalog.silver.processed', format: 'Delta' },
    before: beforeRows,
    after: afterRows,
    beforeStats: { rows: 10000, nulls: 250, dupes: 120 },
    afterStats: { rows: 9630, nulls: 0, dupes: 0 },
  };
}

// Generate explanation for the scenario (what/why/when/how)
function generateExplanation(scenario) {
  const title = scenario.title || 'Scenario';
  const desc = scenario.desc || scenario.description || '';
  const category = scenario.category || scenario.group || '';
  const flow = scenario.flow || '';

  const titleLower = title.toLowerCase();

  // What the scenario does
  const what =
    desc || `This scenario demonstrates "${title}" — a ${category} pattern for data processing.`;

  // Why it matters
  let why = 'Critical for reliable, production-grade data pipelines at enterprise scale.';
  if (titleLower.includes('cdc') || titleLower.includes('change')) {
    why =
      'Essential for keeping downstream systems in sync with source data changes without full reloads.';
  } else if (titleLower.includes('scd')) {
    why =
      'Enables historical tracking of dimension changes, critical for point-in-time analytics and audit.';
  } else if (titleLower.includes('pii') || titleLower.includes('mask')) {
    why =
      'Required for regulatory compliance (GDPR, HIPAA, PCI) and protecting sensitive customer data.';
  } else if (titleLower.includes('stream')) {
    why =
      'Delivers real-time insights for time-sensitive use cases like fraud detection and live dashboards.';
  } else if (titleLower.includes('merge') || titleLower.includes('upsert')) {
    why = 'Efficient way to apply inserts, updates, and deletes atomically in a single operation.';
  } else if (titleLower.includes('dedup')) {
    why = 'Prevents data duplication that skews metrics and wastes storage/compute.';
  } else if (titleLower.includes('quality') || titleLower.includes('validation')) {
    why = 'Catches bad data early to prevent garbage reaching business users and ML models.';
  } else if (titleLower.includes('encrypt') || titleLower.includes('security')) {
    why = 'Protects data at rest and in transit, required for compliance and breach prevention.';
  } else if (titleLower.includes('audit') || titleLower.includes('lineage')) {
    why = 'Provides traceability required for compliance, debugging, and trust in data.';
  } else if (titleLower.includes('kafka') || titleLower.includes('event')) {
    why =
      'Enables event-driven architecture with exactly-once processing and backpressure handling.';
  } else if (titleLower.includes('ingestion') || titleLower.includes('load')) {
    why = 'Foundation of the data pipeline — reliable ingestion determines downstream quality.';
  } else if (titleLower.includes('feature') || titleLower.includes('ml')) {
    why = 'Feature consistency between training and serving is critical for ML model accuracy.';
  } else if (titleLower.includes('rag') || titleLower.includes('embed')) {
    why = 'Core building block of GenAI applications — quality retrieval depends on good chunking.';
  } else if (titleLower.includes('partition') || titleLower.includes('optim')) {
    why = 'Performance directly impacts query cost and user experience at scale.';
  }

  // When to use
  let when =
    'Use when building production data pipelines with enterprise reliability requirements.';
  if (titleLower.includes('cdc') || titleLower.includes('incremental')) {
    when = 'Use when source tables change frequently and full reloads are too expensive.';
  } else if (titleLower.includes('scd type 2')) {
    when =
      'Use when business needs history of changes (customer lifecycle, pricing history, etc.).';
  } else if (titleLower.includes('scd type 1')) {
    when = 'Use when only current state matters and history is not needed.';
  } else if (titleLower.includes('batch')) {
    when = 'Use for periodic processing (hourly, daily) where real-time is not required.';
  } else if (titleLower.includes('stream') || titleLower.includes('real-time')) {
    when = 'Use when latency requirements are seconds/minutes, not hours.';
  } else if (titleLower.includes('pii') || titleLower.includes('mask')) {
    when = 'Use for any dataset containing personally identifiable or regulated information.';
  } else if (titleLower.includes('reconcil')) {
    when = 'Use to verify data consistency between source and target after critical loads.';
  } else if (titleLower.includes('dr') || titleLower.includes('disaster')) {
    when = 'Use for business-critical data requiring multi-region availability (RPO < 1hr).';
  }

  // Key steps
  const steps = [
    'Connect to source system and verify access',
    'Extract data with proper schema handling',
    'Validate data quality and schema',
    'Transform according to business rules',
    'Load to target (Delta Lake) with audit trail',
    'Verify row counts match expectations',
  ];

  // Business impact / use cases
  let impact = 'Improves data reliability, reduces operational cost, enables trusted analytics.';
  if (titleLower.includes('fraud') || titleLower.includes('risk')) {
    impact = 'Directly prevents financial loss by catching suspicious patterns in real-time.';
  } else if (titleLower.includes('customer') || titleLower.includes('crm')) {
    impact = 'Improves customer experience through accurate, unified customer view.';
  } else if (titleLower.includes('finance') || titleLower.includes('erp')) {
    impact = 'Ensures financial reporting accuracy and regulatory compliance.';
  } else if (titleLower.includes('cost') || titleLower.includes('finops')) {
    impact = 'Reduces cloud spend by identifying waste and optimizing resource usage.';
  }

  // Architecture — ASCII flow
  const architecture = `Source System
     |
     v (Extract)
+----------------+
|  Landing Zone  | <- Raw files / API response
+----------------+
     |
     v (Ingest with schema validation)
+----------------+
|  Bronze Layer  | <- Immutable, append-only
+----------------+
     |
     v (Transform: clean, dedupe, enrich)
+----------------+
|  Silver Layer  | <- Clean, validated, typed
+----------------+
     |
     v (Aggregate, business logic)
+----------------+
|   Gold Layer   | <- BI/ML-ready, trusted
+----------------+
     |
     v (Serve)
  BI / ML / API`;

  // Prerequisites
  const prerequisites = [
    'Databricks workspace with Unity Catalog enabled',
    'Source system credentials stored in Databricks Secrets',
    'Target catalog/schema created with proper permissions',
    'Storage credentials + external location configured',
    'Spark cluster or SQL warehouse provisioned',
    'Monitoring/alerting endpoints configured',
  ];

  // Detailed steps (expanded from 6 to 10 with context)
  const detailedSteps = [
    {
      step: 'Connect to Source',
      detail:
        'Authenticate using service principal or managed identity. Verify network reachability and read permissions.',
    },
    {
      step: 'Validate Source',
      detail:
        'Check source table exists, schema matches contract, row count is within expected range.',
    },
    {
      step: 'Extract Data',
      detail:
        'Use appropriate connector (JDBC, Auto Loader, Kafka). Apply partition pruning and column projection to minimize I/O.',
    },
    {
      step: 'Validate Schema',
      detail:
        'Enforce expected schema. Handle schema evolution (addNewColumns) or fail fast on breaking changes.',
    },
    {
      step: 'Data Quality Checks',
      detail:
        'Run completeness, uniqueness, range, and format validations. Route bad records to quarantine.',
    },
    {
      step: 'Transform Data',
      detail:
        'Apply business rules: clean whitespace, standardize formats, enrich with reference data, compute derived fields.',
    },
    {
      step: 'Add Metadata',
      detail:
        'Tag with _ingest_ts, _source_file, _batch_id, _pipeline_run_id for lineage and debugging.',
    },
    {
      step: 'Write to Target',
      detail:
        'Use appropriate write mode (overwrite/append/merge). Enable mergeSchema if needed. Optimize file size (128MB-1GB).',
    },
    {
      step: 'Verify Write',
      detail:
        'Compare source count vs target count. Run reconciliation queries. Log row count delta.',
    },
    {
      step: 'Log Audit Trail',
      detail:
        'Record pipeline run: status, duration, rows processed, bytes read/written. Alert on SLA breach.',
    },
  ];

  // Testing strategy
  const testing = [
    'Unit: Mock source, test transformations in isolation',
    'Integration: End-to-end with test data in sandbox catalog',
    'Data Quality: Great Expectations or custom DQ suite',
    'Performance: Benchmark with 10x expected volume',
    'Recovery: Test retry, backfill, and disaster recovery paths',
  ];

  // Monitoring & observability
  const monitoring = [
    'Pipeline metrics: success rate, duration, rows processed',
    'Data quality metrics: null %, duplicate %, schema drift',
    'SLA tracking: freshness, completion by deadline',
    'Cost metrics: DBUs, storage growth, query cost',
    'Alerting: PagerDuty/Slack for failures and SLA breach',
  ];

  // Common pitfalls
  const pitfalls = [
    'Not handling schema evolution → pipeline breaks on new columns',
    'No idempotency → duplicates on retry',
    'Missing watermark → late data breaks streaming aggregations',
    'Over-aggressive dedup → silently drops valid records',
    'Ignoring partition pruning → full table scans',
    'No backfill strategy → historical reprocessing is painful',
  ];

  // Performance tips
  const performance = [
    'Partition by access pattern (date for time-series, region for multi-tenant)',
    'Use Z-ORDER on high-cardinality columns for query acceleration',
    'Enable Auto Compaction to avoid small file problem',
    'Broadcast small lookup tables (< 10MB) to avoid shuffles',
    'Use incremental processing (CDF / streaming) over full reloads',
  ];

  // Related patterns
  const relatedPatterns = [
    'Medallion Architecture (Bronze → Silver → Gold)',
    'Idempotent Pipelines (safe retries)',
    'SCD Type 2 (historical tracking)',
    'CDC with Delta MERGE',
    'Data Quality Framework (Great Expectations / DLT expectations)',
  ];

  return {
    what,
    why,
    when,
    steps,
    impact,
    flow,
    category,
    architecture,
    prerequisites,
    detailedSteps,
    testing,
    monitoring,
    pitfalls,
    performance,
    relatedPatterns,
  };
}

// Generate code approaches from original code
function generateApproaches(scenario) {
  const code = scenario.code || '# No code provided';
  const title = scenario.title || 'Scenario';
  const t = title.toUpperCase();

  return [
    {
      name: 'Pseudo Code',
      icon: '\ud83d\udccb',
      difficulty: 'Concept',
      pros: 'Understand the logic before writing code',
      cons: 'Not executable — blueprint only',
      code: `ALGORITHM: ${t}\n${'='.repeat(Math.min(t.length + 11, 50))}\n\nSTEPS:\n  1. CONNECT to source system\n  2. EXTRACT data (read from source)\n  3. VALIDATE schema + data quality\n  4. TRANSFORM (clean, enrich, standardize)\n  5. LOAD to target (Delta table)\n  6. VERIFY row counts + quality\n  7. LOG audit trail\n\nERROR HANDLING:\n  - Retry on failure (3x with backoff)\n  - Quarantine bad records\n  - Alert on SLA breach\n\nCOMPLEXITY: O(n) where n = total rows`,
    },
    {
      name: 'PySpark DataFrame',
      icon: '\u26a1',
      difficulty: 'Beginner',
      pros: 'Most common, full control, easy to debug',
      cons: 'Verbose for simple cases',
      code: `# ═══════════════════════════════════════════════════════════\n# PySpark DataFrame API — ${title}\n# ═══════════════════════════════════════════════════════════\n# WHAT: Uses Spark's DataFrame API for distributed processing\n# WHY:  Native Spark, best performance, fully distributed\n# WHEN: When you need full control over transformations\n# ═══════════════════════════════════════════════════════════\n\n${code}\n\n# ─── Tips ──────────────────────────────────────────────\n# 1. Use .cache() for DataFrames reused multiple times\n# 2. Use .repartition() before large shuffles for better parallelism\n# 3. Use broadcast() for small lookup tables (<10MB)\n# 4. Check physical plan with .explain() to spot issues\n# 5. Use .persist(StorageLevel.MEMORY_AND_DISK) for fault tolerance`,
    },
    {
      name: 'Spark SQL (DML)',
      icon: '\ud83d\udcdd',
      difficulty: 'Beginner',
      pros: 'SQL-native, familiar syntax',
      cons: 'Less programmatic control',
      code: `-- ═════════════════════════════════════════════════════════\n-- Spark SQL (DML) — ${title}\n-- ═════════════════════════════════════════════════════════\n-- WHAT: Uses SQL INSERT/MERGE to load data\n-- WHY:  Familiar SQL syntax, works on any SQL engine\n-- WHEN: SQL-native teams, simple transformations\n-- ═════════════════════════════════════════════════════════\n\n-- Step 1: Overwrite target with clean, deduped data\nINSERT OVERWRITE catalog.silver.target_table\nSELECT\n    id,                                -- Primary key\n    TRIM(name) AS name,                -- Clean whitespace\n    LOWER(email) AS email,             -- Standardize email\n    CAST(amount AS DECIMAL(18,2)) AS amount,  -- Enforce type\n    current_timestamp() AS _processed_ts       -- Audit column\nFROM catalog.bronze.source_table\nWHERE id IS NOT NULL                   -- Filter invalid rows\n-- Deduplicate: keep latest row per id based on ingest timestamp\nQUALIFY ROW_NUMBER() OVER (\n    PARTITION BY id\n    ORDER BY _ingest_ts DESC\n) = 1;\n\n-- Step 2: Verify row counts match expectations\nSELECT COUNT(*) AS loaded_rows FROM catalog.silver.target_table;`,
    },
    {
      name: 'Spark SQL (DDL)',
      icon: '\ud83c\udfd7\ufe0f',
      difficulty: 'Beginner',
      pros: 'One-liner table creation, idempotent',
      cons: 'Less flexibility for complex transforms',
      code: `-- Spark SQL (DDL) for: ${title}\nCREATE OR REPLACE TABLE catalog.silver.target_table\nUSING DELTA\nPARTITIONED BY (date_col)\nTBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')\nAS\nSELECT *, current_timestamp() AS _processed_ts\nFROM catalog.bronze.source_table;\n\n-- Or use COPY INTO (idempotent)\nCOPY INTO catalog.bronze.source_table\nFROM 's3://bucket/data/'\nFILEFORMAT = CSV\nFORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');`,
    },
    {
      name: 'Delta Live Tables',
      icon: '\ud83d\ude80',
      difficulty: 'Intermediate',
      pros: 'Declarative, auto-manages dependencies, built-in quality',
      cons: 'Requires DLT pipeline setup',
      code: `# ═══════════════════════════════════════════════════════════\n# Delta Live Tables (DLT) — ${title}\n# ═══════════════════════════════════════════════════════════\n# WHAT: Declarative pipeline with automatic dependency management\n# WHY:  Auto-handles retries, lineage, data quality, and CDC\n# WHEN: Production pipelines needing quality enforcement\n# ═══════════════════════════════════════════════════════════\n\nimport dlt\nfrom pyspark.sql.functions import current_timestamp\n\n# ─── BRONZE: Raw ingestion layer ───\n@dlt.table(\n    name="bronze_data",\n    comment="Raw data from source — immutable, append-only",\n    table_properties={"quality": "bronze"}\n)\ndef ingest():\n    # Auto Loader handles schema evolution + incremental ingestion\n    return (\n        spark.readStream.format("cloudFiles")\n            .option("cloudFiles.format", "csv")\n            .option("header", "true")\n            .load("/mnt/landing/")\n    )\n\n# ─── SILVER: Clean + quality-enforced layer ───\n@dlt.table(\n    name="silver_data",\n    comment="Validated, deduplicated, standardized data"\n)\n@dlt.expect_or_drop("valid_id", "id IS NOT NULL")         # Drop bad rows\n@dlt.expect_or_drop("valid_amount", "amount > 0")          # Quality rule\n@dlt.expect_or_fail("schema_ok", "email IS NOT NULL")      # Hard fail if fails\ndef clean():\n    # Read from bronze, dedupe, add metadata\n    return (\n        dlt.read("bronze_data")\n            .dropDuplicates(["id"])                        # Dedup by primary key\n            .withColumn("_ts", current_timestamp())         # Audit timestamp\n    )`,
    },
    {
      name: 'Auto Loader',
      icon: '\ud83c\udf0a',
      difficulty: 'Intermediate',
      pros: 'Incremental, handles new files automatically, schema evolution',
      cons: 'Requires checkpoint location, streaming complexity',
      code: `# Auto Loader (cloudFiles) for: ${title}\ndf = spark.readStream.format("cloudFiles") \\\n    .option("cloudFiles.format", "json") \\\n    .option("cloudFiles.schemaLocation", "/mnt/schema/pipeline") \\\n    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\\n    .load("s3://bucket/data/")\n\ndf.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/bronze") \\\n    .option("mergeSchema", "true") \\\n    .trigger(availableNow=True) \\\n    .toTable("catalog.bronze.data")`,
    },
    {
      name: 'Structured Streaming',
      icon: '\ud83d\udce1',
      difficulty: 'Intermediate',
      pros: 'True streaming, exactly-once processing, low latency',
      cons: 'Requires watermarking, stateful operations complex',
      code: `# Structured Streaming for: ${title}\nfrom pyspark.sql.functions import col, current_timestamp\n\nstream_df = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker:9092") \\\n    .option("subscribe", "events") \\\n    .option("startingOffsets", "latest") \\\n    .load()\n\nparsed = stream_df.select(col("value").cast("string").alias("event")) \\\n    .withColumn("_ingest_ts", current_timestamp()) \\\n    .withWatermark("_ingest_ts", "5 minutes")\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/stream") \\\n    .outputMode("append") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.bronze.stream_events")`,
    },
    {
      name: 'Pandas on Spark',
      icon: '\ud83d\udc3c',
      difficulty: 'Beginner',
      pros: 'Familiar pandas API, good for small-medium data',
      cons: 'Not ideal for very large datasets, limited Spark optimizations',
      code: `# Pandas on Spark (pandas API on Spark) for: ${title}\nimport pyspark.pandas as ps\n\n# Read with pandas API\npdf = ps.read_csv("s3://bucket/data/orders.csv")\n\n# Familiar pandas operations\npdf["_ingest_ts"] = ps.Timestamp.now()\npdf = pdf.dropna(subset=["id"])\npdf = pdf.drop_duplicates(subset=["id"])\npdf["name"] = pdf["name"].str.strip().str.title()\npdf["email"] = pdf["email"].str.lower()\n\n# Save as Delta\npdf.to_delta("catalog.bronze.csv_data", mode="overwrite")\nprint(f"Loaded {len(pdf)} rows")`,
    },
    {
      name: 'Scala Spark',
      icon: '\ud83c\udfa9',
      difficulty: 'Advanced',
      pros: 'JVM-native performance, type-safe, best for complex UDFs',
      cons: 'Steeper learning curve, less Databricks tooling',
      code: `// Scala Spark for: ${title}\nimport org.apache.spark.sql.functions._\nimport org.apache.spark.sql.SparkSession\n\nval spark = SparkSession.builder().getOrCreate()\n\nval df = spark.read\n  .format("csv")\n  .option("header", "true")\n  .option("inferSchema", "true")\n  .load("s3://bucket/data/")\n\nval cleaned = df\n  .dropDuplicates("id")\n  .filter(col("id").isNotNull)\n  .withColumn("_ingest_ts", current_timestamp())\n\ncleaned.write\n  .format("delta")\n  .mode("overwrite")\n  .saveAsTable("catalog.bronze.data")`,
    },
    {
      name: 'dbutils + Notebook',
      icon: '\ud83d\udcd3',
      difficulty: 'Beginner',
      pros: 'Simple, good for ad-hoc, widget parameters',
      cons: 'Not production-grade, hard to test, no scheduling',
      code: `# dbutils + Notebook widgets for: ${title}\n\n# Widget parameters\ndbutils.widgets.text("source_path", "s3://bucket/data/", "Source")\ndbutils.widgets.dropdown("mode", "overwrite", ["overwrite","append"], "Mode")\ndbutils.widgets.text("env", "dev", "Environment")\n\nsource = dbutils.widgets.get("source_path")\nmode = dbutils.widgets.get("mode")\nenv = dbutils.widgets.get("env")\n\n# List files\nfiles = dbutils.fs.ls(source)\nprint(f"Found {len(files)} files")\n\n# Load\ndf = spark.read.option("header", True).csv(source)\ndf.write.format("delta").mode(mode).saveAsTable(f"{env}.bronze.data")\n\n# Display result\ndisplay(df.limit(10))\nprint(f"Done: {df.count()} rows written")`,
    },
    {
      name: 'Airflow DAG',
      icon: '\ud83d\udd00',
      difficulty: 'Intermediate',
      pros: 'Orchestration, dependencies, retries, monitoring',
      cons: 'External tool, adds complexity',
      code: `# Airflow DAG for: ${title}\nfrom airflow import DAG\nfrom airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator\nfrom datetime import datetime, timedelta\n\ndefault_args = {\n    'owner': 'data-team',\n    'retries': 3,\n    'retry_delay': timedelta(minutes=5),\n    'email_on_failure': True,\n}\n\nwith DAG(\n    dag_id='${title.toLowerCase().replace(/ /g, '_')}',\n    default_args=default_args,\n    schedule_interval='0 6 * * *',  # Daily at 6 AM\n    start_date=datetime(2024, 1, 1),\n    catchup=False,\n) as dag:\n\n    run_pipeline = DatabricksRunNowOperator(\n        task_id='run_databricks_job',\n        databricks_conn_id='databricks_default',\n        job_id=12345,\n        notebook_params={'env': 'prod'},\n    )`,
    },
    {
      name: 'Databricks Jobs API',
      icon: '\ud83d\udd27',
      difficulty: 'Intermediate',
      pros: 'Programmatic automation, CI/CD integration',
      cons: 'Requires API token management',
      code: `# Databricks Jobs API for: ${title}\nimport requests, json\n\ntoken = dbutils.secrets.get("scope", "databricks_token")\nhost = "https://adb-xxx.azuredatabricks.net"\nheaders = {"Authorization": f"Bearer {token}"}\n\n# Create job\njob_config = {\n    "name": "${title}",\n    "tasks": [{\n        "task_key": "main",\n        "notebook_task": {"notebook_path": "/Workflows/pipeline"},\n        "new_cluster": {\n            "spark_version": "14.3.x-scala2.12",\n            "node_type_id": "Standard_DS3_v2",\n            "num_workers": 4\n        }\n    }],\n    "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC"}\n}\n\nresp = requests.post(f"{host}/api/2.1/jobs/create",\n    headers=headers, json=job_config, timeout=30)\njob_id = resp.json()["job_id"]\nprint(f"Created job: {job_id}")\n\n# Trigger run\nrequests.post(f"{host}/api/2.1/jobs/run-now",\n    headers=headers, json={"job_id": job_id}, timeout=30)`,
    },
    {
      name: 'Snowflake SQL',
      icon: '\u2744\ufe0f',
      difficulty: 'Beginner',
      pros: 'Works on Snowflake too, cross-platform SQL',
      cons: 'Snowflake-specific syntax, different from Spark SQL',
      code: `-- Snowflake SQL for: ${title}\n-- Create target table\nCREATE OR REPLACE TABLE MY_DB.SILVER.TARGET_TABLE (\n    id INTEGER,\n    name VARCHAR,\n    email VARCHAR,\n    amount DECIMAL(18,2),\n    _ingest_ts TIMESTAMP_NTZ\n);\n\n-- Copy from stage (S3/ADLS)\nCOPY INTO MY_DB.BRONZE.SOURCE_TABLE\nFROM @MY_STAGE/path/\nFILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)\nON_ERROR = 'CONTINUE';\n\n-- Merge into target\nMERGE INTO MY_DB.SILVER.TARGET_TABLE t\nUSING MY_DB.BRONZE.SOURCE_TABLE s\n    ON t.id = s.id\nWHEN MATCHED THEN UPDATE SET\n    name = s.name, email = LOWER(s.email),\n    _ingest_ts = CURRENT_TIMESTAMP()\nWHEN NOT MATCHED THEN INSERT VALUES (\n    s.id, s.name, LOWER(s.email), s.amount, CURRENT_TIMESTAMP()\n);`,
    },
    {
      name: 'dbt Model',
      icon: '\ud83d\udc24',
      difficulty: 'Intermediate',
      pros: 'Version-controlled SQL, testing, lineage, documentation',
      cons: 'Requires dbt setup, learning curve',
      code: `-- dbt model: models/silver/${title.toLowerCase().replace(/ /g, '_')}.sql\n{{ config(\n    materialized='incremental',\n    unique_key='id',\n    partition_by={'field': 'date_col', 'data_type': 'date'},\n    cluster_by=['customer_id']\n) }}\n\nSELECT\n    id,\n    TRIM(name) AS name,\n    LOWER(email) AS email,\n    CAST(amount AS DECIMAL(18,2)) AS amount,\n    date_col,\n    current_timestamp() AS _processed_ts\nFROM {{ ref('bronze_source_table') }}\nWHERE id IS NOT NULL\n\n{% if is_incremental() %}\n  AND _ingest_ts > (SELECT MAX(_processed_ts) FROM {{ this }})\n{% endif %}\n\nQUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) = 1`,
    },
    {
      name: 'Terraform IaC',
      icon: '\ud83c\udfd7\ufe0f',
      difficulty: 'Advanced',
      pros: 'Infrastructure as code, versioned, reproducible',
      cons: 'Defines jobs, not logic — pairs with other approaches',
      code: `# Terraform: Databricks job for "${title}"\nterraform {\n  required_providers {\n    databricks = { source = "databricks/databricks" }\n  }\n}\n\nresource "databricks_job" "${title.toLowerCase().replace(/ /g, '_')}" {\n  name = "${title}"\n\n  task {\n    task_key = "run_pipeline"\n    \n    new_cluster {\n      spark_version = "14.3.x-scala2.12"\n      node_type_id  = "Standard_DS3_v2"\n      num_workers   = 4\n      autotermination_minutes = 20\n    }\n\n    notebook_task {\n      notebook_path = "/Workflows/pipeline"\n      base_parameters = { env = "prod" }\n    }\n  }\n\n  schedule {\n    quartz_cron_expression = "0 0 6 * * ?"\n    timezone_id = "UTC"\n  }\n\n  email_notifications {\n    on_failure = ["alerts@company.com"]\n  }\n}`,
    },
  ];
}

// Mini data table
function MiniTable({ rows, badge, badgeColor }) {
  if (!rows || rows.length === 0) return null;
  const headers = Object.keys(rows[0]);
  return (
    <div
      style={{
        overflowX: 'auto',
        maxHeight: '200px',
        overflowY: 'auto',
        border: '1px solid var(--border)',
        borderRadius: '6px',
      }}
    >
      <table style={{ fontSize: '0.72rem', width: '100%' }}>
        <thead>
          <tr>
            {headers.map((h) => (
              <th
                key={h}
                style={{
                  padding: '0.3rem 0.5rem',
                  background: badgeColor || '#f5f5f5',
                  fontSize: '0.68rem',
                  whiteSpace: 'nowrap',
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri}>
              {headers.map((h) => {
                const v = row[h];
                const isNull = v === null || v === undefined || v === '';
                return (
                  <td
                    key={h}
                    style={{
                      padding: '0.25rem 0.5rem',
                      whiteSpace: 'nowrap',
                      background: isNull ? '#fef2f2' : undefined,
                      color: isNull
                        ? '#991b1b'
                        : typeof v === 'number' && v < 0
                          ? '#dc2626'
                          : undefined,
                    }}
                  >
                    {isNull ? 'NULL' : String(v)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── File Format Selector + Run Command ──────────────────────────────
const FORMAT_OPTIONS = [
  {
    id: 'text',
    label: 'Text (.txt)',
    ext: '.txt',
    icon: '📝',
    readCmd: 'spark.read.text("dbfs:/data/{slug}.txt")',
    writeCmd: 'df.write.text("dbfs:/data/{slug}.txt")',
  },
  {
    id: 'csv',
    label: 'CSV (.csv)',
    ext: '.csv',
    icon: '📄',
    readCmd: 'spark.read.option("header","true").csv("dbfs:/data/{slug}.csv")',
    writeCmd: 'df.write.option("header","true").csv("dbfs:/data/{slug}.csv")',
  },
  {
    id: 'parquet',
    label: 'Parquet (.parquet)',
    ext: '.parquet',
    icon: '🧱',
    readCmd: 'spark.read.parquet("dbfs:/data/{slug}.parquet")',
    writeCmd: 'df.write.parquet("dbfs:/data/{slug}.parquet")',
  },
  {
    id: 'json',
    label: 'JSON (.json)',
    ext: '.json',
    icon: '{ }',
    readCmd: 'spark.read.json("dbfs:/data/{slug}.json")',
    writeCmd: 'df.write.json("dbfs:/data/{slug}.json")',
  },
  {
    id: 'avro',
    label: 'Avro (.avro)',
    ext: '.avro',
    icon: '🔷',
    readCmd: 'spark.read.format("avro").load("dbfs:/data/{slug}.avro")',
    writeCmd: 'df.write.format("avro").save("dbfs:/data/{slug}.avro")',
  },
  {
    id: 'xml',
    label: 'XML (.xml)',
    ext: '.xml',
    icon: '< >',
    readCmd: 'spark.read.format("xml").option("rowTag","record").load("dbfs:/data/{slug}.xml")',
    writeCmd:
      'df.write.format("xml").option("rootTag","records").option("rowTag","record").save("dbfs:/data/{slug}.xml")',
  },
];

function FileFormatRunner({ scenario, data, processed }) {
  const [selectedFormat, setSelectedFormat] = useState('csv');
  const [running, setRunning] = useState(false);
  const [runResult, setRunResult] = useState(null);

  const slug = (scenario.title || 'scenario')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .slice(0, 40);
  const fmt = FORMAT_OPTIONS.find((f) => f.id === selectedFormat) || FORMAT_OPTIONS[1];

  const getExportRows = () => [
    ...data.before.map((r) => ({ stage: 'before', ...r })),
    ...(processed ? data.after.map((r) => ({ stage: 'after', ...r })) : []),
  ];

  const handleDownload = () => {
    const rows = getExportRows();
    if (selectedFormat === 'text') exportToText(rows, `${slug}.txt`);
    else if (selectedFormat === 'csv') exportToCSV(rows, `${slug}.csv`);
    else if (selectedFormat === 'parquet')
      exportToParquet(rows, `${slug}.parquet.json`, 'ScenarioData');
    else if (selectedFormat === 'json') exportToJSON(rows, `${slug}.json`);
    else if (selectedFormat === 'avro') exportToAvro(rows, `${slug}.avro.json`, 'ScenarioData');
    else if (selectedFormat === 'xml') exportToXML(rows, `${slug}.xml`, 'records', 'record');
  };

  const handleRun = () => {
    setRunning(true);
    setRunResult(null);
    const rows = getExportRows();
    setTimeout(
      () => {
        setRunning(false);
        setRunResult({
          status: 'SUCCESS',
          rows: rows.length,
          format: fmt.label,
          file: `${slug}${fmt.ext}`,
          duration: (Math.random() * 2 + 0.5).toFixed(2),
          readCommand: fmt.readCmd.replace(/\{slug\}/g, slug),
          writeCommand: fmt.writeCmd.replace(/\{slug\}/g, slug),
        });
      },
      Math.floor(Math.random() * 1500) + 800
    );
  };

  return (
    <div
      style={{
        marginBottom: '0.85rem',
        border: '1px solid #e2e8f0',
        borderRadius: '10px',
        overflow: 'hidden',
      }}
    >
      {/* Header */}
      <div
        style={{
          background: 'linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%)',
          padding: '0.65rem 1rem',
          borderBottom: '1px solid #e2e8f0',
          display: 'flex',
          alignItems: 'center',
          gap: '0.75rem',
        }}
      >
        <span style={{ fontWeight: 700, fontSize: '0.82rem', color: '#334155' }}>
          Select File Format:
        </span>
        <span style={{ fontSize: '0.75rem', color: '#94a3b8' }}>
          Choose format → Download or Run
        </span>
      </div>

      {/* Radio Options */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(170px, 1fr))',
          gap: '0.5rem',
          padding: '0.75rem 1rem',
        }}
      >
        {FORMAT_OPTIONS.map((f) => {
          const isSelected = selectedFormat === f.id;
          return (
            <label
              key={f.id}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
                padding: '0.5rem 0.65rem',
                borderRadius: '8px',
                cursor: 'pointer',
                border: isSelected ? '2px solid #3b82f6' : '1px solid #e2e8f0',
                background: isSelected ? '#eff6ff' : '#fff',
                transition: 'all 0.15s',
              }}
            >
              <input
                type="radio"
                name="fileFormat"
                value={f.id}
                checked={isSelected}
                onChange={() => {
                  setSelectedFormat(f.id);
                  setRunResult(null);
                }}
                style={{ accentColor: '#3b82f6' }}
              />
              <span style={{ fontSize: '1rem' }}>{f.icon}</span>
              <span
                style={{
                  fontSize: '0.82rem',
                  fontWeight: isSelected ? 700 : 500,
                  color: isSelected ? '#1e40af' : '#334155',
                }}
              >
                {f.label}
              </span>
            </label>
          );
        })}
      </div>

      {/* Action Buttons */}
      <div
        style={{
          display: 'flex',
          gap: '0.65rem',
          padding: '0 1rem 0.75rem',
          flexWrap: 'wrap',
          alignItems: 'center',
        }}
      >
        <button
          className="btn btn-primary btn-sm"
          onClick={handleRun}
          disabled={running}
          style={{ minWidth: '120px' }}
        >
          {running ? '⏳ Running...' : '▶ Run Command'}
        </button>
        <button className="btn btn-secondary btn-sm" onClick={handleDownload}>
          ⬇ Download {fmt.label}
        </button>
        <span style={{ fontSize: '0.75rem', color: '#94a3b8', marginLeft: 'auto' }}>
          {getExportRows().length} rows · {fmt.label} format
        </span>
      </div>

      {/* Run Result */}
      {runResult && (
        <div
          style={{
            margin: '0 1rem 0.75rem',
            borderRadius: '8px',
            overflow: 'hidden',
            border: '1px solid #bbf7d0',
          }}
        >
          {/* Status bar */}
          <div
            style={{
              background: runResult.status === 'SUCCESS' ? '#f0fdf4' : '#fef2f2',
              padding: '0.5rem 0.85rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.75rem',
              flexWrap: 'wrap',
              borderBottom: '1px solid #bbf7d0',
            }}
          >
            <span style={{ color: '#16a34a', fontWeight: 700, fontSize: '0.82rem' }}>
              ✅ {runResult.status}
            </span>
            <span style={{ fontSize: '0.78rem', color: '#166534' }}>
              {runResult.rows} rows · {runResult.format} · {runResult.duration}s
            </span>
            <span
              style={{
                marginLeft: 'auto',
                fontSize: '0.75rem',
                color: '#166534',
                fontFamily: 'monospace',
              }}
            >
              {runResult.file}
            </span>
          </div>

          {/* Commands */}
          <div style={{ padding: '0.65rem 0.85rem', background: '#fff' }}>
            <div style={{ marginBottom: '0.65rem' }}>
              <div
                style={{
                  fontSize: '0.68rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  color: '#1d4ed8',
                  letterSpacing: '0.04em',
                  marginBottom: '0.3rem',
                }}
              >
                Read Command (PySpark)
              </div>
              <div className="code-block" style={{ fontSize: '0.8rem', padding: '0.5rem 0.75rem' }}>
                {runResult.readCommand}
              </div>
            </div>
            <div>
              <div
                style={{
                  fontSize: '0.68rem',
                  fontWeight: 700,
                  textTransform: 'uppercase',
                  color: '#16a34a',
                  letterSpacing: '0.04em',
                  marginBottom: '0.3rem',
                }}
              >
                Write Command (PySpark)
              </div>
              <div className="code-block" style={{ fontSize: '0.8rem', padding: '0.5rem 0.75rem' }}>
                {runResult.writeCommand}
              </div>
            </div>
          </div>

          {/* Spark SQL equivalent */}
          <div
            style={{
              padding: '0.65rem 0.85rem',
              background: 'linear-gradient(135deg, #1e293b 0%, #334155 100%)',
              color: '#e2e8f0',
            }}
          >
            <div
              style={{
                fontSize: '0.68rem',
                fontWeight: 700,
                textTransform: 'uppercase',
                color: '#93c5fd',
                letterSpacing: '0.04em',
                marginBottom: '0.35rem',
              }}
            >
              Spark SQL — Create Table
            </div>
            <pre
              style={{
                margin: 0,
                fontFamily: 'Fira Code, Consolas, monospace',
                fontSize: '0.78rem',
                lineHeight: 1.6,
                color: '#e0e7ff',
                whiteSpace: 'pre-wrap',
              }}
            >
              {`CREATE TABLE IF NOT EXISTS catalog.bronze.${slug.replace(/-/g, '_')}\nUSING ${selectedFormat === 'parquet' ? 'PARQUET' : selectedFormat === 'avro' ? 'AVRO' : selectedFormat === 'xml' ? 'XML' : selectedFormat === 'json' ? 'JSON' : selectedFormat === 'csv' ? 'CSV' : 'TEXT'}\nOPTIONS (path "dbfs:/data/${runResult.file}")${selectedFormat === 'csv' ? "\n  , (header 'true')" : ''}${selectedFormat === 'xml' ? "\n  , (rowTag 'record')" : ''};`}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}

function ScenarioCard({ scenario }) {
  const [processed, setProcessed] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [activeTab, setActiveTab] = useState(0);
  const [duration, setDuration] = useState(null);
  const timeoutRef = useRef(null);

  // H-4 fix: clear timeout on unmount
  useEffect(() => {
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, []);

  const data = generateSampleData(scenario);
  const explanation = generateExplanation(scenario);
  const approaches = generateApproaches(scenario);

  const runProcess = () => {
    setProcessing(true);
    const time = (Math.random() * 3 + 1).toFixed(1);
    timeoutRef.current = setTimeout(
      () => {
        setProcessed(true);
        setProcessing(false);
        setDuration(time);
      },
      Math.floor(Math.random() * 2000) + 1000
    );
  };

  return (
    <div style={{ marginTop: '1rem' }}>
      {/* What is this scenario? — Explanation */}
      <div
        style={{
          marginBottom: '1rem',
          padding: '0.75rem 1rem',
          background: 'linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%)',
          borderRadius: '8px',
          border: '1px solid #bfdbfe',
        }}
      >
        <div
          style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}
        >
          <span style={{ fontSize: '1rem' }}>{'\ud83d\udca1'}</span>
          <strong style={{ fontSize: '0.85rem', color: '#1e40af' }}>What is this scenario?</strong>
        </div>
        <p
          style={{
            fontSize: '0.8rem',
            color: '#1e3a8a',
            marginBottom: '0.6rem',
            lineHeight: '1.5',
          }}
        >
          {explanation.what}
        </p>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: '1fr 1fr',
            gap: '0.6rem',
            marginBottom: '0.6rem',
          }}
        >
          <div
            style={{
              padding: '0.5rem 0.7rem',
              background: 'rgba(255,255,255,0.7)',
              borderRadius: '6px',
              borderLeft: '3px solid #3b82f6',
            }}
          >
            <div
              style={{
                fontSize: '0.65rem',
                fontWeight: 700,
                color: '#1e40af',
                textTransform: 'uppercase',
                marginBottom: '0.2rem',
              }}
            >
              {'\ud83c\udfaf'} Why It Matters
            </div>
            <div style={{ fontSize: '0.75rem', color: '#1e3a8a' }}>{explanation.why}</div>
          </div>
          <div
            style={{
              padding: '0.5rem 0.7rem',
              background: 'rgba(255,255,255,0.7)',
              borderRadius: '6px',
              borderLeft: '3px solid #8b5cf6',
            }}
          >
            <div
              style={{
                fontSize: '0.65rem',
                fontWeight: 700,
                color: '#6d28d9',
                textTransform: 'uppercase',
                marginBottom: '0.2rem',
              }}
            >
              {'\ud83d\udd52'} When To Use
            </div>
            <div style={{ fontSize: '0.75rem', color: '#4c1d95' }}>{explanation.when}</div>
          </div>
        </div>
        <div
          style={{
            padding: '0.5rem 0.7rem',
            background: 'rgba(255,255,255,0.7)',
            borderRadius: '6px',
            borderLeft: '3px solid #10b981',
          }}
        >
          <div
            style={{
              fontSize: '0.65rem',
              fontWeight: 700,
              color: '#065f46',
              textTransform: 'uppercase',
              marginBottom: '0.3rem',
            }}
          >
            {'\ud83d\udccb'} Key Steps
          </div>
          <ol style={{ fontSize: '0.72rem', color: '#064e3b', margin: 0, paddingLeft: '1.2rem' }}>
            {explanation.steps.map((step, i) => (
              <li key={i} style={{ marginBottom: '0.15rem' }}>
                {step}
              </li>
            ))}
          </ol>
        </div>
        {/* Detailed sections — collapsible */}
        <details style={{ marginTop: '0.5rem' }}>
          <summary
            style={{
              cursor: 'pointer',
              fontSize: '0.75rem',
              fontWeight: 700,
              color: '#1e40af',
              padding: '0.3rem 0',
            }}
          >
            {'\ud83d\udcd6'} View Detailed Pipeline Breakdown (Architecture, Prerequisites, 10
            Steps, Testing, Monitoring, Pitfalls)
          </summary>
          <div
            style={{
              marginTop: '0.5rem',
              display: 'grid',
              gridTemplateColumns: '1fr',
              gap: '0.5rem',
            }}
          >
            {/* Architecture */}
            <div
              style={{
                padding: '0.5rem 0.7rem',
                background: '#1e1e2e',
                color: '#cdd6f4',
                borderRadius: '6px',
              }}
            >
              <div
                style={{
                  fontSize: '0.65rem',
                  fontWeight: 700,
                  color: '#89b4fa',
                  textTransform: 'uppercase',
                  marginBottom: '0.3rem',
                }}
              >
                {'\ud83c\udfd7\ufe0f'} Architecture Flow
              </div>
              <pre
                style={{
                  fontSize: '0.65rem',
                  fontFamily: 'monospace',
                  whiteSpace: 'pre',
                  margin: 0,
                }}
              >
                {explanation.architecture}
              </pre>
            </div>

            {/* Two-column: Prerequisites + Performance */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' }}>
              <div
                style={{
                  padding: '0.5rem 0.7rem',
                  background: 'rgba(255,255,255,0.7)',
                  borderRadius: '6px',
                  borderLeft: '3px solid #f59e0b',
                }}
              >
                <div
                  style={{
                    fontSize: '0.65rem',
                    fontWeight: 700,
                    color: '#92400e',
                    textTransform: 'uppercase',
                    marginBottom: '0.3rem',
                  }}
                >
                  {'\u2705'} Prerequisites
                </div>
                <ul
                  style={{ fontSize: '0.7rem', color: '#78350f', margin: 0, paddingLeft: '1.1rem' }}
                >
                  {explanation.prerequisites.map((p, i) => (
                    <li key={i} style={{ marginBottom: '0.1rem' }}>
                      {p}
                    </li>
                  ))}
                </ul>
              </div>
              <div
                style={{
                  padding: '0.5rem 0.7rem',
                  background: 'rgba(255,255,255,0.7)',
                  borderRadius: '6px',
                  borderLeft: '3px solid #06b6d4',
                }}
              >
                <div
                  style={{
                    fontSize: '0.65rem',
                    fontWeight: 700,
                    color: '#155e75',
                    textTransform: 'uppercase',
                    marginBottom: '0.3rem',
                  }}
                >
                  {'\u26a1'} Performance Tips
                </div>
                <ul
                  style={{ fontSize: '0.7rem', color: '#164e63', margin: 0, paddingLeft: '1.1rem' }}
                >
                  {explanation.performance.map((p, i) => (
                    <li key={i} style={{ marginBottom: '0.1rem' }}>
                      {p}
                    </li>
                  ))}
                </ul>
              </div>
            </div>

            {/* Detailed Steps - expanded */}
            <div
              style={{
                padding: '0.5rem 0.7rem',
                background: 'rgba(255,255,255,0.7)',
                borderRadius: '6px',
                borderLeft: '3px solid #10b981',
              }}
            >
              <div
                style={{
                  fontSize: '0.65rem',
                  fontWeight: 700,
                  color: '#065f46',
                  textTransform: 'uppercase',
                  marginBottom: '0.4rem',
                }}
              >
                {'\ud83d\udccb'} Detailed Pipeline Steps (10)
              </div>
              <ol
                style={{ fontSize: '0.72rem', color: '#064e3b', margin: 0, paddingLeft: '1.3rem' }}
              >
                {explanation.detailedSteps.map((s, i) => (
                  <li key={i} style={{ marginBottom: '0.3rem' }}>
                    <strong>{s.step}:</strong> <span style={{ color: '#047857' }}>{s.detail}</span>
                  </li>
                ))}
              </ol>
            </div>

            {/* Three-column: Testing, Monitoring, Pitfalls */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '0.5rem' }}>
              <div
                style={{
                  padding: '0.5rem 0.7rem',
                  background: 'rgba(255,255,255,0.7)',
                  borderRadius: '6px',
                  borderLeft: '3px solid #8b5cf6',
                }}
              >
                <div
                  style={{
                    fontSize: '0.65rem',
                    fontWeight: 700,
                    color: '#6d28d9',
                    textTransform: 'uppercase',
                    marginBottom: '0.3rem',
                  }}
                >
                  {'\ud83e\uddea'} Testing
                </div>
                <ul
                  style={{ fontSize: '0.68rem', color: '#5b21b6', margin: 0, paddingLeft: '1rem' }}
                >
                  {explanation.testing.map((t, i) => (
                    <li key={i} style={{ marginBottom: '0.1rem' }}>
                      {t}
                    </li>
                  ))}
                </ul>
              </div>
              <div
                style={{
                  padding: '0.5rem 0.7rem',
                  background: 'rgba(255,255,255,0.7)',
                  borderRadius: '6px',
                  borderLeft: '3px solid #3b82f6',
                }}
              >
                <div
                  style={{
                    fontSize: '0.65rem',
                    fontWeight: 700,
                    color: '#1e40af',
                    textTransform: 'uppercase',
                    marginBottom: '0.3rem',
                  }}
                >
                  {'\ud83d\udcca'} Monitoring
                </div>
                <ul
                  style={{ fontSize: '0.68rem', color: '#1e3a8a', margin: 0, paddingLeft: '1rem' }}
                >
                  {explanation.monitoring.map((m, i) => (
                    <li key={i} style={{ marginBottom: '0.1rem' }}>
                      {m}
                    </li>
                  ))}
                </ul>
              </div>
              <div
                style={{
                  padding: '0.5rem 0.7rem',
                  background: '#fef2f2',
                  borderRadius: '6px',
                  borderLeft: '3px solid #ef4444',
                }}
              >
                <div
                  style={{
                    fontSize: '0.65rem',
                    fontWeight: 700,
                    color: '#991b1b',
                    textTransform: 'uppercase',
                    marginBottom: '0.3rem',
                  }}
                >
                  {'\u26a0\ufe0f'} Common Pitfalls
                </div>
                <ul
                  style={{ fontSize: '0.68rem', color: '#7f1d1d', margin: 0, paddingLeft: '1rem' }}
                >
                  {explanation.pitfalls.map((p, i) => (
                    <li key={i} style={{ marginBottom: '0.1rem' }}>
                      {p}
                    </li>
                  ))}
                </ul>
              </div>
            </div>

            {/* Related Patterns */}
            <div
              style={{
                padding: '0.5rem 0.7rem',
                background: 'rgba(255,255,255,0.7)',
                borderRadius: '6px',
                borderLeft: '3px solid #ec4899',
              }}
            >
              <div
                style={{
                  fontSize: '0.65rem',
                  fontWeight: 700,
                  color: '#9f1239',
                  textTransform: 'uppercase',
                  marginBottom: '0.3rem',
                }}
              >
                {'\ud83d\udd17'} Related Patterns
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.3rem' }}>
                {explanation.relatedPatterns.map((r, i) => (
                  <span
                    key={i}
                    style={{
                      padding: '0.15rem 0.5rem',
                      background: '#fce7f3',
                      color: '#9f1239',
                      borderRadius: '10px',
                      fontSize: '0.68rem',
                    }}
                  >
                    {r}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </details>

        <div
          style={{
            marginTop: '0.5rem',
            padding: '0.4rem 0.7rem',
            background: 'rgba(253, 224, 71, 0.2)',
            borderRadius: '6px',
            fontSize: '0.72rem',
            color: '#713f12',
          }}
        >
          <strong>{'\ud83d\udcbc'} Business Impact:</strong> {explanation.impact}
        </div>
      </div>

      {/* Source → Target Path */}
      <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
        <div
          style={{
            flex: 1,
            minWidth: '180px',
            padding: '0.5rem 0.75rem',
            background: '#fef2f2',
            borderRadius: '6px',
            border: '1px solid #fecaca',
          }}
        >
          <div
            style={{
              fontSize: '0.6rem',
              color: '#991b1b',
              fontWeight: 700,
              textTransform: 'uppercase',
            }}
          >
            Source
          </div>
          <div style={{ fontSize: '0.8rem', fontFamily: 'monospace', marginTop: '0.15rem' }}>
            {data.source.path}
          </div>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>
            {data.source.format} | {data.beforeStats.rows.toLocaleString()} rows
          </div>
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            fontSize: '1.5rem',
            color: 'var(--text-secondary)',
          }}
        >
          {'\u2192'}
        </div>
        <div
          style={{
            flex: 1,
            minWidth: '180px',
            padding: '0.5rem 0.75rem',
            background: '#dcfce7',
            borderRadius: '6px',
            border: '1px solid #bbf7d0',
          }}
        >
          <div
            style={{
              fontSize: '0.6rem',
              color: '#166534',
              fontWeight: 700,
              textTransform: 'uppercase',
            }}
          >
            Target
          </div>
          <div style={{ fontSize: '0.8rem', fontFamily: 'monospace', marginTop: '0.15rem' }}>
            {data.target.path}
          </div>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>
            {data.target.format} | {data.afterStats.rows.toLocaleString()} rows
          </div>
        </div>
      </div>

      {/* File Format Selector + Run Command */}
      <FileFormatRunner scenario={scenario} data={data} processed={processed} />

      {/* Before / After Tables */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: '1rem',
          marginBottom: '1rem',
        }}
      >
        <div>
          <div
            style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.3rem' }}
          >
            <span
              style={{
                background: '#fee2e2',
                color: '#991b1b',
                padding: '0.1rem 0.4rem',
                borderRadius: '4px',
                fontSize: '0.6rem',
                fontWeight: 700,
              }}
            >
              BEFORE
            </span>
            <span style={{ fontSize: '0.72rem' }}>
              {data.beforeStats.rows.toLocaleString()} rows | {data.beforeStats.nulls} nulls |{' '}
              {data.beforeStats.dupes} dupes
            </span>
          </div>
          <MiniTable rows={data.before} badgeColor="#fef2f2" />
        </div>
        {processed ? (
          <div>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem',
                marginBottom: '0.3rem',
              }}
            >
              <span
                style={{
                  background: '#dcfce7',
                  color: '#166534',
                  padding: '0.1rem 0.4rem',
                  borderRadius: '4px',
                  fontSize: '0.6rem',
                  fontWeight: 700,
                }}
              >
                AFTER
              </span>
              <span style={{ fontSize: '0.72rem' }}>
                {data.afterStats.rows.toLocaleString()} rows | {data.afterStats.nulls} nulls |{' '}
                {data.afterStats.dupes} dupes
              </span>
            </div>
            <MiniTable rows={data.after} badgeColor="#dcfce7" />
          </div>
        ) : (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              border: '2px dashed var(--border)',
              borderRadius: '6px',
              color: 'var(--text-secondary)',
            }}
          >
            <div style={{ textAlign: 'center', padding: '1rem' }}>
              <div style={{ fontSize: '1.5rem' }}>{processing ? '\u23f3' : '\u25b6\ufe0f'}</div>
              <p style={{ fontSize: '0.8rem' }}>
                {processing ? 'Processing...' : 'Click "Run Process"'}
              </p>
            </div>
          </div>
        )}
      </div>

      {/* Process Button + Stats */}
      <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', marginBottom: '1rem' }}>
        <button className="btn btn-primary btn-sm" disabled={processing} onClick={runProcess}>
          {processing ? '\u23f3 Processing...' : processed ? '\u21bb Re-run' : '\u25b6 Run Process'}
        </button>
        {processed && (
          <div style={{ display: 'flex', gap: '1rem', fontSize: '0.8rem', flexWrap: 'wrap' }}>
            <span style={{ color: 'var(--success)' }}>Completed in {duration}s</span>
            <span>
              Rows: <b style={{ color: '#991b1b' }}>{data.beforeStats.rows.toLocaleString()}</b>{' '}
              {'\u2192'} <b style={{ color: '#166534' }}>{data.afterStats.rows.toLocaleString()}</b>
            </span>
            <span>
              Nulls: <b style={{ color: '#991b1b' }}>{data.beforeStats.nulls}</b> {'\u2192'}{' '}
              <b style={{ color: '#166534' }}>{data.afterStats.nulls}</b>
            </span>
          </div>
        )}
      </div>

      {/* Code Approaches */}
      <div style={{ border: '1px solid var(--border)', borderRadius: '8px', overflow: 'hidden' }}>
        <div
          style={{
            padding: '0.5rem 0.75rem',
            background: '#f8f9fa',
            borderBottom: '1px solid var(--border)',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <strong style={{ fontSize: '0.82rem' }}>{approaches.length} Code Approaches</strong>
          <span style={{ fontSize: '0.68rem', color: 'var(--text-secondary)' }}>
            Click each tab
          </span>
        </div>
        <div
          style={{ display: 'flex', borderBottom: '1px solid var(--border)', overflowX: 'auto' }}
        >
          {approaches.map((a, i) => (
            <button
              key={i}
              onClick={() => setActiveTab(i)}
              style={{
                padding: '0.4rem 0.7rem',
                border: 'none',
                cursor: 'pointer',
                fontSize: '0.73rem',
                whiteSpace: 'nowrap',
                background: activeTab === i ? '#fff' : '#f8f9fa',
                borderBottom:
                  activeTab === i ? '2px solid var(--primary)' : '2px solid transparent',
                color: activeTab === i ? 'var(--primary)' : 'var(--text-secondary)',
                fontWeight: activeTab === i ? 600 : 400,
              }}
            >
              {a.icon} {a.name}
            </button>
          ))}
        </div>
        <div style={{ padding: '0.75rem' }}>
          {/* Meta: difficulty, pros, cons */}
          {approaches[activeTab] && (
            <div
              style={{
                display: 'flex',
                gap: '0.75rem',
                marginBottom: '0.6rem',
                flexWrap: 'wrap',
                fontSize: '0.72rem',
              }}
            >
              <span>
                <b>Difficulty:</b>{' '}
                <span
                  style={{
                    padding: '0.1rem 0.4rem',
                    borderRadius: '3px',
                    fontSize: '0.68rem',
                    background:
                      approaches[activeTab].difficulty === 'Beginner'
                        ? '#dcfce7'
                        : approaches[activeTab].difficulty === 'Intermediate'
                          ? '#fef3c7'
                          : approaches[activeTab].difficulty === 'Advanced'
                            ? '#fee2e2'
                            : '#e0e7ff',
                    color:
                      approaches[activeTab].difficulty === 'Beginner'
                        ? '#166534'
                        : approaches[activeTab].difficulty === 'Intermediate'
                          ? '#92400e'
                          : approaches[activeTab].difficulty === 'Advanced'
                            ? '#991b1b'
                            : '#3730a3',
                  }}
                >
                  {approaches[activeTab].difficulty}
                </span>
              </span>
              <span style={{ color: '#166534' }}>
                <b>Pros:</b> {approaches[activeTab].pros}
              </span>
              <span style={{ color: '#991b1b' }}>
                <b>Cons:</b> {approaches[activeTab].cons}
              </span>
            </div>
          )}
          <div className="code-block" style={{ overflowX: 'auto' }}>
            {(approaches[activeTab]?.code || '').split('\n').map((line, i) => {
              const trimmed = line.trimStart();
              const isComment =
                trimmed.startsWith('#') ||
                trimmed.startsWith('--') ||
                trimmed.startsWith('//') ||
                trimmed.startsWith('*') ||
                trimmed.startsWith('/*');
              return (
                <div
                  key={i}
                  style={{
                    color: isComment ? '#92400e' : '#1e40af',
                    fontStyle: isComment ? 'italic' : 'normal',
                    whiteSpace: 'pre',
                  }}
                >
                  {line || '\u00a0'}
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

export default ScenarioCard;
