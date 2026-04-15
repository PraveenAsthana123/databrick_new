import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

// ═══════════════════════════════════════════════════════════
// 50 Batch Ingestion Scenarios with Scoring Matrix
// ═══════════════════════════════════════════════════════════

const batchScenarios = [
  // ─── 1–10: Structured Enterprise Sources (OLTP → Lakehouse) ───
  {
    id: 1,
    group: 'Enterprise OLTP',
    title: 'Daily RDBMS Full Load',
    desc: 'Oracle full table extract to Delta Lake on daily schedule',
    source: 'Oracle DB',
    target: 'Delta Lake',
    complexity: 2,
    volume: 3,
    latency: 2,
    reliability: 4,
    governance: 3,
    score: 2.8,
    code: `# Daily RDBMS Full Load — Oracle → Delta
df = spark.read.format("jdbc") \\
    .option("url", "jdbc:oracle:thin:@//host:1521/ORCL") \\
    .option("dbtable", "SALES.ORDERS") \\
    .option("user", dbutils.secrets.get("scope", "oracle_user")) \\
    .option("password", dbutils.secrets.get("scope", "oracle_pass")) \\
    .option("fetchsize", "50000") \\
    .option("numPartitions", "8") \\
    .option("partitionColumn", "ORDER_ID") \\
    .option("lowerBound", "1") \\
    .option("upperBound", "10000000") \\
    .load()

df.withColumn("_ingest_ts", current_timestamp()) \\
    .write.format("delta").mode("overwrite") \\
    .saveAsTable("catalog.bronze.oracle_orders")`,
    beforeData: [
      {
        ORDER_ID: 1001,
        CUST_NAME: ' ALICE ',
        AMOUNT: '1500.50',
        ORDER_DATE: '15-JAN-2024',
        STATUS: 'SHIPPED',
      },
      { ORDER_ID: 1002, CUST_NAME: 'BOB', AMOUNT: '0', ORDER_DATE: '16-JAN-2024', STATUS: null },
      {
        ORDER_ID: 1001,
        CUST_NAME: ' ALICE ',
        AMOUNT: '1500.50',
        ORDER_DATE: '15-JAN-2024',
        STATUS: 'SHIPPED',
      },
      { ORDER_ID: 1003, CUST_NAME: null, AMOUNT: '-100', ORDER_DATE: 'INVALID', STATUS: 'PENDING' },
    ],
    afterData: [
      {
        order_id: 1001,
        cust_name: 'ALICE',
        amount: 1500.5,
        order_date: '2024-01-15',
        status: 'SHIPPED',
        _ingest_ts: '2024-04-15T10:00:00Z',
      },
      {
        order_id: 1002,
        cust_name: 'BOB',
        amount: 0,
        order_date: '2024-01-16',
        status: 'UNKNOWN',
        _ingest_ts: '2024-04-15T10:00:00Z',
      },
    ],
    beforeStats: { rows: 5000000, nulls: 12500, dupes: 3200 },
    afterStats: { rows: 4984300, nulls: 0, dupes: 0 },
  },
  {
    id: 2,
    group: 'Enterprise OLTP',
    title: 'Incremental CDC Batch',
    desc: 'SQL Server change data capture with merge into Delta',
    source: 'SQL Server',
    target: 'Delta Lake',
    complexity: 3,
    volume: 3,
    latency: 3,
    reliability: 5,
    governance: 4,
    score: 3.6,
    code: `# Incremental CDC — SQL Server → Delta (MERGE)
from delta.tables import DeltaTable

# Read only changed records since last watermark
watermark = spark.sql("SELECT MAX(_ingest_ts) FROM catalog.bronze.sql_orders").collect()[0][0]

incremental = spark.read.format("jdbc") \\
    .option("url", "jdbc:sqlserver://host:1433;database=Sales") \\
    .option("dbtable", f"(SELECT * FROM orders WHERE modified_at > '{watermark}') sub") \\
    .load()

# MERGE into target
target = DeltaTable.forName(spark, "catalog.bronze.sql_orders")
target.alias("t").merge(incremental.alias("s"), "t.order_id = s.order_id") \\
    .whenMatchedUpdateAll() \\
    .whenNotMatchedInsertAll() \\
    .execute()`,
    beforeData: [
      {
        order_id: 2001,
        customer: 'Acme Corp',
        amount: 5000,
        modified_at: '2024-04-14T23:00:00Z',
        __$operation: 2,
      },
      {
        order_id: 2002,
        customer: 'Beta Inc',
        amount: 3000,
        modified_at: '2024-04-15T01:00:00Z',
        __$operation: 4,
      },
      {
        order_id: 2003,
        customer: 'New Corp',
        amount: 1500,
        modified_at: '2024-04-15T02:00:00Z',
        __$operation: 1,
      },
    ],
    afterData: [
      {
        order_id: 2001,
        customer: 'Acme Corp',
        amount: 5000,
        modified_at: '2024-04-14T23:00:00Z',
        _merge_action: 'UPDATE',
      },
      {
        order_id: 2002,
        customer: 'Beta Inc',
        amount: 3500,
        modified_at: '2024-04-15T01:00:00Z',
        _merge_action: 'UPDATE',
      },
      {
        order_id: 2003,
        customer: 'New Corp',
        amount: 1500,
        modified_at: '2024-04-15T02:00:00Z',
        _merge_action: 'INSERT',
      },
    ],
    beforeStats: { rows: 15000, nulls: 0, dupes: 0 },
    afterStats: { rows: 5015000, nulls: 0, dupes: 0 },
  },
  {
    id: 3,
    group: 'Enterprise OLTP',
    title: 'SAP BW Extract',
    desc: 'SAP Business Warehouse data extraction via RFC/BAPI to Bronze',
    source: 'SAP BW',
    target: 'Bronze Delta',
    complexity: 4,
    volume: 4,
    latency: 3,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `# SAP BW Extract — via JDBC or PyRFC
# Option 1: JDBC via SAP HANA
df = spark.read.format("jdbc") \\
    .option("url", "jdbc:sap://hana-host:30015") \\
    .option("dbtable", "/BIC/AZFINANCE") \\
    .option("user", dbutils.secrets.get("sap", "user")) \\
    .option("password", dbutils.secrets.get("sap", "pass")) \\
    .load()

# Option 2: PyRFC (for BW extractors)
# from pyrfc import Connection
# conn = Connection(ashost='sap-host', sysnr='00', client='100', ...)
# result = conn.call('RFC_READ_TABLE', QUERY_TABLE='VBRK', FIELDS=[...])

df.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.sap_finance")`,
    beforeData: [
      {
        BUKRS: '1000',
        GJAHR: '2024',
        BELNR: '500001',
        DMBTR: 15000.0,
        WAERS: 'USD',
        BUDAT: '20240115',
      },
    ],
    afterData: [
      {
        company_code: '1000',
        fiscal_year: 2024,
        doc_number: '500001',
        amount_lc: 15000.0,
        currency: 'USD',
        posting_date: '2024-01-15',
      },
    ],
    beforeStats: { rows: 2000000, nulls: 5000, dupes: 0 },
    afterStats: { rows: 2000000, nulls: 0, dupes: 0 },
  },
  {
    id: 4,
    group: 'Enterprise OLTP',
    title: 'CRM Data Load',
    desc: 'Salesforce CRM data via Bulk API to Delta Lake',
    source: 'Salesforce',
    target: 'Delta Lake',
    complexity: 3,
    volume: 2,
    latency: 2,
    reliability: 4,
    governance: 4,
    score: 3.0,
    code: `# Salesforce CRM → Delta via simple-salesforce
from simple_salesforce import Salesforce
sf = Salesforce(username=dbutils.secrets.get("sf","user"),
    password=dbutils.secrets.get("sf","pass"),
    security_token=dbutils.secrets.get("sf","token"))

# Bulk query
records = sf.bulk.Account.query("SELECT Id, Name, Industry, AnnualRevenue, CreatedDate FROM Account")
df = spark.createDataFrame(records)
df.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.sf_accounts")`,
    beforeData: [
      {
        Id: '001A000001',
        Name: 'Acme Corp',
        Industry: 'Technology',
        AnnualRevenue: 5000000,
        CreatedDate: '2020-03-15T00:00:00Z',
      },
    ],
    afterData: [
      {
        id: '001A000001',
        name: 'Acme Corp',
        industry: 'Technology',
        annual_revenue: 5000000,
        created_date: '2020-03-15',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 150000, nulls: 8000, dupes: 200 },
    afterStats: { rows: 149800, nulls: 0, dupes: 0 },
  },
  {
    id: 5,
    group: 'Enterprise OLTP',
    title: 'ERP Finance Batch',
    desc: 'SAP S/4HANA finance data via OData to Delta Lake',
    source: 'SAP S/4',
    target: 'Delta Lake',
    complexity: 4,
    volume: 4,
    latency: 4,
    reliability: 5,
    governance: 5,
    score: 4.4,
    code: `# SAP S/4HANA Finance via OData
import requests
url = "https://s4hana.company.com/sap/opu/odata/sap/API_JOURNAL_ENTRY_ITEM_SRV/A_JournalEntryItem"
headers = {"Authorization": f"Bearer {dbutils.secrets.get('sap','oauth_token')}"}
response = requests.get(url, headers=headers, params={"$top": 50000, "$filter": "PostingDate ge '2024-01-01'"}, timeout=60)
data = response.json()["d"]["results"]
df = spark.createDataFrame(data)
df.write.format("delta").mode("append").saveAsTable("catalog.bronze.sap_journal_entries")`,
    beforeData: [
      {
        CompanyCode: '1000',
        FiscalYear: '2024',
        JournalEntry: 'JE001',
        AmountInCompanyCodeCurrency: 25000.0,
      },
    ],
    afterData: [
      {
        company_code: '1000',
        fiscal_year: 2024,
        journal_entry: 'JE001',
        amount: 25000.0,
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 8000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 8000000, nulls: 0, dupes: 0 },
  },
  {
    id: 6,
    group: 'Enterprise OLTP',
    title: 'HR Payroll Ingestion',
    desc: 'Workday HR and payroll data to Lakehouse',
    source: 'Workday',
    target: 'Lakehouse',
    complexity: 3,
    volume: 2,
    latency: 2,
    reliability: 4,
    governance: 5,
    score: 3.2,
    code: `# Workday HR → Delta via REST API (RaaS Report)
import requests
url = "https://wd5-impl-services1.workday.com/ccx/service/tenant/Report-As-A-Service"
auth = (dbutils.secrets.get("wd","user"), dbutils.secrets.get("wd","pass"))
response = requests.get(url, auth=auth, params={"format": "json"}, timeout=30)
employees = response.json()["Report_Entry"]
df = spark.createDataFrame(employees)
df.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.hr_employees")`,
    beforeData: [
      {
        Worker_ID: 'EMP001',
        Full_Name: 'Alice Johnson',
        Department: 'Engineering',
        Annual_Salary: 120000,
      },
    ],
    afterData: [
      {
        worker_id: 'EMP001',
        full_name: 'Alice Johnson',
        department: 'Engineering',
        salary: 120000,
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 50000, nulls: 250, dupes: 0 },
    afterStats: { rows: 50000, nulls: 0, dupes: 0 },
  },
  {
    id: 7,
    group: 'Enterprise OLTP',
    title: 'POS Sales Batch',
    desc: 'Point-of-sale transaction data from retail stores',
    source: 'POS DB',
    target: 'Delta Lake',
    complexity: 3,
    volume: 4,
    latency: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `# POS Sales → Delta (multi-store JDBC)
stores = ["store_nyc", "store_la", "store_chi", "store_sf"]
for store in stores:
    df = spark.read.format("jdbc") \\
        .option("url", f"jdbc:mysql://{store}-db:3306/pos") \\
        .option("dbtable", "daily_transactions") \\
        .option("user", dbutils.secrets.get("pos", "user")) \\
        .option("password", dbutils.secrets.get("pos", "pass")) \\
        .load() \\
        .withColumn("store_id", lit(store))
    df.write.format("delta").mode("append").saveAsTable("catalog.bronze.pos_sales")`,
    beforeData: [
      {
        txn_id: 'T001',
        store: 'NYC',
        items: 3,
        total: 45.99,
        payment: 'CARD',
        ts: '2024-04-15 14:30:00',
      },
    ],
    afterData: [
      {
        txn_id: 'T001',
        store_id: 'store_nyc',
        items: 3,
        total: 45.99,
        payment: 'CARD',
        txn_date: '2024-04-15',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 15000000, nulls: 500, dupes: 1200 },
    afterStats: { rows: 14998800, nulls: 0, dupes: 0 },
  },
  {
    id: 8,
    group: 'Enterprise OLTP',
    title: 'Banking Transactions',
    desc: 'Core banking transaction data with PII masking',
    source: 'Core Banking',
    target: 'Delta Lake',
    complexity: 4,
    volume: 5,
    latency: 4,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `# Banking Transactions → Delta with PII Masking
from pyspark.sql.functions import sha2, concat, lit

df = spark.read.format("jdbc") \\
    .option("url", "jdbc:db2://mainframe:50000/BANKDB") \\
    .option("dbtable", "DAILY_TRANSACTIONS") \\
    .load()

# Mask PII before writing to Bronze
masked = df \\
    .withColumn("account_hash", sha2(concat(col("account_no"), lit("salt")), 256)) \\
    .withColumn("ssn_masked", lit("***-**-") + col("ssn").substr(-4, 4)) \\
    .drop("account_no", "ssn")

masked.write.format("delta").mode("append") \\
    .option("dataChange", "true") \\
    .saveAsTable("catalog.bronze.bank_transactions")`,
    beforeData: [
      {
        txn_id: 'BT001',
        account_no: '1234567890',
        ssn: '123-45-6789',
        amount: 5000.0,
        type: 'TRANSFER',
      },
    ],
    afterData: [
      {
        txn_id: 'BT001',
        account_hash: 'a3f2c1...',
        ssn_masked: '***-**-6789',
        amount: 5000.0,
        type: 'TRANSFER',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 50000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 50000000, nulls: 0, dupes: 0 },
  },
  {
    id: 9,
    group: 'Enterprise OLTP',
    title: 'Inventory System Load',
    desc: 'Warehouse inventory management system daily extract',
    source: 'Warehouse DB',
    target: 'Delta Lake',
    complexity: 2,
    volume: 3,
    latency: 2,
    reliability: 4,
    governance: 3,
    score: 2.8,
    code: `df = spark.read.format("jdbc") \\
    .option("url", "jdbc:postgresql://warehouse-db:5432/inventory") \\
    .option("dbtable", "stock_levels") \\
    .load()
df.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.inventory")`,
    beforeData: [
      { sku: 'SKU-001', warehouse: 'WH-NYC', quantity: 500, last_updated: '2024-04-15' },
    ],
    afterData: [
      {
        sku: 'SKU-001',
        warehouse: 'WH-NYC',
        quantity: 500,
        last_updated: '2024-04-15',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 500000, nulls: 100, dupes: 0 },
    afterStats: { rows: 500000, nulls: 0, dupes: 0 },
  },
  {
    id: 10,
    group: 'Enterprise OLTP',
    title: 'Customer Master Load',
    desc: 'Master Data Management customer golden record sync',
    source: 'MDM',
    target: 'Delta Lake',
    complexity: 3,
    volume: 3,
    latency: 2,
    reliability: 5,
    governance: 5,
    score: 3.6,
    code: `df = spark.read.format("jdbc") \\
    .option("url", "jdbc:oracle:thin:@//mdm-host:1521/MDM") \\
    .option("dbtable", "CUSTOMER_GOLDEN_RECORD") \\
    .load()
df.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.customer_master")`,
    beforeData: [
      {
        cust_id: 'C001',
        name: 'Alice Corp',
        segment: 'Enterprise',
        region: 'NA',
        created: '2020-01-15',
      },
    ],
    afterData: [
      {
        cust_id: 'C001',
        name: 'Alice Corp',
        segment: 'Enterprise',
        region: 'NA',
        created: '2020-01-15',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 2000000, nulls: 5000, dupes: 800 },
    afterStats: { rows: 1999200, nulls: 0, dupes: 0 },
  },

  // ─── 11–20: File-Based Batch Ingestion ───
  {
    id: 11,
    group: 'File-Based',
    title: 'CSV Bulk Ingestion',
    desc: 'S3/ADLS CSV files with Auto Loader to Bronze',
    source: 'S3/ADLS',
    target: 'Bronze Delta',
    complexity: 2,
    volume: 4,
    latency: 2,
    reliability: 3,
    governance: 2,
    score: 2.6,
    code: `df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://data-lake/csv/")
df.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.csv_bulk")`,
    beforeData: [{ id: '1', name: 'Alice', value: '100.5' }],
    afterData: [{ id: 1, name: 'Alice', value: 100.5, _ingest_ts: '2024-04-15' }],
    beforeStats: { rows: 10000000, nulls: 50000, dupes: 25000 },
    afterStats: { rows: 9925000, nulls: 0, dupes: 0 },
  },
  {
    id: 12,
    group: 'File-Based',
    title: 'JSON Logs Batch',
    desc: 'Application log files in JSON format to Delta',
    source: 'Log Files',
    target: 'Delta Lake',
    complexity: 3,
    volume: 5,
    latency: 3,
    reliability: 4,
    governance: 2,
    score: 3.4,
    code: `df = spark.read.json("s3://logs/app/*.json")\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.app_logs")`,
    beforeData: [
      {
        level: 'ERROR',
        message: 'Connection timeout',
        ts: '2024-04-15T10:30:00Z',
        service: 'api-gw',
      },
    ],
    afterData: [
      {
        level: 'ERROR',
        message: 'Connection timeout',
        event_ts: '2024-04-15T10:30:00Z',
        service: 'api-gw',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 50000000, nulls: 100000, dupes: 0 },
    afterStats: { rows: 50000000, nulls: 0, dupes: 0 },
  },
  {
    id: 13,
    group: 'File-Based',
    title: 'XML Ingestion',
    desc: 'External partner XML files parsing and Delta load',
    source: 'External XML',
    target: 'Delta Lake',
    complexity: 4,
    volume: 3,
    latency: 2,
    reliability: 4,
    governance: 3,
    score: 3.2,
    code: `df = spark.read.format("com.databricks.spark.xml").option("rowTag","order").load("/mnt/xml/")
df.write.format("delta").saveAsTable("catalog.bronze.xml_orders")`,
    beforeData: [{ order_id: 'X001', item: 'Widget', qty: 10, price: 29.99 }],
    afterData: [
      { order_id: 'X001', item: 'Widget', qty: 10, price: 29.99, _ingest_ts: '2024-04-15' },
    ],
    beforeStats: { rows: 500000, nulls: 2000, dupes: 100 },
    afterStats: { rows: 499900, nulls: 0, dupes: 0 },
  },
  {
    id: 14,
    group: 'File-Based',
    title: 'Parquet Ingestion',
    desc: 'Data lake Parquet files to Delta with partition pruning',
    source: 'Data Lake',
    target: 'Delta Lake',
    complexity: 2,
    volume: 5,
    latency: 2,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `df = spark.read.parquet("s3://data-lake/parquet/year=2024/")\ndf.write.format("delta").partitionBy("month").saveAsTable("catalog.bronze.parquet_data")`,
    beforeData: [{ user_id: 101, event: 'login', month: 1 }],
    afterData: [{ user_id: 101, event: 'login', month: 1, _ingest_ts: '2024-04-15' }],
    beforeStats: { rows: 100000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 100000000, nulls: 0, dupes: 0 },
  },
  {
    id: 15,
    group: 'File-Based',
    title: 'Excel Ingestion',
    desc: 'SharePoint Excel spreadsheets via pandas to Delta',
    source: 'SharePoint',
    target: 'Delta Lake',
    complexity: 3,
    volume: 2,
    latency: 1,
    reliability: 3,
    governance: 3,
    score: 2.4,
    code: `import pandas as pd\npdf = pd.read_excel("/dbfs/mnt/sharepoint/report.xlsx", sheet_name="Data")\ndf = spark.createDataFrame(pdf)\ndf.write.format("delta").saveAsTable("catalog.bronze.excel_data")`,
    beforeData: [{ Region: 'North', Sales: 15000, Quarter: 'Q1' }],
    afterData: [{ region: 'North', sales: 15000, quarter: 'Q1', _ingest_ts: '2024-04-15' }],
    beforeStats: { rows: 50000, nulls: 500, dupes: 100 },
    afterStats: { rows: 49900, nulls: 0, dupes: 0 },
  },
  {
    id: 16,
    group: 'File-Based',
    title: 'Fixed-Width Files',
    desc: 'Legacy mainframe fixed-width format files',
    source: 'Legacy System',
    target: 'Delta Lake',
    complexity: 4,
    volume: 3,
    latency: 2,
    reliability: 4,
    governance: 3,
    score: 3.2,
    code: `from pyspark.sql.types import *\nschema = StructType([StructField("acct",StringType()),StructField("name",StringType()),StructField("bal",StringType())])\ndf = spark.read.text("/mnt/legacy/fixed_width.dat")\nparsed = df.select(df.value.substr(1,10).alias("acct"),df.value.substr(11,30).alias("name"),df.value.substr(41,15).alias("bal"))\nparsed.write.format("delta").saveAsTable("catalog.bronze.legacy_data")`,
    beforeData: [{ raw_line: '1234567890Alice Johnson                  000015000.50' }],
    afterData: [{ acct: '1234567890', name: 'Alice Johnson', bal: 15000.5 }],
    beforeStats: { rows: 1000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 1000000, nulls: 0, dupes: 0 },
  },
  {
    id: 17,
    group: 'File-Based',
    title: 'Multi-File Batch Merge',
    desc: 'Merge multiple file drops from folder into single Delta',
    source: 'Folder Drop',
    target: 'Delta Lake',
    complexity: 3,
    volume: 4,
    latency: 3,
    reliability: 4,
    governance: 2,
    score: 3.2,
    code: `from functools import reduce\nfiles = dbutils.fs.ls("/mnt/daily_drops/")\ndfs = [spark.read.csv(f.path, header=True) for f in files if f.name.endswith(".csv")]\nmerged = reduce(lambda a,b: a.unionByName(b, allowMissingColumns=True), dfs)\nmerged.write.format("delta").mode("append").saveAsTable("catalog.bronze.daily_merged")`,
    beforeData: [
      { file: 'drop_01.csv', rows: 5000 },
      { file: 'drop_02.csv', rows: 3000 },
    ],
    afterData: [{ id: 1, source_file: 'drop_01.csv', _ingest_ts: '2024-04-15' }],
    beforeStats: { rows: 50000, nulls: 200, dupes: 500 },
    afterStats: { rows: 49500, nulls: 0, dupes: 0 },
  },
  {
    id: 18,
    group: 'File-Based',
    title: 'Daily Report Ingestion',
    desc: 'FTP daily report files to Delta Lake',
    source: 'FTP Server',
    target: 'Delta Lake',
    complexity: 2,
    volume: 2,
    latency: 2,
    reliability: 3,
    governance: 3,
    score: 2.4,
    code: `# Download from FTP first, then ingest\nimport ftplib\nftp = ftplib.FTP("ftp.company.com")\nftp.login(dbutils.secrets.get("ftp","user"), dbutils.secrets.get("ftp","pass"))\nftp.retrbinary("RETR daily_report.csv", open("/tmp/report.csv","wb").write)\ndf = spark.read.csv("/tmp/report.csv", header=True)\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.daily_reports")`,
    beforeData: [{ date: '2024-04-15', metric: 'revenue', value: 125000 }],
    afterData: [
      { date: '2024-04-15', metric: 'revenue', value: 125000.0, _ingest_ts: '2024-04-15' },
    ],
    beforeStats: { rows: 1000, nulls: 0, dupes: 0 },
    afterStats: { rows: 1000, nulls: 0, dupes: 0 },
  },
  {
    id: 19,
    group: 'File-Based',
    title: 'Archive File Ingestion',
    desc: 'Cold storage archive data restoration to Delta',
    source: 'Cold Storage',
    target: 'Delta Lake',
    complexity: 2,
    volume: 5,
    latency: 1,
    reliability: 3,
    governance: 2,
    score: 2.6,
    code: `# Restore from archive tier first\ndf = spark.read.parquet("s3://archive-bucket/2023/")\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.archive_2023")`,
    beforeData: [{ id: 1, data: 'archived_record', year: 2023 }],
    afterData: [{ id: 1, data: 'archived_record', year: 2023, _restored_at: '2024-04-15' }],
    beforeStats: { rows: 500000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 500000000, nulls: 0, dupes: 0 },
  },
  {
    id: 20,
    group: 'File-Based',
    title: 'Schema-Evolving Files',
    desc: 'Handle files with changing schemas using mergeSchema',
    source: 'Data Lake',
    target: 'Delta Lake',
    complexity: 5,
    volume: 4,
    latency: 3,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `df = spark.read.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\
    .option("cloudFiles.schemaLocation", "/mnt/schema/evolving") \\
    .load("/mnt/landing/evolving/")
df.writeStream.format("delta") \\
    .option("checkpointLocation", "/mnt/cp/evolving") \\
    .option("mergeSchema", "true") \\
    .trigger(availableNow=True) \\
    .toTable("catalog.bronze.evolving_data")`,
    beforeData: [{ id: 1, name: 'Alice', v1_field: 'old' }],
    afterData: [{ id: 1, name: 'Alice', v1_field: 'old', v2_new_field: null, _schema_version: 2 }],
    beforeStats: { rows: 5000000, nulls: 50000, dupes: 0 },
    afterStats: { rows: 5000000, nulls: 0, dupes: 0 },
  },

  // ─── 21–30: Semi-Structured & Unstructured ───
  {
    id: 21,
    group: 'Semi/Unstructured',
    title: 'PDF Ingestion (RAG Prep)',
    desc: 'Extract text from PDFs for RAG vector pipeline',
    source: 'Documents',
    target: 'Delta + Vector',
    complexity: 5,
    volume: 4,
    latency: 3,
    reliability: 4,
    governance: 5,
    score: 4.2,
    code: `# PDF → Text → Chunks → Delta\nfrom pypdf import PdfReader\nimport os\npdfs = dbutils.fs.ls("/mnt/documents/pdfs/")\nall_chunks = []\nfor pdf_file in pdfs:\n    reader = PdfReader(f"/dbfs{pdf_file.path}")\n    text = "\\n".join([p.extract_text() for p in reader.pages])\n    chunks = [text[i:i+500] for i in range(0, len(text), 450)]\n    for i, chunk in enumerate(chunks):\n        all_chunks.append({"source": pdf_file.name, "chunk_id": i, "text": chunk})\ndf = spark.createDataFrame(all_chunks)\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.pdf_chunks")`,
    beforeData: [{ file: 'report.pdf', pages: 45, size_kb: 2500 }],
    afterData: [
      { source: 'report.pdf', chunk_id: 0, text: 'Executive Summary...', char_count: 500 },
    ],
    beforeStats: { rows: 500, nulls: 0, dupes: 0 },
    afterStats: { rows: 15000, nulls: 0, dupes: 0 },
  },
  {
    id: 22,
    group: 'Semi/Unstructured',
    title: 'Image Metadata Ingestion',
    desc: 'Extract EXIF metadata from images to Delta',
    source: 'Image Store',
    target: 'Delta Lake',
    complexity: 3,
    volume: 4,
    latency: 2,
    reliability: 3,
    governance: 3,
    score: 3.0,
    code: `df = spark.read.format("binaryFile").load("/mnt/images/")\ndf.select("path","length","modificationTime").write.format("delta").saveAsTable("catalog.bronze.image_meta")`,
    beforeData: [{ path: '/images/photo.jpg', size: 2500000, modified: '2024-04-15' }],
    afterData: [
      {
        file_path: '/images/photo.jpg',
        size_bytes: 2500000,
        modified_at: '2024-04-15',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 100000, nulls: 0, dupes: 500 },
    afterStats: { rows: 99500, nulls: 0, dupes: 0 },
  },
  {
    id: 23,
    group: 'Semi/Unstructured',
    title: 'Audio Batch Ingestion',
    desc: 'Audio file metadata and transcription to Lake',
    source: 'Audio Files',
    target: 'Delta Lake',
    complexity: 4,
    volume: 4,
    latency: 2,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `# Audio metadata ingestion\ndf = spark.read.format("binaryFile").load("/mnt/audio/")\ndf.select("path","length").write.format("delta").saveAsTable("catalog.bronze.audio_files")`,
    beforeData: [{ file: 'call_001.wav', duration_sec: 300 }],
    afterData: [{ file: 'call_001.wav', duration_sec: 300, _ingest_ts: '2024-04-15' }],
    beforeStats: { rows: 50000, nulls: 0, dupes: 0 },
    afterStats: { rows: 50000, nulls: 0, dupes: 0 },
  },
  {
    id: 24,
    group: 'Semi/Unstructured',
    title: 'Video Metadata Ingestion',
    desc: 'Video file metadata extraction to Delta',
    source: 'Video Store',
    target: 'Delta Lake',
    complexity: 4,
    volume: 5,
    latency: 2,
    reliability: 3,
    governance: 2,
    score: 3.2,
    code: `df = spark.read.format("binaryFile").load("/mnt/video/")\ndf.select("path","length","modificationTime").write.format("delta").saveAsTable("catalog.bronze.video_meta")`,
    beforeData: [{ file: 'training.mp4', size_gb: 1.2 }],
    afterData: [{ file: 'training.mp4', size_gb: 1.2, _ingest_ts: '2024-04-15' }],
    beforeStats: { rows: 10000, nulls: 0, dupes: 0 },
    afterStats: { rows: 10000, nulls: 0, dupes: 0 },
  },
  {
    id: 25,
    group: 'Semi/Unstructured',
    title: 'Log Archive Ingestion',
    desc: 'Historical log archive restoration',
    source: 'Log Archive',
    target: 'Delta Lake',
    complexity: 3,
    volume: 5,
    latency: 2,
    reliability: 4,
    governance: 2,
    score: 3.2,
    code: `df = spark.read.text("/mnt/logs/archive/2023/")\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.log_archive")`,
    beforeData: [{ line: '2023-01-15 ERROR Connection timeout' }],
    afterData: [{ timestamp: '2023-01-15', level: 'ERROR', message: 'Connection timeout' }],
    beforeStats: { rows: 500000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 500000000, nulls: 0, dupes: 0 },
  },
  {
    id: 26,
    group: 'Semi/Unstructured',
    title: 'Email Ingestion',
    desc: 'Exchange/Outlook email data to Delta',
    source: 'Exchange',
    target: 'Delta Lake',
    complexity: 4,
    volume: 3,
    latency: 2,
    reliability: 4,
    governance: 5,
    score: 3.6,
    code: `# Microsoft Graph API for emails\nimport requests\nheaders = {"Authorization": f"Bearer {dbutils.secrets.get('ms','token')}"}\nresponse = requests.get("https://graph.microsoft.com/v1.0/users/inbox/messages", headers=headers, timeout=30)\nemails = response.json()["value"]\ndf = spark.createDataFrame(emails)\ndf.write.format("delta").saveAsTable("catalog.bronze.emails")`,
    beforeData: [{ subject: 'Q1 Report', from: 'alice@company.com', date: '2024-04-15' }],
    afterData: [
      {
        subject: 'Q1 Report',
        sender: 'alice@company.com',
        received_date: '2024-04-15',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 100000, nulls: 500, dupes: 200 },
    afterStats: { rows: 99800, nulls: 0, dupes: 0 },
  },
  {
    id: 27,
    group: 'Semi/Unstructured',
    title: 'Document Ingestion (SharePoint)',
    desc: 'SharePoint document library files to Delta',
    source: 'SharePoint',
    target: 'Delta Lake',
    complexity: 4,
    volume: 3,
    latency: 2,
    reliability: 5,
    governance: 5,
    score: 3.8,
    code: `# SharePoint via Graph API\nimport requests\nheaders = {"Authorization": f"Bearer {dbutils.secrets.get('sp','token')}"}\nfiles = requests.get("https://graph.microsoft.com/v1.0/sites/{site}/drives/{drive}/items", headers=headers, timeout=30).json()["value"]\ndf = spark.createDataFrame(files)\ndf.write.format("delta").saveAsTable("catalog.bronze.sharepoint_docs")`,
    beforeData: [{ name: 'policy.docx', size: 250000, modified: '2024-04-10' }],
    afterData: [
      {
        name: 'policy.docx',
        size_bytes: 250000,
        modified_at: '2024-04-10',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 25000, nulls: 0, dupes: 0 },
    afterStats: { rows: 25000, nulls: 0, dupes: 0 },
  },
  {
    id: 28,
    group: 'Semi/Unstructured',
    title: 'Social Media Batch',
    desc: 'Social media data via APIs to Delta',
    source: 'Social APIs',
    target: 'Delta Lake',
    complexity: 4,
    volume: 4,
    latency: 3,
    reliability: 3,
    governance: 2,
    score: 3.2,
    code: `# Twitter/X API batch\nimport requests\nheaders = {"Authorization": f"Bearer {dbutils.secrets.get('twitter','token')}"}\ntweets = requests.get("https://api.twitter.com/2/tweets/search/recent", headers=headers, params={"query": "databricks"}, timeout=30).json()["data"]\ndf = spark.createDataFrame(tweets)\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.social_media")`,
    beforeData: [
      { id: 'tw001', text: 'Great talk on Databricks!', likes: 42, created: '2024-04-15' },
    ],
    afterData: [
      {
        tweet_id: 'tw001',
        text: 'Great talk on Databricks!',
        likes: 42,
        tweet_date: '2024-04-15',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 500000, nulls: 5000, dupes: 1000 },
    afterStats: { rows: 499000, nulls: 0, dupes: 0 },
  },
  {
    id: 29,
    group: 'Semi/Unstructured',
    title: 'IoT Batch Dump',
    desc: 'Edge device IoT data batch upload',
    source: 'Edge Devices',
    target: 'Delta Lake',
    complexity: 3,
    volume: 5,
    latency: 3,
    reliability: 4,
    governance: 3,
    score: 3.6,
    code: `df = spark.read.json("/mnt/iot/daily_dump/")\ndf.write.format("delta").mode("append").partitionBy("device_type").saveAsTable("catalog.bronze.iot_data")`,
    beforeData: [{ device_id: 'D001', temp: 72.5, humidity: 45, ts: '2024-04-15T10:00:00Z' }],
    afterData: [
      {
        device_id: 'D001',
        temperature: 72.5,
        humidity: 45,
        reading_ts: '2024-04-15T10:00:00Z',
        device_type: 'sensor',
      },
    ],
    beforeStats: { rows: 100000000, nulls: 500000, dupes: 10000 },
    afterStats: { rows: 99490000, nulls: 0, dupes: 0 },
  },
  {
    id: 30,
    group: 'Semi/Unstructured',
    title: 'Sensor Historical Ingestion',
    desc: 'Historical sensor data from edge devices',
    source: 'Sensor DB',
    target: 'Delta Lake',
    complexity: 3,
    volume: 5,
    latency: 2,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `df = spark.read.format("jdbc").option("url","jdbc:timescaledb://sensors:5432/tsdb").option("dbtable","sensor_readings").load()\ndf.write.format("delta").partitionBy("sensor_type","date").saveAsTable("catalog.bronze.sensor_history")`,
    beforeData: [{ sensor_id: 'S001', value: 98.6, unit: 'F', ts: '2024-04-15' }],
    afterData: [
      {
        sensor_id: 'S001',
        value: 98.6,
        unit: 'F',
        reading_date: '2024-04-15',
        sensor_type: 'temperature',
      },
    ],
    beforeStats: { rows: 1000000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 1000000000, nulls: 0, dupes: 0 },
  },

  // ─── 31–40: Advanced Enterprise / AI Pipelines ───
  {
    id: 31,
    group: 'AI/ML Pipeline',
    title: 'Feature Store Ingestion',
    desc: 'Delta tables to Databricks Feature Store',
    source: 'Delta Tables',
    target: 'Feature Store',
    complexity: 4,
    volume: 4,
    latency: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `from databricks.feature_engineering import FeatureEngineeringClient\nfe = FeatureEngineeringClient()\ndf = spark.table("catalog.gold.customer_features")\nfe.create_table(name="catalog.ml.customer_features", primary_keys=["customer_id"], df=df)`,
    beforeData: [{ customer_id: 1, total_orders: 15, avg_amount: 250.0, churn_risk: 0.05 }],
    afterData: [
      {
        customer_id: 1,
        total_orders: 15,
        avg_amount: 250.0,
        churn_risk: 0.05,
        _feature_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 1000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 1000000, nulls: 0, dupes: 0 },
  },
  {
    id: 32,
    group: 'AI/ML Pipeline',
    title: 'Training Dataset Prep',
    desc: 'Raw data to ML-ready training dataset',
    source: 'Raw Data',
    target: 'ML Dataset',
    complexity: 5,
    volume: 5,
    latency: 3,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `# Prepare ML training dataset\ndf = spark.table("catalog.gold.features")\ntrain, test = df.randomSplit([0.8, 0.2], seed=42)\ntrain.write.format("delta").saveAsTable("catalog.ml.train_data")\ntest.write.format("delta").saveAsTable("catalog.ml.test_data")`,
    beforeData: [{ id: 1, feature_1: 0.5, feature_2: 1.2, label: 1 }],
    afterData: [{ id: 1, feature_1: 0.5, feature_2: 1.2, label: 1, split: 'train' }],
    beforeStats: { rows: 5000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 5000000, nulls: 0, dupes: 0 },
  },
  {
    id: 33,
    group: 'AI/ML Pipeline',
    title: 'RAG Chunking Pipeline',
    desc: 'Document chunking for RAG retrieval',
    source: 'Documents',
    target: 'Chunk Store',
    complexity: 5,
    volume: 4,
    latency: 3,
    reliability: 5,
    governance: 5,
    score: 4.4,
    code: `# Chunk documents for RAG\ndef chunk_text(text, size=500, overlap=50):\n    return [text[i:i+size] for i in range(0, len(text), size-overlap)]\nchunk_udf = udf(chunk_text, ArrayType(StringType()))\ndf = spark.table("catalog.bronze.documents")\nchunked = df.withColumn("chunks", chunk_udf("content")).select("doc_id", explode("chunks").alias("chunk"))\nchunked.write.format("delta").saveAsTable("catalog.ml.doc_chunks")`,
    beforeData: [{ doc_id: 'D001', content: 'Long document text...', pages: 20 }],
    afterData: [{ doc_id: 'D001', chunk_id: 0, chunk: 'Long document te...', char_count: 500 }],
    beforeStats: { rows: 10000, nulls: 0, dupes: 0 },
    afterStats: { rows: 500000, nulls: 0, dupes: 0 },
  },
  {
    id: 34,
    group: 'AI/ML Pipeline',
    title: 'Embedding Generation Batch',
    desc: 'Generate vector embeddings for text chunks',
    source: 'Text Chunks',
    target: 'Vector DB',
    complexity: 5,
    volume: 4,
    latency: 4,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `# Generate embeddings\nfrom sentence_transformers import SentenceTransformer\nmodel = SentenceTransformer("all-MiniLM-L6-v2")\n\n@pandas_udf("array<float>")\ndef embed(texts):\n    return pd.Series(model.encode(texts.tolist()).tolist())\n\ndf = spark.table("catalog.ml.doc_chunks")\ndf.withColumn("embedding", embed("chunk")).write.format("delta").saveAsTable("catalog.ml.embeddings")`,
    beforeData: [{ chunk_id: 0, text: 'Machine learning is...' }],
    afterData: [
      { chunk_id: 0, text: 'Machine learning is...', embedding: '[0.12, -0.34, ...]', dim: 384 },
    ],
    beforeStats: { rows: 500000, nulls: 0, dupes: 0 },
    afterStats: { rows: 500000, nulls: 0, dupes: 0 },
  },
  {
    id: 35,
    group: 'AI/ML Pipeline',
    title: 'Data Enrichment Batch',
    desc: 'Enrich records with external API data',
    source: 'External APIs',
    target: 'Delta Lake',
    complexity: 4,
    volume: 3,
    latency: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `# Enrich with geocoding\n@pandas_udf("string")\ndef geocode(addresses):\n    import requests\n    results = [requests.get(f"https://geocode.api/v1?q={a}", timeout=5).json().get("lat_lon","") for a in addresses]\n    return pd.Series(results)\n\ndf = spark.table("catalog.bronze.customers")\ndf.withColumn("geo", geocode("address")).write.format("delta").saveAsTable("catalog.silver.customers_enriched")`,
    beforeData: [{ id: 1, address: '123 Main St, NYC' }],
    afterData: [{ id: 1, address: '123 Main St, NYC', lat: 40.7128, lon: -74.006 }],
    beforeStats: { rows: 100000, nulls: 0, dupes: 0 },
    afterStats: { rows: 100000, nulls: 0, dupes: 0 },
  },
  {
    id: 36,
    group: 'AI/ML Pipeline',
    title: 'Data Masking Ingestion',
    desc: 'Ingest with PII masking before storage',
    source: 'Raw PII',
    target: 'Secure Delta',
    complexity: 4,
    volume: 4,
    latency: 3,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `from pyspark.sql.functions import sha2, regexp_replace\ndf = spark.read.format("jdbc").option("dbtable","customers").load()\nmasked = df.withColumn("email_hash", sha2("email",256)).withColumn("phone_masked", regexp_replace("phone","(\\\\d{3})(\\\\d{3})(\\\\d{4})","***-***-$3")).drop("email","phone")\nmasked.write.format("delta").saveAsTable("catalog.bronze.customers_masked")`,
    beforeData: [{ name: 'Alice', email: 'alice@test.com', phone: '5551234567' }],
    afterData: [{ name: 'Alice', email_hash: 'a3f2...', phone_masked: '***-***-4567' }],
    beforeStats: { rows: 2000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 2000000, nulls: 0, dupes: 0 },
  },
  {
    id: 37,
    group: 'AI/ML Pipeline',
    title: 'Data Anonymization Batch',
    desc: 'Full anonymization pipeline for safe analytics',
    source: 'PII Data',
    target: 'Safe Zone',
    complexity: 4,
    volume: 3,
    latency: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `from pyspark.sql.functions import hash, abs as spark_abs\ndf = spark.table("catalog.bronze.raw_customers")\nanon = df.withColumn("anon_id", spark_abs(hash("customer_id"))).drop("customer_id","name","ssn","address")\nanon.write.format("delta").saveAsTable("catalog.secure.anonymized_customers")`,
    beforeData: [{ customer_id: 'C001', name: 'Alice', ssn: '123-45-6789' }],
    afterData: [{ anon_id: 847291, age_group: '30-40', region: 'Northeast' }],
    beforeStats: { rows: 500000, nulls: 0, dupes: 0 },
    afterStats: { rows: 500000, nulls: 0, dupes: 0 },
  },
  {
    id: 38,
    group: 'AI/ML Pipeline',
    title: 'Model Scoring Batch',
    desc: 'Batch score features with registered MLflow model',
    source: 'Feature Table',
    target: 'Predictions',
    complexity: 4,
    volume: 4,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `import mlflow\nmodel = mlflow.pyfunc.load_model("models:/churn_model/Production")\ndf = spark.table("catalog.ml.customer_features").toPandas()\npredictions = model.predict(df)\nresult = spark.createDataFrame(df.assign(churn_score=predictions))\nresult.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.churn_predictions")`,
    beforeData: [{ customer_id: 1, total_orders: 15, avg_amount: 250.0 }],
    afterData: [{ customer_id: 1, churn_score: 0.05, risk_tier: 'Low', model_version: 'v2.1' }],
    beforeStats: { rows: 1000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 1000000, nulls: 0, dupes: 0 },
  },
  {
    id: 39,
    group: 'AI/ML Pipeline',
    title: 'Drift Detection Dataset',
    desc: 'Ingest model prediction logs for drift monitoring',
    source: 'Prediction Logs',
    target: 'ML Monitoring',
    complexity: 5,
    volume: 4,
    latency: 3,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `# Monitor model drift\ndf = spark.table("catalog.gold.churn_predictions")\nstats = df.groupBy("prediction_date").agg(\n    avg("churn_score").alias("avg_score"),\n    stddev("churn_score").alias("std_score"),\n    count("*").alias("total")\n)\nstats.write.format("delta").mode("append").saveAsTable("catalog.ml.drift_monitoring")`,
    beforeData: [{ date: '2024-04-15', avg_score: 0.35, std: 0.12, count: 50000 }],
    afterData: [
      {
        date: '2024-04-15',
        avg_score: 0.35,
        std_score: 0.12,
        total: 50000,
        psi: 0.03,
        drift_alert: false,
      },
    ],
    beforeStats: { rows: 365, nulls: 0, dupes: 0 },
    afterStats: { rows: 365, nulls: 0, dupes: 0 },
  },
  {
    id: 40,
    group: 'AI/ML Pipeline',
    title: 'Data Quality Audit Ingestion',
    desc: 'Quality check results to metrics Delta table',
    source: 'Quality Logs',
    target: 'Metrics Table',
    complexity: 3,
    volume: 3,
    latency: 2,
    reliability: 5,
    governance: 4,
    score: 3.4,
    code: `# Log data quality metrics\nfrom pyspark.sql.functions import count, when, isnull\ntables = ["catalog.silver.customers", "catalog.silver.orders"]\nfor t in tables:\n    df = spark.table(t)\n    total = df.count()\n    nulls = {c: df.filter(isnull(c)).count() for c in df.columns}\n    quality = spark.createDataFrame([{"table": t, "rows": total, "max_null_pct": max(nulls.values())/total*100}])\n    quality.write.format("delta").mode("append").saveAsTable("catalog.audit.quality_metrics")`,
    beforeData: [{ table: 'customers', rows: 1000000, null_pct: 2.5 }],
    afterData: [
      {
        table: 'customers',
        rows: 1000000,
        max_null_pct: 2.5,
        check_ts: '2024-04-15',
        status: 'WARN',
      },
    ],
    beforeStats: { rows: 100, nulls: 0, dupes: 0 },
    afterStats: { rows: 100, nulls: 0, dupes: 0 },
  },

  // ─── 41–50: Operational / Governance / Platform ───
  {
    id: 41,
    group: 'Operations/Gov',
    title: 'Metadata Ingestion',
    desc: 'System metadata sync to Unity Catalog',
    source: 'Systems',
    target: 'Unity Catalog',
    complexity: 4,
    volume: 2,
    latency: 2,
    reliability: 5,
    governance: 5,
    score: 3.6,
    code: `# Sync metadata\ndf = spark.sql("SHOW TABLES IN catalog.bronze")\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.audit.table_inventory")`,
    beforeData: [{ database: 'bronze', table: 'orders', type: 'MANAGED' }],
    afterData: [
      {
        database: 'bronze',
        table: 'orders',
        type: 'MANAGED',
        row_count: 5000000,
        _sync_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 500, nulls: 0, dupes: 0 },
    afterStats: { rows: 500, nulls: 0, dupes: 0 },
  },
  {
    id: 42,
    group: 'Operations/Gov',
    title: 'Audit Logs Ingestion',
    desc: 'System audit logs to Delta for compliance',
    source: 'Audit Systems',
    target: 'Delta Lake',
    complexity: 3,
    volume: 4,
    latency: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `df = spark.read.json("/mnt/audit/logs/")\ndf.write.format("delta").mode("append").saveAsTable("catalog.audit.system_logs")`,
    beforeData: [
      {
        event: 'TABLE_READ',
        user: 'alice@company.com',
        table: 'customers',
        ts: '2024-04-15T10:00:00Z',
      },
    ],
    afterData: [
      {
        event_type: 'TABLE_READ',
        user_email: 'alice@company.com',
        target_table: 'customers',
        event_ts: '2024-04-15T10:00:00Z',
      },
    ],
    beforeStats: { rows: 10000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 10000000, nulls: 0, dupes: 0 },
  },
  {
    id: 43,
    group: 'Operations/Gov',
    title: 'Access Logs Ingestion',
    desc: 'IAM access logs for security monitoring',
    source: 'IAM',
    target: 'Delta Lake',
    complexity: 3,
    volume: 3,
    latency: 3,
    reliability: 5,
    governance: 5,
    score: 3.8,
    code: `df = spark.read.json("/mnt/iam/access_logs/")\ndf.write.format("delta").mode("append").saveAsTable("catalog.audit.access_logs")`,
    beforeData: [
      { principal: 'user@company.com', action: 'READ', resource: 'catalog.silver.customers' },
    ],
    afterData: [
      {
        principal: 'user@company.com',
        action: 'READ',
        resource: 'catalog.silver.customers',
        _ingest_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 5000000, nulls: 0, dupes: 0 },
    afterStats: { rows: 5000000, nulls: 0, dupes: 0 },
  },
  {
    id: 44,
    group: 'Operations/Gov',
    title: 'Cost Usage Ingestion',
    desc: 'Cloud cost and usage data to Delta',
    source: 'Cloud Billing',
    target: 'Delta Lake',
    complexity: 3,
    volume: 3,
    latency: 2,
    reliability: 4,
    governance: 4,
    score: 3.2,
    code: `# AWS Cost Explorer or Azure Cost Management\nimport requests\nheaders = {"Authorization": f"Bearer {dbutils.secrets.get('cloud','token')}"}\ncost_data = requests.get("https://management.azure.com/subscriptions/.../cost", headers=headers, timeout=30).json()\ndf = spark.createDataFrame(cost_data["properties"]["rows"])\ndf.write.format("delta").mode("append").saveAsTable("catalog.audit.cloud_costs")`,
    beforeData: [{ date: '2024-04-15', service: 'Databricks', cost: 450.0, currency: 'USD' }],
    afterData: [
      { date: '2024-04-15', service: 'Databricks', cost_usd: 450.0, _ingest_ts: '2024-04-15' },
    ],
    beforeStats: { rows: 10000, nulls: 0, dupes: 0 },
    afterStats: { rows: 10000, nulls: 0, dupes: 0 },
  },
  {
    id: 45,
    group: 'Operations/Gov',
    title: 'SLA Monitoring Ingestion',
    desc: 'Pipeline SLA metrics to Delta',
    source: 'Pipeline Metrics',
    target: 'Delta Lake',
    complexity: 3,
    volume: 2,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 3.6,
    code: `# Capture pipeline SLA metrics\nmetrics = [{"pipeline": "daily_etl", "expected_by": "06:00", "completed_at": "05:45", "status": "ON_TIME", "rows_processed": 5000000}]\ndf = spark.createDataFrame(metrics)\ndf.write.format("delta").mode("append").saveAsTable("catalog.audit.sla_tracking")`,
    beforeData: [{ pipeline: 'daily_etl', expected: '06:00', actual: '05:45' }],
    afterData: [
      {
        pipeline: 'daily_etl',
        expected_by: '06:00',
        completed_at: '05:45',
        status: 'ON_TIME',
        sla_met: true,
      },
    ],
    beforeStats: { rows: 365, nulls: 0, dupes: 0 },
    afterStats: { rows: 365, nulls: 0, dupes: 0 },
  },
  {
    id: 46,
    group: 'Operations/Gov',
    title: 'Job Metrics Ingestion',
    desc: 'Databricks job run metrics to Delta',
    source: 'Databricks API',
    target: 'Delta Lake',
    complexity: 3,
    volume: 3,
    latency: 3,
    reliability: 5,
    governance: 4,
    score: 3.6,
    code: `# Databricks Jobs API\nimport requests\ntoken = dbutils.secrets.get("db","token")\nheaders = {"Authorization": f"Bearer {token}"}\nruns = requests.get("https://adb-xxx.azuredatabricks.net/api/2.1/jobs/runs/list", headers=headers, timeout=30).json()["runs"]\ndf = spark.createDataFrame(runs)\ndf.write.format("delta").mode("append").saveAsTable("catalog.audit.job_metrics")`,
    beforeData: [{ run_id: 12345, job_id: 100, state: 'SUCCESS', duration_ms: 45000 }],
    afterData: [
      { run_id: 12345, job_id: 100, state: 'SUCCESS', duration_sec: 45, _ingest_ts: '2024-04-15' },
    ],
    beforeStats: { rows: 50000, nulls: 0, dupes: 0 },
    afterStats: { rows: 50000, nulls: 0, dupes: 0 },
  },
  {
    id: 47,
    group: 'Operations/Gov',
    title: 'Pipeline Lineage Ingestion',
    desc: 'ETL pipeline lineage metadata to Unity Catalog',
    source: 'ETL Metadata',
    target: 'Unity Catalog',
    complexity: 4,
    volume: 3,
    latency: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `# Capture lineage\nlineage = [{"source_table": "bronze.orders", "target_table": "silver.orders", "transform": "dedup+clean", "pipeline": "daily_etl"}]\ndf = spark.createDataFrame(lineage)\ndf.write.format("delta").mode("append").saveAsTable("catalog.audit.pipeline_lineage")`,
    beforeData: [{ source: 'bronze.orders', target: 'silver.orders', transform: 'dedup' }],
    afterData: [
      {
        source_table: 'bronze.orders',
        target_table: 'silver.orders',
        transform_type: 'dedup+clean',
        pipeline: 'daily_etl',
        _ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 500, nulls: 0, dupes: 0 },
    afterStats: { rows: 500, nulls: 0, dupes: 0 },
  },
  {
    id: 48,
    group: 'Operations/Gov',
    title: 'Data Catalog Sync',
    desc: 'External data catalog sync to Unity Catalog',
    source: 'External Catalog',
    target: 'Unity Catalog',
    complexity: 4,
    volume: 2,
    latency: 2,
    reliability: 5,
    governance: 5,
    score: 3.6,
    code: `# Sync from external catalog (Collibra/Alation)\nimport requests\nheaders = {"Authorization": f"Bearer {dbutils.secrets.get('catalog','token')}"}\nassets = requests.get("https://collibra.company.com/api/assets", headers=headers, timeout=30).json()\ndf = spark.createDataFrame(assets)\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.audit.external_catalog")`,
    beforeData: [{ asset_id: 'A001', name: 'Customer Table', domain: 'CRM', owner: 'data-team' }],
    afterData: [
      {
        asset_id: 'A001',
        name: 'Customer Table',
        domain: 'CRM',
        owner: 'data-team',
        _sync_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 5000, nulls: 0, dupes: 0 },
    afterStats: { rows: 5000, nulls: 0, dupes: 0 },
  },
  {
    id: 49,
    group: 'Operations/Gov',
    title: 'Backup Ingestion',
    desc: 'Delta table backup to archive storage',
    source: 'Delta Tables',
    target: 'Archive',
    complexity: 2,
    volume: 5,
    latency: 1,
    reliability: 5,
    governance: 3,
    score: 3.2,
    code: `# Backup Delta table to archive\ntables = ["catalog.gold.customers", "catalog.gold.orders", "catalog.gold.products"]\nfor t in tables:\n    df = spark.table(t)\n    df.write.format("delta").mode("overwrite").save(f"s3://backup-bucket/{t.replace('.','/')}/")`,
    beforeData: [{ table: 'gold.customers', rows: 2000000, size_gb: 5.2 }],
    afterData: [
      {
        table: 'gold.customers',
        rows: 2000000,
        backup_path: 's3://backup/gold/customers/',
        backup_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 3, nulls: 0, dupes: 0 },
    afterStats: { rows: 3, nulls: 0, dupes: 0 },
  },
  {
    id: 50,
    group: 'Operations/Gov',
    title: 'Disaster Recovery Ingestion',
    desc: 'Cross-region Delta table replication for DR',
    source: 'Primary Region',
    target: 'DR Region',
    complexity: 5,
    volume: 5,
    latency: 5,
    reliability: 5,
    governance: 5,
    score: 5.0,
    code: `# Cross-region Deep Clone for DR\ntables = spark.sql("SHOW TABLES IN catalog.gold").collect()\nfor row in tables:\n    table = f"catalog.gold.{row.tableName}"\n    dr_table = f"catalog_dr.gold.{row.tableName}"\n    spark.sql(f"CREATE OR REPLACE TABLE {dr_table} DEEP CLONE {table}")\n    print(f"Cloned {table} -> {dr_table}")`,
    beforeData: [{ table: 'gold.customers', region: 'us-east-1', rows: 2000000 }],
    afterData: [
      {
        table: 'gold.customers',
        source_region: 'us-east-1',
        dr_region: 'us-west-2',
        rows: 2000000,
        clone_ts: '2024-04-15',
      },
    ],
    beforeStats: { rows: 50, nulls: 0, dupes: 0 },
    afterStats: { rows: 50, nulls: 0, dupes: 0 },
  },
];

const groups = [...new Set(batchScenarios.map((s) => s.group))];

// ─── Mini Table Component ─────────────────────
function MiniTable({ rows, badge, badgeColor }) {
  if (!rows || rows.length === 0) return null;
  const headers = Object.keys(rows[0]);
  return (
    <div
      style={{
        overflowX: 'auto',
        maxHeight: '180px',
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
                      color: isNull ? '#991b1b' : undefined,
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

// ─── Score Bar Component ──────────────────────
function ScoreBar({ label, value, max }) {
  const pct = (value / max) * 100;
  const color = value >= 4 ? '#22c55e' : value >= 3 ? '#f59e0b' : '#ef4444';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '0.7rem' }}>
      <span style={{ width: '70px', color: 'var(--text-secondary)' }}>{label}</span>
      <div style={{ flex: 1, height: '8px', background: '#f3f4f6', borderRadius: '4px' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: '4px' }} />
      </div>
      <span style={{ width: '20px', textAlign: 'right', fontWeight: 600 }}>{value}</span>
    </div>
  );
}

function BatchIngestion() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [processedIds, setProcessedIds] = useState({});
  const [processingId, setProcessingId] = useState(null);
  const [sortBy, setSortBy] = useState('id');

  const filtered = batchScenarios
    .filter((s) => {
      const matchGroup = selectedGroup === 'All' || s.group === selectedGroup;
      const matchSearch =
        s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
        s.desc.toLowerCase().includes(searchTerm.toLowerCase());
      return matchGroup && matchSearch;
    })
    .sort((a, b) => (sortBy === 'score' ? b.score - a.score : a.id - b.id));

  const runProcess = (id) => {
    setProcessingId(id);
    setTimeout(
      () => {
        setProcessedIds((prev) => ({
          ...prev,
          [id]: {
            status: 'done',
            duration: (Math.random() * 3 + 1).toFixed(1),
            time: new Date().toISOString(),
          },
        }));
        setProcessingId(null);
      },
      Math.floor(Math.random() * 2000) + 1500
    );
  };

  const downloadScorecard = () => {
    exportToCSV(
      batchScenarios.map((s) => ({
        id: s.id,
        group: s.group,
        title: s.title,
        source: s.source,
        target: s.target,
        complexity: s.complexity,
        volume: s.volume,
        latency: s.latency,
        reliability: s.reliability,
        governance: s.governance,
        score: s.score,
      })),
      'batch-ingestion-scorecard.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Batch Ingestion Scenarios</h1>
          <p>50 enterprise batch ingestion patterns with scoring matrix</p>
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">{'📦'}</div>
          <div className="stat-info">
            <h4>50</h4>
            <p>Total Scenarios</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">{'🏢'}</div>
          <div className="stat-info">
            <h4>{groups.length}</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">{'⭐'}</div>
          <div className="stat-info">
            <h4>{(batchScenarios.reduce((s, x) => s + x.score, 0) / 50).toFixed(1)}</h4>
            <p>Avg Score</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'🏆'}</div>
          <div className="stat-info">
            <h4>{batchScenarios.filter((s) => s.score >= 4.0).length}</h4>
            <p>Advanced (4.0+)</p>
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search scenarios..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '250px' }}
          />
          <select
            className="form-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
            style={{ maxWidth: '220px' }}
          >
            <option value="All">All Groups (50)</option>
            {groups.map((g) => (
              <option key={g} value={g}>
                {g} ({batchScenarios.filter((s) => s.group === g).length})
              </option>
            ))}
          </select>
          <select
            className="form-input"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            style={{ maxWidth: '150px' }}
          >
            <option value="id">Sort by #</option>
            <option value="score">Sort by Score</option>
          </select>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadScorecard}
            style={{ marginLeft: 'auto' }}
          >
            Download Scorecard (CSV)
          </button>
        </div>
      </div>

      {/* Scenarios */}
      {filtered.map((s) => {
        const isExpanded = expandedId === s.id;
        const processed = processedIds[s.id];
        const isProcessing = processingId === s.id;

        return (
          <div key={s.id} className="card" style={{ marginBottom: '0.75rem' }}>
            {/* Header */}
            <div
              onClick={() => setExpandedId(isExpanded ? null : s.id)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div style={{ flex: 1 }}>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    marginBottom: '0.25rem',
                    flexWrap: 'wrap',
                  }}
                >
                  <span className="badge running">{s.group}</span>
                  <strong>
                    #{s.id} — {s.title}
                  </strong>
                  <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                    {s.source} → {s.target}
                  </span>
                  {processed && <span className="badge success">Processed</span>}
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{s.desc}</p>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <span
                  style={{
                    fontSize: '1.1rem',
                    fontWeight: 700,
                    color:
                      s.score >= 4
                        ? 'var(--success)'
                        : s.score >= 3
                          ? 'var(--warning)'
                          : 'var(--error)',
                  }}
                >
                  {s.score}
                </span>
                <span style={{ color: 'var(--text-secondary)' }}>
                  {isExpanded ? '\u25BC' : '\u25B6'}
                </span>
              </div>
            </div>

            {/* Expanded */}
            {isExpanded && (
              <div style={{ marginTop: '1rem' }}>
                {/* Scoring Matrix */}
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr 1fr',
                    gap: '1rem',
                    marginBottom: '1rem',
                  }}
                >
                  <div style={{ padding: '0.75rem', background: '#f8f9fa', borderRadius: '6px' }}>
                    <div style={{ fontSize: '0.75rem', fontWeight: 700, marginBottom: '0.5rem' }}>
                      Scoring Matrix
                    </div>
                    <ScoreBar label="Complexity" value={s.complexity} max={5} />
                    <ScoreBar label="Volume" value={s.volume} max={5} />
                    <ScoreBar label="Latency" value={s.latency} max={5} />
                    <ScoreBar label="Reliability" value={s.reliability} max={5} />
                    <ScoreBar label="Governance" value={s.governance} max={5} />
                    <div style={{ marginTop: '0.5rem', fontSize: '0.8rem', fontWeight: 700 }}>
                      Overall:{' '}
                      <span
                        style={{
                          color: s.score >= 4 ? '#22c55e' : s.score >= 3 ? '#f59e0b' : '#ef4444',
                        }}
                      >
                        {s.score}/5.0
                      </span>
                    </div>
                  </div>

                  {/* Data Path */}
                  <div>
                    <div
                      style={{
                        padding: '0.5rem 0.75rem',
                        background: '#fef2f2',
                        borderRadius: '6px',
                        marginBottom: '0.5rem',
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
                      <div style={{ fontSize: '0.8rem', fontWeight: 600 }}>{s.source}</div>
                    </div>
                    <div
                      style={{
                        textAlign: 'center',
                        fontSize: '1.2rem',
                        color: 'var(--text-secondary)',
                        margin: '0.25rem 0',
                      }}
                    >
                      {'\u2193'}
                    </div>
                    <div
                      style={{
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
                      <div style={{ fontSize: '0.8rem', fontWeight: 600 }}>{s.target}</div>
                    </div>
                  </div>
                </div>

                {/* Before / After Data */}
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
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.5rem',
                        marginBottom: '0.3rem',
                      }}
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
                      <span style={{ fontSize: '0.75rem' }}>
                        {s.beforeStats.rows.toLocaleString()} rows |{' '}
                        {s.beforeStats.nulls.toLocaleString()} nulls |{' '}
                        {s.beforeStats.dupes.toLocaleString()} dupes
                      </span>
                    </div>
                    <MiniTable rows={s.beforeData} badgeColor="#fef2f2" />
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
                        <span style={{ fontSize: '0.75rem' }}>
                          {s.afterStats.rows.toLocaleString()} rows | {s.afterStats.nulls} nulls |{' '}
                          {s.afterStats.dupes} dupes
                        </span>
                      </div>
                      <MiniTable rows={s.afterData} badgeColor="#dcfce7" />
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
                        <div style={{ fontSize: '1.5rem' }}>
                          {isProcessing ? '\u23f3' : '\u25b6\ufe0f'}
                        </div>
                        <p style={{ fontSize: '0.8rem' }}>
                          {isProcessing ? 'Processing...' : 'Click Run Process'}
                        </p>
                      </div>
                    </div>
                  )}
                </div>

                {/* Process Button */}
                <div
                  style={{
                    display: 'flex',
                    gap: '1rem',
                    alignItems: 'center',
                    marginBottom: '1rem',
                  }}
                >
                  <button
                    className="btn btn-primary"
                    disabled={isProcessing}
                    onClick={() => runProcess(s.id)}
                  >
                    {isProcessing
                      ? '\u23f3 Processing...'
                      : processed
                        ? '\u21bb Re-run'
                        : '\u25b6 Run Process'}
                  </button>
                  {processed && (
                    <span style={{ fontSize: '0.8rem', color: 'var(--success)' }}>
                      Completed in {processed.duration}s
                    </span>
                  )}
                </div>

                {/* Code */}
                <details>
                  <summary
                    style={{
                      cursor: 'pointer',
                      fontSize: '0.85rem',
                      fontWeight: 600,
                      color: 'var(--text-secondary)',
                    }}
                  >
                    View PySpark Code
                  </summary>
                  <div
                    className="code-block"
                    style={{ marginTop: '0.5rem', maxHeight: '300px', overflowY: 'auto' }}
                  >
                    {s.code}
                  </div>
                </details>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default BatchIngestion;
