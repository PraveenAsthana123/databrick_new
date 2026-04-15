import React, { useState } from 'react';
import { exportToCSV } from '../../utils/fileExport';

const batchPipelines = [
  // ─── 1–10: Simple Source-to-Bronze ───
  {
    id: 1,
    group: 'Source-to-Bronze',
    title: 'Daily CSV Ingestion Pipeline',
    flow: 'ADLS/S3 \u2192 Bronze Delta',
    complexity: 2,
    volume: 3,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.4,
    code: `# Daily CSV → Bronze\ndf = spark.readStream.format("cloudFiles") \\\n    .option("cloudFiles.format", "csv") \\\n    .option("header", "true") \\\n    .option("cloudFiles.schemaLocation", "/mnt/schema/csv") \\\n    .load("s3://landing/csv/")\ndf.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/csv_bronze") \\\n    .trigger(availableNow=True) \\\n    .toTable("catalog.bronze.csv_data")`,
    before: [
      {
        file: 'orders_20240415.csv',
        rows: 50000,
        size_mb: 12,
        format: 'CSV',
        issues: 'nulls, dupes',
      },
    ],
    after: [
      {
        table: 'bronze.csv_data',
        rows: 49200,
        nulls: 0,
        dupes: 0,
        format: 'Delta',
        partitions: 'date',
      },
    ],
  },
  {
    id: 2,
    group: 'Source-to-Bronze',
    title: 'Daily JSON Ingestion Pipeline',
    flow: 'Cloud Storage \u2192 Bronze Delta',
    complexity: 2,
    volume: 4,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.6,
    code: `df = spark.read.json("s3://landing/json/")\ndf.withColumn("_ingest_ts", current_timestamp()).write.format("delta").mode("append").saveAsTable("catalog.bronze.json_data")`,
    before: [{ file: 'events_*.json', rows: 500000, format: 'JSON', nested: true }],
    after: [{ table: 'bronze.json_data', rows: 498000, format: 'Delta', flattened: true }],
  },
  {
    id: 3,
    group: 'Source-to-Bronze',
    title: 'Parquet Batch Pipeline',
    flow: 'Storage \u2192 Bronze Delta',
    complexity: 1,
    volume: 4,
    sla: 2,
    reliability: 3,
    governance: 2,
    score: 2.4,
    code: `df = spark.read.parquet("s3://data-lake/parquet/")\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.parquet_data")`,
    before: [{ files: 120, total_rows: 10000000, format: 'Parquet' }],
    after: [{ table: 'bronze.parquet_data', rows: 10000000, format: 'Delta' }],
  },
  {
    id: 4,
    group: 'Source-to-Bronze',
    title: 'Excel File Pipeline',
    flow: 'SharePoint/Blob \u2192 Bronze',
    complexity: 3,
    volume: 2,
    sla: 1,
    reliability: 3,
    governance: 3,
    score: 2.4,
    code: `import pandas as pd\npdf = pd.read_excel("/dbfs/mnt/sharepoint/report.xlsx", sheet_name="Data")\ndf = spark.createDataFrame(pdf)\ndf.write.format("delta").saveAsTable("catalog.bronze.excel_data")`,
    before: [{ file: 'report.xlsx', sheets: 3, rows: 25000 }],
    after: [{ table: 'bronze.excel_data', rows: 24800, format: 'Delta' }],
  },
  {
    id: 5,
    group: 'Source-to-Bronze',
    title: 'XML Batch Pipeline',
    flow: 'SFTP/Storage \u2192 Bronze',
    complexity: 4,
    volume: 3,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.2,
    code: `df = spark.read.format("com.databricks.spark.xml").option("rowTag", "record").load("/mnt/sftp/xml/")\ndf.write.format("delta").saveAsTable("catalog.bronze.xml_data")`,
    before: [{ files: 50, format: 'XML', row_tag: 'record' }],
    after: [{ table: 'bronze.xml_data', rows: 150000, format: 'Delta' }],
  },
  {
    id: 6,
    group: 'Source-to-Bronze',
    title: 'Fixed-Width File Pipeline',
    flow: 'Legacy File \u2192 Bronze',
    complexity: 4,
    volume: 3,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.2,
    code: `df = spark.read.text("/mnt/legacy/fixed/")\nparsed = df.select(df.value.substr(1,10).alias("acct"), df.value.substr(11,30).alias("name"), df.value.substr(41,12).alias("balance"))\nparsed.write.format("delta").saveAsTable("catalog.bronze.legacy_data")`,
    before: [{ format: 'Fixed-Width', encoding: 'EBCDIC', record_len: 200 }],
    after: [{ table: 'bronze.legacy_data', columns: 8, format: 'Delta' }],
  },
  {
    id: 7,
    group: 'Source-to-Bronze',
    title: 'Multi-File Folder Pipeline',
    flow: 'Folder \u2192 Bronze Consolidation',
    complexity: 3,
    volume: 4,
    sla: 2,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `from functools import reduce\nfiles = dbutils.fs.ls("/mnt/daily_drops/")\ndfs = [spark.read.csv(f.path, header=True) for f in files]\nmerged = reduce(lambda a,b: a.unionByName(b, allowMissingColumns=True), dfs)\nmerged.write.format("delta").mode("append").saveAsTable("catalog.bronze.daily_merged")`,
    before: [{ files: 25, formats: 'CSV mixed', total_rows: 200000 }],
    after: [{ table: 'bronze.daily_merged', rows: 198000, format: 'Delta' }],
  },
  {
    id: 8,
    group: 'Source-to-Bronze',
    title: 'Daily Zipped File Pipeline',
    flow: 'Compressed \u2192 Extract \u2192 Bronze',
    complexity: 3,
    volume: 3,
    sla: 2,
    reliability: 4,
    governance: 2,
    score: 2.8,
    code: `import zipfile, os\nfor zf in dbutils.fs.ls("/mnt/landing/zipped/"):\n    with zipfile.ZipFile(f"/dbfs{zf.path}") as z:\n        z.extractall("/dbfs/tmp/extracted/")\ndf = spark.read.csv("/tmp/extracted/", header=True)\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.zipped_data")`,
    before: [{ files: 5, format: 'ZIP > CSV', compressed_mb: 500 }],
    after: [{ table: 'bronze.zipped_data', rows: 2000000, format: 'Delta' }],
  },
  {
    id: 9,
    group: 'Source-to-Bronze',
    title: 'Historical Backfill Pipeline',
    flow: 'Archive Storage \u2192 Bronze',
    complexity: 3,
    volume: 5,
    sla: 1,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `# Backfill years of historical data\nfor year in range(2020, 2025):\n    df = spark.read.parquet(f"s3://archive/data/year={year}/")\n    df.write.format("delta").mode("append").partitionBy("month").saveAsTable("catalog.bronze.historical")`,
    before: [{ years: '2020-2024', total_rows: 500000000, size_tb: 2.5 }],
    after: [{ table: 'bronze.historical', rows: 500000000, partitions: 60 }],
  },
  {
    id: 10,
    group: 'Source-to-Bronze',
    title: 'Schema-Evolving File Pipeline',
    flow: 'Mixed Schema Files \u2192 Bronze',
    complexity: 5,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `df = spark.readStream.format("cloudFiles") \\\n    .option("cloudFiles.format", "json") \\\n    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\\n    .option("cloudFiles.schemaLocation", "/mnt/schema/evolving") \\\n    .load("/mnt/landing/evolving/")\ndf.writeStream.option("mergeSchema", "true") \\\n    .option("checkpointLocation", "/mnt/cp/evolving") \\\n    .trigger(availableNow=True).toTable("catalog.bronze.evolving")`,
    before: [{ schema_versions: 5, new_columns: 12, format: 'JSON' }],
    after: [{ table: 'bronze.evolving', schema_version: 5, total_columns: 35 }],
  },

  // ─── 11–20: Database Extraction ───
  {
    id: 11,
    group: 'DB Extraction',
    title: 'Full Table Extraction Pipeline',
    flow: 'Oracle/SQL Server \u2192 Bronze',
    complexity: 2,
    volume: 4,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.0,
    code: `df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "orders").option("numPartitions", 8).load()\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.orders")`,
    before: [{ source: 'Oracle', table: 'ORDERS', rows: 5000000 }],
    after: [{ table: 'bronze.orders', rows: 5000000, format: 'Delta' }],
  },
  {
    id: 12,
    group: 'DB Extraction',
    title: 'Incremental Timestamp Pipeline',
    flow: 'DB \u2192 Incremental \u2192 Bronze',
    complexity: 3,
    volume: 3,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 3.4,
    code: `watermark = spark.sql("SELECT MAX(modified_at) FROM catalog.bronze.orders").collect()[0][0]\ndf = spark.read.format("jdbc").option("dbtable", f"(SELECT * FROM orders WHERE modified_at > '{watermark}') t").load()\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.orders")`,
    before: [{ mode: 'Incremental', watermark: '2024-04-14', new_rows: 15000 }],
    after: [{ table: 'bronze.orders', appended: 15000, total: 5015000 }],
  },
  {
    id: 13,
    group: 'DB Extraction',
    title: 'CDC Batch Merge Pipeline',
    flow: 'DB Logs \u2192 Stage \u2192 Merge to Delta',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `from delta.tables import DeltaTable\ncdc = spark.read.format("jdbc").option("dbtable", "cdc_log").load()\ntarget = DeltaTable.forName(spark, "catalog.bronze.customers")\ntarget.alias("t").merge(cdc.alias("s"), "t.id = s.id") \\\n    .whenMatchedUpdateAll() \\\n    .whenNotMatchedInsertAll() \\\n    .whenNotMatchedBySourceDelete() \\\n    .execute()`,
    before: [{ cdc_records: 50000, inserts: 5000, updates: 40000, deletes: 5000 }],
    after: [{ table: 'bronze.customers', merged: 50000, version: 15 }],
  },
  {
    id: 14,
    group: 'DB Extraction',
    title: 'Master Data Batch Pipeline',
    flow: 'MDM \u2192 Bronze/Silver',
    complexity: 3,
    volume: 2,
    sla: 2,
    reliability: 5,
    governance: 5,
    score: 3.4,
    code: `df = spark.read.format("jdbc").option("dbtable", "golden_records").load()\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.master_data")\n# Silver with validation\nclean = df.filter("id IS NOT NULL").dropDuplicates(["id"])\nclean.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.master_data")`,
    before: [{ source: 'MDM', golden_records: 500000 }],
    after: [{ bronze: 500000, silver: 498500, quality: '99.7%' }],
  },
  {
    id: 15,
    group: 'DB Extraction',
    title: 'Customer Table Sync Pipeline',
    flow: 'CRM DB \u2192 Bronze \u2192 Silver',
    complexity: 3,
    volume: 3,
    sla: 3,
    reliability: 4,
    governance: 4,
    score: 3.4,
    code: `df = spark.read.format("jdbc").option("dbtable", "customers").load()\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.crm_customers")\nclean = spark.table("catalog.bronze.crm_customers").dropDuplicates(["customer_id"]).filter("email IS NOT NULL")\nclean.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.customers")`,
    before: [{ source: 'CRM', rows: 2000000, nulls: 5000 }],
    after: [{ bronze: 2000000, silver: 1995000, cleaned: true }],
  },
  {
    id: 16,
    group: 'DB Extraction',
    title: 'Finance Ledger Pipeline',
    flow: 'ERP Finance DB \u2192 Bronze/Silver',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.4,
    code: `df = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//erp:1521/FIN").option("dbtable", "GL_ENTRIES").load()\ndf.write.format("delta").mode("append").saveAsTable("catalog.bronze.gl_entries")\n# Silver with currency conversion\nfrom pyspark.sql.functions import when\nsilver = df.withColumn("amount_usd", when(col("currency")=="EUR", col("amount")*1.08).otherwise(col("amount")))\nsilver.write.format("delta").mode("append").saveAsTable("catalog.silver.gl_entries")`,
    before: [{ source: 'Oracle ERP', entries: 8000000, currencies: 5 }],
    after: [{ bronze: 8000000, silver: 8000000, normalized: 'USD' }],
  },
  {
    id: 17,
    group: 'DB Extraction',
    title: 'HR Data Pipeline',
    flow: 'HRMS \u2192 Bronze/Silver Secure',
    complexity: 3,
    volume: 2,
    sla: 2,
    reliability: 4,
    governance: 5,
    score: 3.2,
    code: `df = spark.read.format("jdbc").option("dbtable", "employees").load()\n# Mask PII\nfrom pyspark.sql.functions import sha2\nmasked = df.withColumn("ssn_hash", sha2("ssn", 256)).drop("ssn")\nmasked.write.format("delta").saveAsTable("catalog.secure.hr_employees")`,
    before: [{ source: 'HRMS', employees: 50000, pii_columns: 5 }],
    after: [{ table: 'secure.hr_employees', rows: 50000, pii_masked: true }],
  },
  {
    id: 18,
    group: 'DB Extraction',
    title: 'Inventory Snapshot Pipeline',
    flow: 'ERP/WMS \u2192 Bronze/Silver',
    complexity: 2,
    volume: 3,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 2.8,
    code: `df = spark.read.format("jdbc").option("dbtable", "inventory_snapshot").load()\ndf.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.inventory")`,
    before: [{ source: 'WMS', skus: 500000, warehouses: 12 }],
    after: [{ table: 'bronze.inventory', rows: 500000, snapshot_date: '2024-04-15' }],
  },
  {
    id: 19,
    group: 'DB Extraction',
    title: 'Core Banking Batch Pipeline',
    flow: 'Banking DB \u2192 Bronze/Silver',
    complexity: 5,
    volume: 5,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `# High-security banking pipeline\ndf = spark.read.format("jdbc").option("url", "jdbc:db2://core:50000/BANK").option("dbtable", "TRANSACTIONS").option("numPartitions", 16).load()\n# Encrypt sensitive columns\nfrom pyspark.sql.functions import sha2, concat, lit\nencrypted = df.withColumn("acct_hash", sha2(concat("account_no", lit("SALT")), 256)).drop("account_no")\nencrypted.write.format("delta").mode("append").saveAsTable("catalog.secure.bank_txns")`,
    before: [{ source: 'DB2 Mainframe', daily_txns: 50000000, security: 'HIGH' }],
    after: [{ table: 'secure.bank_txns', rows: 50000000, encrypted: true }],
  },
  {
    id: 20,
    group: 'DB Extraction',
    title: 'POS Sales Batch Pipeline',
    flow: 'Store DB \u2192 Bronze/Silver',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `stores = ["nyc","la","chi","sf","sea"]\nfor store in stores:\n    df = spark.read.format("jdbc").option("url", f"jdbc:mysql://{store}-db/pos").option("dbtable", "transactions").load()\n    df.withColumn("store_id", lit(store)).write.format("delta").mode("append").saveAsTable("catalog.bronze.pos_sales")`,
    before: [{ stores: 5, daily_txns_per_store: 100000 }],
    after: [{ table: 'bronze.pos_sales', total_rows: 500000, stores: 5 }],
  },

  // ─── 21–30: Enterprise Application ───
  {
    id: 21,
    group: 'Enterprise App',
    title: 'SAP Extraction Pipeline',
    flow: 'SAP \u2192 Landing \u2192 Bronze',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `df = spark.read.format("jdbc").option("url", "jdbc:sap://hana:30015").option("dbtable", "/BIC/AZFINANCE").load()\ndf.write.format("delta").saveAsTable("catalog.bronze.sap_finance")`,
    before: [{ source: 'SAP HANA', module: 'Finance', rows: 2000000 }],
    after: [{ table: 'bronze.sap_finance', rows: 2000000 }],
  },
  {
    id: 22,
    group: 'Enterprise App',
    title: 'Salesforce Batch Pipeline',
    flow: 'Salesforce API \u2192 Bronze',
    complexity: 3,
    volume: 2,
    sla: 2,
    reliability: 4,
    governance: 4,
    score: 3.0,
    code: `from simple_salesforce import Salesforce\nsf = Salesforce(username=dbutils.secrets.get("sf","user"), password=dbutils.secrets.get("sf","pass"), security_token=dbutils.secrets.get("sf","token"))\nrecords = sf.bulk.Account.query("SELECT Id, Name, Industry FROM Account")\ndf = spark.createDataFrame(records)\ndf.write.format("delta").saveAsTable("catalog.bronze.sf_accounts")`,
    before: [{ source: 'Salesforce', objects: 'Account', records: 150000 }],
    after: [{ table: 'bronze.sf_accounts', rows: 150000 }],
  },
  {
    id: 23,
    group: 'Enterprise App',
    title: 'Workday Employee Pipeline',
    flow: 'Workday \u2192 Secure Bronze',
    complexity: 3,
    volume: 2,
    sla: 2,
    reliability: 4,
    governance: 5,
    score: 3.2,
    code: `import requests\nresp = requests.get("https://wd5.workday.com/ccx/service/tenant/RaaS", auth=(dbutils.secrets.get("wd","user"), dbutils.secrets.get("wd","pass")), timeout=30)\ndf = spark.createDataFrame(resp.json()["Report_Entry"])\ndf.write.format("delta").saveAsTable("catalog.secure.workday_employees")`,
    before: [{ source: 'Workday RaaS', employees: 50000 }],
    after: [{ table: 'secure.workday_employees', rows: 50000, pii_zone: true }],
  },
  {
    id: 24,
    group: 'Enterprise App',
    title: 'ServiceNow Ticket Pipeline',
    flow: 'ServiceNow \u2192 Bronze/Silver',
    complexity: 3,
    volume: 2,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 2.8,
    code: `import requests\nheaders = {"Authorization": f"Bearer {dbutils.secrets.get('snow','token')}"}\ntickets = requests.get("https://company.service-now.com/api/now/table/incident", headers=headers, timeout=30).json()["result"]\ndf = spark.createDataFrame(tickets)\ndf.write.format("delta").saveAsTable("catalog.bronze.snow_incidents")`,
    before: [{ source: 'ServiceNow', incidents: 100000 }],
    after: [{ table: 'bronze.snow_incidents', rows: 100000 }],
  },
  {
    id: 25,
    group: 'Enterprise App',
    title: 'SharePoint Document Metadata Pipeline',
    flow: 'SharePoint \u2192 Bronze',
    complexity: 4,
    volume: 3,
    sla: 2,
    reliability: 5,
    governance: 5,
    score: 3.8,
    code: `import requests\nheaders = {"Authorization": f"Bearer {dbutils.secrets.get('sp','token')}"}\nfiles = requests.get("https://graph.microsoft.com/v1.0/sites/{site}/drives/{drive}/items", headers=headers, timeout=30).json()["value"]\ndf = spark.createDataFrame(files)\ndf.write.format("delta").saveAsTable("catalog.bronze.sharepoint_docs")`,
    before: [{ source: 'SharePoint', documents: 25000 }],
    after: [{ table: 'bronze.sharepoint_docs', rows: 25000, metadata: true }],
  },
  {
    id: 26,
    group: 'Enterprise App',
    title: 'Procurement Pipeline',
    flow: 'ERP Procurement \u2192 Bronze/Silver',
    complexity: 3,
    volume: 3,
    sla: 3,
    reliability: 4,
    governance: 4,
    score: 3.4,
    code: `df = spark.read.format("jdbc").option("dbtable", "purchase_orders").load()\ndf.write.format("delta").saveAsTable("catalog.bronze.procurement")\nclean = df.filter("amount > 0").dropDuplicates(["po_number"])\nclean.write.format("delta").saveAsTable("catalog.silver.procurement")`,
    before: [{ source: 'ERP', purchase_orders: 500000 }],
    after: [{ bronze: 500000, silver: 498000 }],
  },
  {
    id: 27,
    group: 'Enterprise App',
    title: 'Contract Batch Pipeline',
    flow: 'Contract Repo \u2192 Bronze/RAG Prep',
    complexity: 5,
    volume: 3,
    sla: 2,
    reliability: 4,
    governance: 5,
    score: 3.8,
    code: `# Extract contract text for RAG\nfrom pypdf import PdfReader\nimport os\ncontracts = dbutils.fs.ls("/mnt/contracts/")\nall_data = []\nfor c in contracts:\n    reader = PdfReader(f"/dbfs{c.path}")\n    text = "\\n".join([p.extract_text() for p in reader.pages])\n    all_data.append({"file": c.name, "text": text, "pages": len(reader.pages)})\ndf = spark.createDataFrame(all_data)\ndf.write.format("delta").saveAsTable("catalog.bronze.contracts")`,
    before: [{ source: 'Contract Repository', documents: 5000, format: 'PDF' }],
    after: [{ table: 'bronze.contracts', rows: 5000, text_extracted: true }],
  },
  {
    id: 28,
    group: 'Enterprise App',
    title: 'Claims Data Pipeline',
    flow: 'Insurance Claims \u2192 Bronze/Silver',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `df = spark.read.format("jdbc").option("dbtable", "claims").load()\ndf.write.format("delta").saveAsTable("catalog.bronze.claims")\n# Silver with validation\nvalid = df.filter("claim_amount > 0 AND policy_id IS NOT NULL")\nvalid.write.format("delta").saveAsTable("catalog.silver.claims")`,
    before: [{ source: 'Claims System', claims: 2000000 }],
    after: [{ bronze: 2000000, silver: 1980000, validated: true }],
  },
  {
    id: 29,
    group: 'Enterprise App',
    title: 'Loan Application Pipeline',
    flow: 'Loan System \u2192 Bronze/Silver',
    complexity: 4,
    volume: 3,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `df = spark.read.format("jdbc").option("dbtable", "loan_applications").load()\n# Mask PII\nmasked = df.withColumn("ssn_masked", lit("***-**-") + col("ssn").substr(-4,4)).drop("ssn")\nmasked.write.format("delta").saveAsTable("catalog.secure.loan_applications")`,
    before: [{ source: 'Loan System', applications: 500000, pii: true }],
    after: [{ table: 'secure.loan_applications', rows: 500000, pii_masked: true }],
  },
  {
    id: 30,
    group: 'Enterprise App',
    title: 'Regulatory Reporting Pipeline',
    flow: 'Multiple Apps \u2192 Curated Reporting',
    complexity: 5,
    volume: 4,
    sla: 5,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `# Multi-source regulatory report\nsources = {\n    "finance": "catalog.silver.gl_entries",\n    "risk": "catalog.silver.risk_scores",\n    "compliance": "catalog.silver.compliance_checks"\n}\nfor name, table in sources.items():\n    df = spark.table(table)\n    df.write.format("delta").mode("overwrite").saveAsTable(f"catalog.regulatory.{name}_report")\n# Generate consolidated report\nspark.sql("CREATE OR REPLACE TABLE catalog.regulatory.consolidated AS SELECT * FROM catalog.regulatory.finance_report f JOIN catalog.regulatory.risk_scores r ON f.entity_id = r.entity_id")`,
    before: [{ sources: 3, tables: 'finance, risk, compliance' }],
    after: [{ table: 'regulatory.consolidated', sources: 3, sla: 'T+1' }],
  },

  // ─── 31–40: Bronze-to-Silver Processing ───
  {
    id: 31,
    group: 'Bronze-to-Silver',
    title: 'Standard Cleansing Pipeline',
    flow: 'Bronze \u2192 Clean Silver',
    complexity: 2,
    volume: 4,
    sla: 2,
    reliability: 4,
    governance: 3,
    score: 3.0,
    code: `df = spark.table("catalog.bronze.raw_data")\nclean = df.dropDuplicates().filter("id IS NOT NULL").withColumn("name", trim(lower(col("name"))))\nclean.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.clean_data")`,
    before: [{ source: 'bronze.raw_data', rows: 5000000, issues: 'dupes, nulls, whitespace' }],
    after: [{ table: 'silver.clean_data', rows: 4850000, clean: true }],
  },
  {
    id: 32,
    group: 'Bronze-to-Silver',
    title: 'Deduplication Pipeline',
    flow: 'Bronze \u2192 Dedupe \u2192 Silver',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 3,
    score: 3.6,
    code: `from pyspark.sql.window import Window\nfrom pyspark.sql.functions import row_number\ndf = spark.table("catalog.bronze.customers")\nwindow = Window.partitionBy("email").orderBy(col("updated_at").desc())\ndeduped = df.withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")\ndeduped.write.format("delta").mode("overwrite").saveAsTable("catalog.silver.customers")`,
    before: [{ rows: 2000000, duplicates: 150000 }],
    after: [{ rows: 1850000, duplicates: 0 }],
  },
  {
    id: 33,
    group: 'Bronze-to-Silver',
    title: 'Schema Conformance Pipeline',
    flow: 'Bronze \u2192 Canonical Silver',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `# Conform to canonical schema\ncanonical = spark.table("catalog.bronze.raw").select(\n    col("CUST_ID").alias("customer_id").cast("long"),\n    col("CUST_NAME").alias("name").cast("string"),\n    col("CREATED_DT").alias("created_date").cast("date"),\n    col("AMT").alias("amount").cast("decimal(18,2)")\n)\ncanonical.write.format("delta").saveAsTable("catalog.silver.customers_canonical")`,
    before: [{ columns: 'CUST_ID, CUST_NAME, CREATED_DT, AMT', types: 'mixed' }],
    after: [{ columns: 'customer_id, name, created_date, amount', types: 'enforced' }],
  },
  {
    id: 34,
    group: 'Bronze-to-Silver',
    title: 'PII Masking Pipeline',
    flow: 'Bronze \u2192 Masked Silver',
    complexity: 4,
    volume: 3,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `from pyspark.sql.functions import sha2, regexp_replace\ndf = spark.table("catalog.bronze.customers")\nmasked = df.withColumn("email_hash", sha2("email", 256)) \\\n    .withColumn("phone_masked", regexp_replace("phone", "(\\\\d{3})(\\\\d{3})(\\\\d{4})", "***-***-$3")) \\\n    .drop("email", "phone", "ssn")\nmasked.write.format("delta").saveAsTable("catalog.silver.customers_safe")`,
    before: [{ pii_columns: 'email, phone, ssn' }],
    after: [{ masked: 'email_hash, phone_masked', dropped: 'ssn' }],
  },
  {
    id: 35,
    group: 'Bronze-to-Silver',
    title: 'Data Quality Rules Pipeline',
    flow: 'Bronze \u2192 Validate/Quarantine/Silver',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `df = spark.table("catalog.bronze.orders")\n# Quality rules\nvalid = df.filter("amount > 0 AND customer_id IS NOT NULL AND order_date >= '2020-01-01'")\nquarantine = df.subtract(valid)\nvalid.write.format("delta").saveAsTable("catalog.silver.orders")\nquarantine.write.format("delta").saveAsTable("catalog.quarantine.orders")`,
    before: [{ rows: 5000000 }],
    after: [{ silver: 4850000, quarantine: 150000, quality: '97%' }],
  },
  {
    id: 36,
    group: 'Bronze-to-Silver',
    title: 'Reference Enrichment Pipeline',
    flow: 'Bronze + Lookup \u2192 Silver',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `orders = spark.table("catalog.bronze.orders")\ncustomers = spark.table("catalog.silver.customer_dim")\nenriched = orders.join(customers, "customer_id", "left") \\\n    .withColumn("customer_segment", coalesce("segment", lit("Unknown")))\nenriched.write.format("delta").saveAsTable("catalog.silver.orders_enriched")`,
    before: [{ orders: 5000000, lookup_table: 'customer_dim' }],
    after: [{ rows: 5000000, enriched_columns: 3 }],
  },
  {
    id: 37,
    group: 'Bronze-to-Silver',
    title: 'SCD Logic Pipeline',
    flow: 'Bronze \u2192 SCD \u2192 Silver',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `from delta.tables import DeltaTable\nsource = spark.table("catalog.bronze.customers")\ntarget = DeltaTable.forName(spark, "catalog.silver.customers_scd2")\ntarget.alias("t").merge(source.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true") \\\n    .whenMatchedUpdate(condition="t.email != s.email OR t.city != s.city", set={"is_current": "false", "valid_to": "current_date()"}) \\\n    .whenNotMatchedInsert(values={"customer_id": "s.customer_id", "email": "s.email", "city": "s.city", "is_current": "true", "valid_from": "current_date()", "valid_to": "lit('9999-12-31')"}) \\\n    .execute()`,
    before: [{ source_rows: 500000, changes: 15000 }],
    after: [{ closed_records: 15000, new_records: 15000, total_history: 1200000 }],
  },
  {
    id: 38,
    group: 'Bronze-to-Silver',
    title: 'Late-Arriving Data Pipeline',
    flow: 'Bronze \u2192 Reconcile \u2192 Silver',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `# Handle late-arriving records with MERGE\nfrom delta.tables import DeltaTable\nlate = spark.table("catalog.bronze.late_arrivals")\ntarget = DeltaTable.forName(spark, "catalog.silver.orders")\ntarget.alias("t").merge(late.alias("s"), "t.order_id = s.order_id") \\\n    .whenMatchedUpdateAll() \\\n    .whenNotMatchedInsertAll() \\\n    .execute()`,
    before: [{ late_records: 5000, avg_delay_hours: 24 }],
    after: [{ merged: 5000, updated: 2000, inserted: 3000 }],
  },
  {
    id: 39,
    group: 'Bronze-to-Silver',
    title: 'Multi-Source Harmonization Pipeline',
    flow: 'Many Bronze \u2192 Unified Silver',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `# Harmonize 3 customer sources\ncrm = spark.table("catalog.bronze.crm_customers").select(col("id").alias("customer_id"), "name", "email")\nerp = spark.table("catalog.bronze.erp_customers").select(col("cust_no").alias("customer_id"), col("full_name").alias("name"), "email")\nweb = spark.table("catalog.bronze.web_users").select(col("user_id").alias("customer_id"), col("display_name").alias("name"), "email")\nunified = crm.unionByName(erp).unionByName(web).dropDuplicates(["email"])\nunified.write.format("delta").saveAsTable("catalog.silver.unified_customers")`,
    before: [{ sources: 3, crm: 500000, erp: 300000, web: 1000000 }],
    after: [{ unified: 1200000, deduped_across_sources: true }],
  },
  {
    id: 40,
    group: 'Bronze-to-Silver',
    title: 'Secure Data Segregation Pipeline',
    flow: 'Bronze \u2192 Public/Private Silver',
    complexity: 4,
    volume: 3,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `df = spark.table("catalog.bronze.employees")\n# Public zone (no PII)\npublic = df.select("department", "job_title", "hire_year", "location")\npublic.write.format("delta").saveAsTable("catalog.silver_public.employees")\n# Private zone (with PII, restricted access)\nprivate = df.select("employee_id", "name", "email", "salary", "department")\nprivate.write.format("delta").saveAsTable("catalog.silver_secure.employees")`,
    before: [{ columns: 15, pii_columns: 6 }],
    after: [{ public_columns: 4, private_columns: 5, segregated: true }],
  },

  // ─── 41–50: Silver-to-Gold / AI / Analytics ───
  {
    id: 41,
    group: 'Silver-to-Gold/AI',
    title: 'Fact Table Build Pipeline',
    flow: 'Silver \u2192 Gold Fact',
    complexity: 3,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 3.8,
    code: `spark.sql("""\nCREATE OR REPLACE TABLE catalog.gold.fact_sales AS\nSELECT o.order_id, o.order_date, o.customer_id, c.segment,\n    o.product_id, p.category, o.quantity, o.amount,\n    o.amount * o.quantity AS total_revenue\nFROM catalog.silver.orders o\nJOIN catalog.silver.customers c ON o.customer_id = c.customer_id\nJOIN catalog.silver.products p ON o.product_id = p.product_id\n""")`,
    before: [{ silver_tables: 3, join_keys: 'customer_id, product_id' }],
    after: [{ table: 'gold.fact_sales', rows: 10000000, dimensions: 2 }],
  },
  {
    id: 42,
    group: 'Silver-to-Gold/AI',
    title: 'Dimension Table Build Pipeline',
    flow: 'Silver \u2192 Gold Dimensions',
    complexity: 3,
    volume: 3,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 3.6,
    code: `spark.sql("""\nCREATE OR REPLACE TABLE catalog.gold.dim_customer AS\nSELECT customer_id, name, email, segment, region,\n    first_order_date, total_orders, lifetime_value,\n    CASE WHEN lifetime_value > 10000 THEN 'VIP' ELSE 'Standard' END AS tier\nFROM catalog.silver.customers\n""")`,
    before: [{ source: 'silver.customers', rows: 2000000 }],
    after: [{ table: 'gold.dim_customer', rows: 2000000, computed_cols: 2 }],
  },
  {
    id: 43,
    group: 'Silver-to-Gold/AI',
    title: 'KPI Aggregation Pipeline',
    flow: 'Silver \u2192 Gold KPI Marts',
    complexity: 3,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 3,
    score: 3.8,
    code: `spark.sql("""\nCREATE OR REPLACE TABLE catalog.gold.kpi_daily AS\nSELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue,\n    AVG(amount) AS avg_order, COUNT(DISTINCT customer_id) AS unique_customers\nFROM catalog.silver.orders GROUP BY order_date\n""")`,
    before: [{ source: 'silver.orders', rows: 10000000 }],
    after: [{ table: 'gold.kpi_daily', rows: 365, metrics: 4 }],
  },
  {
    id: 44,
    group: 'Silver-to-Gold/AI',
    title: 'ML Feature Engineering Pipeline',
    flow: 'Silver \u2192 Feature Tables',
    complexity: 4,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `from databricks.feature_engineering import FeatureEngineeringClient\nfe = FeatureEngineeringClient()\nfeatures = spark.sql("""\nSELECT customer_id, COUNT(*) as total_orders, AVG(amount) as avg_order,\n    DATEDIFF(current_date(), MAX(order_date)) as days_since_last,\n    SUM(CASE WHEN amount > 100 THEN 1 ELSE 0 END) as high_value_orders\nFROM catalog.silver.orders GROUP BY customer_id\n""")\nfe.create_table(name="catalog.ml.customer_features", primary_keys=["customer_id"], df=features)`,
    before: [{ source: 'silver.orders', rows: 10000000 }],
    after: [{ feature_table: 'ml.customer_features', features: 4, customers: 500000 }],
  },
  {
    id: 45,
    group: 'Silver-to-Gold/AI',
    title: 'Model Scoring Batch Pipeline',
    flow: 'Features \u2192 Predictions',
    complexity: 4,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `import mlflow\nmodel = mlflow.pyfunc.load_model("models:/churn_model/Production")\ndf = spark.table("catalog.ml.customer_features").toPandas()\npredictions = model.predict(df)\nresult = spark.createDataFrame(df.assign(churn_score=predictions))\nresult.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.churn_predictions")`,
    before: [{ features: 500000, model: 'churn_model v2.1' }],
    after: [{ predictions: 500000, accuracy: '94.2%', auc: 0.97 }],
  },
  {
    id: 46,
    group: 'Silver-to-Gold/AI',
    title: 'RAG Chunking Pipeline',
    flow: 'Documents \u2192 Chunks/Metadata',
    complexity: 5,
    volume: 4,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.4,
    code: `# Chunk for RAG\ndef chunk(text, size=500, overlap=50):\n    return [text[i:i+size] for i in range(0, len(text), size-overlap)]\nchunk_udf = udf(chunk, ArrayType(StringType()))\ndf = spark.table("catalog.bronze.documents")\nchunked = df.withColumn("chunks", chunk_udf("content")).select("doc_id", explode("chunks").alias("chunk_text"))\nchunked.write.format("delta").saveAsTable("catalog.ml.doc_chunks")`,
    before: [{ documents: 10000, avg_pages: 15 }],
    after: [{ chunks: 500000, avg_chunk_size: 500 }],
  },
  {
    id: 47,
    group: 'Silver-to-Gold/AI',
    title: 'Embedding Generation Pipeline',
    flow: 'Chunks \u2192 Embeddings/Vector',
    complexity: 5,
    volume: 4,
    sla: 4,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `from sentence_transformers import SentenceTransformer\nmodel = SentenceTransformer("all-MiniLM-L6-v2")\n@pandas_udf("array<float>")\ndef embed(texts):\n    return pd.Series(model.encode(texts.tolist()).tolist())\ndf = spark.table("catalog.ml.doc_chunks")\ndf.withColumn("embedding", embed("chunk_text")).write.format("delta").saveAsTable("catalog.ml.embeddings")`,
    before: [{ chunks: 500000 }],
    after: [{ embeddings: 500000, dimensions: 384, model: 'MiniLM-L6-v2' }],
  },
  {
    id: 48,
    group: 'Silver-to-Gold/AI',
    title: 'Audit and Lineage Pipeline',
    flow: 'Job Metadata \u2192 Audit Delta/UC',
    complexity: 4,
    volume: 3,
    sla: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `# Capture pipeline lineage\nlineage = [{"source": "bronze.orders", "target": "silver.orders", "transform": "dedup+clean", "pipeline": "daily_etl", "ts": str(current_timestamp())}]\ndf = spark.createDataFrame(lineage)\ndf.write.format("delta").mode("append").saveAsTable("catalog.audit.lineage")`,
    before: [{ pipelines_tracked: 50 }],
    after: [{ lineage_records: 500, pipelines: 50 }],
  },
  {
    id: 49,
    group: 'Silver-to-Gold/AI',
    title: 'Reconciliation Pipeline',
    flow: 'Source vs Target Counts/Checksums',
    complexity: 4,
    volume: 3,
    sla: 4,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `from pyspark.sql.functions import md5, concat_ws\nsource = spark.read.format("jdbc").option("dbtable", "orders").load()\ntarget = spark.table("catalog.silver.orders")\nsource_count = source.count()\ntarget_count = target.count()\nmatch = source_count == target_count\nreport = spark.createDataFrame([{"source_count": source_count, "target_count": target_count, "match": match, "check_ts": str(current_timestamp())}])\nreport.write.format("delta").mode("append").saveAsTable("catalog.audit.reconciliation")`,
    before: [{ source_rows: 5000000 }],
    after: [{ source: 5000000, target: 5000000, match: true }],
  },
  {
    id: 50,
    group: 'Silver-to-Gold/AI',
    title: 'Disaster Recovery Replication Pipeline',
    flow: 'Region A Delta \u2192 Region B Delta',
    complexity: 5,
    volume: 5,
    sla: 5,
    reliability: 5,
    governance: 5,
    score: 5.0,
    code: `# Cross-region Deep Clone for DR\ntables = spark.sql("SHOW TABLES IN catalog.gold").collect()\nfor row in tables:\n    source = f"catalog.gold.{row.tableName}"\n    dr = f"catalog_dr.gold.{row.tableName}"\n    spark.sql(f"CREATE OR REPLACE TABLE {dr} DEEP CLONE {source}")\n    print(f"Replicated {source} -> {dr}")`,
    before: [{ source_region: 'us-east-1', tables: 25, total_size_tb: 5 }],
    after: [{ dr_region: 'us-west-2', tables: 25, replicated: true, rpo: '< 1hr' }],
  },
];

const groups = [...new Set(batchPipelines.map((p) => p.group))];

function ScoreBar({ label, value, max }) {
  const pct = (value / max) * 100;
  const color = value >= 4 ? '#22c55e' : value >= 3 ? '#f59e0b' : '#ef4444';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '0.7rem' }}>
      <span style={{ width: '65px', color: 'var(--text-secondary)' }}>{label}</span>
      <div style={{ flex: 1, height: '8px', background: '#f3f4f6', borderRadius: '4px' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: '4px' }} />
      </div>
      <span style={{ width: '18px', textAlign: 'right', fontWeight: 600 }}>{value}</span>
    </div>
  );
}

function BatchPipelines() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = batchPipelines
    .filter((p) => {
      const matchGroup = selectedGroup === 'All' || p.group === selectedGroup;
      const matchSearch =
        p.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
        p.flow.toLowerCase().includes(searchTerm.toLowerCase());
      return matchGroup && matchSearch;
    })
    .sort((a, b) => (sortBy === 'score' ? b.score - a.score : a.id - b.id));

  const downloadScorecard = () => {
    exportToCSV(
      batchPipelines.map((p) => ({
        id: p.id,
        group: p.group,
        title: p.title,
        flow: p.flow,
        complexity: p.complexity,
        volume: p.volume,
        sla: p.sla,
        reliability: p.reliability,
        governance: p.governance,
        score: p.score,
      })),
      'batch-pipelines-scorecard.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Batch Pipeline Scenarios</h1>
          <p>
            50 end-to-end batch pipelines with KPI scoring — Source-to-Bronze, DB Extraction,
            Enterprise, Bronze-to-Silver, Gold/AI
          </p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">{'\u26a1'}</div>
          <div className="stat-info">
            <h4>50</h4>
            <p>Pipelines</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">{'\ud83c\udfc2'}</div>
          <div className="stat-info">
            <h4>{groups.length}</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">{'\u2b50'}</div>
          <div className="stat-info">
            <h4>{(batchPipelines.reduce((s, x) => s + x.score, 0) / 50).toFixed(1)}</h4>
            <p>Avg Score</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'\ud83c\udfc6'}</div>
          <div className="stat-info">
            <h4>{batchPipelines.filter((p) => p.score >= 4.0).length}</h4>
            <p>Advanced (4.0+)</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search pipelines..."
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
                {g} ({batchPipelines.filter((p) => p.group === g).length})
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

      {filtered.map((p) => {
        const isExpanded = expandedId === p.id;
        return (
          <div key={p.id} className="card" style={{ marginBottom: '0.75rem' }}>
            <div
              onClick={() => setExpandedId(isExpanded ? null : p.id)}
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
                    flexWrap: 'wrap',
                    marginBottom: '0.2rem',
                  }}
                >
                  <span className="badge running">{p.group}</span>
                  <strong>
                    #{p.id} \u2014 {p.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{p.flow}</p>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <span
                  style={{
                    fontSize: '1.1rem',
                    fontWeight: 700,
                    color:
                      p.score >= 4
                        ? 'var(--success)'
                        : p.score >= 3
                          ? 'var(--warning)'
                          : 'var(--error)',
                  }}
                >
                  {p.score}
                </span>
                <span style={{ color: 'var(--text-secondary)' }}>
                  {isExpanded ? '\u25BC' : '\u25B6'}
                </span>
              </div>
            </div>

            {isExpanded && (
              <div style={{ marginTop: '1rem' }}>
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr 1fr 1fr',
                    gap: '1rem',
                    marginBottom: '1rem',
                  }}
                >
                  {/* Scoring */}
                  <div style={{ padding: '0.6rem', background: '#f8f9fa', borderRadius: '6px' }}>
                    <div style={{ fontSize: '0.72rem', fontWeight: 700, marginBottom: '0.4rem' }}>
                      KPI Scoring
                    </div>
                    <ScoreBar label="Complexity" value={p.complexity} max={5} />
                    <ScoreBar label="Volume" value={p.volume} max={5} />
                    <ScoreBar label="SLA" value={p.sla} max={5} />
                    <ScoreBar label="Reliability" value={p.reliability} max={5} />
                    <ScoreBar label="Governance" value={p.governance} max={5} />
                    <div style={{ marginTop: '0.4rem', fontSize: '0.8rem', fontWeight: 700 }}>
                      Overall:{' '}
                      <span
                        style={{
                          color: p.score >= 4 ? '#22c55e' : p.score >= 3 ? '#f59e0b' : '#ef4444',
                        }}
                      >
                        {p.score}/5.0
                      </span>
                    </div>
                  </div>

                  {/* Before */}
                  <div
                    style={{
                      padding: '0.6rem',
                      background: '#fef2f2',
                      borderRadius: '6px',
                      border: '1px solid #fecaca',
                    }}
                  >
                    <div
                      style={{
                        fontSize: '0.6rem',
                        fontWeight: 700,
                        color: '#991b1b',
                        textTransform: 'uppercase',
                        marginBottom: '0.3rem',
                      }}
                    >
                      Input (Before)
                    </div>
                    {p.before.map((row, i) => (
                      <div key={i}>
                        {Object.entries(row).map(([k, v]) => (
                          <div
                            key={k}
                            style={{ fontSize: '0.72rem', display: 'flex', gap: '0.3rem' }}
                          >
                            <span style={{ color: 'var(--text-secondary)' }}>{k}:</span>
                            <span style={{ fontWeight: 500 }}>{String(v)}</span>
                          </div>
                        ))}
                      </div>
                    ))}
                  </div>

                  {/* After */}
                  <div
                    style={{
                      padding: '0.6rem',
                      background: '#dcfce7',
                      borderRadius: '6px',
                      border: '1px solid #bbf7d0',
                    }}
                  >
                    <div
                      style={{
                        fontSize: '0.6rem',
                        fontWeight: 700,
                        color: '#166534',
                        textTransform: 'uppercase',
                        marginBottom: '0.3rem',
                      }}
                    >
                      Output (After)
                    </div>
                    {p.after.map((row, i) => (
                      <div key={i}>
                        {Object.entries(row).map(([k, v]) => (
                          <div
                            key={k}
                            style={{ fontSize: '0.72rem', display: 'flex', gap: '0.3rem' }}
                          >
                            <span style={{ color: 'var(--text-secondary)' }}>{k}:</span>
                            <span style={{ fontWeight: 500, color: '#166534' }}>{String(v)}</span>
                          </div>
                        ))}
                      </div>
                    ))}
                  </div>
                </div>

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
                    {p.code}
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

export default BatchPipelines;
