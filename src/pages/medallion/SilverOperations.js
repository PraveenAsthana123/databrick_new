import React, { useState } from 'react';
import ScenarioCard from '../../components/common/ScenarioCard';
import { exportToCSV } from '../../utils/fileExport';

const silverOperations = [
  // ─── 1–10: Core Transform ───
  {
    id: 1,
    group: 'Core Transform',
    title: 'Data Cleansing',
    desc: 'Remove noise, whitespace, and invalid characters from raw fields',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_customers")
cleaned = df.withColumn("name", F.trim(F.regexp_replace("name", r"[^\\w\\s]", ""))) \
            .withColumn("email", F.lower(F.trim("email"))) \
            .withColumn("phone", F.regexp_replace("phone", r"[^\\d+]", "")) \
            .filter(F.col("name").isNotNull() & (F.length("name") > 0))
cleaned.write.format("delta").mode("overwrite").saveAsTable("silver.customers")`,
  },
  {
    id: 2,
    group: 'Core Transform',
    title: 'Standardization',
    desc: 'Enforce consistent formats for dates, codes, and categorical values',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_orders")
std = df.withColumn("order_date", F.to_date("order_date", "MM/dd/yyyy")) \
        .withColumn("country_code", F.upper(F.trim("country_code"))) \
        .withColumn("status", F.when(F.col("status").isin("C","CLOSED"), "Closed")
                               .when(F.col("status").isin("O","OPEN"), "Open")
                               .otherwise("Unknown")) \
        .withColumn("currency", F.upper(F.col("currency")))
std.write.format("delta").mode("overwrite").saveAsTable("silver.orders")`,
  },
  {
    id: 3,
    group: 'Core Transform',
    title: 'Type Casting',
    desc: 'Cast string fields from bronze to correct data types in silver',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, TimestampType

df = spark.table("bronze.raw_transactions")
typed = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2))) \
          .withColumn("quantity", F.col("quantity").cast(IntegerType())) \
          .withColumn("created_at", F.col("created_at").cast(TimestampType())) \
          .withColumn("is_active", F.col("is_active").cast("boolean"))
typed.write.format("delta").mode("overwrite").saveAsTable("silver.transactions")`,
  },
  {
    id: 4,
    group: 'Core Transform',
    title: 'Column Renaming',
    desc: 'Align bronze field names to enterprise naming conventions',
    code: `df = spark.table("bronze.raw_products")

rename_map = {
    "prod_id": "product_id",
    "prod_nm": "product_name",
    "cat_cd": "category_code",
    "lst_prc": "list_price",
    "crt_dt": "created_date",
}
renamed = df
for old, new in rename_map.items():
    if old in df.columns:
        renamed = renamed.withColumnRenamed(old, new)
renamed.write.format("delta").mode("overwrite").saveAsTable("silver.products")`,
  },
  {
    id: 5,
    group: 'Core Transform',
    title: 'Data Normalization',
    desc: 'Normalize numeric fields to 0-1 range for downstream ML pipelines',
    code: `from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql import functions as F

df = spark.table("bronze.raw_metrics")
assembler = VectorAssembler(inputCols=["revenue", "units", "margin"], outputCol="features")
assembled = assembler.transform(df)
scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
model = scaler.fit(assembled)
normalized = model.transform(assembled).drop("features")
normalized.write.format("delta").mode("overwrite").saveAsTable("silver.metrics_normalized")`,
  },
  {
    id: 6,
    group: 'Core Transform',
    title: 'Flatten Nested',
    desc: 'Explode arrays and flatten structs from JSON-sourced bronze tables',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_events")
flat = df.withColumn("event_item", F.explode("items")) \
         .withColumn("item_id", F.col("event_item.id")) \
         .withColumn("item_price", F.col("event_item.price")) \
         .withColumn("item_qty", F.col("event_item.qty")) \
         .withColumn("user_id", F.col("user.id")) \
         .withColumn("user_country", F.col("user.country")) \
         .drop("items", "user", "event_item")
flat.write.format("delta").mode("overwrite").saveAsTable("silver.events_flat")`,
  },
  {
    id: 7,
    group: 'Core Transform',
    title: 'Data Enrichment',
    desc: 'Join bronze data with reference tables to add descriptive attributes',
    code: `from pyspark.sql import functions as F

orders = spark.table("bronze.raw_orders")
customers = spark.table("silver.customers")
products = spark.table("silver.products")

enriched = orders.join(customers, "customer_id", "left") \
                 .join(products, "product_id", "left") \
                 .select("order_id", "order_date", "customer_name",
                         "product_name", "category_code", "amount", "status")
enriched.write.format("delta").mode("overwrite").saveAsTable("silver.orders_enriched")`,
  },
  {
    id: 8,
    group: 'Core Transform',
    title: 'Derived Columns',
    desc: 'Compute business metrics as new columns from existing fields',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.orders")
derived = df.withColumn("order_year", F.year("order_date")) \
            .withColumn("order_month", F.month("order_date")) \
            .withColumn("order_quarter", F.quarter("order_date")) \
            .withColumn("revenue", F.col("unit_price") * F.col("quantity")) \
            .withColumn("discount_amount", F.col("revenue") * F.col("discount_pct") / 100) \
            .withColumn("net_revenue", F.col("revenue") - F.col("discount_amount"))
derived.write.format("delta").mode("overwrite").saveAsTable("silver.orders_derived")`,
  },
  {
    id: 9,
    group: 'Core Transform',
    title: 'Unit Conversion',
    desc: 'Convert imperial/metric units and currency to standard canonical values',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_measurements")
converted = df.withColumn("weight_kg",
                F.when(F.col("unit") == "lbs", F.col("weight") * 0.453592)
                 .otherwise(F.col("weight"))) \
              .withColumn("height_cm",
                F.when(F.col("unit") == "inches", F.col("height") * 2.54)
                 .otherwise(F.col("height"))) \
              .withColumn("price_usd",
                F.col("price") * F.col("fx_rate_to_usd"))
converted.write.format("delta").mode("overwrite").saveAsTable("silver.measurements")`,
  },
  {
    id: 10,
    group: 'Core Transform',
    title: 'Canonical Mapping',
    desc: 'Map source system codes to enterprise canonical reference values',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_orders")
status_map = spark.createDataFrame([
    ("C", "Closed"), ("O", "Open"), ("P", "Pending"),
    ("X", "Cancelled"), ("H", "On-Hold")
], ["src_code", "canonical_status"])

canonical = df.join(status_map, df.status == status_map.src_code, "left") \
              .withColumn("order_status", F.coalesce("canonical_status", F.lit("Unknown"))) \
              .drop("src_code", "canonical_status", "status")
canonical.write.format("delta").mode("overwrite").saveAsTable("silver.orders_canonical")`,
  },

  // ─── 11–20: Data Quality ───
  {
    id: 11,
    group: 'Data Quality',
    title: 'Null Handling',
    desc: 'Apply fill strategies: drop, impute, or sentinel for nulls per column',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_customers")
handled = df.fillna({"country": "Unknown", "segment": "Unassigned"}) \
            .withColumn("age", F.when(F.col("age").isNull(), F.lit(0)).otherwise(F.col("age"))) \
            .withColumn("revenue", F.coalesce("revenue", F.lit(0.0))) \
            .filter(F.col("customer_id").isNotNull())
handled.write.format("delta").mode("overwrite").saveAsTable("silver.customers_clean")`,
  },
  {
    id: 12,
    group: 'Data Quality',
    title: 'Duplicate Removal',
    desc: 'Deduplicate records using window function to keep latest version',
    code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table("bronze.raw_orders")
window = Window.partitionBy("order_id").orderBy(F.desc("updated_at"))
deduped = df.withColumn("rn", F.row_number().over(window)) \
            .filter(F.col("rn") == 1) \
            .drop("rn")
deduped.write.format("delta").mode("overwrite").saveAsTable("silver.orders_deduped")`,
  },
  {
    id: 13,
    group: 'Data Quality',
    title: 'Constraint Validation',
    desc: 'Enforce Delta table constraints to reject rows violating business rules',
    code: `-- Add Delta constraints to silver table
ALTER TABLE silver.orders
  ADD CONSTRAINT positive_amount CHECK (amount > 0);

ALTER TABLE silver.orders
  ADD CONSTRAINT valid_status CHECK (status IN ('Open','Closed','Pending','Cancelled'));

ALTER TABLE silver.customers
  ADD CONSTRAINT valid_email CHECK (email LIKE '%@%.%');

-- View all constraints
SHOW TBLPROPERTIES silver.orders;`,
  },
  {
    id: 14,
    group: 'Data Quality',
    title: 'Data Validation Rules',
    desc: 'Apply multi-rule validation and route bad records to quarantine table',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_transactions")
valid = df.filter(
    F.col("amount").between(0.01, 1_000_000) &
    F.col("transaction_date").isNotNull() &
    F.length("account_id").between(8, 20) &
    F.col("currency").rlike("^[A-Z]{3}$")
)
invalid = df.subtract(valid)
valid.write.format("delta").mode("append").saveAsTable("silver.transactions")
invalid.write.format("delta").mode("append").saveAsTable("silver.quarantine_transactions")`,
  },
  {
    id: 15,
    group: 'Data Quality',
    title: 'Outlier Detection',
    desc: 'Detect and flag statistical outliers using IQR method',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_sales")
stats = df.approxQuantile("sale_amount", [0.25, 0.75], 0.01)
q1, q3 = stats[0], stats[1]
iqr = q3 - q1
lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr

flagged = df.withColumn("is_outlier",
    F.when((F.col("sale_amount") < lower) | (F.col("sale_amount") > upper), True)
     .otherwise(False))
flagged.write.format("delta").mode("overwrite").saveAsTable("silver.sales_with_outliers")`,
  },
  {
    id: 16,
    group: 'Data Quality',
    title: 'Range Checks',
    desc: 'Validate numeric and date fields fall within expected business ranges',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_hr_data")
range_checked = df.withColumn("age_valid", F.col("age").between(16, 80)) \
                  .withColumn("salary_valid", F.col("salary").between(20000, 2_000_000)) \
                  .withColumn("hire_valid", F.col("hire_date") >= F.lit("1970-01-01"))

pass_df = range_checked.filter(F.col("age_valid") & F.col("salary_valid") & F.col("hire_valid")) \
                        .drop("age_valid", "salary_valid", "hire_valid")
pass_df.write.format("delta").mode("overwrite").saveAsTable("silver.hr_data")`,
  },
  {
    id: 17,
    group: 'Data Quality',
    title: 'Pattern Validation',
    desc: 'Validate fields against regex patterns for emails, phone, postal codes',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_contacts")
validated = df.withColumn("email_ok", F.col("email").rlike(r"^[\\w.+\\-]+@[\\w\\-]+\\.[a-z]{2,}$")) \
              .withColumn("phone_ok", F.col("phone").rlike(r"^\\+?[1-9]\\d{6,14}$")) \
              .withColumn("zip_ok", F.col("zip_code").rlike(r"^\\d{5}(-\\d{4})?$"))

clean = validated.filter(F.col("email_ok") & F.col("phone_ok") & F.col("zip_ok")) \
                 .drop("email_ok", "phone_ok", "zip_ok")
clean.write.format("delta").mode("overwrite").saveAsTable("silver.contacts")`,
  },
  {
    id: 18,
    group: 'Data Quality',
    title: 'Data Completeness',
    desc: 'Profile completeness per column and raise alert if below threshold',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_orders")
total = df.count()
completeness = df.select([
    (F.count(F.col(c)) / total * 100).alias(c) for c in df.columns
]).collect()[0].asDict()

threshold = 95.0
failing = {col: pct for col, pct in completeness.items() if pct < threshold}
if failing:
    print(f"COMPLETENESS ALERT: {failing}")
    raise ValueError(f"Columns below {threshold}% completeness: {list(failing.keys())}")`,
  },
  {
    id: 19,
    group: 'Data Quality',
    title: 'Cross-Table Validation',
    desc: 'Validate referential integrity between silver tables',
    code: `from pyspark.sql import functions as F

orders = spark.table("bronze.raw_orders")
customers = spark.table("silver.customers").select("customer_id")

orphans = orders.join(customers, "customer_id", "left_anti")
orphan_count = orphans.count()
if orphan_count > 0:
    orphans.write.format("delta").mode("overwrite").saveAsTable("silver.dq_orphan_orders")
    print(f"WARNING: {orphan_count} orders with no matching customer")
else:
    print("Referential integrity check passed")`,
  },
  {
    id: 20,
    group: 'Data Quality',
    title: 'Quarantine Bad Data',
    desc: 'Route all failed DQ rows to a quarantine table with failure reasons',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_orders")
df = df.withColumn("dq_reason",
    F.when(F.col("amount").isNull(), "null_amount")
     .when(F.col("amount") <= 0, "non_positive_amount")
     .when(F.col("order_date").isNull(), "null_date")
     .when(~F.col("status").isin("Open","Closed","Pending"), "invalid_status")
     .otherwise(None))

good = df.filter(F.col("dq_reason").isNull()).drop("dq_reason")
bad  = df.filter(F.col("dq_reason").isNotNull()) \
         .withColumn("quarantine_ts", F.current_timestamp())
good.write.format("delta").mode("append").saveAsTable("silver.orders")
bad.write.format("delta").mode("append").saveAsTable("silver.quarantine_orders")`,
  },

  // ─── 21–30: Data Integration ───
  {
    id: 21,
    group: 'Data Integration',
    title: 'Multi-Source Join',
    desc: 'Combine data from multiple bronze sources into a single silver table',
    code: `from pyspark.sql import functions as F

crm    = spark.table("bronze.crm_customers")
erp    = spark.table("bronze.erp_accounts")
web    = spark.table("bronze.web_signups")

integrated = crm.join(erp, crm.crm_id == erp.account_ref, "full") \
                .join(web, crm.email == web.email, "left") \
                .select("crm_id", "account_ref", "email",
                        "crm.name", "erp.company", "web.signup_date") \
                .withColumn("source", F.lit("multi_source"))
integrated.write.format("delta").mode("overwrite").saveAsTable("silver.customers_integrated")`,
  },
  {
    id: 22,
    group: 'Data Integration',
    title: 'Lookup Joins',
    desc: 'Enrich records by joining to static lookup/reference tables',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_orders")
country_lkp = spark.table("reference.country_codes")
product_lkp = spark.table("reference.product_categories")

enriched = df.join(F.broadcast(country_lkp), "country_code", "left") \
             .join(F.broadcast(product_lkp), "category_id", "left") \
             .withColumn("region", F.coalesce("region", F.lit("Unknown"))) \
             .withColumn("category_name", F.coalesce("category_name", F.lit("Uncategorized")))
enriched.write.format("delta").mode("overwrite").saveAsTable("silver.orders_enriched")`,
  },
  {
    id: 23,
    group: 'Data Integration',
    title: 'Master Data Integration',
    desc: 'Unify customer master records from multiple authoritative source systems',
    code: `from pyspark.sql import functions as F

mdm_source   = spark.table("bronze.mdm_customers")   # authoritative
crm_source   = spark.table("bronze.crm_customers")
erp_source   = spark.table("bronze.erp_customers")

# MDM wins on conflict; CRM and ERP fill gaps
unified = mdm_source.alias("m") \
    .join(crm_source.alias("c"), "global_id", "left") \
    .join(erp_source.alias("e"), "global_id", "left") \
    .select("global_id",
            F.coalesce("m.name","c.name","e.name").alias("name"),
            F.coalesce("m.email","c.email").alias("email"),
            F.coalesce("m.phone","c.phone","e.phone").alias("phone"))
unified.write.format("delta").mode("overwrite").saveAsTable("silver.customers_master")`,
  },
  {
    id: 24,
    group: 'Data Integration',
    title: 'Data Harmonization',
    desc: 'Align schemas from heterogeneous sources to a common data model',
    code: `from pyspark.sql import functions as F

def harmonize(df, source_name, field_map):
    for src, tgt in field_map.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, tgt)
    return df.withColumn("source_system", F.lit(source_name))

sap_map = {"KUNNR":"customer_id","NAME1":"name","LAND1":"country"}
sf_map  = {"AccountId":"customer_id","Name":"name","BillingCountry":"country"}

sap_df = harmonize(spark.table("bronze.sap_customers"), "SAP", sap_map)
sf_df  = harmonize(spark.table("bronze.sf_accounts"), "Salesforce", sf_map)
unified = sap_df.unionByName(sf_df, allowMissingColumns=True)
unified.write.format("delta").mode("overwrite").saveAsTable("silver.customers_harmonized")`,
  },
  {
    id: 25,
    group: 'Data Integration',
    title: 'Key Mapping',
    desc: 'Build a cross-system key mapping table for customer identity resolution',
    code: `from pyspark.sql import functions as F

sap = spark.table("bronze.sap_customers").select("sap_id", "email")
crm = spark.table("bronze.crm_customers").select("crm_id", "email")
erp = spark.table("bronze.erp_accounts").select("erp_id", "email")

key_map = sap.join(crm, "email", "full") \
             .join(erp, "email", "full") \
             .select("email", "sap_id", "crm_id", "erp_id") \
             .withColumn("global_id", F.sha2(F.col("email"), 256)) \
             .withColumn("mapped_at", F.current_timestamp())
key_map.write.format("delta").mode("overwrite").saveAsTable("silver.customer_key_map")`,
  },
  {
    id: 26,
    group: 'Data Integration',
    title: 'Entity Resolution',
    desc: 'Identify and merge duplicate entities across sources using blocking keys',
    code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table("bronze.raw_contacts")
df = df.withColumn("blocking_key",
    F.concat(F.upper(F.substring("last_name", 1, 3)),
             F.regexp_replace("phone", r"\\D", "").substr(-4, 4)))

window = Window.partitionBy("blocking_key").orderBy("created_at")
resolved = df.withColumn("entity_id", F.first("contact_id").over(window)) \
             .withColumn("is_duplicate", F.col("contact_id") != F.col("entity_id"))
resolved.write.format("delta").mode("overwrite").saveAsTable("silver.contacts_resolved")`,
  },
  {
    id: 27,
    group: 'Data Integration',
    title: 'Conflict Resolution',
    desc: 'Resolve field-level conflicts from multiple source systems by priority',
    code: `from pyspark.sql import functions as F

# Priority: MDM > CRM > ERP > Web
sources = ["mdm", "crm", "erp", "web"]
dfs = {s: spark.table(f"bronze.{s}_customers") for s in sources}

base = dfs["mdm"]
for src in ["crm", "erp", "web"]:
    base = base.join(dfs[src].alias(src), "customer_id", "left")

resolved = base.select("customer_id",
    F.coalesce(*[F.col(f"{s}.name") for s in sources]).alias("name"),
    F.coalesce(*[F.col(f"{s}.email") for s in sources]).alias("email"))
resolved.write.format("delta").mode("overwrite").saveAsTable("silver.customers_resolved")`,
  },
  {
    id: 28,
    group: 'Data Integration',
    title: 'Schema Alignment',
    desc: 'Align partial schemas from multiple sources before union',
    code: `from pyspark.sql import functions as F

common_cols = ["customer_id", "name", "email", "country", "segment",
               "created_at", "source_system"]

def align_schema(df, source):
    for col in common_cols:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))
    return df.select(common_cols).withColumn("source_system", F.lit(source))

sap = align_schema(spark.table("bronze.sap_customers"), "SAP")
crm = align_schema(spark.table("bronze.crm_customers"), "CRM")
aligned = sap.union(crm)
aligned.write.format("delta").mode("overwrite").saveAsTable("silver.customers_aligned")`,
  },
  {
    id: 29,
    group: 'Data Integration',
    title: 'Data Merging',
    desc: 'Merge incremental changes from source into silver table using MERGE',
    code: `-- MERGE incremental source into silver
MERGE INTO silver.customers AS target
USING (
    SELECT customer_id, name, email, phone, updated_at
    FROM bronze.crm_incremental
    WHERE updated_at > (SELECT MAX(updated_at) FROM silver.customers)
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET name=source.name, email=source.email, updated_at=source.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, phone, updated_at)
  VALUES (source.customer_id, source.name, source.email, source.phone, source.updated_at);`,
  },
  {
    id: 30,
    group: 'Data Integration',
    title: 'Source Prioritization',
    desc: 'Rank source systems and pick the highest-priority non-null value',
    code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assign priority ranks: 1=highest
df = spark.table("bronze.all_sources") \
          .withColumn("priority",
              F.when(F.col("source") == "MDM", 1)
               .when(F.col("source") == "CRM", 2)
               .when(F.col("source") == "ERP", 3)
               .otherwise(9))

window = Window.partitionBy("customer_id").orderBy("priority")
prioritized = df.withColumn("rn", F.row_number().over(window)) \
                .filter(F.col("rn") == 1).drop("rn", "priority")
prioritized.write.format("delta").mode("overwrite").saveAsTable("silver.customers_prioritized")`,
  },

  // ─── 31–40: SCD & History ───
  {
    id: 31,
    group: 'SCD & History',
    title: 'SCD Type 1',
    desc: 'Overwrite existing records in silver with the latest source values',
    code: `-- SCD Type 1: overwrite (no history)
MERGE INTO silver.customers AS target
USING bronze.crm_snapshot AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    target.name      = source.name,
    target.email     = source.email,
    target.segment   = source.segment,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, segment, updated_at)
  VALUES (source.customer_id, source.name, source.email,
          source.segment, current_timestamp());`,
  },
  {
    id: 32,
    group: 'SCD & History',
    title: 'SCD Type 2',
    desc: 'Preserve full history by adding new rows with effective date ranges',
    code: `-- SCD Type 2: full history with effective dates
MERGE INTO silver.customers_history AS target
USING (
    SELECT *, current_date() AS eff_start, '9999-12-31' AS eff_end, True AS is_current
    FROM bronze.crm_snapshot
) AS source
ON target.customer_id = source.customer_id AND target.is_current = True
WHEN MATCHED AND (target.name <> source.name OR target.segment <> source.segment) THEN
  UPDATE SET target.eff_end = current_date() - 1, target.is_current = False
WHEN NOT MATCHED THEN
  INSERT *;`,
  },
  {
    id: 33,
    group: 'SCD & History',
    title: 'SCD Type 3',
    desc: 'Track only the previous value alongside the current value per column',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.crm_snapshot")
current = spark.table("silver.customers_scd3")

scd3 = current.join(df, "customer_id", "left") \
              .withColumn("prev_segment", F.col("current.segment")) \
              .withColumn("segment", F.col("crm_snapshot.segment")) \
              .withColumn("prev_email", F.col("current.email")) \
              .withColumn("email", F.col("crm_snapshot.email")) \
              .withColumn("changed_at", F.current_timestamp())
scd3.write.format("delta").mode("overwrite").saveAsTable("silver.customers_scd3")`,
  },
  {
    id: 34,
    group: 'SCD & History',
    title: 'Effective Date Tracking',
    desc: 'Maintain start/end effective dates for each record version in silver',
    code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table("bronze.product_changes")
window = Window.partitionBy("product_id").orderBy("change_date")

with_dates = df.withColumn("eff_start", F.col("change_date")) \
               .withColumn("eff_end",
                   F.lead("change_date", 1, "9999-12-31").over(window)) \
               .withColumn("is_current", F.col("eff_end") == F.lit("9999-12-31"))
with_dates.write.format("delta").mode("overwrite").saveAsTable("silver.product_history")`,
  },
  {
    id: 35,
    group: 'SCD & History',
    title: 'Versioning',
    desc: 'Add monotonically increasing version numbers to each record change',
    code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table("bronze.raw_customers")
window = Window.partitionBy("customer_id").orderBy("updated_at")

versioned = df.withColumn("version", F.row_number().over(window)) \
              .withColumn("is_latest",
                  F.row_number().over(
                      Window.partitionBy("customer_id").orderBy(F.desc("updated_at"))
                  ) == 1)
versioned.write.format("delta").mode("overwrite").saveAsTable("silver.customers_versioned")`,
  },
  {
    id: 36,
    group: 'SCD & History',
    title: 'Late Arriving Data',
    desc: 'Reprocess out-of-order records and patch historical silver partitions',
    code: `from pyspark.sql import functions as F

late = spark.table("bronze.late_arriving_orders") \
            .filter(F.col("order_date") < F.current_date() - 7)

# Patch the correct historical date partition
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "silver.orders")
dt.merge(late, "silver_orders.order_id = late.order_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
print(f"Patched {late.count()} late-arriving records into silver.orders")`,
  },
  {
    id: 37,
    group: 'SCD & History',
    title: 'Change Detection',
    desc: 'Detect which fields changed between source snapshot and silver current',
    code: `from pyspark.sql import functions as F

current = spark.table("silver.customers").filter(F.col("is_current") == True)
incoming = spark.table("bronze.crm_snapshot")

cols = ["name", "email", "segment", "phone"]
joined = incoming.alias("new").join(current.alias("old"), "customer_id", "inner")

changes = joined.withColumn("changed_fields",
    F.array(*[
        F.when(F.col(f"new.{c}") != F.col(f"old.{c}"), F.lit(c))
         .otherwise(F.lit(None)) for c in cols
    ])).withColumn("changed_fields", F.array_compact("changed_fields")) \
    .filter(F.size("changed_fields") > 0)
changes.write.format("delta").mode("overwrite").saveAsTable("silver.customer_changes")`,
  },
  {
    id: 38,
    group: 'SCD & History',
    title: 'Merge UPSERT',
    desc: 'Upsert new and changed records; preserve unchanged ones in silver',
    code: `from delta.tables import DeltaTable
from pyspark.sql import functions as F

source = spark.table("bronze.crm_latest")
target = DeltaTable.forName(spark, "silver.customers")

target.alias("t").merge(source.alias("s"), "t.customer_id = s.customer_id") \
    .whenMatchedUpdate(
        condition="s.updated_at > t.updated_at",
        set={"name":"s.name","email":"s.email","updated_at":"s.updated_at"}) \
    .whenNotMatchedInsert(values={
        "customer_id":"s.customer_id","name":"s.name",
        "email":"s.email","updated_at":"s.updated_at"}) \
    .execute()`,
  },
  {
    id: 39,
    group: 'SCD & History',
    title: 'Snapshot Comparison',
    desc: 'Compare two Delta table snapshots to identify net changes over a period',
    code: `-- Compare silver snapshot at two points using Delta time travel
WITH snap_today AS (
    SELECT customer_id, segment FROM silver.customers VERSION AS OF 10
),
snap_last_week AS (
    SELECT customer_id, segment FROM silver.customers
    TIMESTAMP AS OF '2024-01-01 00:00:00'
)
SELECT
    t.customer_id,
    l.segment AS old_segment,
    t.segment AS new_segment
FROM snap_today t
JOIN snap_last_week l ON t.customer_id = l.customer_id
WHERE t.segment <> l.segment;`,
  },
  {
    id: 40,
    group: 'SCD & History',
    title: 'History Reconstruction',
    desc: 'Reconstruct full change history from audit log into a history table',
    code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

audit = spark.table("bronze.audit_log") \
             .filter(F.col("table_name") == "customers") \
             .select("customer_id", "field", "old_val", "new_val", "changed_at")

window = Window.partitionBy("customer_id").orderBy("changed_at")
reconstructed = audit.withColumn("version", F.row_number().over(window)) \
                     .withColumn("eff_end",
                         F.lead("changed_at").over(window))
reconstructed.write.format("delta").mode("overwrite") \
             .saveAsTable("silver.customers_reconstructed_history")`,
  },

  // ─── 41–50: Data Modeling Prep ───
  {
    id: 41,
    group: 'Data Modeling Prep',
    title: 'Surrogate Key Generation',
    desc: 'Generate stable surrogate keys using hash of natural keys',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.customers")
with_sk = df.withColumn("customer_sk",
    F.sha2(F.concat_ws("|", "customer_id", "source_system"), 256)) \
            .withColumn("sk_generated_at", F.current_timestamp())
with_sk.write.format("delta").mode("overwrite").saveAsTable("silver.customers_with_sk")`,
  },
  {
    id: 42,
    group: 'Data Modeling Prep',
    title: 'Grain Definition',
    desc: 'Define and enforce the grain (one row per entity) of the silver table',
    code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Target grain: one row per order_id + product_id combination
df = spark.table("bronze.raw_order_items")
window = Window.partitionBy("order_id", "product_id").orderBy(F.desc("updated_at"))
grain_enforced = df.withColumn("rn", F.row_number().over(window)) \
                   .filter(F.col("rn") == 1).drop("rn")
assert grain_enforced.groupBy("order_id","product_id").count().filter("count > 1").count() == 0
grain_enforced.write.format("delta").mode("overwrite").saveAsTable("silver.order_items")`,
  },
  {
    id: 43,
    group: 'Data Modeling Prep',
    title: 'Fact-Ready Shaping',
    desc: 'Shape transactional data into fact table structure with FK references',
    code: `from pyspark.sql import functions as F

orders = spark.table("silver.orders")
customers = spark.table("silver.customers").select("customer_id","customer_sk")
products  = spark.table("silver.products").select("product_id","product_sk")
dates     = spark.table("silver.dim_date").select("date_id","date_sk")

fact = orders.join(customers, "customer_id") \
             .join(products, "product_id") \
             .join(dates, orders.order_date == dates.date_id) \
             .select("order_id","customer_sk","product_sk","date_sk",
                     "quantity","amount","net_revenue","status")
fact.write.format("delta").mode("overwrite").saveAsTable("silver.fact_orders_ready")`,
  },
  {
    id: 44,
    group: 'Data Modeling Prep',
    title: 'Dimension-Ready Shaping',
    desc: 'Shape entity tables into conformed dimension structure for gold',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.customers")
dim = df.select(
    F.col("customer_sk"),
    F.col("customer_id").alias("natural_key"),
    F.col("name").alias("customer_name"),
    F.col("email"),
    F.col("segment").alias("customer_segment"),
    F.col("country"),
    F.col("region"),
    F.col("eff_start"),
    F.col("eff_end"),
    F.col("is_current")
).filter(F.col("is_current") == True)
dim.write.format("delta").mode("overwrite").saveAsTable("silver.dim_customers_ready")`,
  },
  {
    id: 45,
    group: 'Data Modeling Prep',
    title: 'Partial Denormalization',
    desc: 'Pre-join frequently co-queried tables to reduce gold-layer join costs',
    code: `from pyspark.sql import functions as F

orders   = spark.table("silver.orders")
customers = spark.table("silver.customers").select("customer_id","name","segment","region")
products  = spark.table("silver.products").select("product_id","product_name","category_code")

denorm = orders.join(F.broadcast(customers), "customer_id", "left") \
               .join(F.broadcast(products), "product_id", "left") \
               .drop("customer_id", "product_id")
denorm.write.format("delta").mode("overwrite").saveAsTable("silver.orders_denorm")`,
  },
  {
    id: 46,
    group: 'Data Modeling Prep',
    title: 'Column Pruning',
    desc: 'Drop unnecessary columns from bronze to reduce silver storage footprint',
    code: `df = spark.table("bronze.raw_customers")

# Keep only columns needed for gold modeling
keep_cols = [
    "customer_id", "name", "email", "phone",
    "country", "region", "segment", "tier",
    "created_at", "updated_at", "source_system"
]
drop_cols = [c for c in df.columns if c not in keep_cols]
print(f"Pruning {len(drop_cols)} columns: {drop_cols}")
pruned = df.select(keep_cols)
pruned.write.format("delta").mode("overwrite").saveAsTable("silver.customers_pruned")`,
  },
  {
    id: 47,
    group: 'Data Modeling Prep',
    title: 'Data Grouping',
    desc: 'Pre-aggregate data into summary groups to support dimensional models',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.orders")
grouped = df.groupBy("customer_id", "segment", "region",
                     F.year("order_date").alias("year"),
                     F.month("order_date").alias("month")) \
            .agg(F.count("order_id").alias("order_count"),
                 F.sum("net_revenue").alias("total_revenue"),
                 F.avg("net_revenue").alias("avg_order_value")) \
            .withColumn("aggregated_at", F.current_timestamp())
grouped.write.format("delta").mode("overwrite").saveAsTable("silver.orders_monthly_agg")`,
  },
  {
    id: 48,
    group: 'Data Modeling Prep',
    title: 'Key Standardization',
    desc: 'Standardize natural keys to a consistent format before surrogate assignment',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_products")
std_keys = df.withColumn("product_id",
    F.upper(F.trim(F.regexp_replace("product_id", r"[^A-Za-z0-9\\-]", "")))) \
             .withColumn("sku",
    F.lpad(F.regexp_replace("sku", r"[^\\d]", ""), 12, "0")) \
             .withColumn("category_id",
    F.lower(F.trim("category_id")))
std_keys.write.format("delta").mode("overwrite").saveAsTable("silver.products_std_keys")`,
  },
  {
    id: 49,
    group: 'Data Modeling Prep',
    title: 'Data Partitioning',
    desc: 'Write silver tables partitioned by date and region for query performance',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_orders")
prepared = df.withColumn("order_year", F.year("order_date")) \
             .withColumn("order_month", F.month("order_date"))

prepared.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month", "region") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.orders_partitioned")`,
  },
  {
    id: 50,
    group: 'Data Modeling Prep',
    title: 'Indexing Hints',
    desc: 'Apply Z-order clustering on gold-candidate columns in the silver table',
    code: `-- Apply Z-order clustering on high-cardinality filter columns
OPTIMIZE silver.orders
  ZORDER BY (customer_id, order_date, product_id);

-- Set liquid clustering on new tables (DBR 13+)
ALTER TABLE silver.customers
  CLUSTER BY (region, segment, is_current);

-- Verify clustering columns
DESCRIBE DETAIL silver.orders;
DESCRIBE DETAIL silver.customers;`,
  },

  // ─── 51–60: Security & PII ───
  {
    id: 51,
    group: 'Security & PII',
    title: 'PII Masking',
    desc: 'Apply dynamic column masks to PII fields based on user group membership',
    code: `-- Create masking function for email
CREATE OR REPLACE FUNCTION silver.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_authorized') THEN email
  ELSE CONCAT(LEFT(email, 2), '***@***.com')
END;

-- Apply mask to silver table
ALTER TABLE silver.customers
  ALTER COLUMN email SET MASK silver.mask_email;

-- Verify mask application
SHOW CREATE TABLE silver.customers;`,
  },
  {
    id: 52,
    group: 'Security & PII',
    title: 'Tokenization',
    desc: 'Replace sensitive identifiers with deterministic tokens in silver',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_customers")
SALT = spark.conf.get("spark.token.salt", "default_salt_change_in_prod")

tokenized = df.withColumn("customer_token",
    F.sha2(F.concat(F.col("customer_id"), F.lit(SALT)), 256)) \
              .withColumn("ssn_token",
    F.sha2(F.concat(F.col("ssn"), F.lit(SALT)), 256)) \
              .drop("ssn")
tokenized.write.format("delta").mode("overwrite").saveAsTable("silver.customers_tokenized")`,
  },
  {
    id: 53,
    group: 'Security & PII',
    title: 'Column-Level Encryption',
    desc: 'Encrypt sensitive columns at rest using AES encryption in Databricks',
    code: `from pyspark.sql import functions as F

KEY_ALIAS = "silver_pii_key"

df = spark.table("bronze.raw_customers")
encrypted = df.withColumn("ssn_enc",
    F.aes_encrypt(F.col("ssn").cast("binary"),
                  F.lit(KEY_ALIAS), F.lit("GCM"))) \
              .withColumn("account_num_enc",
    F.aes_encrypt(F.col("account_num").cast("binary"),
                  F.lit(KEY_ALIAS), F.lit("GCM"))) \
              .drop("ssn", "account_num")
encrypted.write.format("delta").mode("overwrite").saveAsTable("silver.customers_encrypted")`,
  },
  {
    id: 54,
    group: 'Security & PII',
    title: 'Row-Level Filtering',
    desc: 'Restrict row visibility by region based on user group membership',
    code: `-- Row-level security on silver customer table
CREATE OR REPLACE FUNCTION silver.customer_row_filter(region STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('global_admin') THEN TRUE
  WHEN is_member('us_team') AND region = 'US' THEN TRUE
  WHEN is_member('eu_team') AND region = 'EU' THEN TRUE
  WHEN is_member('apac_team') AND region = 'APAC' THEN TRUE
  ELSE FALSE
END;

ALTER TABLE silver.customers
  SET ROW FILTER silver.customer_row_filter ON (region);`,
  },
  {
    id: 55,
    group: 'Security & PII',
    title: 'Column-Level Security',
    desc: 'Grant SELECT on specific columns only to restrict sensitive field access',
    code: `-- Grant column-level access for non-PII columns only
GRANT SELECT (customer_id, name, segment, region, country, created_at)
  ON TABLE silver.customers TO \`analyst_group\`;

-- PII-authorized group gets full access
GRANT SELECT ON TABLE silver.customers TO \`pii_authorized\`;

-- Verify column-level grants
SHOW GRANTS ON TABLE silver.customers;`,
  },
  {
    id: 56,
    group: 'Security & PII',
    title: 'Data Classification Enforcement',
    desc: 'Tag all PII and sensitive columns with classification labels',
    code: `-- Tag PII columns for governance tracking
ALTER TABLE silver.customers
  ALTER COLUMN email     SET TAGS ('pii'='email',       'classification'='confidential');
ALTER TABLE silver.customers
  ALTER COLUMN phone     SET TAGS ('pii'='phone',       'classification'='confidential');
ALTER TABLE silver.customers
  ALTER COLUMN ssn_token SET TAGS ('pii'='ssn_token',   'classification'='restricted');

-- Query classification tags
SELECT table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE catalog_name='main' AND schema_name='silver';`,
  },
  {
    id: 57,
    group: 'Security & PII',
    title: 'Sensitive Data Segregation',
    desc: 'Move high-sensitivity PII to a restricted secure schema in silver',
    code: `-- Create secure schema with restricted access
CREATE SCHEMA IF NOT EXISTS silver_secure;

-- Revoke all public access to secure schema
REVOKE ALL ON SCHEMA silver_secure FROM \`account users\`;

-- Grant only to pii_authorized group
GRANT USE SCHEMA ON SCHEMA silver_secure TO \`pii_authorized\`;
GRANT SELECT ON SCHEMA silver_secure TO \`pii_authorized\`;

-- Move PII table to secure schema
ALTER TABLE silver.customers_pii RENAME TO silver_secure.customers_pii;`,
  },
  {
    id: 58,
    group: 'Security & PII',
    title: 'Access Policy Enforcement',
    desc: 'Combine row filter + column mask + IP allow-list for full policy enforcement',
    code: `-- Full access policy: row + column + network
-- 1. Row filter (region-scoped)
ALTER TABLE silver.customers
  SET ROW FILTER silver.customer_row_filter ON (region);

-- 2. Column mask (PII fields)
ALTER TABLE silver.customers
  ALTER COLUMN email SET MASK silver.mask_email;

-- 3. Network policy (via Databricks workspace IP ACL)
-- Set via Databricks Admin Console → Security → IP Access Lists
-- Restricts silver schema access to corporate IP ranges only`,
  },
  {
    id: 59,
    group: 'Security & PII',
    title: 'Data Anonymization',
    desc: 'Produce an anonymized silver variant safe for analytics and sharing',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.customers")
anon = df.withColumn("anon_id", F.sha2(F.col("customer_id").cast("string"), 256)) \
         .withColumn("age_band",
             F.when(F.col("age") < 25, "18-24")
              .when(F.col("age") < 35, "25-34")
              .when(F.col("age") < 50, "35-49")
              .otherwise("50+")) \
         .select("anon_id","age_band","region","segment",
                 "country","tier","order_count","lifetime_value")
anon.write.format("delta").mode("overwrite").saveAsTable("silver.customers_anon")`,
  },
  {
    id: 60,
    group: 'Security & PII',
    title: 'Audit Tagging',
    desc: 'Add audit metadata columns to silver rows for data lineage and compliance',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_customers")
audited = df.withColumn("silver_ingested_at", F.current_timestamp()) \
            .withColumn("silver_ingested_by", F.lit("etl-service-principal")) \
            .withColumn("source_system", F.lit("CRM")) \
            .withColumn("source_file", F.input_file_name()) \
            .withColumn("processing_run_id",
                F.lit(dbutils.widgets.get("run_id") if "dbutils" in dir() else "local"))
audited.write.format("delta").mode("append").saveAsTable("silver.customers")`,
  },

  // ─── 61–70: Streaming Silver ───
  {
    id: 61,
    group: 'Streaming Silver',
    title: 'Deduplication Stream',
    desc: 'Deduplicate streaming records using foreachBatch and Delta MERGE',
    code: `from delta.tables import DeltaTable
from pyspark.sql import functions as F

def upsert_to_delta(micro_batch, batch_id):
    dt = DeltaTable.forName(spark, "silver.events_stream")
    dt.alias("t").merge(micro_batch.alias("s"), "t.event_id = s.event_id") \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()

stream = spark.readStream.format("delta").table("bronze.raw_events_stream") \
    .dropDuplicates(["event_id"])
stream.writeStream.foreachBatch(upsert_to_delta).outputMode("update").start()`,
  },
  {
    id: 62,
    group: 'Streaming Silver',
    title: 'Window Aggregation',
    desc: 'Aggregate streaming events over tumbling time windows into silver',
    code: `from pyspark.sql import functions as F

stream = spark.readStream.format("delta").table("bronze.clickstream")

windowed = stream.withWatermark("event_ts", "10 minutes") \
    .groupBy(
        F.window("event_ts", "5 minutes"),
        "product_id", "event_type"
    ).agg(
        F.count("event_id").alias("event_count"),
        F.countDistinct("user_id").alias("unique_users")
    )
windowed.writeStream.format("delta").outputMode("append") \
    .option("checkpointLocation", "/chk/silver_windows") \
    .table("silver.clickstream_5m_agg")`,
  },
  {
    id: 63,
    group: 'Streaming Silver',
    title: 'Stream Joins',
    desc: 'Join a real-time event stream with a static silver dimension table',
    code: `from pyspark.sql import functions as F

events = spark.readStream.format("delta").table("bronze.raw_events")
products = spark.table("silver.products")  # static dimension

enriched_stream = events.join(
    F.broadcast(products),
    events.product_id == products.product_id,
    "left"
).select("event_id","event_ts","user_id","event_type",
          "product_name","category_code","list_price")

enriched_stream.writeStream.format("delta") \
    .option("checkpointLocation", "/chk/silver_event_join") \
    .table("silver.events_enriched")`,
  },
  {
    id: 64,
    group: 'Streaming Silver',
    title: 'Watermark Handling',
    desc: 'Set watermark to handle late-arriving streaming data gracefully',
    code: `from pyspark.sql import functions as F

stream = spark.readStream.format("delta").table("bronze.iot_telemetry")

with_watermark = stream.withWatermark("device_ts", "15 minutes") \
    .filter(F.col("sensor_value").between(-50, 200)) \
    .withColumn("ingested_at", F.current_timestamp())

with_watermark.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/chk/silver_iot") \
    .partitionBy("device_id") \
    .table("silver.iot_telemetry_clean")`,
  },
  {
    id: 65,
    group: 'Streaming Silver',
    title: 'Stateful Processing',
    desc: 'Maintain per-user state across micro-batches using mapGroupsWithState',
    code: `from pyspark.sql import functions as F

stream = spark.readStream.format("delta").table("bronze.user_sessions")

# Track session duration using stateful aggregation
session_agg = stream.withWatermark("event_ts", "30 minutes") \
    .groupBy("user_id", F.window("event_ts", "30 minutes")) \
    .agg(F.count("event_id").alias("events_in_session"),
         F.min("event_ts").alias("session_start"),
         F.max("event_ts").alias("session_end")) \
    .withColumn("duration_sec",
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start"))
session_agg.writeStream.format("delta").outputMode("append") \
    .option("checkpointLocation", "/chk/silver_sessions").table("silver.user_sessions")`,
  },
  {
    id: 66,
    group: 'Streaming Silver',
    title: 'Event Ordering',
    desc: 'Sort and sequence micro-batch events by event timestamp within each batch',
    code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

def order_and_write(batch_df, batch_id):
    window = Window.partitionBy("device_id").orderBy("event_ts")
    ordered = batch_df.withColumn("seq", F.row_number().over(window)) \
                      .withColumn("prev_event_ts",
                          F.lag("event_ts").over(window)) \
                      .withColumn("gap_sec",
                          F.unix_timestamp("event_ts") - F.unix_timestamp("prev_event_ts"))
    ordered.write.format("delta").mode("append").saveAsTable("silver.events_ordered")

spark.readStream.format("delta").table("bronze.iot_events") \
    .writeStream.foreachBatch(order_and_write).start().awaitTermination()`,
  },
  {
    id: 67,
    group: 'Streaming Silver',
    title: 'Real-Time Enrichment',
    desc: 'Enrich streaming records with lookups inside foreachBatch micro-batches',
    code: `from pyspark.sql import functions as F

country_ref = spark.table("reference.country_codes").cache()

def enrich_batch(batch_df, batch_id):
    enriched = batch_df.join(F.broadcast(country_ref), "country_code", "left") \
                       .withColumn("region", F.coalesce("region", F.lit("Unknown"))) \
                       .withColumn("enriched_at", F.current_timestamp())
    enriched.write.format("delta").mode("append").saveAsTable("silver.events_enriched_rt")

spark.readStream.format("delta").table("bronze.raw_events") \
    .writeStream.foreachBatch(enrich_batch).start().awaitTermination()`,
  },
  {
    id: 68,
    group: 'Streaming Silver',
    title: 'Stream Validation',
    desc: 'Apply DQ rules inside foreachBatch and route failures to quarantine',
    code: `from pyspark.sql import functions as F

def validate_and_split(batch_df, batch_id):
    valid = batch_df.filter(
        F.col("amount") > 0 &
        F.col("account_id").isNotNull() &
        F.length("account_id").between(6, 20)
    )
    invalid = batch_df.subtract(valid) \
                      .withColumn("fail_reason", F.lit("dq_failure")) \
                      .withColumn("batch_id", F.lit(batch_id))
    valid.write.format("delta").mode("append").saveAsTable("silver.transactions_stream")
    invalid.write.format("delta").mode("append").saveAsTable("silver.quarantine_stream")

spark.readStream.format("delta").table("bronze.transactions_stream") \
    .writeStream.foreachBatch(validate_and_split).start()`,
  },
  {
    id: 69,
    group: 'Streaming Silver',
    title: 'Micro-Batch Processing',
    desc: 'Control micro-batch trigger interval for latency vs throughput trade-off',
    code: `from pyspark.sql import functions as F

stream = spark.readStream \
    .format("delta") \
    .option("maxFilesPerTrigger", 10) \
    .table("bronze.raw_orders_stream")

transformed = stream.withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
                    .withColumn("processed_at", F.current_timestamp()) \
                    .filter(F.col("amount") > 0)

transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .option("checkpointLocation", "/chk/silver_orders_micro") \
    .table("silver.orders_stream")`,
  },
  {
    id: 70,
    group: 'Streaming Silver',
    title: 'Stream-to-Silver Write',
    desc: 'Write enriched streaming data to a partitioned silver Delta table',
    code: `from pyspark.sql import functions as F

stream = spark.readStream.format("delta").table("bronze.events_stream")
enriched = stream.withColumn("event_date", F.to_date("event_ts")) \
                 .withColumn("event_hour", F.hour("event_ts")) \
                 .withColumn("silver_loaded_at", F.current_timestamp())

enriched.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("event_date") \
    .option("checkpointLocation", "/chk/stream_to_silver") \
    .option("mergeSchema", "true") \
    .table("silver.events")`,
  },

  // ─── 71–80: AI/RAG Prep ───
  {
    id: 71,
    group: 'AI/RAG Prep',
    title: 'Text Cleaning',
    desc: 'Remove HTML, special characters, and normalize whitespace for NLP',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_articles")
cleaned = df.withColumn("text",
    F.regexp_replace("raw_text", r"<[^>]+>", "")) \
            .withColumn("text",
    F.regexp_replace("text", r"[^\\w\\s.,!?;:\\-]", " ")) \
            .withColumn("text",
    F.regexp_replace("text", r"\\s+", " ")) \
            .withColumn("text", F.trim("text")) \
            .filter(F.length("text") > 50)
cleaned.write.format("delta").mode("overwrite").saveAsTable("silver.articles_text_clean")`,
  },
  {
    id: 72,
    group: 'AI/RAG Prep',
    title: 'Chunking Preparation',
    desc: 'Split long documents into fixed-size overlapping chunks for RAG indexing',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

@F.udf(returnType=ArrayType(StringType()))
def chunk_text(text, size=512, overlap=64):
    words = text.split()
    step = size - overlap
    return [" ".join(words[i:i+size]) for i in range(0, len(words), step) if words[i:i+size]]

df = spark.table("silver.articles_text_clean")
chunked = df.withColumn("chunks", chunk_text(F.col("text"))) \
            .withColumn("chunk", F.explode("chunks")) \
            .withColumn("chunk_index", F.monotonically_increasing_id()) \
            .select("article_id","chunk_index","chunk","title","published_date")
chunked.write.format("delta").mode("overwrite").saveAsTable("silver.article_chunks")`,
  },
  {
    id: 73,
    group: 'AI/RAG Prep',
    title: 'Metadata Enrichment',
    desc: 'Attach document metadata to each chunk for filtered retrieval in RAG',
    code: `from pyspark.sql import functions as F

chunks = spark.table("silver.article_chunks")
meta   = spark.table("silver.articles_meta").select(
    "article_id","author","category","language","domain","published_date")

enriched = chunks.join(meta, "article_id", "left") \
                 .withColumn("source_url",
                     F.concat(F.lit("https://docs.example.com/"), F.col("article_id"))) \
                 .withColumn("word_count", F.size(F.split("chunk", " "))) \
                 .withColumn("is_rag_ready", F.col("word_count") >= 30)
enriched.write.format("delta").mode("overwrite") \
        .saveAsTable("silver.article_chunks_enriched")`,
  },
  {
    id: 74,
    group: 'AI/RAG Prep',
    title: 'PII Removal in Text',
    desc: 'Scrub PII patterns from free-text fields before embedding generation',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.article_chunks_enriched")
scrubbed = df \
    .withColumn("chunk", F.regexp_replace("chunk",
        r"\\b[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[A-Z|a-z]{2,}\\b", "[EMAIL]")) \
    .withColumn("chunk", F.regexp_replace("chunk",
        r"\\b\\d{3}[-.]?\\d{3}[-.]?\\d{4}\\b", "[PHONE]")) \
    .withColumn("chunk", F.regexp_replace("chunk",
        r"\\b\\d{3}-\\d{2}-\\d{4}\\b", "[SSN]"))
scrubbed.write.format("delta").mode("overwrite") \
        .saveAsTable("silver.article_chunks_pii_scrubbed")`,
  },
  {
    id: 75,
    group: 'AI/RAG Prep',
    title: 'Language Normalization',
    desc: 'Detect language and filter to target languages for RAG corpus',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Use langdetect via UDF (install on cluster)
from langdetect import detect

@F.udf(returnType=StringType())
def detect_lang(text):
    try:
        return detect(text[:200])
    except Exception:
        return "unknown"

df = spark.table("silver.article_chunks_pii_scrubbed")
lang_tagged = df.withColumn("detected_lang", detect_lang("chunk"))
en_only = lang_tagged.filter(F.col("detected_lang") == "en")
en_only.write.format("delta").mode("overwrite").saveAsTable("silver.rag_corpus_en")`,
  },
  {
    id: 76,
    group: 'AI/RAG Prep',
    title: 'Tokenization NLP',
    desc: 'Tokenize text into word tokens and compute token counts for LLM sizing',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType

@F.udf(returnType=ArrayType(StringType()))
def simple_tokenize(text):
    import re
    return re.findall(r"\\b\\w+\\b", text.lower()) if text else []

df = spark.table("silver.rag_corpus_en")
tokenized = df.withColumn("tokens", simple_tokenize("chunk")) \
              .withColumn("token_count", F.size("tokens")) \
              .withColumn("fits_context_4k", F.col("token_count") <= 4096) \
              .withColumn("fits_context_8k", F.col("token_count") <= 8192)
tokenized.write.format("delta").mode("overwrite").saveAsTable("silver.rag_tokenized")`,
  },
  {
    id: 77,
    group: 'AI/RAG Prep',
    title: 'Feature Extraction',
    desc: 'Extract structured features from text for ML classification models',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.rag_corpus_en")
features = df.withColumn("char_count", F.length("chunk")) \
             .withColumn("word_count", F.size(F.split("chunk", " "))) \
             .withColumn("sentence_count",
                 F.size(F.split("chunk", r"[.!?]+"))) \
             .withColumn("avg_word_len",
                 F.col("char_count") / F.col("word_count")) \
             .withColumn("has_code",
                 F.col("chunk").rlike(r"(def |class |import |SELECT |FROM )"))
features.write.format("delta").mode("overwrite").saveAsTable("silver.rag_features")`,
  },
  {
    id: 78,
    group: 'AI/RAG Prep',
    title: 'Label Generation',
    desc: 'Generate training labels from heuristics and keyword matching for ML',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.support_tickets")
labeled = df.withColumn("category",
    F.when(F.col("text").ilike("%billing%"), "billing")
     .when(F.col("text").ilike("%cancel%"), "cancellation")
     .when(F.col("text").ilike("%error%") | F.col("text").ilike("%bug%"), "technical")
     .when(F.col("text").ilike("%refund%"), "refund")
     .otherwise("general")) \
    .withColumn("sentiment",
    F.when(F.col("rating") >= 4, "positive")
     .when(F.col("rating") <= 2, "negative")
     .otherwise("neutral"))
labeled.write.format("delta").mode("overwrite").saveAsTable("silver.tickets_labeled")`,
  },
  {
    id: 79,
    group: 'AI/RAG Prep',
    title: 'Data Balancing',
    desc: 'Balance imbalanced training datasets by undersampling the majority class',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.tickets_labeled")
counts = df.groupBy("category").count().collect()
min_count = min(r["count"] for r in counts)

fractions = {r["category"]: min_count / r["count"] for r in counts}

balanced = df.sampleBy("category", fractions=fractions, seed=42)
print(f"Original: {df.count()} | Balanced: {balanced.count()}")
balanced.write.format("delta").mode("overwrite") \
        .saveAsTable("silver.tickets_balanced")`,
  },
  {
    id: 80,
    group: 'AI/RAG Prep',
    title: 'Training Dataset Prep',
    desc: 'Split silver data into train/val/test sets with stratification',
    code: `from pyspark.sql import functions as F

df = spark.table("silver.tickets_labeled") \
          .withColumn("split_rand", F.rand(seed=42))

train = df.filter(F.col("split_rand") < 0.70)
val   = df.filter((F.col("split_rand") >= 0.70) & (F.col("split_rand") < 0.85))
test  = df.filter(F.col("split_rand") >= 0.85)

train.write.format("delta").mode("overwrite").saveAsTable("silver.ml_train")
val.write.format("delta").mode("overwrite").saveAsTable("silver.ml_val")
test.write.format("delta").mode("overwrite").saveAsTable("silver.ml_test")
print(f"Train:{train.count()} Val:{val.count()} Test:{test.count()}")`,
  },

  // ─── 81–90: Error Handling ───
  {
    id: 81,
    group: 'Error Handling',
    title: 'Retry Failed Transforms',
    desc: 'Wrap transform steps with retry logic for transient failures',
    code: `import time
from pyspark.sql import functions as F

def retry(func, max_attempts=3, delay=5):
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                raise
            time.sleep(delay * attempt)

def run_transform():
    df = spark.table("bronze.raw_orders")
    df.write.format("delta").mode("overwrite").saveAsTable("silver.orders")

retry(run_transform)`,
  },
  {
    id: 82,
    group: 'Error Handling',
    title: 'Idempotent Processing',
    desc: 'Make silver writes idempotent using MERGE to avoid duplicate inserts',
    code: `from delta.tables import DeltaTable
from pyspark.sql import functions as F

source = spark.table("bronze.raw_orders")
target = DeltaTable.forName(spark, "silver.orders")

# Idempotent: same run twice produces same result
target.alias("t").merge(
    source.alias("s"),
    "t.order_id = s.order_id AND t.updated_at = s.updated_at"
) \
.whenNotMatchedInsert(values={
    "order_id": "s.order_id",
    "amount": "s.amount",
    "status": "s.status",
    "updated_at": "s.updated_at"
}).execute()`,
  },
  {
    id: 83,
    group: 'Error Handling',
    title: 'Partial Load Recovery',
    desc: 'Use Delta RESTORE to roll back a failed partial silver write',
    code: `from delta.tables import DeltaTable
from pyspark.sql import functions as F

try:
    df = spark.table("bronze.raw_orders")
    df.write.format("delta").mode("overwrite").saveAsTable("silver.orders")
    print("Silver write succeeded")
except Exception as e:
    print(f"Write failed: {e} — restoring previous version")
    dt = DeltaTable.forName(spark, "silver.orders")
    history = dt.history(1).collect()
    prev_version = history[0]["version"] - 1
    spark.sql(f"RESTORE TABLE silver.orders TO VERSION AS OF {prev_version}")
    raise`,
  },
  {
    id: 84,
    group: 'Error Handling',
    title: 'Reprocessing Pipelines',
    desc: 'Replay a date range of bronze data into silver for backfill or correction',
    code: `from pyspark.sql import functions as F

REPROCESS_START = "2024-01-01"
REPROCESS_END   = "2024-01-31"

bronze = spark.table("bronze.raw_orders") \
    .filter(F.col("order_date").between(REPROCESS_START, REPROCESS_END))

from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "silver.orders")
dt.alias("t").merge(bronze.alias("s"), "t.order_id = s.order_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
print(f"Reprocessed {bronze.count()} orders for {REPROCESS_START} to {REPROCESS_END}")`,
  },
  {
    id: 85,
    group: 'Error Handling',
    title: 'Data Reconciliation',
    desc: 'Reconcile row counts and sums between bronze and silver after each load',
    code: `from pyspark.sql import functions as F

bronze = spark.table("bronze.raw_orders")
silver = spark.table("silver.orders")

b_cnt = bronze.count()
s_cnt = silver.count()
b_sum = bronze.agg(F.sum("amount")).collect()[0][0]
s_sum = silver.agg(F.sum("amount")).collect()[0][0]

print(f"Bronze rows: {b_cnt} | Silver rows: {s_cnt}")
print(f"Bronze sum:  {b_sum} | Silver sum:  {s_sum}")
assert abs(b_cnt - s_cnt) / b_cnt < 0.001, "Row count mismatch exceeds 0.1% threshold"
assert abs(b_sum - s_sum) / b_sum < 0.001, "Amount sum mismatch exceeds 0.1% threshold"`,
  },
  {
    id: 86,
    group: 'Error Handling',
    title: 'Error Logging',
    desc: 'Log transform errors with context to a silver DQ error log table',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

error_schema = StructType([
    StructField("run_id", StringType()),
    StructField("table_name", StringType()),
    StructField("error_message", StringType()),
    StructField("error_ts", TimestampType()),
    StructField("batch_date", StringType()),
])

def log_error(run_id, table, message, batch_date):
    from datetime import datetime
    row = [(run_id, table, message, datetime.utcnow(), batch_date)]
    err_df = spark.createDataFrame(row, error_schema)
    err_df.write.format("delta").mode("append").saveAsTable("silver.dq_error_log")`,
  },
  {
    id: 87,
    group: 'Error Handling',
    title: 'Data Rollback',
    desc: 'Roll back silver table to a known good version using Delta time travel',
    code: `from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "silver.orders")

# View version history
history = dt.history(10)
history.select("version","timestamp","operation","operationMetrics").show(truncate=False)

# Roll back to version 5 (or specific timestamp)
TARGET_VERSION = 5
spark.sql(f"RESTORE TABLE silver.orders TO VERSION AS OF {TARGET_VERSION}")
print(f"Rolled back silver.orders to version {TARGET_VERSION}")`,
  },
  {
    id: 88,
    group: 'Error Handling',
    title: 'Version Control Delta',
    desc: 'Use Delta versioning to audit every change applied to the silver table',
    code: `-- Inspect version history for silver table
DESCRIBE HISTORY silver.orders;

-- Time-travel query: state as of 24 hours ago
SELECT COUNT(*) AS row_count, SUM(amount) AS total_amount
FROM silver.orders
TIMESTAMP AS OF (current_timestamp() - INTERVAL 24 HOURS);

-- Compare current vs yesterday
SELECT 'current' AS snapshot, COUNT(*) AS cnt FROM silver.orders
UNION ALL
SELECT 'yesterday', COUNT(*) FROM silver.orders
TIMESTAMP AS OF (current_timestamp() - INTERVAL 24 HOURS);`,
  },
  {
    id: 89,
    group: 'Error Handling',
    title: 'Audit Tracking',
    desc: 'Record every silver pipeline run with status, row counts, and duration',
    code: `import time
from pyspark.sql import functions as F

run_id = dbutils.widgets.get("run_id") if "dbutils" in dir() else "local_run"
start_ts = time.time()

try:
    df = spark.table("bronze.raw_orders")
    row_count = df.count()
    df.write.format("delta").mode("overwrite").saveAsTable("silver.orders")
    status = "SUCCESS"
except Exception as e:
    row_count = 0
    status = f"FAILED: {str(e)[:200]}"

duration = round(time.time() - start_ts, 2)
audit = spark.createDataFrame(
    [(run_id, "silver.orders", status, row_count, duration, F.current_timestamp())],
    ["run_id","table_name","status","row_count","duration_sec","logged_at"])
audit.write.format("delta").mode("append").saveAsTable("silver.pipeline_audit_log")`,
  },
  {
    id: 90,
    group: 'Error Handling',
    title: 'Alerting',
    desc: 'Send a Slack or webhook alert when silver DQ thresholds are breached',
    code: `import requests
from pyspark.sql import functions as F

WEBHOOK_URL = spark.conf.get("spark.alert.webhook_url", "")
df = spark.table("silver.orders")
total = df.count()
bad = df.filter(F.col("amount").isNull() | (F.col("amount") <= 0)).count()
bad_pct = (bad / total * 100) if total > 0 else 0

if bad_pct > 1.0 and WEBHOOK_URL:
    msg = (f"SILVER DQ ALERT: silver.orders has {bad_pct:.2f}% bad rows "
           f"({bad}/{total}). Threshold: 1.0%")
    requests.post(WEBHOOK_URL, json={"text": msg}, timeout=10)
    print(f"Alert sent: {msg}")`,
  },

  // ─── 91–100: Performance ───
  {
    id: 91,
    group: 'Performance',
    title: 'Partition Optimization',
    desc: 'Right-size partitions before writing large silver datasets',
    code: `from pyspark.sql import functions as F

df = spark.table("bronze.raw_orders")
target_partition_size_mb = 128
estimated_size_mb = df.count() * 500 / 1_000_000  # rough estimate
num_partitions = max(1, int(estimated_size_mb / target_partition_size_mb))

print(f"Repartitioning to {num_partitions} partitions")
optimized = df.repartition(num_partitions, "region")
optimized.write.format("delta").mode("overwrite") \
    .partitionBy("order_year","order_month").saveAsTable("silver.orders_opt")`,
  },
  {
    id: 92,
    group: 'Performance',
    title: 'Z-Order Clustering',
    desc: 'Apply Z-order on high-cardinality columns to improve filter predicate pushdown',
    code: `-- Z-order on most frequently filtered columns
OPTIMIZE silver.orders
  ZORDER BY (customer_id, order_date, product_id);

OPTIMIZE silver.customers
  ZORDER BY (region, segment);

-- Verify bytes scanned improvement
EXPLAIN FORMATTED
SELECT * FROM silver.orders
WHERE customer_id = 'C123' AND order_date >= '2024-01-01';`,
  },
  {
    id: 93,
    group: 'Performance',
    title: 'Caching',
    desc: 'Cache frequently reused silver DataFrames in Spark memory',
    code: `from pyspark.sql import functions as F

# Cache reference tables used in multiple joins
dim_customers = spark.table("silver.customers").cache()
dim_products  = spark.table("silver.products").cache()
dim_dates     = spark.table("silver.dim_date").cache()

dim_customers.count()  # materialize cache
dim_products.count()
dim_dates.count()
print("Silver dimension tables cached in Spark memory")

# Build fact using cached dims
fact = spark.table("silver.orders") \
    .join(dim_customers, "customer_id") \
    .join(dim_products, "product_id")
fact.write.format("delta").mode("overwrite").saveAsTable("silver.fact_ready")`,
  },
  {
    id: 94,
    group: 'Performance',
    title: 'Broadcast Joins',
    desc: 'Force broadcast of small dimension tables to avoid shuffle in joins',
    code: `from pyspark.sql import functions as F

orders = spark.table("silver.orders")

# Broadcast small reference tables (< 200 MB)
country_ref  = F.broadcast(spark.table("reference.country_codes"))
category_ref = F.broadcast(spark.table("reference.product_categories"))
currency_ref = F.broadcast(spark.table("reference.fx_rates"))

result = orders.join(country_ref, "country_code", "left") \
               .join(category_ref, "category_id", "left") \
               .join(currency_ref, "currency", "left")
result.write.format("delta").mode("overwrite").saveAsTable("silver.orders_enriched_opt")`,
  },
  {
    id: 95,
    group: 'Performance',
    title: 'Data Skew Handling',
    desc: 'Handle key skew in joins by salting the skewed key',
    code: `from pyspark.sql import functions as F

SALT_COUNT = 8

orders = spark.table("silver.orders") \
    .withColumn("salt", (F.rand() * SALT_COUNT).cast("int")) \
    .withColumn("salted_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt")))

customers = spark.table("silver.customers")
customers_salted = customers.crossJoin(
    spark.range(SALT_COUNT).withColumnRenamed("id", "salt")
).withColumn("salted_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt")))

joined = orders.join(customers_salted, "salted_key", "left").drop("salt","salted_key")
joined.write.format("delta").mode("overwrite").saveAsTable("silver.orders_skew_fixed")`,
  },
  {
    id: 96,
    group: 'Performance',
    title: 'Query Optimization',
    desc: 'Use AQE and predicate pushdown hints for optimal silver query plans',
    code: `-- Enable Adaptive Query Execution (AQE)
SET spark.sql.adaptive.enabled=true;
SET spark.sql.adaptive.skewJoin.enabled=true;
SET spark.sql.adaptive.coalescePartitions.enabled=true;

-- Query with predicate pushdown
SELECT o.order_id, o.amount, c.segment, c.region
FROM silver.orders o
JOIN silver.customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
  AND c.region = 'US'
  AND o.status = 'Open';

-- View plan to verify pushdown
EXPLAIN COST SELECT * FROM silver.orders WHERE order_date >= '2024-01-01';`,
  },
  {
    id: 97,
    group: 'Performance',
    title: 'File Compaction',
    desc: 'Run OPTIMIZE to compact small silver Delta files into larger ones',
    code: `-- Compact all silver tables to remove small files
OPTIMIZE silver.orders;
OPTIMIZE silver.customers;
OPTIMIZE silver.transactions;
OPTIMIZE silver.events;

-- Vacuum stale Delta log files (retain 7 days)
SET spark.databricks.delta.retentionDurationCheck.enabled=false;
VACUUM silver.orders   RETAIN 168 HOURS;
VACUUM silver.customers RETAIN 168 HOURS;

-- Check file stats after compaction
DESCRIBE DETAIL silver.orders;`,
  },
  {
    id: 98,
    group: 'Performance',
    title: 'Incremental Processing',
    desc: 'Process only new bronze records since last silver watermark using CDC',
    code: `from pyspark.sql import functions as F

# Read watermark from audit log
last_run = spark.sql("""
    SELECT MAX(max_updated_at) AS wm FROM silver.pipeline_audit_log
    WHERE table_name='silver.orders' AND status='SUCCESS'
""").collect()[0]["wm"]

print(f"Processing bronze records after: {last_run}")
incremental = spark.table("bronze.raw_orders") \
    .filter(F.col("updated_at") > F.lit(last_run))

from delta.tables import DeltaTable
DeltaTable.forName(spark, "silver.orders").alias("t") \
    .merge(incremental.alias("s"), "t.order_id = s.order_id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
print(f"Merged {incremental.count()} incremental records")`,
  },
  {
    id: 99,
    group: 'Performance',
    title: 'Parallel Processing',
    desc: 'Parallelize silver writes across multiple table partitions concurrently',
    code: `from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F

tables = ["orders", "customers", "products", "transactions", "events"]

def process_table(table_name):
    try:
        df = spark.table(f"bronze.raw_{table_name}")
        df.write.format("delta").mode("overwrite") \
          .saveAsTable(f"silver.{table_name}")
        return f"SUCCESS: silver.{table_name}"
    except Exception as e:
        return f"FAILED: silver.{table_name} — {e}"

with ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(process_table, tables))
for r in results:
    print(r)`,
  },
  {
    id: 100,
    group: 'Performance',
    title: 'Resource Tuning',
    desc: 'Configure Spark resource settings for optimal silver pipeline throughput',
    code: `-- Cluster-level resource tuning for Silver layer processing
SET spark.sql.shuffle.partitions=400;
SET spark.sql.adaptive.enabled=true;
SET spark.sql.adaptive.skewJoin.enabled=true;
SET spark.databricks.delta.optimizeWrite.enabled=true;
SET spark.databricks.delta.autoCompact.enabled=true;
SET spark.sql.execution.arrow.pyspark.enabled=true;

-- Delta-specific performance settings
SET spark.databricks.delta.merge.optimizeInsertOnlyMerge.enabled=true;
SET spark.databricks.delta.schema.autoMerge.enabled=true;

-- Verify settings
SET spark.sql.shuffle.partitions;
SET spark.sql.adaptive.enabled;`,
  },
];

const groups = [...new Set(silverOperations.map((op) => op.group))];

function SilverOperations() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = silverOperations
    .filter((op) => {
      const matchGroup = selectedGroup === 'All' || op.group === selectedGroup;
      const matchSearch =
        op.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
        op.desc.toLowerCase().includes(searchTerm.toLowerCase()) ||
        op.group.toLowerCase().includes(searchTerm.toLowerCase());
      return matchGroup && matchSearch;
    })
    .sort((a, b) => (sortBy === 'title' ? a.title.localeCompare(b.title) : a.id - b.id));

  const downloadCSV = () => {
    exportToCSV(
      silverOperations.map((op) => ({
        id: op.id,
        group: op.group,
        title: op.title,
        desc: op.desc,
      })),
      'silver-operations.csv'
    );
  };

  const groupColor = {
    'Core Transform': '#3b82f6',
    'Data Quality': '#22c55e',
    'Data Integration': '#8b5cf6',
    'SCD & History': '#f59e0b',
    'Data Modeling Prep': '#06b6d4',
    'Security & PII': '#ef4444',
    'Streaming Silver': '#f97316',
    'AI/RAG Prep': '#ec4899',
    'Error Handling': '#64748b',
    Performance: '#10b981',
  };

  return (
    <div>
      {/* Page Header */}
      <div className="page-header">
        <div>
          <h1>Silver Layer Operations</h1>
          <p>100 operations — Transform, Quality, Integration, SCD, Security, Streaming, AI/RAG</p>
        </div>
      </div>

      {/* Flow Banner */}
      <div
        className="card"
        style={{
          marginBottom: '1rem',
          background: 'linear-gradient(135deg, #1e293b 0%, #334155 100%)',
          color: '#fff',
          padding: '0.85rem 1.25rem',
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
            flexWrap: 'wrap',
            fontSize: '0.88rem',
            fontWeight: 600,
          }}
        >
          {[
            'Bronze',
            'Clean',
            'Validate',
            'Standardize',
            'Enrich',
            'Deduplicate',
            'Store (Silver)',
          ].map((step, i, arr) => (
            <React.Fragment key={step}>
              <span
                style={{
                  background: i === 0 ? '#92400e' : i === arr.length - 1 ? '#1d4ed8' : '#374151',
                  color: '#fff',
                  padding: '0.2rem 0.6rem',
                  borderRadius: '999px',
                  fontSize: '0.78rem',
                }}
              >
                {step}
              </span>
              {i < arr.length - 1 && <span style={{ color: '#94a3b8' }}>{'\u2192'}</span>}
            </React.Fragment>
          ))}
        </div>
      </div>

      {/* Stat Cards */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">{'⚙️'}</div>
          <div className="stat-info">
            <h4>100</h4>
            <p>Operations</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">{'📂'}</div>
          <div className="stat-info">
            <h4>10</h4>
            <p>Categories</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon orange">{'🔥'}</div>
          <div className="stat-info">
            <h4>PySpark</h4>
            <p>+ SQL Code</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'🛡️'}</div>
          <div className="stat-info">
            <h4>Delta</h4>
            <p>Lake Ready</p>
          </div>
        </div>
      </div>

      {/* Anti-Patterns Card */}
      <div
        className="card"
        style={{
          marginBottom: '1rem',
          borderLeft: '4px solid #ef4444',
          background: '#fef2f2',
        }}
      >
        <div
          style={{ fontWeight: 700, color: '#dc2626', marginBottom: '0.5rem', fontSize: '0.9rem' }}
        >
          {'⚠️'} Silver Layer Anti-Patterns — What NOT to Do
        </div>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
            gap: '0.4rem',
            fontSize: '0.8rem',
            color: '#7f1d1d',
          }}
        >
          {[
            'Writing raw unvalidated data directly to Silver',
            'Using f-string SQL with user inputs (SQL injection risk)',
            'No deduplication — duplicates silently corrupt Gold',
            'Storing PII in plain text without masking or encryption',
            'SELECT * from Bronze — always prune unused columns',
            'Overwriting Silver entirely instead of using MERGE',
            'No type casting — leaving amounts as strings',
            'Missing null handling — nulls propagate to Gold/reports',
            'No partitioning — full table scans on every query',
            'Skipping OPTIMIZE/ZORDER — thousands of tiny files',
          ].map((ap) => (
            <div key={ap} style={{ display: 'flex', gap: '0.4rem', alignItems: 'flex-start' }}>
              <span style={{ color: '#ef4444', flexShrink: 0 }}>✗</span>
              <span>{ap}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Filter / Search / Download Bar */}
      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search operations..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '260px' }}
          />
          <select
            className="form-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
            style={{ maxWidth: '240px' }}
          >
            <option value="All">All Categories (100)</option>
            {groups.map((g) => (
              <option key={g} value={g}>
                {g} ({silverOperations.filter((op) => op.group === g).length})
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
            <option value="title">Sort by Title</option>
          </select>
          <button
            className="btn btn-secondary btn-sm"
            onClick={downloadCSV}
            style={{ marginLeft: 'auto' }}
          >
            Download CSV
          </button>
        </div>
        {filtered.length !== silverOperations.length && (
          <div style={{ marginTop: '0.5rem', fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
            Showing {filtered.length} of {silverOperations.length} operations
          </div>
        )}
      </div>

      {/* Operations List */}
      {filtered.map((op) => {
        const isExpanded = expandedId === op.id;
        const color = groupColor[op.group] || '#6b7280';
        return (
          <div key={op.id} className="card" style={{ marginBottom: '0.6rem' }}>
            <div
              onClick={() => setExpandedId(isExpanded ? null : op.id)}
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
                  <span
                    style={{
                      background: color,
                      color: '#fff',
                      padding: '0.15rem 0.55rem',
                      borderRadius: '999px',
                      fontSize: '0.72rem',
                      fontWeight: 600,
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {op.group}
                  </span>
                  <strong style={{ fontSize: '0.92rem' }}>
                    #{op.id} &mdash; {op.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.82rem', margin: 0 }}>
                  {op.desc}
                </p>
              </div>
              <span
                style={{
                  color: 'var(--text-secondary)',
                  marginLeft: '1rem',
                  fontSize: '0.75rem',
                  flexShrink: 0,
                }}
              >
                {isExpanded ? '\u25BC' : '\u25B6'}
              </span>
            </div>

            {isExpanded && (
              <div style={{ marginTop: '1rem' }}>
                <ScenarioCard scenario={op} />
              </div>
            )}
          </div>
        );
      })}

      {filtered.length === 0 && (
        <div
          className="card"
          style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '2rem' }}
        >
          No operations match your search. Try a different term or category.
        </div>
      )}
    </div>
  );
}

export default SilverOperations;
