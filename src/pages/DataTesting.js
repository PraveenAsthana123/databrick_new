import React, { useState } from 'react';
import ScenarioCard from '../components/common/ScenarioCard';

const testingScenarios = [
  // ===== Schema Validation (1-5) =====
  {
    id: 1,
    category: 'Schema Validation',
    title: 'Column Name Validation',
    desc: 'Verify DataFrame column names match expected schema',
    code: `from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def validate_column_names(df, expected_columns):
    """Validate that DataFrame contains all expected columns."""
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    missing = expected_set - actual_columns
    extra = actual_columns - expected_set

    results = {
        "status": "PASS" if not missing else "FAIL",
        "missing_columns": list(missing),
        "extra_columns": list(extra),
        "expected_count": len(expected_columns),
        "actual_count": len(df.columns)
    }
    return results

# Usage
expected = ["id", "name", "age", "email", "created_at"]
df = spark.read.table("production.users")
result = validate_column_names(df, expected)
print(f"Schema validation: {result['status']}")
if result['missing_columns']:
    raise AssertionError(f"Missing columns: {result['missing_columns']}")`,
  },

  {
    id: 2,
    category: 'Schema Validation',
    title: 'Data Type Validation',
    desc: 'Ensure column data types match expected schema definition',
    code: `from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

expected_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("created_at", TimestampType(), True),
])

df = spark.read.table("production.transactions")

def validate_data_types(df, expected_schema):
    """Compare actual vs expected data types for each column."""
    expected_map = {f.name: f.dataType for f in expected_schema.fields}
    actual_map = {f.name: f.dataType for f in df.schema.fields}
    mismatches = []
    for col_name, expected_type in expected_map.items():
        if col_name in actual_map:
            if actual_map[col_name] != expected_type:
                mismatches.append({
                    "column": col_name,
                    "expected": str(expected_type),
                    "actual": str(actual_map[col_name])
                })
    return {"status": "PASS" if not mismatches else "FAIL", "mismatches": mismatches}

result = validate_data_types(df, expected_schema)
assert result["status"] == "PASS", f"Type mismatches: {result['mismatches']}"`,
  },

  {
    id: 3,
    category: 'Schema Validation',
    title: 'Nullable Constraint Check',
    desc: 'Validate nullable constraints for required fields',
    code: `from pyspark.sql import functions as F

def validate_nullable_constraints(df, non_nullable_columns):
    """Check that non-nullable columns contain no nulls."""
    violations = []
    total_rows = df.count()
    for col_name in non_nullable_columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            violations.append({
                "column": col_name,
                "null_count": null_count,
                "null_pct": round(null_count / total_rows * 100, 2)
            })
    return {
        "status": "PASS" if not violations else "FAIL",
        "total_rows": total_rows,
        "violations": violations
    }

df = spark.read.table("production.orders")
non_nullable = ["order_id", "customer_id", "order_date", "total_amount"]
result = validate_nullable_constraints(df, non_nullable)
print(f"Nullable check: {result['status']}")
for v in result['violations']:
    print(f"  Column '{v['column']}': {v['null_count']} nulls ({v['null_pct']}%)")`,
  },

  {
    id: 4,
    category: 'Schema Validation',
    title: 'Schema Drift Detection',
    desc: 'Detect schema changes between pipeline runs',
    code: `import json
from pyspark.sql.types import StructType

def detect_schema_drift(current_df, baseline_schema_json):
    """Compare current schema against a stored baseline."""
    baseline_schema = StructType.fromJson(json.loads(baseline_schema_json))
    current_schema = current_df.schema

    baseline_fields = {f.name: (str(f.dataType), f.nullable) for f in baseline_schema.fields}
    current_fields = {f.name: (str(f.dataType), f.nullable) for f in current_schema.fields}

    added = set(current_fields.keys()) - set(baseline_fields.keys())
    removed = set(baseline_fields.keys()) - set(current_fields.keys())
    modified = []
    for col in set(baseline_fields.keys()) & set(current_fields.keys()):
        if baseline_fields[col] != current_fields[col]:
            modified.append({
                "column": col,
                "baseline": baseline_fields[col],
                "current": current_fields[col]
            })

    drift_detected = bool(added or removed or modified)
    return {
        "drift_detected": drift_detected,
        "added_columns": list(added),
        "removed_columns": list(removed),
        "modified_columns": modified
    }

# Save baseline after first validated run
df = spark.read.table("production.events")
baseline_json = df.schema.json()
dbutils.fs.put("/mnt/schemas/events_baseline.json", baseline_json, overwrite=True)

# On subsequent runs, compare
stored = dbutils.fs.head("/mnt/schemas/events_baseline.json")
drift = detect_schema_drift(df, stored)
if drift["drift_detected"]:
    raise RuntimeError(f"Schema drift detected: {drift}")`,
  },

  {
    id: 5,
    category: 'Schema Validation',
    title: 'Schema Evolution Tracking',
    desc: 'Track and validate Delta table schema evolution over time',
    code: `from delta.tables import DeltaTable
from pyspark.sql import functions as F

def track_schema_evolution(table_path):
    """Track schema changes in Delta table history."""
    dt = DeltaTable.forPath(spark, table_path)
    history = dt.history().select(
        "version", "timestamp", "operation", "operationParameters"
    ).orderBy(F.col("version").desc())

    schema_changes = history.filter(
        F.col("operation").isin(["SET TBLPROPERTIES", "CHANGE COLUMN", "ADD COLUMNS", "REPLACE COLUMNS"])
    )
    return schema_changes

def validate_schema_evolution(table_path, allowed_operations=None):
    """Ensure only allowed schema modifications were made."""
    if allowed_operations is None:
        allowed_operations = ["ADD COLUMNS"]
    dt = DeltaTable.forPath(spark, table_path)
    history = dt.history()
    schema_ops = history.filter(
        F.col("operation").isin(["CHANGE COLUMN", "ADD COLUMNS", "REPLACE COLUMNS"])
    )
    disallowed = schema_ops.filter(~F.col("operation").isin(allowed_operations))
    count = disallowed.count()
    return {
        "status": "PASS" if count == 0 else "FAIL",
        "disallowed_changes": count,
        "details": disallowed.collect() if count > 0 else []
    }

result = validate_schema_evolution("/mnt/delta/customers")
assert result["status"] == "PASS", f"Disallowed schema changes found: {result['details']}"`,
  },

  // ===== Data Quality (6-10) =====
  {
    id: 6,
    category: 'Data Quality',
    title: 'Null Check Analysis',
    desc: 'Comprehensive null analysis across all columns',
    code: `from pyspark.sql import functions as F

def null_check_analysis(df):
    """Analyze null values across all columns."""
    total_rows = df.count()
    results = []
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        results.append({
            "column": col_name,
            "null_count": null_count,
            "null_pct": round(null_count / total_rows * 100, 2) if total_rows > 0 else 0,
            "status": "PASS" if null_count == 0 else "WARN"
        })

    summary = spark.createDataFrame(results)
    summary.orderBy(F.col("null_pct").desc()).show(truncate=False)

    failing = [r for r in results if r["null_pct"] > 10]
    return {
        "status": "FAIL" if failing else "PASS",
        "total_rows": total_rows,
        "columns_with_nulls": len([r for r in results if r["null_count"] > 0]),
        "critical_columns": failing
    }

df = spark.read.table("production.customer_data")
result = null_check_analysis(df)
print(f"Null analysis: {result['status']}")`,
  },

  {
    id: 7,
    category: 'Data Quality',
    title: 'Range and Bounds Checking',
    desc: 'Validate numeric values fall within expected ranges',
    code: `from pyspark.sql import functions as F

def range_check(df, column, min_val=None, max_val=None):
    """Check if column values fall within specified range."""
    violations = df
    if min_val is not None:
        violations = violations.filter(
            (F.col(column) < min_val) | F.col(column).isNull()
        )
    if max_val is not None:
        violations = violations.filter(
            (F.col(column) > max_val) | F.col(column).isNull()
        )

    violation_count = violations.count()
    total = df.count()
    return {
        "column": column,
        "min": min_val,
        "max": max_val,
        "violations": violation_count,
        "violation_pct": round(violation_count / total * 100, 2) if total > 0 else 0,
        "status": "PASS" if violation_count == 0 else "FAIL"
    }

df = spark.read.table("production.transactions")
checks = [
    range_check(df, "amount", min_val=0, max_val=1000000),
    range_check(df, "quantity", min_val=1, max_val=10000),
    range_check(df, "discount_pct", min_val=0, max_val=100),
]
for c in checks:
    print(f"  {c['column']}: {c['status']} (violations: {c['violations']})")`,
  },

  {
    id: 8,
    category: 'Data Quality',
    title: 'Pattern Matching Validation',
    desc: 'Validate data formats using regex patterns',
    code: `from pyspark.sql import functions as F

def validate_pattern(df, column, pattern, description=""):
    """Validate column values match a regex pattern."""
    non_null = df.filter(F.col(column).isNotNull())
    matching = non_null.filter(F.col(column).rlike(pattern))
    non_matching = non_null.count() - matching.count()
    return {
        "column": column,
        "pattern": pattern,
        "description": description,
        "non_matching": non_matching,
        "status": "PASS" if non_matching == 0 else "FAIL"
    }

df = spark.read.table("production.customers")

pattern_checks = [
    validate_pattern(df, "email", r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z]{2,}$", "Email format"),
    validate_pattern(df, "phone", r"^\\+?[1-9]\\d{1,14}$", "E.164 phone format"),
    validate_pattern(df, "zip_code", r"^\\d{5}(-\\d{4})?$", "US ZIP code"),
    validate_pattern(df, "ssn", r"^\\d{3}-\\d{2}-\\d{4}$", "SSN format"),
    validate_pattern(df, "ip_address", r"^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$", "IPv4 format"),
]
for p in pattern_checks:
    print(f"  {p['description']}: {p['status']} (failures: {p['non_matching']})")`,
  },

  {
    id: 9,
    category: 'Data Quality',
    title: 'Outlier Detection',
    desc: 'Statistical outlier detection using IQR and Z-score methods',
    code: `from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Summarizer

def detect_outliers_iqr(df, column, multiplier=1.5):
    """Detect outliers using IQR method."""
    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1
    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    outliers = df.filter(
        (F.col(column) < lower_bound) | (F.col(column) > upper_bound)
    )
    return {
        "column": column,
        "q1": q1, "q3": q3, "iqr": iqr,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "outlier_count": outliers.count(),
        "total_rows": df.count()
    }

def detect_outliers_zscore(df, column, threshold=3.0):
    """Detect outliers using Z-score method."""
    stats = df.select(
        F.mean(column).alias("mean"),
        F.stddev(column).alias("stddev")
    ).collect()[0]
    mean_val, stddev_val = stats["mean"], stats["stddev"]

    outliers = df.filter(
        F.abs((F.col(column) - mean_val) / stddev_val) > threshold
    )
    return {
        "column": column,
        "mean": mean_val, "stddev": stddev_val,
        "threshold": threshold,
        "outlier_count": outliers.count()
    }

df = spark.read.table("production.sales")
iqr_result = detect_outliers_iqr(df, "order_total")
zscore_result = detect_outliers_zscore(df, "order_total")
print(f"IQR outliers: {iqr_result['outlier_count']}")
print(f"Z-score outliers: {zscore_result['outlier_count']}")`,
  },

  {
    id: 10,
    category: 'Data Quality',
    title: 'Data Profiling',
    desc: 'Comprehensive data profiling with statistics per column',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType, TimestampType

def profile_dataframe(df):
    """Generate comprehensive profile for all columns."""
    total_rows = df.count()
    profiles = []

    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType
        null_count = df.filter(F.col(col_name).isNull()).count()
        distinct_count = df.select(col_name).distinct().count()

        profile = {
            "column": col_name,
            "type": str(col_type),
            "null_count": null_count,
            "null_pct": round(null_count / total_rows * 100, 2),
            "distinct_count": distinct_count,
            "uniqueness": round(distinct_count / total_rows * 100, 2)
        }

        if isinstance(col_type, NumericType):
            stats = df.select(
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("stddev"),
                F.percentile_approx(col_name, 0.5).alias("median")
            ).collect()[0]
            profile.update({
                "min": stats["min"], "max": stats["max"],
                "mean": round(stats["mean"], 4) if stats["mean"] else None,
                "stddev": round(stats["stddev"], 4) if stats["stddev"] else None,
                "median": stats["median"]
            })
        elif isinstance(col_type, StringType):
            len_stats = df.select(
                F.min(F.length(col_name)).alias("min_len"),
                F.max(F.length(col_name)).alias("max_len"),
                F.mean(F.length(col_name)).alias("avg_len")
            ).collect()[0]
            profile.update({
                "min_length": len_stats["min_len"],
                "max_length": len_stats["max_len"],
                "avg_length": round(len_stats["avg_len"], 1) if len_stats["avg_len"] else None
            })
        profiles.append(profile)

    return {"total_rows": total_rows, "total_columns": len(df.columns), "profiles": profiles}

df = spark.read.table("production.customers")
profile = profile_dataframe(df)
for p in profile["profiles"]:
    print(f"{p['column']}: {p['type']} | nulls={p['null_pct']}% | distinct={p['distinct_count']}")`,
  },

  // ===== Completeness (11-15) =====
  {
    id: 11,
    category: 'Completeness',
    title: 'Missing Values Percentage',
    desc: 'Calculate missing value percentages with thresholds',
    code: `from pyspark.sql import functions as F

def check_missing_values(df, threshold_pct=5.0):
    """Check missing value percentage against threshold."""
    total_rows = df.count()
    results = []
    for col_name in df.columns:
        missing = df.filter(
            F.col(col_name).isNull() | (F.col(col_name) == "") | F.isnan(col_name)
        ).count()
        pct = round(missing / total_rows * 100, 2) if total_rows > 0 else 0
        results.append({
            "column": col_name,
            "missing_count": missing,
            "missing_pct": pct,
            "status": "PASS" if pct <= threshold_pct else "FAIL"
        })

    failing = [r for r in results if r["status"] == "FAIL"]
    return {
        "status": "PASS" if not failing else "FAIL",
        "threshold_pct": threshold_pct,
        "total_rows": total_rows,
        "failing_columns": failing,
        "all_results": results
    }

df = spark.read.table("production.customer_profiles")
result = check_missing_values(df, threshold_pct=2.0)
print(f"Missing values check: {result['status']}")
for f in result["failing_columns"]:
    print(f"  FAIL: {f['column']} = {f['missing_pct']}% missing")`,
  },

  {
    id: 12,
    category: 'Completeness',
    title: 'Required Fields Validation',
    desc: 'Ensure all required fields are populated',
    code: `from pyspark.sql import functions as F

def validate_required_fields(df, required_fields):
    """Validate that required fields are never null or empty."""
    violations = {}
    total_rows = df.count()

    for field in required_fields:
        null_or_empty = df.filter(
            F.col(field).isNull() |
            (F.col(field).cast("string") == "") |
            (F.col(field).cast("string") == "null")
        ).count()
        if null_or_empty > 0:
            violations[field] = {
                "count": null_or_empty,
                "pct": round(null_or_empty / total_rows * 100, 2)
            }

    return {
        "status": "PASS" if not violations else "FAIL",
        "total_rows": total_rows,
        "violations": violations
    }

df = spark.read.table("production.orders")
required = ["order_id", "customer_id", "order_date", "status", "total_amount"]
result = validate_required_fields(df, required)
assert result["status"] == "PASS", f"Required field violations: {result['violations']}"`,
  },

  {
    id: 13,
    category: 'Completeness',
    title: 'Partial Record Detection',
    desc: 'Identify records with incomplete data across field groups',
    code: `from pyspark.sql import functions as F

def detect_partial_records(df, field_groups):
    """Detect records that have some but not all fields in a group populated."""
    results = []
    for group_name, fields in field_groups.items():
        null_counts = [F.when(F.col(f).isNull(), 1).otherwise(0) for f in fields]
        null_sum = sum(null_counts)
        total_fields = len(fields)

        partial = df.withColumn("_nulls", null_sum).filter(
            (F.col("_nulls") > 0) & (F.col("_nulls") < total_fields)
        )
        partial_count = partial.count()
        results.append({
            "group": group_name,
            "fields": fields,
            "partial_records": partial_count,
            "status": "PASS" if partial_count == 0 else "WARN"
        })

    return results

df = spark.read.table("production.customer_profiles")
field_groups = {
    "address": ["street", "city", "state", "zip_code"],
    "contact": ["phone", "email", "preferred_contact"],
    "demographics": ["age", "gender", "income_bracket"]
}
results = detect_partial_records(df, field_groups)
for r in results:
    print(f"  {r['group']}: {r['status']} ({r['partial_records']} partial records)")`,
  },

  {
    id: 14,
    category: 'Completeness',
    title: 'Empty String Detection',
    desc: 'Find columns with empty strings masking as populated data',
    code: `from pyspark.sql import functions as F

def detect_empty_strings(df):
    """Find empty strings, whitespace-only values, and placeholder nulls."""
    total_rows = df.count()
    results = []

    string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]

    for col_name in string_cols:
        empty_count = df.filter(F.col(col_name) == "").count()
        whitespace_count = df.filter(F.trim(F.col(col_name)) == "").count()
        placeholder_count = df.filter(
            F.lower(F.col(col_name)).isin(["n/a", "na", "none", "null", "-", "unknown", ""])
        ).count()

        results.append({
            "column": col_name,
            "empty_strings": empty_count,
            "whitespace_only": whitespace_count - empty_count,
            "placeholders": placeholder_count,
            "total_suspicious": placeholder_count,
            "pct": round(placeholder_count / total_rows * 100, 2) if total_rows > 0 else 0
        })

    return {"total_rows": total_rows, "results": results}

df = spark.read.table("production.survey_responses")
report = detect_empty_strings(df)
for r in report["results"]:
    if r["total_suspicious"] > 0:
        print(f"  {r['column']}: {r['total_suspicious']} suspicious values ({r['pct']}%)")`,
  },

  {
    id: 15,
    category: 'Completeness',
    title: 'Default Value Detection',
    desc: 'Identify columns dominated by default or sentinel values',
    code: `from pyspark.sql import functions as F

def detect_default_values(df, default_map=None):
    """Detect columns where default/sentinel values dominate."""
    if default_map is None:
        default_map = {
            "numeric": [0, -1, 9999, -9999, 999999],
            "string": ["unknown", "n/a", "default", "none", "tbd", "placeholder"],
            "date": ["1900-01-01", "1970-01-01", "9999-12-31"]
        }
    total_rows = df.count()
    alerts = []

    for field in df.schema.fields:
        col_name = field.name
        col_type = str(field.dataType)

        if "Integer" in col_type or "Double" in col_type or "Long" in col_type:
            for default_val in default_map["numeric"]:
                count = df.filter(F.col(col_name) == default_val).count()
                pct = round(count / total_rows * 100, 2) if total_rows > 0 else 0
                if pct > 50:
                    alerts.append({
                        "column": col_name, "default_value": default_val,
                        "count": count, "pct": pct
                    })
        elif "String" in col_type:
            for default_val in default_map["string"]:
                count = df.filter(F.lower(F.col(col_name)) == default_val).count()
                pct = round(count / total_rows * 100, 2) if total_rows > 0 else 0
                if pct > 50:
                    alerts.append({
                        "column": col_name, "default_value": default_val,
                        "count": count, "pct": pct
                    })

    return {
        "status": "PASS" if not alerts else "WARN",
        "alerts": alerts
    }

df = spark.read.table("production.records")
result = detect_default_values(df)
for a in result["alerts"]:
    print(f"  WARN: {a['column']} has {a['pct']}% default value '{a['default_value']}'")`,
  },

  // ===== Accuracy (16-20) =====
  {
    id: 16,
    category: 'Accuracy',
    title: 'Cross-Source Validation',
    desc: 'Compare data between two sources for accuracy',
    code: `from pyspark.sql import functions as F

def cross_source_validation(source_df, target_df, join_keys, compare_columns):
    """Compare data between source and target on specified columns."""
    joined = source_df.alias("src").join(
        target_df.alias("tgt"), on=join_keys, how="full_outer"
    )

    mismatches = {}
    for col in compare_columns:
        mismatch_count = joined.filter(
            (F.col(f"src.{col}") != F.col(f"tgt.{col}")) |
            (F.col(f"src.{col}").isNull() != F.col(f"tgt.{col}").isNull())
        ).count()
        mismatches[col] = mismatch_count

    src_only = joined.filter(F.col(f"tgt.{join_keys[0]}").isNull()).count()
    tgt_only = joined.filter(F.col(f"src.{join_keys[0]}").isNull()).count()

    return {
        "status": "PASS" if all(v == 0 for v in mismatches.values()) and src_only == 0 and tgt_only == 0 else "FAIL",
        "column_mismatches": mismatches,
        "source_only_rows": src_only,
        "target_only_rows": tgt_only
    }

source = spark.read.table("raw.customers")
target = spark.read.table("curated.customers")
result = cross_source_validation(source, target, ["customer_id"], ["name", "email", "status"])
print(f"Cross-source validation: {result['status']}")`,
  },

  {
    id: 17,
    category: 'Accuracy',
    title: 'Checksum Validation',
    desc: 'Validate data integrity using row-level checksums',
    code: `from pyspark.sql import functions as F
import hashlib

def add_row_checksum(df, columns=None, checksum_col="row_checksum"):
    """Add MD5 checksum column based on specified columns."""
    if columns is None:
        columns = df.columns
    concat_expr = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns])
    return df.withColumn(checksum_col, F.md5(concat_expr))

def validate_checksums(source_df, target_df, key_columns, data_columns):
    """Compare checksums between source and target."""
    src_with_hash = add_row_checksum(source_df, data_columns, "src_checksum")
    tgt_with_hash = add_row_checksum(target_df, data_columns, "tgt_checksum")

    joined = src_with_hash.alias("s").join(
        tgt_with_hash.alias("t"), on=key_columns, how="inner"
    )
    mismatches = joined.filter(F.col("s.src_checksum") != F.col("t.tgt_checksum"))
    mismatch_count = mismatches.count()

    return {
        "status": "PASS" if mismatch_count == 0 else "FAIL",
        "total_compared": joined.count(),
        "mismatches": mismatch_count,
        "mismatch_sample": mismatches.select(key_columns + ["s.src_checksum", "t.tgt_checksum"]).limit(10).collect()
    }

source = spark.read.table("staging.orders")
target = spark.read.table("production.orders")
result = validate_checksums(source, target, ["order_id"], ["amount", "status", "ship_date"])
print(f"Checksum validation: {result['status']} ({result['mismatches']} mismatches)")`,
  },

  {
    id: 18,
    category: 'Accuracy',
    title: 'Reconciliation Testing',
    desc: 'Aggregate reconciliation between source and target datasets',
    code: `from pyspark.sql import functions as F

def reconcile_aggregates(source_df, target_df, group_cols, agg_rules):
    """Reconcile aggregate values between source and target."""
    results = []
    for rule in agg_rules:
        col_name = rule["column"]
        agg_func = rule["function"]
        tolerance = rule.get("tolerance", 0.01)

        if agg_func == "sum":
            src_val = source_df.agg(F.sum(col_name)).collect()[0][0]
            tgt_val = target_df.agg(F.sum(col_name)).collect()[0][0]
        elif agg_func == "count":
            src_val = source_df.count()
            tgt_val = target_df.count()
        elif agg_func == "avg":
            src_val = source_df.agg(F.avg(col_name)).collect()[0][0]
            tgt_val = target_df.agg(F.avg(col_name)).collect()[0][0]

        diff = abs(src_val - tgt_val) if src_val and tgt_val else None
        pct_diff = (diff / src_val * 100) if src_val and diff else 0

        results.append({
            "column": col_name,
            "function": agg_func,
            "source_value": src_val,
            "target_value": tgt_val,
            "difference": diff,
            "pct_diff": round(pct_diff, 4),
            "status": "PASS" if pct_diff <= tolerance else "FAIL"
        })

    return results

source = spark.read.table("raw.transactions")
target = spark.read.table("curated.transactions")
rules = [
    {"column": "amount", "function": "sum", "tolerance": 0.001},
    {"column": "amount", "function": "count", "tolerance": 0},
    {"column": "amount", "function": "avg", "tolerance": 0.01},
]
results = reconcile_aggregates(source, target, [], rules)
for r in results:
    print(f"  {r['function']}({r['column']}): {r['status']} (diff={r['pct_diff']}%)")`,
  },

  {
    id: 19,
    category: 'Accuracy',
    title: 'Statistical Distribution Tests',
    desc: 'Validate data distributions using statistical tests',
    code: `from pyspark.sql import functions as F
from pyspark.ml.stat import ChiSquareTest, KolmogorovSmirnovTest
from pyspark.ml.feature import VectorAssembler

def ks_test(df, column, distribution="norm"):
    """Kolmogorov-Smirnov test for distribution fitting."""
    stats = df.select(
        F.mean(column).alias("mean"),
        F.stddev(column).alias("std")
    ).collect()[0]

    result = KolmogorovSmirnovTest.test(
        df.select(F.col(column).alias("value")),
        "value", distribution, stats["mean"], stats["std"]
    ).head()

    return {
        "test": "Kolmogorov-Smirnov",
        "column": column,
        "statistic": result.statistic,
        "pValue": result.pValue,
        "is_normal": result.pValue > 0.05
    }

def compare_distributions(df1, df2, column, num_bins=20):
    """Compare distributions between two DataFrames using histogram overlap."""
    combined_min = min(
        df1.agg(F.min(column)).collect()[0][0],
        df2.agg(F.min(column)).collect()[0][0]
    )
    combined_max = max(
        df1.agg(F.max(column)).collect()[0][0],
        df2.agg(F.max(column)).collect()[0][0]
    )
    bin_width = (combined_max - combined_min) / num_bins

    def get_histogram(df):
        return df.withColumn(
            "bin", F.floor((F.col(column) - combined_min) / bin_width)
        ).groupBy("bin").count().orderBy("bin")

    hist1 = get_histogram(df1).collect()
    hist2 = get_histogram(df2).collect()
    return {"histogram_1": hist1, "histogram_2": hist2}

df = spark.read.table("production.scores")
ks_result = ks_test(df, "credit_score")
print(f"KS test: p-value={ks_result['pValue']:.4f}, normal={ks_result['is_normal']}")`,
  },

  {
    id: 20,
    category: 'Accuracy',
    title: 'Sampling-Based Validation',
    desc: 'Validate data accuracy using stratified sampling',
    code: `from pyspark.sql import functions as F

def stratified_sample_validation(df, strata_col, sample_fraction=0.01, seed=42):
    """Perform stratified sampling for manual validation."""
    strata_counts = df.groupBy(strata_col).count().collect()
    fractions = {row[strata_col]: sample_fraction for row in strata_counts}
    sample = df.sampleBy(strata_col, fractions, seed=seed)

    sample_stats = sample.groupBy(strata_col).agg(
        F.count("*").alias("sample_count"),
        F.countDistinct("*").alias("distinct_count")
    )
    return sample, sample_stats

def validation_report(original_df, sample_df, numeric_cols):
    """Generate accuracy report comparing sample stats to population."""
    results = []
    for col in numeric_cols:
        orig_stats = original_df.agg(
            F.mean(col).alias("pop_mean"),
            F.stddev(col).alias("pop_std")
        ).collect()[0]
        sample_stats = sample_df.agg(
            F.mean(col).alias("sample_mean"),
            F.stddev(col).alias("sample_std")
        ).collect()[0]

        mean_diff = abs(orig_stats["pop_mean"] - sample_stats["sample_mean"])
        results.append({
            "column": col,
            "population_mean": round(orig_stats["pop_mean"], 4),
            "sample_mean": round(sample_stats["sample_mean"], 4),
            "mean_diff": round(mean_diff, 4),
            "within_2pct": mean_diff / orig_stats["pop_mean"] < 0.02 if orig_stats["pop_mean"] else True
        })
    return results

df = spark.read.table("production.transactions")
sample, stats = stratified_sample_validation(df, "category", sample_fraction=0.05)
report = validation_report(df, sample, ["amount", "quantity"])
for r in report:
    print(f"  {r['column']}: pop_mean={r['population_mean']}, sample_mean={r['sample_mean']}, ok={r['within_2pct']}")`,
  },

  // ===== Consistency (21-25) =====
  {
    id: 21,
    category: 'Consistency',
    title: 'Format Consistency Check',
    desc: 'Verify data format consistency within columns',
    code: `from pyspark.sql import functions as F

def check_format_consistency(df, column, expected_patterns):
    """Check what percentage of values match each expected pattern."""
    total = df.filter(F.col(column).isNotNull()).count()
    results = []
    matched_total = 0

    for pattern_name, regex in expected_patterns.items():
        match_count = df.filter(F.col(column).rlike(regex)).count()
        matched_total += match_count
        results.append({
            "pattern": pattern_name,
            "regex": regex,
            "match_count": match_count,
            "match_pct": round(match_count / total * 100, 2) if total > 0 else 0
        })

    unmatched = total - matched_total
    return {
        "column": column,
        "total_non_null": total,
        "unmatched": unmatched,
        "consistency_pct": round((total - unmatched) / total * 100, 2) if total > 0 else 0,
        "patterns": results
    }

df = spark.read.table("production.contacts")
phone_patterns = {
    "US_standard": r"^\\(\\d{3}\\) \\d{3}-\\d{4}$",
    "US_dashes": r"^\\d{3}-\\d{3}-\\d{4}$",
    "E164": r"^\\+1\\d{10}$",
    "digits_only": r"^\\d{10}$"
}
result = check_format_consistency(df, "phone_number", phone_patterns)
print(f"Phone format consistency: {result['consistency_pct']}%")
for p in result["patterns"]:
    print(f"  {p['pattern']}: {p['match_pct']}%")`,
  },

  {
    id: 22,
    category: 'Consistency',
    title: 'Date Format Consistency',
    desc: 'Validate consistent date formatting across datasets',
    code: `from pyspark.sql import functions as F

def validate_date_formats(df, date_columns, expected_format="yyyy-MM-dd"):
    """Validate date columns can be parsed with expected format."""
    results = []
    for col_name in date_columns:
        parsed = df.withColumn(
            "_parsed", F.to_date(F.col(col_name).cast("string"), expected_format)
        )
        unparseable = parsed.filter(
            F.col(col_name).isNotNull() & F.col("_parsed").isNull()
        ).count()
        total = df.filter(F.col(col_name).isNotNull()).count()

        results.append({
            "column": col_name,
            "expected_format": expected_format,
            "unparseable_count": unparseable,
            "parse_success_pct": round((total - unparseable) / total * 100, 2) if total > 0 else 0,
            "status": "PASS" if unparseable == 0 else "FAIL"
        })

    # Check for future dates
    for col_name in date_columns:
        future = df.filter(F.col(col_name) > F.current_date()).count()
        if future > 0:
            results.append({
                "column": col_name,
                "issue": "future_dates",
                "count": future,
                "status": "WARN"
            })

    return results

df = spark.read.table("production.events")
results = validate_date_formats(df, ["created_at", "updated_at", "event_date"])
for r in results:
    print(f"  {r['column']}: {r['status']}")`,
  },

  {
    id: 23,
    category: 'Consistency',
    title: 'Currency Consistency',
    desc: 'Validate currency values and code consistency',
    code: `from pyspark.sql import functions as F

def validate_currency_consistency(df, amount_col, currency_col):
    """Validate currency amounts and codes are consistent."""
    valid_currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR"]
    invalid_currency = df.filter(
        ~F.col(currency_col).isin(valid_currencies) & F.col(currency_col).isNotNull()
    )
    negative_amounts = df.filter(F.col(amount_col) < 0)
    excessive_decimals = df.filter(
        F.col(amount_col) != F.round(F.col(amount_col), 2)
    )

    # Check JPY should have no decimals
    jpy_with_decimals = df.filter(
        (F.col(currency_col) == "JPY") &
        (F.col(amount_col) != F.round(F.col(amount_col), 0))
    )

    return {
        "invalid_currency_codes": invalid_currency.count(),
        "negative_amounts": negative_amounts.count(),
        "excessive_decimals": excessive_decimals.count(),
        "jpy_decimal_violations": jpy_with_decimals.count(),
        "currency_distribution": df.groupBy(currency_col).count().collect(),
        "status": "PASS" if invalid_currency.count() == 0 and negative_amounts.count() == 0 else "FAIL"
    }

df = spark.read.table("production.payments")
result = validate_currency_consistency(df, "amount", "currency_code")
print(f"Currency consistency: {result['status']}")
print(f"  Invalid codes: {result['invalid_currency_codes']}")
print(f"  Negative amounts: {result['negative_amounts']}")`,
  },

  {
    id: 24,
    category: 'Consistency',
    title: 'Encoding Consistency',
    desc: 'Detect and validate character encoding issues',
    code: `from pyspark.sql import functions as F

def detect_encoding_issues(df, string_columns=None):
    """Detect encoding inconsistencies in string columns."""
    if string_columns is None:
        string_columns = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]

    results = []
    for col_name in string_columns:
        non_ascii = df.filter(
            F.col(col_name).rlike("[^\\x00-\\x7F]")
        ).count()
        control_chars = df.filter(
            F.col(col_name).rlike("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]")
        ).count()
        replacement_char = df.filter(
            F.col(col_name).contains("\\ufffd")
        ).count()
        null_bytes = df.filter(
            F.col(col_name).contains("\\x00")
        ).count()

        results.append({
            "column": col_name,
            "non_ascii_rows": non_ascii,
            "control_chars": control_chars,
            "replacement_chars": replacement_char,
            "null_bytes": null_bytes,
            "status": "PASS" if control_chars == 0 and null_bytes == 0 else "WARN"
        })

    return results

df = spark.read.table("production.user_content")
results = detect_encoding_issues(df, ["name", "description", "comments"])
for r in results:
    if r["status"] != "PASS":
        print(f"  {r['column']}: control_chars={r['control_chars']}, null_bytes={r['null_bytes']}")`,
  },

  {
    id: 25,
    category: 'Consistency',
    title: 'Case Sensitivity Check',
    desc: 'Detect case inconsistencies in categorical fields',
    code: `from pyspark.sql import functions as F

def check_case_consistency(df, columns):
    """Check for case inconsistencies in categorical columns."""
    results = []
    for col_name in columns:
        distinct_values = df.select(col_name).distinct().filter(
            F.col(col_name).isNotNull()
        )
        distinct_lower = df.select(
            F.lower(F.col(col_name)).alias(col_name)
        ).distinct().filter(F.col(col_name).isNotNull())

        original_count = distinct_values.count()
        lower_count = distinct_lower.count()

        if original_count != lower_count:
            case_variants = df.groupBy(col_name).count().withColumn(
                "_lower", F.lower(F.col(col_name))
            ).groupBy("_lower").agg(
                F.collect_list(col_name).alias("variants"),
                F.sum("count").alias("total")
            ).filter(F.size("variants") > 1)

            variants = case_variants.collect()
            results.append({
                "column": col_name,
                "distinct_original": original_count,
                "distinct_normalized": lower_count,
                "inconsistent_groups": len(variants),
                "examples": [{
                    "normalized": row["_lower"],
                    "variants": row["variants"]
                } for row in variants[:5]],
                "status": "FAIL"
            })
        else:
            results.append({"column": col_name, "status": "PASS"})

    return results

df = spark.read.table("production.products")
results = check_case_consistency(df, ["category", "brand", "status", "country"])
for r in results:
    if r["status"] == "FAIL":
        print(f"  {r['column']}: {r['inconsistent_groups']} inconsistent groups")
        for e in r.get("examples", []):
            print(f"    '{e['normalized']}' -> {e['variants']}")`,
  },

  // ===== Timeliness (26-30) =====
  {
    id: 26,
    category: 'Timeliness',
    title: 'Data Freshness Check',
    desc: 'Monitor data freshness and staleness thresholds',
    code: `from pyspark.sql import functions as F
from datetime import datetime, timedelta

def check_data_freshness(df, timestamp_col, max_age_hours=24):
    """Check if data is fresh enough based on latest timestamp."""
    latest = df.agg(F.max(timestamp_col)).collect()[0][0]
    earliest = df.agg(F.min(timestamp_col)).collect()[0][0]
    now = datetime.now()

    age_hours = (now - latest).total_seconds() / 3600 if latest else None
    span_days = (latest - earliest).days if latest and earliest else None

    return {
        "latest_record": str(latest),
        "earliest_record": str(earliest),
        "age_hours": round(age_hours, 2) if age_hours else None,
        "data_span_days": span_days,
        "threshold_hours": max_age_hours,
        "is_fresh": age_hours <= max_age_hours if age_hours else False,
        "status": "PASS" if age_hours and age_hours <= max_age_hours else "FAIL"
    }

# Check multiple tables
tables = [
    ("production.orders", "created_at", 2),
    ("production.events", "event_time", 1),
    ("production.logs", "timestamp", 0.5),
]
for table, col, threshold in tables:
    df = spark.read.table(table)
    result = check_data_freshness(df, col, threshold)
    print(f"  {table}: {result['status']} (age={result['age_hours']}h, threshold={threshold}h)")`,
  },

  {
    id: 27,
    category: 'Timeliness',
    title: 'SLA Monitoring',
    desc: 'Monitor data pipeline SLA compliance',
    code: `from pyspark.sql import functions as F
from datetime import datetime, timedelta

def check_sla_compliance(table_name, sla_config):
    """Check if data delivery meets SLA requirements."""
    df = spark.read.table(table_name)
    now = datetime.now()

    results = {
        "table": table_name,
        "check_time": str(now),
        "checks": []
    }

    # Freshness SLA
    if "max_age_hours" in sla_config:
        latest = df.agg(F.max(sla_config["timestamp_col"])).collect()[0][0]
        age_hours = (now - latest).total_seconds() / 3600 if latest else float("inf")
        results["checks"].append({
            "type": "freshness",
            "threshold": sla_config["max_age_hours"],
            "actual": round(age_hours, 2),
            "status": "PASS" if age_hours <= sla_config["max_age_hours"] else "BREACH"
        })

    # Row count SLA
    if "min_daily_rows" in sla_config:
        today = now.date()
        today_count = df.filter(
            F.col(sla_config["timestamp_col"]).cast("date") == str(today)
        ).count()
        results["checks"].append({
            "type": "volume",
            "threshold": sla_config["min_daily_rows"],
            "actual": today_count,
            "status": "PASS" if today_count >= sla_config["min_daily_rows"] else "BREACH"
        })

    # Completeness SLA
    if "max_null_pct" in sla_config:
        for col in sla_config.get("critical_columns", []):
            null_pct = df.filter(F.col(col).isNull()).count() / df.count() * 100
            results["checks"].append({
                "type": f"completeness_{col}",
                "threshold": sla_config["max_null_pct"],
                "actual": round(null_pct, 2),
                "status": "PASS" if null_pct <= sla_config["max_null_pct"] else "BREACH"
            })

    overall = "PASS" if all(c["status"] == "PASS" for c in results["checks"]) else "BREACH"
    results["overall_status"] = overall
    return results

sla = {
    "timestamp_col": "created_at",
    "max_age_hours": 4,
    "min_daily_rows": 10000,
    "max_null_pct": 1.0,
    "critical_columns": ["customer_id", "amount"]
}
result = check_sla_compliance("production.orders", sla)
print(f"SLA Status: {result['overall_status']}")`,
  },

  {
    id: 28,
    category: 'Timeliness',
    title: 'Pipeline Latency Monitoring',
    desc: 'Track and alert on pipeline processing latency',
    code: `from pyspark.sql import functions as F
from datetime import datetime

def measure_pipeline_latency(df, event_time_col, process_time_col):
    """Measure end-to-end pipeline latency."""
    latency_df = df.withColumn(
        "latency_seconds",
        F.unix_timestamp(F.col(process_time_col)) - F.unix_timestamp(F.col(event_time_col))
    ).filter(F.col("latency_seconds").isNotNull())

    stats = latency_df.agg(
        F.avg("latency_seconds").alias("avg_latency"),
        F.percentile_approx("latency_seconds", 0.5).alias("p50_latency"),
        F.percentile_approx("latency_seconds", 0.95).alias("p95_latency"),
        F.percentile_approx("latency_seconds", 0.99).alias("p99_latency"),
        F.max("latency_seconds").alias("max_latency"),
        F.min("latency_seconds").alias("min_latency"),
        F.count("*").alias("total_records")
    ).collect()[0]

    return {
        "avg_latency_sec": round(stats["avg_latency"], 2),
        "p50_latency_sec": stats["p50_latency"],
        "p95_latency_sec": stats["p95_latency"],
        "p99_latency_sec": stats["p99_latency"],
        "max_latency_sec": stats["max_latency"],
        "min_latency_sec": stats["min_latency"],
        "total_records": stats["total_records"]
    }

def check_latency_sla(latency_stats, p95_threshold_sec=300, p99_threshold_sec=600):
    """Check if latency meets SLA thresholds."""
    p95_ok = latency_stats["p95_latency_sec"] <= p95_threshold_sec
    p99_ok = latency_stats["p99_latency_sec"] <= p99_threshold_sec
    return {
        **latency_stats,
        "p95_threshold": p95_threshold_sec,
        "p99_threshold": p99_threshold_sec,
        "status": "PASS" if p95_ok and p99_ok else "FAIL"
    }

df = spark.read.table("production.events")
stats = measure_pipeline_latency(df, "event_time", "processed_at")
result = check_latency_sla(stats, p95_threshold_sec=120)
print(f"Latency SLA: {result['status']} (p95={result['p95_latency_sec']}s, p99={result['p99_latency_sec']}s)")`,
  },

  {
    id: 29,
    category: 'Timeliness',
    title: 'Data Arrival Monitoring',
    desc: 'Monitor expected data arrival windows',
    code: `from pyspark.sql import functions as F
from datetime import datetime, time

def check_data_arrival(table_name, timestamp_col, expected_arrival_hour, tolerance_hours=2):
    """Check if data arrived within expected time window."""
    df = spark.read.table(table_name)
    today = datetime.now().date()

    today_data = df.filter(
        F.col(timestamp_col).cast("date") == str(today)
    )
    row_count = today_data.count()

    if row_count == 0:
        return {
            "table": table_name,
            "date": str(today),
            "status": "MISSING",
            "expected_by": f"{expected_arrival_hour}:00",
            "rows_found": 0
        }

    first_arrival = today_data.agg(F.min(timestamp_col)).collect()[0][0]
    arrival_hour = first_arrival.hour if first_arrival else None

    on_time = arrival_hour <= (expected_arrival_hour + tolerance_hours) if arrival_hour is not None else False

    # Hourly volume distribution
    hourly = today_data.withColumn(
        "hour", F.hour(F.col(timestamp_col))
    ).groupBy("hour").count().orderBy("hour")

    return {
        "table": table_name,
        "date": str(today),
        "first_arrival": str(first_arrival),
        "arrival_hour": arrival_hour,
        "expected_by": f"{expected_arrival_hour}:00",
        "on_time": on_time,
        "rows_found": row_count,
        "status": "PASS" if on_time else "LATE"
    }

schedules = [
    ("production.daily_sales", "created_at", 6),
    ("production.inventory", "updated_at", 8),
    ("production.web_events", "event_time", 1),
]
for table, col, hour in schedules:
    result = check_data_arrival(table, col, hour)
    print(f"  {table}: {result['status']} (first arrival: {result.get('first_arrival', 'N/A')})")`,
  },

  {
    id: 30,
    category: 'Timeliness',
    title: 'Timestamp Validation',
    desc: 'Validate timestamp columns for anomalies',
    code: `from pyspark.sql import functions as F
from datetime import datetime, timedelta

def validate_timestamps(df, timestamp_cols):
    """Comprehensive timestamp validation."""
    now = datetime.now()
    results = []

    for col_name in timestamp_cols:
        total = df.filter(F.col(col_name).isNotNull()).count()

        # Future timestamps
        future = df.filter(F.col(col_name) > F.lit(now)).count()

        # Very old timestamps (before 2000)
        ancient = df.filter(F.col(col_name) < F.lit(datetime(2000, 1, 1))).count()

        # Midnight clustering (possible default values)
        midnight = df.filter(
            (F.hour(F.col(col_name)) == 0) &
            (F.minute(F.col(col_name)) == 0) &
            (F.second(F.col(col_name)) == 0)
        ).count()
        midnight_pct = round(midnight / total * 100, 2) if total > 0 else 0

        # Timezone issues - check for exactly-on-the-hour timestamps
        on_hour = df.filter(
            (F.minute(F.col(col_name)) == 0) & (F.second(F.col(col_name)) == 0)
        ).count()

        # Gaps in time series
        date_counts = df.withColumn(
            "_date", F.col(col_name).cast("date")
        ).groupBy("_date").count().orderBy("_date")

        results.append({
            "column": col_name,
            "total_records": total,
            "future_timestamps": future,
            "ancient_timestamps": ancient,
            "midnight_cluster_pct": midnight_pct,
            "on_hour_count": on_hour,
            "status": "PASS" if future == 0 and ancient == 0 else "FAIL"
        })

    return results

df = spark.read.table("production.audit_log")
results = validate_timestamps(df, ["created_at", "updated_at", "event_time"])
for r in results:
    print(f"  {r['column']}: {r['status']} (future={r['future_timestamps']}, ancient={r['ancient_timestamps']})")`,
  },

  // ===== Uniqueness (31-35) =====
  {
    id: 31,
    category: 'Uniqueness',
    title: 'Duplicate Detection',
    desc: 'Detect and analyze duplicate records',
    code: `from pyspark.sql import functions as F

def detect_duplicates(df, key_columns, detail_columns=None):
    """Detect duplicate records based on key columns."""
    total_rows = df.count()
    distinct_rows = df.select(key_columns).distinct().count()
    duplicate_count = total_rows - distinct_rows

    # Find the actual duplicates
    duplicates = df.groupBy(key_columns).agg(
        F.count("*").alias("dup_count")
    ).filter(F.col("dup_count") > 1).orderBy(F.col("dup_count").desc())

    dup_groups = duplicates.count()
    total_dup_rows = duplicates.agg(F.sum("dup_count")).collect()[0][0] or 0

    # Sample duplicates for investigation
    sample = duplicates.limit(10).collect()

    return {
        "total_rows": total_rows,
        "distinct_rows": distinct_rows,
        "duplicate_rows": duplicate_count,
        "duplicate_pct": round(duplicate_count / total_rows * 100, 2) if total_rows > 0 else 0,
        "duplicate_groups": dup_groups,
        "total_affected_rows": total_dup_rows,
        "top_duplicates": sample,
        "status": "PASS" if duplicate_count == 0 else "FAIL"
    }

df = spark.read.table("production.transactions")
result = detect_duplicates(df, ["transaction_id"])
print(f"Duplicate check: {result['status']}")
print(f"  Duplicates: {result['duplicate_rows']} ({result['duplicate_pct']}%)")
print(f"  Groups: {result['duplicate_groups']}")`,
  },

  {
    id: 32,
    category: 'Uniqueness',
    title: 'Primary Key Validation',
    desc: 'Validate primary key uniqueness and non-null constraints',
    code: `from pyspark.sql import functions as F

def validate_primary_key(df, pk_columns):
    """Validate primary key uniqueness and completeness."""
    total_rows = df.count()

    # Check for nulls in PK columns
    null_condition = F.lit(False)
    for col in pk_columns:
        null_condition = null_condition | F.col(col).isNull()
    null_pk_count = df.filter(null_condition).count()

    # Check uniqueness
    distinct_count = df.select(pk_columns).distinct().count()
    duplicate_count = total_rows - distinct_count

    # Find duplicate PK values
    duplicates = df.groupBy(pk_columns).agg(
        F.count("*").alias("count")
    ).filter(F.col("count") > 1)

    return {
        "pk_columns": pk_columns,
        "total_rows": total_rows,
        "null_pks": null_pk_count,
        "duplicates": duplicate_count,
        "duplicate_groups": duplicates.count(),
        "is_valid_pk": null_pk_count == 0 and duplicate_count == 0,
        "status": "PASS" if null_pk_count == 0 and duplicate_count == 0 else "FAIL"
    }

# Test single and composite PKs
df_orders = spark.read.table("production.orders")
result_single = validate_primary_key(df_orders, ["order_id"])
print(f"orders PK: {result_single['status']}")

df_line_items = spark.read.table("production.order_lines")
result_composite = validate_primary_key(df_line_items, ["order_id", "line_number"])
print(f"order_lines PK: {result_composite['status']}")`,
  },

  {
    id: 33,
    category: 'Uniqueness',
    title: 'Composite Key Uniqueness',
    desc: 'Validate uniqueness of composite keys across dimensions',
    code: `from pyspark.sql import functions as F

def validate_composite_key(df, key_columns, context_columns=None):
    """Validate composite key uniqueness with context analysis."""
    total = df.count()

    # Check uniqueness of composite key
    duplicates = df.groupBy(key_columns).agg(
        F.count("*").alias("row_count")
    ).filter(F.col("row_count") > 1)

    dup_count = duplicates.count()

    # Analyze which key component contributes most to duplicates
    component_analysis = {}
    for col in key_columns:
        col_dups = df.groupBy(col).agg(
            F.count("*").alias("count")
        ).filter(F.col("count") > 1).count()
        component_analysis[col] = col_dups

    # If context columns provided, show conflicting values
    conflicts = None
    if context_columns and dup_count > 0:
        dup_keys = duplicates.select(key_columns)
        conflict_df = df.join(dup_keys, on=key_columns, how="inner")
        conflicts = conflict_df.groupBy(key_columns).agg(
            *[F.countDistinct(c).alias(f"{c}_variants") for c in context_columns]
        ).filter(
            F.greatest(*[F.col(f"{c}_variants") for c in context_columns]) > 1
        )

    return {
        "key_columns": key_columns,
        "total_rows": total,
        "duplicate_groups": dup_count,
        "component_analysis": component_analysis,
        "conflicts": conflicts.count() if conflicts else 0,
        "status": "PASS" if dup_count == 0 else "FAIL"
    }

df = spark.read.table("production.fact_sales")
result = validate_composite_key(
    df, ["date_key", "product_key", "store_key"],
    context_columns=["quantity", "revenue"]
)
print(f"Composite key validation: {result['status']}")`,
  },

  {
    id: 34,
    category: 'Uniqueness',
    title: 'Fuzzy Matching Detection',
    desc: 'Detect near-duplicate records using fuzzy matching',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

def levenshtein_similarity(df, col1_name, col2_name, threshold=0.85):
    """Find similar records using Levenshtein distance."""
    df_with_len = df.withColumn("_len1", F.length(F.col(col1_name)))

    # Self-join for comparison (use sampling for large datasets)
    sample = df.select("id", col1_name).limit(10000)
    cross = sample.alias("a").crossJoin(sample.alias("b")).filter(
        F.col("a.id") < F.col("b.id")
    )

    # Calculate similarity
    result = cross.withColumn(
        "distance", F.levenshtein(F.col(f"a.{col1_name}"), F.col(f"b.{col1_name}"))
    ).withColumn(
        "max_len", F.greatest(F.length(F.col(f"a.{col1_name}")), F.length(F.col(f"b.{col1_name}")))
    ).withColumn(
        "similarity", 1 - (F.col("distance") / F.col("max_len"))
    ).filter(F.col("similarity") >= threshold)

    return result.select(
        F.col("a.id").alias("id_1"),
        F.col("b.id").alias("id_2"),
        F.col(f"a.{col1_name}").alias("value_1"),
        F.col(f"b.{col1_name}").alias("value_2"),
        "similarity"
    ).orderBy(F.col("similarity").desc())

df = spark.read.table("production.customers")
fuzzy_matches = levenshtein_similarity(df, "company_name", "company_name", threshold=0.80)
match_count = fuzzy_matches.count()
print(f"Fuzzy matches found: {match_count}")
fuzzy_matches.show(20, truncate=False)`,
  },

  {
    id: 35,
    category: 'Uniqueness',
    title: 'Near-Duplicate Detection',
    desc: 'Detect near-duplicates using multiple field comparison',
    code: `from pyspark.sql import functions as F

def detect_near_duplicates(df, match_fields, id_col="id", threshold=0.7):
    """Detect near-duplicate records using weighted field matching."""
    weights = {f: 1.0 / len(match_fields) for f in match_fields}

    # Create blocking key for efficient comparison
    block_col = match_fields[0]
    df_blocked = df.withColumn(
        "_block_key", F.soundex(F.col(block_col)) if df.schema[block_col].dataType.simpleString() == "string"
        else F.col(block_col)
    )

    # Self-join within blocks
    pairs = df_blocked.alias("a").join(
        df_blocked.alias("b"),
        (F.col("a._block_key") == F.col("b._block_key")) &
        (F.col(f"a.{id_col}") < F.col(f"b.{id_col}")),
        "inner"
    )

    # Calculate field-by-field similarity
    similarity_expr = F.lit(0.0)
    for field, weight in weights.items():
        field_sim = F.when(
            F.col(f"a.{field}") == F.col(f"b.{field}"), weight
        ).when(
            F.col(f"a.{field}").isNull() | F.col(f"b.{field}").isNull(), 0.0
        ).otherwise(
            F.lit(weight) * (1 - F.levenshtein(
                F.lower(F.col(f"a.{field}").cast("string")),
                F.lower(F.col(f"b.{field}").cast("string"))
            ) / F.greatest(
                F.length(F.col(f"a.{field}").cast("string")),
                F.length(F.col(f"b.{field}").cast("string")),
                F.lit(1)
            ))
        )
        similarity_expr = similarity_expr + field_sim

    near_dups = pairs.withColumn(
        "similarity_score", similarity_expr
    ).filter(F.col("similarity_score") >= threshold)

    return near_dups.select(
        F.col(f"a.{id_col}").alias("id_1"),
        F.col(f"b.{id_col}").alias("id_2"),
        "similarity_score",
        *[F.col(f"a.{f}").alias(f"{f}_1") for f in match_fields],
        *[F.col(f"b.{f}").alias(f"{f}_2") for f in match_fields]
    ).orderBy(F.col("similarity_score").desc())

df = spark.read.table("production.contacts")
near_dups = detect_near_duplicates(df, ["name", "email", "phone", "address"], threshold=0.75)
print(f"Near-duplicates found: {near_dups.count()}")
near_dups.show(10, truncate=False)`,
  },

  // ===== Referential Integrity (36-40) =====
  {
    id: 36,
    category: 'Referential Integrity',
    title: 'Foreign Key Validation',
    desc: 'Validate foreign key references exist in parent tables',
    code: `from pyspark.sql import functions as F

def validate_foreign_key(child_df, parent_df, child_fk_col, parent_pk_col):
    """Validate all FK values exist in parent table."""
    child_keys = child_df.select(child_fk_col).distinct().filter(
        F.col(child_fk_col).isNotNull()
    )
    parent_keys = parent_df.select(parent_pk_col).distinct()

    orphans = child_keys.alias("c").join(
        parent_keys.alias("p"),
        F.col(f"c.{child_fk_col}") == F.col(f"p.{parent_pk_col}"),
        "left_anti"
    )
    orphan_count = orphans.count()
    total_refs = child_keys.count()

    return {
        "child_fk": child_fk_col,
        "parent_pk": parent_pk_col,
        "total_references": total_refs,
        "orphan_count": orphan_count,
        "integrity_pct": round((total_refs - orphan_count) / total_refs * 100, 2) if total_refs > 0 else 100,
        "sample_orphans": orphans.limit(10).collect(),
        "status": "PASS" if orphan_count == 0 else "FAIL"
    }

orders = spark.read.table("production.orders")
customers = spark.read.table("production.customers")
products = spark.read.table("production.products")
order_lines = spark.read.table("production.order_lines")

fk_checks = [
    validate_foreign_key(orders, customers, "customer_id", "customer_id"),
    validate_foreign_key(order_lines, orders, "order_id", "order_id"),
    validate_foreign_key(order_lines, products, "product_id", "product_id"),
]
for check in fk_checks:
    print(f"  {check['child_fk']}: {check['status']} (orphans={check['orphan_count']})")`,
  },

  {
    id: 37,
    category: 'Referential Integrity',
    title: 'Orphan Record Detection',
    desc: 'Find orphan records with no parent reference',
    code: `from pyspark.sql import functions as F

def find_orphan_records(child_df, parent_df, child_fk, parent_pk, child_table="child", parent_table="parent"):
    """Find orphan records and analyze their characteristics."""
    orphans = child_df.alias("c").join(
        parent_df.alias("p"),
        F.col(f"c.{child_fk}") == F.col(f"p.{parent_pk}"),
        "left_anti"
    )
    orphan_count = orphans.count()
    total = child_df.count()

    analysis = {}
    if orphan_count > 0:
        # Analyze orphan patterns
        if "created_at" in child_df.columns:
            date_dist = orphans.withColumn(
                "orphan_date", F.col("created_at").cast("date")
            ).groupBy("orphan_date").count().orderBy("orphan_date")
            analysis["date_distribution"] = date_dist.collect()

        # Sample orphan FK values
        analysis["sample_orphan_keys"] = orphans.select(child_fk).distinct().limit(20).collect()

        # Check if orphan keys follow a pattern
        analysis["orphan_key_stats"] = orphans.select(
            F.min(child_fk).alias("min_key"),
            F.max(child_fk).alias("max_key"),
            F.countDistinct(child_fk).alias("distinct_keys")
        ).collect()[0]

    return {
        "child_table": child_table,
        "parent_table": parent_table,
        "total_child_rows": total,
        "orphan_count": orphan_count,
        "orphan_pct": round(orphan_count / total * 100, 2) if total > 0 else 0,
        "analysis": analysis,
        "status": "PASS" if orphan_count == 0 else "FAIL"
    }

orders = spark.read.table("production.orders")
customers = spark.read.table("production.customers")
result = find_orphan_records(orders, customers, "customer_id", "customer_id", "orders", "customers")
print(f"Orphan records: {result['orphan_count']} ({result['orphan_pct']}%)")`,
  },

  {
    id: 38,
    category: 'Referential Integrity',
    title: 'Cascade Validation',
    desc: 'Validate cascade delete/update consistency',
    code: `from pyspark.sql import functions as F

def validate_cascade_consistency(tables_config):
    """Validate that cascade operations maintained consistency."""
    results = []
    for config in tables_config:
        parent_df = spark.read.table(config["parent_table"])
        child_df = spark.read.table(config["child_table"])

        parent_pk = config["parent_pk"]
        child_fk = config["child_fk"]

        # Check for orphans (failed cascade delete)
        orphans = child_df.select(child_fk).distinct().join(
            parent_df.select(parent_pk),
            F.col(child_fk) == F.col(parent_pk),
            "left_anti"
        ).count()

        # Check for soft-deleted parent with active children
        if "is_deleted" in parent_df.columns:
            deleted_parents = parent_df.filter(F.col("is_deleted") == True)
            active_children_of_deleted = child_df.join(
                deleted_parents.select(parent_pk),
                F.col(child_fk) == F.col(parent_pk),
                "inner"
            )
            if "is_deleted" in child_df.columns:
                active_children_of_deleted = active_children_of_deleted.filter(
                    F.col("is_deleted") == False
                )
            cascade_violations = active_children_of_deleted.count()
        else:
            cascade_violations = 0

        results.append({
            "parent": config["parent_table"],
            "child": config["child_table"],
            "orphan_records": orphans,
            "cascade_violations": cascade_violations,
            "status": "PASS" if orphans == 0 and cascade_violations == 0 else "FAIL"
        })

    return results

cascade_config = [
    {"parent_table": "production.customers", "child_table": "production.orders",
     "parent_pk": "customer_id", "child_fk": "customer_id"},
    {"parent_table": "production.orders", "child_table": "production.order_lines",
     "parent_pk": "order_id", "child_fk": "order_id"},
]
results = validate_cascade_consistency(cascade_config)
for r in results:
    print(f"  {r['parent']} -> {r['child']}: {r['status']}")`,
  },

  {
    id: 39,
    category: 'Referential Integrity',
    title: 'Cross-Table Consistency',
    desc: 'Validate consistency across related tables',
    code: `from pyspark.sql import functions as F

def validate_cross_table_consistency(checks):
    """Run cross-table consistency checks."""
    results = []
    for check in checks:
        table_a = spark.read.table(check["table_a"])
        table_b = spark.read.table(check["table_b"])

        if check["type"] == "count_match":
            count_a = table_a.agg(F.count(check["col_a"])).collect()[0][0]
            count_b = table_b.agg(F.count(check["col_b"])).collect()[0][0]
            results.append({
                "check": check["name"],
                "type": "count_match",
                "value_a": count_a, "value_b": count_b,
                "status": "PASS" if count_a == count_b else "FAIL"
            })

        elif check["type"] == "sum_match":
            sum_a = table_a.agg(F.sum(check["col_a"])).collect()[0][0]
            sum_b = table_b.agg(F.sum(check["col_b"])).collect()[0][0]
            tolerance = check.get("tolerance", 0.01)
            diff = abs(sum_a - sum_b) / sum_a if sum_a else 0
            results.append({
                "check": check["name"],
                "type": "sum_match",
                "value_a": sum_a, "value_b": sum_b,
                "diff_pct": round(diff * 100, 4),
                "status": "PASS" if diff <= tolerance else "FAIL"
            })

        elif check["type"] == "subset":
            keys_a = set(table_a.select(check["col_a"]).distinct().rdd.flatMap(lambda x: x).collect())
            keys_b = set(table_b.select(check["col_b"]).distinct().rdd.flatMap(lambda x: x).collect())
            missing = keys_a - keys_b
            results.append({
                "check": check["name"],
                "type": "subset",
                "missing_count": len(missing),
                "status": "PASS" if len(missing) == 0 else "FAIL"
            })

    return results

checks = [
    {"name": "order_line_sum", "table_a": "production.orders", "table_b": "production.order_lines",
     "col_a": "total_amount", "col_b": "line_total", "type": "sum_match", "tolerance": 0.001},
    {"name": "product_coverage", "table_a": "production.order_lines", "table_b": "production.products",
     "col_a": "product_id", "col_b": "product_id", "type": "subset"},
]
results = validate_cross_table_consistency(checks)
for r in results:
    print(f"  {r['check']}: {r['status']}")`,
  },

  {
    id: 40,
    category: 'Referential Integrity',
    title: 'Cross-Database Validation',
    desc: 'Validate referential integrity across databases',
    code: `from pyspark.sql import functions as F

def cross_database_validation(source_config, target_config, join_key, compare_cols):
    """Validate data consistency across different databases."""
    # Read from source database
    source_df = (spark.read.format("jdbc")
        .option("url", source_config["url"])
        .option("dbtable", source_config["table"])
        .option("user", source_config["user"])
        .option("password", dbutils.secrets.get(scope="db", key=source_config["secret_key"]))
        .load())

    # Read from target database
    target_df = (spark.read.format("jdbc")
        .option("url", target_config["url"])
        .option("dbtable", target_config["table"])
        .option("user", target_config["user"])
        .option("password", dbutils.secrets.get(scope="db", key=target_config["secret_key"]))
        .load())

    # Row count comparison
    src_count = source_df.count()
    tgt_count = target_df.count()

    # Key coverage
    src_keys = source_df.select(join_key).distinct()
    tgt_keys = target_df.select(join_key).distinct()
    src_only = src_keys.subtract(tgt_keys).count()
    tgt_only = tgt_keys.subtract(src_keys).count()

    # Value comparison
    joined = source_df.alias("s").join(target_df.alias("t"), on=join_key, how="inner")
    mismatches = {}
    for col in compare_cols:
        mismatch = joined.filter(
            F.col(f"s.{col}") != F.col(f"t.{col}")
        ).count()
        mismatches[col] = mismatch

    return {
        "source_count": src_count,
        "target_count": tgt_count,
        "count_match": src_count == tgt_count,
        "source_only_keys": src_only,
        "target_only_keys": tgt_only,
        "column_mismatches": mismatches,
        "status": "PASS" if src_count == tgt_count and src_only == 0 and all(v == 0 for v in mismatches.values()) else "FAIL"
    }

source = {"url": "jdbc:postgresql://source-db:5432/prod", "table": "customers",
          "user": "reader", "secret_key": "source-db-password"}
target = {"url": "jdbc:sqlserver://target-db:1433;database=warehouse", "table": "dim_customer",
          "user": "reader", "secret_key": "target-db-password"}
result = cross_database_validation(source, target, "customer_id", ["name", "email", "status"])
print(f"Cross-DB validation: {result['status']}")`,
  },

  // ===== Business Rules (41-45) =====
  {
    id: 41,
    category: 'Business Rules',
    title: 'Custom Business Validators',
    desc: 'Implement custom business rule validation framework',
    code: `from pyspark.sql import functions as F
from dataclasses import dataclass
from typing import Callable

@dataclass
class BusinessRule:
    name: str
    description: str
    severity: str  # "critical", "warning", "info"
    validate: Callable

def create_rule(name, description, severity, condition_expr):
    """Factory to create business rules from Spark SQL expressions."""
    def validate(df):
        violations = df.filter(~condition_expr)
        return {
            "rule": name,
            "description": description,
            "severity": severity,
            "violations": violations.count(),
            "total_rows": df.count(),
            "status": "PASS" if violations.count() == 0 else "FAIL"
        }
    return validate

# Define business rules
rules = [
    create_rule(
        "positive_amount", "Order amount must be positive", "critical",
        F.col("total_amount") > 0
    ),
    create_rule(
        "valid_status", "Order status must be in allowed values", "critical",
        F.col("status").isin(["pending", "confirmed", "shipped", "delivered", "cancelled"])
    ),
    create_rule(
        "ship_after_order", "Ship date must be after order date", "critical",
        (F.col("ship_date").isNull()) | (F.col("ship_date") >= F.col("order_date"))
    ),
    create_rule(
        "discount_limit", "Discount cannot exceed 50%", "warning",
        (F.col("discount_pct").isNull()) | (F.col("discount_pct") <= 50)
    ),
]

df = spark.read.table("production.orders")
results = [rule(df) for rule in rules]
for r in results:
    symbol = "PASS" if r["status"] == "PASS" else "FAIL"
    print(f"  [{r['severity']}] {r['rule']}: {symbol} ({r['violations']} violations)")`,
  },

  {
    id: 42,
    category: 'Business Rules',
    title: 'Conditional Logic Validation',
    desc: 'Validate complex conditional business rules',
    code: `from pyspark.sql import functions as F

def validate_conditional_rules(df, rules):
    """Validate rules where condition depends on other column values."""
    results = []
    for rule in rules:
        name = rule["name"]
        condition = rule["when"]
        then_check = rule["then"]

        applicable = df.filter(condition)
        applicable_count = applicable.count()
        violations = applicable.filter(~then_check)
        violation_count = violations.count()

        results.append({
            "rule": name,
            "applicable_rows": applicable_count,
            "violations": violation_count,
            "compliance_pct": round((applicable_count - violation_count) / applicable_count * 100, 2)
                if applicable_count > 0 else 100,
            "status": "PASS" if violation_count == 0 else "FAIL"
        })

    return results

df = spark.read.table("production.orders")
conditional_rules = [
    {
        "name": "shipped_must_have_tracking",
        "when": F.col("status") == "shipped",
        "then": F.col("tracking_number").isNotNull() & (F.col("tracking_number") != "")
    },
    {
        "name": "cancelled_no_ship_date",
        "when": F.col("status") == "cancelled",
        "then": F.col("ship_date").isNull()
    },
    {
        "name": "delivered_has_delivery_date",
        "when": F.col("status") == "delivered",
        "then": F.col("delivery_date").isNotNull()
    },
    {
        "name": "free_shipping_over_100",
        "when": F.col("total_amount") > 100,
        "then": (F.col("shipping_cost") == 0) | F.col("shipping_cost").isNull()
    },
    {
        "name": "bulk_discount_applied",
        "when": F.col("quantity") >= 10,
        "then": F.col("discount_pct") >= 5
    },
]
results = validate_conditional_rules(df, conditional_rules)
for r in results:
    print(f"  {r['rule']}: {r['status']} (compliance={r['compliance_pct']}%)")`,
  },

  {
    id: 43,
    category: 'Business Rules',
    title: 'Aggregate Rule Validation',
    desc: 'Validate aggregate-level business rules',
    code: `from pyspark.sql import functions as F

def validate_aggregate_rules(df, rules):
    """Validate rules that apply to aggregated data."""
    results = []
    for rule in rules:
        name = rule["name"]
        group_by = rule.get("group_by", [])
        agg_expr = rule["aggregate"]
        check = rule["check"]

        if group_by:
            agg_df = df.groupBy(group_by).agg(agg_expr.alias("_agg_value"))
        else:
            agg_df = df.agg(agg_expr.alias("_agg_value"))

        violations = agg_df.filter(~check(F.col("_agg_value")))
        violation_count = violations.count()
        total_groups = agg_df.count()

        results.append({
            "rule": name,
            "total_groups": total_groups,
            "violations": violation_count,
            "status": "PASS" if violation_count == 0 else "FAIL",
            "sample_violations": violations.limit(5).collect()
        })

    return results

df = spark.read.table("production.orders")
agg_rules = [
    {
        "name": "daily_revenue_minimum",
        "group_by": [F.col("order_date").cast("date").alias("date")],
        "aggregate": F.sum("total_amount"),
        "check": lambda col: col >= 1000  # Min $1000 daily revenue
    },
    {
        "name": "customer_order_limit",
        "group_by": ["customer_id"],
        "aggregate": F.count("*"),
        "check": lambda col: col <= 1000  # Max 1000 orders per customer
    },
    {
        "name": "avg_order_value_range",
        "group_by": ["product_category"],
        "aggregate": F.avg("total_amount"),
        "check": lambda col: (col >= 10) & (col <= 10000)
    },
]
results = validate_aggregate_rules(df, agg_rules)
for r in results:
    print(f"  {r['rule']}: {r['status']} ({r['violations']}/{r['total_groups']} groups failed)")`,
  },

  {
    id: 44,
    category: 'Business Rules',
    title: 'Threshold Alert Validation',
    desc: 'Monitor metrics against configurable thresholds',
    code: `from pyspark.sql import functions as F
from datetime import datetime

def check_metric_thresholds(df, metrics_config):
    """Check metric values against warning and critical thresholds."""
    results = []
    for metric in metrics_config:
        name = metric["name"]
        value = df.agg(metric["expression"]).collect()[0][0]

        status = "OK"
        if metric.get("critical_max") and value > metric["critical_max"]:
            status = "CRITICAL"
        elif metric.get("critical_min") and value < metric["critical_min"]:
            status = "CRITICAL"
        elif metric.get("warning_max") and value > metric["warning_max"]:
            status = "WARNING"
        elif metric.get("warning_min") and value < metric["warning_min"]:
            status = "WARNING"

        results.append({
            "metric": name,
            "value": round(value, 4) if value else None,
            "status": status,
            "thresholds": {
                "warning_min": metric.get("warning_min"),
                "warning_max": metric.get("warning_max"),
                "critical_min": metric.get("critical_min"),
                "critical_max": metric.get("critical_max"),
            },
            "timestamp": str(datetime.now())
        })

    overall = "CRITICAL" if any(r["status"] == "CRITICAL" for r in results) else \
              "WARNING" if any(r["status"] == "WARNING" for r in results) else "OK"

    return {"overall_status": overall, "metrics": results}

df = spark.read.table("production.transactions")
metrics = [
    {"name": "null_rate", "expression": F.sum(F.when(F.col("amount").isNull(), 1).otherwise(0)) / F.count("*") * 100,
     "warning_max": 1.0, "critical_max": 5.0},
    {"name": "avg_amount", "expression": F.avg("amount"),
     "warning_min": 50, "warning_max": 500, "critical_min": 10, "critical_max": 1000},
    {"name": "daily_volume", "expression": F.count("*"),
     "warning_min": 1000, "critical_min": 100},
]
result = check_metric_thresholds(df, metrics)
print(f"Overall: {result['overall_status']}")
for m in result["metrics"]:
    print(f"  {m['metric']}: {m['value']} [{m['status']}]")`,
  },

  {
    id: 45,
    category: 'Business Rules',
    title: 'Derived Field Validation',
    desc: 'Validate calculated and derived fields match expectations',
    code: `from pyspark.sql import functions as F

def validate_derived_fields(df, derivation_rules):
    """Validate that derived/calculated fields match their formulas."""
    results = []
    for rule in derivation_rules:
        name = rule["name"]
        target_col = rule["target_column"]
        expected_expr = rule["expected_expression"]
        tolerance = rule.get("tolerance", 0.001)

        validated = df.withColumn("_expected", expected_expr)

        if rule.get("exact_match", False):
            mismatches = validated.filter(
                F.col(target_col) != F.col("_expected")
            )
        else:
            mismatches = validated.filter(
                F.abs(F.col(target_col) - F.col("_expected")) > tolerance
            )

        mismatch_count = mismatches.count()
        total = df.filter(F.col(target_col).isNotNull()).count()

        results.append({
            "rule": name,
            "target": target_col,
            "mismatches": mismatch_count,
            "total": total,
            "accuracy_pct": round((total - mismatch_count) / total * 100, 2) if total > 0 else 100,
            "status": "PASS" if mismatch_count == 0 else "FAIL",
            "sample_mismatches": mismatches.select(
                target_col, "_expected", *[c for c in rule.get("source_columns", [])]
            ).limit(5).collect() if mismatch_count > 0 else []
        })

    return results

df = spark.read.table("production.order_lines")
derivations = [
    {
        "name": "line_total_calc",
        "target_column": "line_total",
        "expected_expression": F.col("unit_price") * F.col("quantity") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0)) / 100),
        "source_columns": ["unit_price", "quantity", "discount_pct"],
        "tolerance": 0.01
    },
    {
        "name": "tax_calculation",
        "target_column": "tax_amount",
        "expected_expression": F.col("subtotal") * F.col("tax_rate"),
        "source_columns": ["subtotal", "tax_rate"],
        "tolerance": 0.01
    },
    {
        "name": "full_name_concat",
        "target_column": "full_name",
        "expected_expression": F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
        "source_columns": ["first_name", "last_name"],
        "exact_match": True
    },
]
results = validate_derived_fields(df, derivations)
for r in results:
    print(f"  {r['rule']}: {r['status']} (accuracy={r['accuracy_pct']}%)")`,
  },

  // ===== Performance (46-50) =====
  {
    id: 46,
    category: 'Performance',
    title: 'Query Timing Analysis',
    desc: 'Measure and benchmark query execution times',
    code: `import time
from pyspark.sql import functions as F

def time_query(name, query_func, iterations=3):
    """Time a query function and return statistics."""
    times = []
    result = None
    for i in range(iterations):
        start = time.time()
        result = query_func()
        if hasattr(result, 'count'):
            result.count()  # Force materialization
        elapsed = time.time() - start
        times.append(elapsed)

    return {
        "query": name,
        "iterations": iterations,
        "avg_seconds": round(sum(times) / len(times), 3),
        "min_seconds": round(min(times), 3),
        "max_seconds": round(max(times), 3),
        "std_seconds": round((sum((t - sum(times)/len(times))**2 for t in times) / len(times))**0.5, 3),
        "status": "PASS" if sum(times) / len(times) < 30 else "SLOW"
    }

df = spark.read.table("production.transactions")

benchmarks = [
    time_query("full_scan", lambda: df.count()),
    time_query("filtered_scan", lambda: df.filter(F.col("status") == "completed")),
    time_query("aggregation", lambda: df.groupBy("category").agg(
        F.sum("amount"), F.count("*"), F.avg("amount")
    )),
    time_query("join_query", lambda: df.alias("a").join(
        spark.read.table("production.customers").alias("b"),
        F.col("a.customer_id") == F.col("b.customer_id")
    )),
    time_query("window_function", lambda: df.withColumn(
        "running_total", F.sum("amount").over(
            Window.partitionBy("customer_id").orderBy("created_at")
        )
    )),
]
for b in benchmarks:
    print(f"  {b['query']}: avg={b['avg_seconds']}s [{b['status']}]")`,
  },

  {
    id: 47,
    category: 'Performance',
    title: 'Partition Optimization Check',
    desc: 'Analyze and validate table partitioning strategy',
    code: `from pyspark.sql import functions as F

def analyze_partitioning(table_path):
    """Analyze Delta table partitioning efficiency."""
    df = spark.read.format("delta").load(table_path)
    detail = spark.sql(f"DESCRIBE DETAIL delta.\\\`{table_path}\\\`").collect()[0]

    partition_columns = detail["partitionColumns"]
    num_files = detail["numFiles"]
    size_bytes = detail["sizeInBytes"]

    results = {
        "table": table_path,
        "partition_columns": partition_columns,
        "total_files": num_files,
        "total_size_gb": round(size_bytes / (1024**3), 2),
        "avg_file_size_mb": round(size_bytes / num_files / (1024**2), 2) if num_files > 0 else 0
    }

    # Check partition skew
    if partition_columns:
        for pcol in partition_columns:
            partition_stats = df.groupBy(pcol).agg(
                F.count("*").alias("row_count")
            )
            stats = partition_stats.agg(
                F.min("row_count").alias("min_rows"),
                F.max("row_count").alias("max_rows"),
                F.avg("row_count").alias("avg_rows"),
                F.stddev("row_count").alias("std_rows"),
                F.count("*").alias("num_partitions")
            ).collect()[0]

            skew_ratio = stats["max_rows"] / stats["min_rows"] if stats["min_rows"] > 0 else float("inf")
            results[f"{pcol}_partitions"] = stats["num_partitions"]
            results[f"{pcol}_skew_ratio"] = round(skew_ratio, 2)

    # File size distribution check
    optimal_min_mb = 32
    optimal_max_mb = 256
    avg_mb = results["avg_file_size_mb"]
    results["file_size_status"] = "OPTIMAL" if optimal_min_mb <= avg_mb <= optimal_max_mb else "SUBOPTIMAL"
    results["recommendation"] = "OPTIMIZE" if avg_mb < optimal_min_mb else "REPARTITION" if avg_mb > optimal_max_mb else "OK"

    return results

result = analyze_partitioning("/mnt/delta/production/transactions")
print(f"Partition analysis: {result['file_size_status']}")
print(f"  Files: {result['total_files']}, Avg size: {result['avg_file_size_mb']}MB")
print(f"  Recommendation: {result['recommendation']}")`,
  },

  {
    id: 48,
    category: 'Performance',
    title: 'File Size Analysis',
    desc: 'Analyze file sizes for optimal storage and query performance',
    code: `from pyspark.sql import functions as F

def analyze_file_sizes(table_path):
    """Analyze file size distribution in Delta table."""
    files = spark.sql(f"""
        DESCRIBE DETAIL delta.\\\`{table_path}\\\`
    """).collect()[0]

    # Get file-level details using Delta log
    file_stats = spark.sql(f"""
        SELECT
            size,
            modificationTime,
            partitionValues
        FROM delta.\\\`{table_path}\\\`._delta_log
        WHERE add IS NOT NULL
    """)

    if file_stats.count() == 0:
        # Alternative: use input_file_name
        df = spark.read.format("delta").load(table_path)
        file_stats = df.select(
            F.input_file_name().alias("file_path")
        ).distinct()

    total_files = files["numFiles"]
    total_size = files["sizeInBytes"]
    avg_size_mb = total_size / total_files / (1024**2) if total_files > 0 else 0

    small_files = 0  # < 1MB
    large_files = 0  # > 1GB
    optimal_files = 0  # 32MB - 256MB

    recommendations = []
    if avg_size_mb < 32:
        recommendations.append("Run OPTIMIZE to compact small files")
    if avg_size_mb > 256:
        recommendations.append("Consider repartitioning to create smaller files")
    if total_files > 10000:
        recommendations.append("Consider OPTIMIZE ZORDER for improved query performance")

    return {
        "table": table_path,
        "total_files": total_files,
        "total_size_gb": round(total_size / (1024**3), 2),
        "avg_file_size_mb": round(avg_size_mb, 2),
        "small_file_warning": avg_size_mb < 32,
        "large_file_warning": avg_size_mb > 256,
        "recommendations": recommendations
    }

result = analyze_file_sizes("/mnt/delta/production/events")
print(f"File analysis: {result['total_files']} files, avg={result['avg_file_size_mb']}MB")
for rec in result["recommendations"]:
    print(f"  Recommendation: {rec}")`,
  },

  {
    id: 49,
    category: 'Performance',
    title: 'Compaction Analysis',
    desc: 'Analyze and trigger Delta table compaction when needed',
    code: `from pyspark.sql import functions as F
from datetime import datetime

def analyze_compaction_need(table_path, small_file_threshold_mb=32, target_file_size_mb=128):
    """Analyze if Delta table needs compaction."""
    detail = spark.sql(f"DESCRIBE DETAIL delta.\\\`{table_path}\\\`").collect()[0]
    history = spark.sql(f"DESCRIBE HISTORY delta.\\\`{table_path}\\\`").orderBy(F.col("version").desc())

    num_files = detail["numFiles"]
    size_bytes = detail["sizeInBytes"]
    avg_file_mb = size_bytes / num_files / (1024**2) if num_files > 0 else 0

    # Check last OPTIMIZE
    last_optimize = history.filter(
        F.col("operation") == "OPTIMIZE"
    ).limit(1).collect()

    last_optimize_time = last_optimize[0]["timestamp"] if last_optimize else None
    hours_since_optimize = None
    if last_optimize_time:
        hours_since_optimize = (datetime.now() - last_optimize_time).total_seconds() / 3600

    needs_compaction = avg_file_mb < small_file_threshold_mb or num_files > 1000

    result = {
        "table": table_path,
        "num_files": num_files,
        "avg_file_size_mb": round(avg_file_mb, 2),
        "total_size_gb": round(size_bytes / (1024**3), 2),
        "last_optimize": str(last_optimize_time) if last_optimize_time else "Never",
        "hours_since_optimize": round(hours_since_optimize, 1) if hours_since_optimize else None,
        "needs_compaction": needs_compaction,
        "status": "NEEDS_COMPACTION" if needs_compaction else "OK"
    }

    if needs_compaction:
        result["compaction_command"] = f"""
spark.sql("OPTIMIZE delta.\\\`{table_path}\\\`")
-- Or with Z-ORDER:
spark.sql("OPTIMIZE delta.\\\`{table_path}\\\` ZORDER BY (date, customer_id)")
"""
    return result

result = analyze_compaction_need("/mnt/delta/production/events")
print(f"Compaction status: {result['status']}")
print(f"  Files: {result['num_files']}, Avg: {result['avg_file_size_mb']}MB")
if result["needs_compaction"]:
    print(f"  Run: {result['compaction_command']}")`,
  },

  {
    id: 50,
    category: 'Performance',
    title: 'Cache Hit Ratio Analysis',
    desc: 'Monitor Spark cache utilization and hit ratios',
    code: `from pyspark.sql import functions as F
import json

def analyze_cache_performance(spark):
    """Analyze Spark cache performance metrics."""
    # Get storage info from Spark UI API
    sc = spark.sparkContext
    storage_info = sc._jsc.sc().getRDDStorageInfo()

    cached_rdds = []
    for rdd_info in storage_info:
        cached_rdds.append({
            "name": rdd_info.name(),
            "num_partitions": rdd_info.numPartitions(),
            "num_cached_partitions": rdd_info.numCachedPartitions(),
            "mem_size_mb": round(rdd_info.memSize() / (1024**2), 2),
            "disk_size_mb": round(rdd_info.diskSize() / (1024**2), 2),
            "storage_level": str(rdd_info.storageLevel())
        })

    total_mem = sum(r["mem_size_mb"] for r in cached_rdds)
    total_disk = sum(r["disk_size_mb"] for r in cached_rdds)

    # Executor memory metrics
    executor_mem = sc._jsc.sc().getExecutorMemoryStatus()
    executor_stats = []
    for executor_id, mem_info in executor_mem.items():
        max_mem = mem_info._1() / (1024**3)
        remaining_mem = mem_info._2() / (1024**3)
        executor_stats.append({
            "executor": executor_id,
            "max_mem_gb": round(max_mem, 2),
            "remaining_gb": round(remaining_mem, 2),
            "used_pct": round((1 - remaining_mem / max_mem) * 100, 1) if max_mem > 0 else 0
        })

    return {
        "cached_datasets": len(cached_rdds),
        "total_cached_mem_mb": round(total_mem, 2),
        "total_cached_disk_mb": round(total_disk, 2),
        "executor_stats": executor_stats,
        "cached_details": cached_rdds,
        "recommendations": [
            "Consider unpersisting unused cached DataFrames" if total_mem > 1024 else None,
            "High memory pressure detected" if any(e["used_pct"] > 80 for e in executor_stats) else None,
        ]
    }

# Usage
result = analyze_cache_performance(spark)
print(f"Cached datasets: {result['cached_datasets']}")
print(f"Total cached: {result['total_cached_mem_mb']}MB (memory) + {result['total_cached_disk_mb']}MB (disk)")
for e in result["executor_stats"]:
    print(f"  Executor {e['executor']}: {e['used_pct']}% memory used")`,
  },

  // ===== Great Expectations (51-55) =====
  {
    id: 51,
    category: 'Great Expectations',
    title: 'Expectation Suite Setup',
    desc: 'Create and configure Great Expectations suites in Databricks',
    code: `# Install: %pip install great-expectations
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# Initialize GX context
context = gx.get_context()

# Add Spark datasource
datasource_config = {
    "name": "spark_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",
        "force_reuse_spark_context": True
    },
    "data_connectors": {
        "runtime_connector": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["batch_id"]
        }
    }
}
context.add_datasource(**datasource_config)

# Create expectation suite
suite_name = "production_orders_suite"
suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# Build expectations using a validator
df = spark.read.table("production.orders")
batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="runtime_connector",
    data_asset_name="orders",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"batch_id": "validation_run_001"}
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

# Add expectations
validator.expect_column_to_exist("order_id")
validator.expect_column_to_exist("customer_id")
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_not_be_null("total_amount")
validator.expect_column_values_to_be_between("total_amount", min_value=0, max_value=1000000)
validator.expect_column_values_to_be_in_set("status", ["pending", "confirmed", "shipped", "delivered", "cancelled"])
validator.expect_table_row_count_to_be_between(min_value=1000)

validator.save_expectation_suite(discard_failed_expectations=False)
print(f"Suite '{suite_name}' created with {len(suite.expectations)} expectations")`,
  },

  {
    id: 52,
    category: 'Great Expectations',
    title: 'Checkpoint Execution',
    desc: 'Run Great Expectations checkpoints for automated validation',
    code: `import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import Checkpoint

context = gx.get_context()

# Define checkpoint configuration
checkpoint_config = {
    "name": "production_orders_checkpoint",
    "config_version": 1.0,
    "class_name": "Checkpoint",
    "run_name_template": "orders_validation_%Y%m%d_%H%M%S",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "spark_datasource",
                "data_connector_name": "runtime_connector",
                "data_asset_name": "orders",
            },
            "expectation_suite_name": "production_orders_suite",
        }
    ],
    "action_list": [
        {"name": "store_validation_result", "action": {"class_name": "StoreValidationResultAction"}},
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
}

checkpoint = Checkpoint(**checkpoint_config, data_context=context)
context.add_or_update_checkpoint(checkpoint=checkpoint)

# Run checkpoint with runtime data
df = spark.read.table("production.orders")
result = checkpoint.run(
    validations=[{
        "batch_request": RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_connector",
            data_asset_name="orders",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": "daily_check"}
        ),
        "expectation_suite_name": "production_orders_suite",
    }]
)

# Check results
success = result.success
stats = result.to_json_dict()["run_results"]
print(f"Checkpoint result: {'PASS' if success else 'FAIL'}")
for run_id, run_result in stats.items():
    validation = run_result["validation_result"]
    print(f"  Evaluated: {validation['statistics']['evaluated_expectations']}")
    print(f"  Successful: {validation['statistics']['successful_expectations']}")
    print(f"  Failed: {validation['statistics']['unsuccessful_expectations']}")`,
  },

  {
    id: 53,
    category: 'Great Expectations',
    title: 'Data Docs Generation',
    desc: 'Generate and serve Great Expectations data documentation',
    code: `import great_expectations as gx

context = gx.get_context()

# Configure data docs site for DBFS
data_docs_config = {
    "class_name": "SiteBuilder",
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    "store_backend": {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": "/dbfs/great_expectations/data_docs/",
    },
}

context.add_data_docs_site(site_name="dbfs_site", site_config=data_docs_config)

# Build data docs
context.build_data_docs(site_names=["dbfs_site"])
print("Data docs generated at /dbfs/great_expectations/data_docs/")

# Generate profiling report
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.core.batch import RuntimeBatchRequest

df = spark.read.table("production.customers")
batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="runtime_connector",
    data_asset_name="customers",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"batch_id": "profiling"}
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="customers_profile"
)

# Auto-generate expectations from data profiling
profiler = BasicDatasetProfiler()
suite, evr = profiler.profile(validator)
print(f"Auto-generated {len(suite.expectations)} expectations from profiling")

# Save and rebuild docs
validator.save_expectation_suite()
context.build_data_docs()
print("Updated data docs with profiling results")`,
  },

  {
    id: 54,
    category: 'Great Expectations',
    title: 'Custom Expectations',
    desc: 'Build custom Great Expectations for domain-specific validation',
    code: `from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ColumnAggregateExpectation,
)
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from pyspark.sql import functions as F

# Custom column map expectation: valid email domain
class ExpectColumnValuesToHaveValidEmailDomain(ColumnMapExpectation):
    """Expect email addresses to have valid company domains."""
    map_metric = "column_values.valid_email_domain"
    success_keys = ("allowed_domains",)
    default_kwarg_values = {
        "allowed_domains": ["company.com", "subsidiary.com"],
        "result_format": "BASIC",
    }

class ColumnValuesValidEmailDomain(ColumnMapMetricProvider):
    condition_metric_name = "column_values.valid_email_domain"
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, allowed_domains, **kwargs):
        domain_pattern = "|".join(allowed_domains)
        return column.rlike(f"@({domain_pattern})$")

# Custom aggregate expectation: freshness check
class ExpectTableToHaveRecentData(ColumnAggregateExpectation):
    """Expect table to have data within specified hours."""
    metric_dependencies = ("column.max",)
    success_keys = ("max_age_hours",)

    def _validate(self, metrics, runtime_configuration=None, execution_engine=None):
        from datetime import datetime, timedelta
        max_timestamp = metrics["column.max"]
        max_age = self._get_success_kwargs().get("max_age_hours", 24)
        threshold = datetime.now() - timedelta(hours=max_age)
        return {"success": max_timestamp >= threshold, "result": {"observed_value": str(max_timestamp)}}

# Usage in validator
validator.expect_column_values_to_have_valid_email_domain(
    column="email",
    allowed_domains=["acme.com", "acme.io"]
)
validator.expect_table_to_have_recent_data(
    column="created_at",
    max_age_hours=4
)
print("Custom expectations applied and validated")`,
  },

  {
    id: 55,
    category: 'Great Expectations',
    title: 'Validation Results Pipeline',
    desc: 'Build a complete validation results pipeline with alerting',
    code: `import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType

def run_validation_pipeline(table_configs):
    """Run full validation pipeline across multiple tables."""
    context = gx.get_context()
    all_results = []

    for config in table_configs:
        table_name = config["table"]
        suite_name = config["suite"]
        df = spark.read.table(table_name)

        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="runtime_connector",
            data_asset_name=table_name.split(".")[-1],
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
        )

        # Run validation
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        result = validator.validate()

        stats = result.statistics
        all_results.append({
            "table": table_name,
            "suite": suite_name,
            "success": result.success,
            "evaluated": stats["evaluated_expectations"],
            "passed": stats["successful_expectations"],
            "failed": stats["unsuccessful_expectations"],
            "success_pct": stats["success_percent"],
            "run_time": str(datetime.now()),
            "failed_expectations": [
                {
                    "expectation": r.expectation_config.expectation_type,
                    "column": r.expectation_config.kwargs.get("column"),
                    "details": r.result
                }
                for r in result.results if not r.success
            ]
        })

    # Store results as Delta table
    schema = StructType([
        StructField("table", StringType()),
        StructField("suite", StringType()),
        StructField("success", BooleanType()),
        StructField("evaluated", IntegerType()),
        StructField("passed", IntegerType()),
        StructField("failed", IntegerType()),
        StructField("success_pct", IntegerType()),
        StructField("run_time", StringType()),
    ])

    results_df = spark.createDataFrame(
        [(r["table"], r["suite"], r["success"], r["evaluated"], r["passed"],
          r["failed"], int(r["success_pct"]), r["run_time"]) for r in all_results],
        schema=schema
    )
    results_df.write.format("delta").mode("append").saveAsTable("monitoring.validation_results")

    # Alert on failures
    failures = [r for r in all_results if not r["success"]]
    if failures:
        alert_msg = f"VALIDATION FAILURES at {datetime.now()}:\\n"
        for f in failures:
            alert_msg += f"  Table: {f['table']} - {f['failed']}/{f['evaluated']} checks failed\\n"
            for exp in f["failed_expectations"][:3]:
                alert_msg += f"    - {exp['expectation']} on {exp['column']}\\n"
        print(alert_msg)
        # Optionally send to Slack, email, PagerDuty, etc.

    return all_results

# Run pipeline
configs = [
    {"table": "production.orders", "suite": "production_orders_suite"},
    {"table": "production.customers", "suite": "production_customers_suite"},
    {"table": "production.transactions", "suite": "production_transactions_suite"},
]
results = run_validation_pipeline(configs)
overall = all(r["success"] for r in results)
print(f"\\nPipeline result: {'ALL PASSED' if overall else 'FAILURES DETECTED'}")
for r in results:
    print(f"  {r['table']}: {'PASS' if r['success'] else 'FAIL'} ({r['success_pct']}%)")`,
  },
];

const categories = [...new Set(testingScenarios.map((s) => s.category))];

function DataTesting() {
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = testingScenarios.filter((s) => {
    const matchCategory = selectedCategory === 'All' || s.category === selectedCategory;
    const matchSearch =
      s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.desc.toLowerCase().includes(searchTerm.toLowerCase());
    return matchCategory && matchSearch;
  });

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Data Testing Scenarios</h1>
          <p>{testingScenarios.length} PySpark data testing patterns for Databricks</p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search scenarios..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '300px' }}
          />
          <select
            className="form-input"
            value={selectedCategory}
            onChange={(e) => setSelectedCategory(e.target.value)}
            style={{ maxWidth: '200px' }}
          >
            <option value="All">All Categories ({testingScenarios.length})</option>
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat} ({testingScenarios.filter((s) => s.category === cat).length})
              </option>
            ))}
          </select>
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Showing {filtered.length} of {testingScenarios.length}
          </span>
        </div>
      </div>

      <div className="scenarios-list">
        {filtered.map((scenario) => (
          <div key={scenario.id} className="card scenario-card" style={{ marginBottom: '0.75rem' }}>
            <div
              className="scenario-header"
              onClick={() => setExpandedId(expandedId === scenario.id ? null : scenario.id)}
              style={{
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    marginBottom: '0.25rem',
                  }}
                >
                  <span className="badge running">{scenario.category}</span>
                  <strong>
                    #{scenario.id} — {scenario.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                  {scenario.desc}
                </p>
              </div>
              <span style={{ fontSize: '1.2rem', color: 'var(--text-secondary)' }}>
                {expandedId === scenario.id ? '▼' : '▶'}
              </span>
            </div>
            {expandedId === scenario.id && <ScenarioCard scenario={scenario} />}
          </div>
        ))}
      </div>
    </div>
  );
}

export default DataTesting;
