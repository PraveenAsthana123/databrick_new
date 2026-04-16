import React, { useState } from 'react';
import ScenarioCard from '../../components/common/ScenarioCard';
import { exportToCSV } from '../../utils/fileExport';

const streamScenarios = [
  // ─── 1–10: Basic Streaming (Event → Bronze) ───
  {
    id: 1,
    group: 'Event to Bronze',
    title: 'Kafka Topic Ingestion',
    flow: 'Kafka \u2192 Structured Streaming \u2192 Bronze Delta',
    complexity: 2,
    throughput: 4,
    latency: 4,
    reliability: 4,
    governance: 2,
    score: 3.2,
    code: `# Kafka → Bronze Delta\ndf = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \\\n    .option("subscribe", "events") \\\n    .option("startingOffsets", "latest") \\\n    .option("maxOffsetsPerTrigger", 100000) \\\n    .load()\n\n# Parse key/value\nfrom pyspark.sql.functions import from_json, col, current_timestamp\nschema = "event_type STRING, user_id STRING, data STRING, ts LONG"\n\nparsed = df.select(\n    col("key").cast("string"),\n    from_json(col("value").cast("string"), schema).alias("event"),\n    col("topic"), col("partition"), col("offset"), col("timestamp")\n).select("event.*", "topic", "partition", "offset", "timestamp") \\\n .withColumn("_ingest_ts", current_timestamp())\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/kafka_bronze") \\\n    .outputMode("append") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.bronze.kafka_events")`,
  },
  {
    id: 2,
    group: 'Event to Bronze',
    title: 'Event Hub Ingestion',
    flow: 'Azure Event Hub \u2192 Databricks \u2192 Bronze',
    complexity: 2,
    throughput: 4,
    latency: 4,
    reliability: 4,
    governance: 2,
    score: 3.2,
    code: `# Azure Event Hub → Bronze\nconn = dbutils.secrets.get("eh", "connection_string")\n\ndf = spark.readStream.format("eventhubs") \\\n    .option("eventhubs.connectionString", sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn)) \\\n    .option("eventhubs.consumerGroup", "$Default") \\\n    .option("eventhubs.startingPosition", '{"offset":"-1","isInclusive":true}') \\\n    .load()\n\nparsed = df.select(\n    col("body").cast("string").alias("raw_event"),\n    col("enqueuedTime").alias("event_time"),\n    col("offset"), col("sequenceNumber"), col("partitionId")\n).withColumn("_ingest_ts", current_timestamp())\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/eh_bronze") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.bronze.eventhub_events")`,
  },
  {
    id: 3,
    group: 'Event to Bronze',
    title: 'IoT Device Stream',
    flow: 'Devices \u2192 MQTT/Kafka \u2192 Bronze',
    complexity: 3,
    throughput: 5,
    latency: 5,
    reliability: 4,
    governance: 3,
    score: 4.0,
    code: `# IoT streaming via Kafka\ndf = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "iot-broker:9092") \\\n    .option("subscribe", "iot.sensors") \\\n    .load()\n\nfrom pyspark.sql.functions import from_json, col\niot_schema = "device_id STRING, sensor_type STRING, value DOUBLE, unit STRING, ts LONG"\n\nparsed = df.select(from_json(col("value").cast("string"), iot_schema).alias("d")) \\\n    .select("d.*") \\\n    .withColumn("event_time", (col("ts")/1000).cast("timestamp")) \\\n    .withColumn("_ingest_ts", current_timestamp())\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/iot_bronze") \\\n    .partitionBy("sensor_type") \\\n    .trigger(processingTime="5 seconds") \\\n    .toTable("catalog.bronze.iot_readings")`,
  },
  {
    id: 4,
    group: 'Event to Bronze',
    title: 'Log Streaming Ingestion',
    flow: 'App Logs \u2192 Kafka \u2192 Bronze',
    complexity: 2,
    throughput: 5,
    latency: 4,
    reliability: 4,
    governance: 2,
    score: 3.4,
    code: `df = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker:9092") \\\n    .option("subscribe", "app.logs") \\\n    .load()\n\nfrom pyspark.sql.functions import from_json, col\nlog_schema = "level STRING, service STRING, message STRING, timestamp STRING, trace_id STRING"\n\nparsed = df.select(from_json(col("value").cast("string"), log_schema).alias("log")) \\\n    .select("log.*").withColumn("_ingest_ts", current_timestamp())\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/logs_bronze") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.bronze.app_logs")`,
  },
  {
    id: 5,
    group: 'Event to Bronze',
    title: 'Clickstream Ingestion',
    flow: 'Web/App \u2192 Kafka \u2192 Bronze',
    complexity: 3,
    throughput: 5,
    latency: 5,
    reliability: 4,
    governance: 2,
    score: 3.8,
    code: `df = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker:9092") \\\n    .option("subscribe", "clickstream") \\\n    .load()\n\nclick_schema = "user_id STRING, session_id STRING, page STRING, action STRING, ts LONG, device STRING"\nparsed = df.select(from_json(col("value").cast("string"), click_schema).alias("c")).select("c.*") \\\n    .withColumn("event_time", (col("ts")/1000).cast("timestamp"))\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/clicks_bronze") \\\n    .trigger(processingTime="5 seconds") \\\n    .toTable("catalog.bronze.clickstream")`,
  },
  {
    id: 6,
    group: 'Event to Bronze',
    title: 'Auto Loader File Streaming',
    flow: 'Storage \u2192 Incremental Files \u2192 Bronze',
    complexity: 2,
    throughput: 4,
    latency: 3,
    reliability: 4,
    governance: 2,
    score: 3.0,
    code: `df = spark.readStream.format("cloudFiles") \\\n    .option("cloudFiles.format", "json") \\\n    .option("cloudFiles.schemaLocation", "/mnt/schema/autoloader") \\\n    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \\\n    .load("s3://landing/events/")\n\ndf.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/autoloader_bronze") \\\n    .option("mergeSchema", "true") \\\n    .trigger(availableNow=True) \\\n    .toTable("catalog.bronze.file_events")`,
  },
  {
    id: 7,
    group: 'Event to Bronze',
    title: 'CDC Streaming Ingestion',
    flow: 'DB Logs \u2192 Kafka \u2192 Bronze',
    complexity: 4,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `# Debezium CDC → Kafka → Bronze\ndf = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker:9092") \\\n    .option("subscribe", "dbserver1.dbo.customers") \\\n    .load()\n\ncdc_schema = "before STRUCT<id:LONG,name:STRING,email:STRING>, after STRUCT<id:LONG,name:STRING,email:STRING>, op STRING, ts_ms LONG"\nparsed = df.select(from_json(col("value").cast("string"), cdc_schema).alias("cdc")).select("cdc.*") \\\n    .withColumn("event_time", (col("ts_ms")/1000).cast("timestamp")) \\\n    .withColumn("operation", when(col("op")=="c","INSERT").when(col("op")=="u","UPDATE").when(col("op")=="d","DELETE").otherwise("UNKNOWN"))\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/cdc_bronze") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.bronze.cdc_events")`,
  },
  {
    id: 8,
    group: 'Event to Bronze',
    title: 'API Event Ingestion',
    flow: 'API Gateway \u2192 Stream \u2192 Bronze',
    complexity: 3,
    throughput: 3,
    latency: 4,
    reliability: 4,
    governance: 3,
    score: 3.4,
    code: `# API Gateway events via Kafka\ndf = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker:9092") \\\n    .option("subscribe", "api.events") \\\n    .load()\n\napi_schema = "method STRING, path STRING, status INT, latency_ms INT, user_agent STRING, ip STRING, ts LONG"\nparsed = df.select(from_json(col("value").cast("string"), api_schema).alias("api")).select("api.*") \\\n    .withColumn("event_time", (col("ts")/1000).cast("timestamp"))\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/api_bronze") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.bronze.api_events")`,
  },
  {
    id: 9,
    group: 'Event to Bronze',
    title: 'Sensor Streaming Ingestion',
    flow: 'Edge Sensors \u2192 Stream \u2192 Bronze',
    complexity: 3,
    throughput: 5,
    latency: 5,
    reliability: 4,
    governance: 3,
    score: 4.0,
    code: `df = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "sensor-broker:9092") \\\n    .option("subscribe", "sensors.readings") \\\n    .load()\n\nsensor_schema = "sensor_id STRING, location STRING, temperature DOUBLE, humidity DOUBLE, pressure DOUBLE, ts LONG"\nparsed = df.select(from_json(col("value").cast("string"), sensor_schema).alias("s")).select("s.*") \\\n    .withColumn("reading_time", (col("ts")/1000).cast("timestamp")) \\\n    .withWatermark("reading_time", "1 minute")\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/sensors_bronze") \\\n    .trigger(processingTime="5 seconds") \\\n    .toTable("catalog.bronze.sensor_readings")`,
  },
  {
    id: 10,
    group: 'Event to Bronze',
    title: 'Payment Event Ingestion',
    flow: 'Payment System \u2192 Kafka \u2192 Bronze',
    complexity: 4,
    throughput: 4,
    latency: 5,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `# Payment events — high reliability + governance\ndf = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "payments-broker:9092") \\\n    .option("subscribe", "payments.transactions") \\\n    .option("kafka.group.id", "payments-ingestion") \\\n    .option("failOnDataLoss", "true") \\\n    .load()\n\npay_schema = "txn_id STRING, account_id STRING, amount DOUBLE, currency STRING, merchant STRING, status STRING, ts LONG"\nparsed = df.select(from_json(col("value").cast("string"), pay_schema).alias("p")).select("p.*") \\\n    .withColumn("account_hash", sha2(concat("account_id", lit("SALT")), 256)) \\\n    .drop("account_id") \\\n    .withColumn("event_time", (col("ts")/1000).cast("timestamp"))\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/payments_bronze") \\\n    .trigger(processingTime="1 second") \\\n    .toTable("catalog.secure.payment_events")`,
  },

  // ─── 11–20: Stream Processing (Bronze → Silver) ───
  {
    id: 11,
    group: 'Bronze to Silver Stream',
    title: 'Stream Cleansing Pipeline',
    flow: 'Bronze Stream \u2192 Clean \u2192 Silver',
    complexity: 3,
    throughput: 5,
    latency: 4,
    reliability: 5,
    governance: 3,
    score: 4.0,
    code: `df = spark.readStream.format("delta").table("catalog.bronze.kafka_events")\n\ncleaned = df.filter(col("event_type").isNotNull()) \\\n    .withColumn("user_id", trim(lower(col("user_id")))) \\\n    .withColumn("event_time", col("ts").cast("timestamp")) \\\n    .drop("_ingest_ts")\n\ncleaned.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/clean_silver") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.silver.clean_events")`,
  },
  {
    id: 12,
    group: 'Bronze to Silver Stream',
    title: 'Deduplication Stream',
    flow: 'Stream \u2192 Remove Duplicates \u2192 Silver',
    complexity: 4,
    throughput: 5,
    latency: 4,
    reliability: 5,
    governance: 3,
    score: 4.2,
    code: `df = spark.readStream.format("delta").table("catalog.bronze.events")\n\n# Watermark + dropDuplicates for exactly-once\ndeduped = df.withWatermark("event_time", "1 hour") \\\n    .dropDuplicatesWithinWatermark(["event_id"])\n\ndeduped.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/dedup_silver") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.silver.deduped_events")`,
  },
  {
    id: 13,
    group: 'Bronze to Silver Stream',
    title: 'Schema Enforcement Stream',
    flow: 'Stream \u2192 Enforce Schema \u2192 Silver',
    complexity: 3,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 3,
    score: 3.8,
    code: `from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType\n\nexpected_schema = StructType([\n    StructField("event_id", StringType(), False),\n    StructField("user_id", StringType(), False),\n    StructField("amount", DoubleType(), True),\n    StructField("event_time", TimestampType(), False)\n])\n\ndf = spark.readStream.format("delta").table("catalog.bronze.events")\nvalid = df.select([col(f.name).cast(f.dataType).alias(f.name) for f in expected_schema.fields]) \\\n    .filter(col("event_id").isNotNull() & col("user_id").isNotNull())\n\nvalid.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/schema_silver") \\\n    .toTable("catalog.silver.typed_events")`,
  },
  {
    id: 14,
    group: 'Bronze to Silver Stream',
    title: 'PII Masking Stream',
    flow: 'Stream \u2192 Mask Sensitive \u2192 Silver',
    complexity: 4,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 5,
    score: 4.4,
    code: `df = spark.readStream.format("delta").table("catalog.bronze.customer_events")\n\nmasked = df \\\n    .withColumn("email_hash", sha2("email", 256)) \\\n    .withColumn("phone_masked", concat(lit("***-***-"), col("phone").substr(-4,4))) \\\n    .withColumn("ip_masked", regexp_replace("ip_address", "(\\\\d+)\\\\.(\\\\d+)\\\\.","***.***.\")) \\\n    .drop("email", "phone", "ip_address")\n\nmasked.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/pii_silver") \\\n    .toTable("catalog.silver.masked_events")`,
  },
  {
    id: 15,
    group: 'Bronze to Silver Stream',
    title: 'Data Quality Validation Stream',
    flow: 'Stream \u2192 Validate \u2192 Quarantine \u2192 Silver',
    complexity: 4,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `df = spark.readStream.format("delta").table("catalog.bronze.orders")\n\n# Split into valid + quarantine\nvalid = df.filter((col("amount") > 0) & (col("customer_id").isNotNull()) & (col("order_date").isNotNull()))\ninvalid = df.filter((col("amount") <= 0) | col("customer_id").isNull() | col("order_date").isNull())\n\nvalid.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/valid_silver") \\\n    .toTable("catalog.silver.valid_orders")\n\ninvalid.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/quarantine") \\\n    .toTable("catalog.quarantine.bad_orders")`,
  },
  {
    id: 16,
    group: 'Bronze to Silver Stream',
    title: 'Event Enrichment Stream',
    flow: 'Stream + Lookup \u2192 Enriched',
    complexity: 4,
    throughput: 5,
    latency: 4,
    reliability: 5,
    governance: 3,
    score: 4.2,
    code: `# Stream-static join for enrichment\nevents = spark.readStream.format("delta").table("catalog.bronze.orders")\ncustomers = spark.table("catalog.silver.customer_dim")  # static lookup\n\nenriched = events.join(customers, "customer_id", "left") \\\n    .withColumn("customer_segment", coalesce("segment", lit("Unknown")))\n\nenriched.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/enriched_silver") \\\n    .trigger(processingTime="10 seconds") \\\n    .toTable("catalog.silver.enriched_orders")`,
  },
  {
    id: 17,
    group: 'Bronze to Silver Stream',
    title: 'Reference Join Stream',
    flow: 'Stream + Dimension Join',
    complexity: 4,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 3,
    score: 4.0,
    code: `events = spark.readStream.format("delta").table("catalog.bronze.transactions")\nproducts = spark.table("catalog.silver.product_dim")  # broadcast\ncategories = spark.table("catalog.reference.categories")\n\nresult = events \\\n    .join(broadcast(products), "product_id") \\\n    .join(broadcast(categories), "category_id") \\\n    .select("txn_id","product_name","category_name","amount","event_time")\n\nresult.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/ref_join") \\\n    .toTable("catalog.silver.enriched_transactions")`,
  },
  {
    id: 18,
    group: 'Bronze to Silver Stream',
    title: 'Multi-Stream Join Pipeline',
    flow: 'Stream A + Stream B \u2192 Unified',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 3,
    score: 4.6,
    code: `# Join two streams with watermark\norders = spark.readStream.format("delta").table("catalog.bronze.orders") \\\n    .withWatermark("order_time", "10 minutes")\n\npayments = spark.readStream.format("delta").table("catalog.bronze.payments") \\\n    .withWatermark("payment_time", "10 minutes")\n\njoined = orders.join(payments,\n    (orders.order_id == payments.order_id) &\n    (payments.payment_time >= orders.order_time) &\n    (payments.payment_time <= orders.order_time + expr("INTERVAL 30 MINUTES")),\n    "leftOuter"\n)\n\njoined.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/multi_join") \\\n    .toTable("catalog.silver.order_payments")`,
  },
  {
    id: 19,
    group: 'Bronze to Silver Stream',
    title: 'Window Aggregation Stream',
    flow: 'Stream \u2192 Windowed Metrics \u2192 Silver',
    complexity: 4,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 3,
    score: 4.4,
    code: `from pyspark.sql.functions import window, count, sum as spark_sum, avg\n\nevents = spark.readStream.format("delta").table("catalog.bronze.clickstream") \\\n    .withWatermark("event_time", "5 minutes")\n\nagg = events.groupBy(\n    window("event_time", "5 minutes", "1 minute"),\n    "page"\n).agg(\n    count("*").alias("page_views"),\n    countDistinct("user_id").alias("unique_users"),\n    avg("duration_sec").alias("avg_duration")\n)\n\nagg.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/window_agg") \\\n    .outputMode("append") \\\n    .trigger(processingTime="30 seconds") \\\n    .toTable("catalog.silver.page_metrics_5min")`,
  },
  {
    id: 20,
    group: 'Bronze to Silver Stream',
    title: 'Late Event Handling Stream',
    flow: 'Stream \u2192 Watermark \u2192 Correct Ordering',
    complexity: 5,
    throughput: 4,
    latency: 5,
    reliability: 5,
    governance: 3,
    score: 4.4,
    code: `# Handle late-arriving events with watermark\nevents = spark.readStream.format("delta").table("catalog.bronze.events") \\\n    .withWatermark("event_time", "2 hours")  # Accept events up to 2hrs late\n\n# Aggregate with late data tolerance\nagg = events.groupBy(\n    window("event_time", "1 hour"),\n    "event_type"\n).agg(count("*").alias("event_count"))\n\n# Append mode: only emit when watermark passes window\nagg.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/late_handling") \\\n    .outputMode("append") \\\n    .toTable("catalog.silver.hourly_events")`,
  },

  // ─── 21–30: Real-Time Business Processing ───
  {
    id: 21,
    group: 'Real-Time Business',
    title: 'Real-Time Fraud Detection',
    flow: 'Transactions \u2192 Stream \u2192 Scoring \u2192 Alert',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 5,
    score: 5.0,
    code: `import mlflow\nmodel = mlflow.pyfunc.load_model("models:/fraud_model/Production")\n\ndef score_batch(batch_df, batch_id):\n    # Score each micro-batch\n    features = batch_df.select("amount","merchant_category","hour_of_day","is_international").toPandas()\n    scores = model.predict(features)\n    result = spark.createDataFrame(batch_df.toPandas().assign(fraud_score=scores))\n    \n    # Alert on high-risk\n    alerts = result.filter(col("fraud_score") > 0.8)\n    alerts.write.format("delta").mode("append").saveAsTable("catalog.alerts.fraud_alerts")\n    \n    # All results to silver\n    result.write.format("delta").mode("append").saveAsTable("catalog.silver.scored_transactions")\n\ntxns = spark.readStream.format("delta").table("catalog.bronze.payment_events")\ntxns.writeStream.foreachBatch(score_batch) \\\n    .option("checkpointLocation", "/mnt/cp/fraud_scoring") \\\n    .trigger(processingTime="1 second").start()`,
  },
  {
    id: 22,
    group: 'Real-Time Business',
    title: 'Real-Time Recommendations',
    flow: 'User Events \u2192 Stream \u2192 Recommendation',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 4,
    governance: 3,
    score: 4.4,
    code: `def recommend_batch(batch_df, batch_id):\n    # Get user features + recent behavior\n    users = batch_df.select("user_id").distinct()\n    features = users.join(spark.table("catalog.ml.user_features"), "user_id")\n    \n    # Score recommendation model\n    import mlflow\n    model = mlflow.pyfunc.load_model("models:/recommender/Production")\n    recs = model.predict(features.toPandas())\n    \n    result = spark.createDataFrame(features.toPandas().assign(recommendations=recs))\n    result.write.format("delta").mode("append").saveAsTable("catalog.silver.recommendations")\n\nevents = spark.readStream.format("delta").table("catalog.bronze.clickstream")\nevents.writeStream.foreachBatch(recommend_batch) \\\n    .option("checkpointLocation", "/mnt/cp/recs") \\\n    .trigger(processingTime="5 seconds").start()`,
  },
  {
    id: 23,
    group: 'Real-Time Business',
    title: 'Real-Time Inventory Update',
    flow: 'POS \u2192 Stream \u2192 Inventory Update',
    complexity: 4,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 3,
    score: 4.4,
    code: `def update_inventory(batch_df, batch_id):\n    from delta.tables import DeltaTable\n    \n    # Aggregate sales per product\n    sales = batch_df.groupBy("product_id").agg(spark_sum("quantity").alias("qty_sold"))\n    \n    # Update inventory\n    inv = DeltaTable.forName(spark, "catalog.silver.inventory")\n    inv.alias("i").merge(sales.alias("s"), "i.product_id = s.product_id") \\\n        .whenMatchedUpdate(set={"quantity": "i.quantity - s.qty_sold", "last_sale_ts": "current_timestamp()"}) \\\n        .execute()\n\npos = spark.readStream.format("delta").table("catalog.bronze.pos_events")\npos.writeStream.foreachBatch(update_inventory) \\\n    .option("checkpointLocation", "/mnt/cp/inv_update") \\\n    .trigger(processingTime="5 seconds").start()`,
  },
  {
    id: 24,
    group: 'Real-Time Business',
    title: 'Real-Time Order Tracking',
    flow: 'Orders \u2192 Stream \u2192 Status Updates',
    complexity: 4,
    throughput: 4,
    latency: 5,
    reliability: 5,
    governance: 3,
    score: 4.2,
    code: `def update_order_status(batch_df, batch_id):\n    from delta.tables import DeltaTable\n    target = DeltaTable.forName(spark, "catalog.silver.order_status")\n    target.alias("t").merge(batch_df.alias("s"), "t.order_id = s.order_id") \\\n        .whenMatchedUpdate(set={"status": "s.new_status", "updated_at": "current_timestamp()"}) \\\n        .whenNotMatchedInsert(values={"order_id": "s.order_id", "status": "s.new_status", "created_at": "current_timestamp()", "updated_at": "current_timestamp()"}) \\\n        .execute()\n\nevents = spark.readStream.format("delta").table("catalog.bronze.order_events")\nevents.writeStream.foreachBatch(update_order_status) \\\n    .option("checkpointLocation", "/mnt/cp/order_track") \\\n    .trigger(processingTime="2 seconds").start()`,
  },
  {
    id: 25,
    group: 'Real-Time Business',
    title: 'Payment Authorization Stream',
    flow: 'Payment \u2192 Validation \u2192 Approval',
    complexity: 5,
    throughput: 4,
    latency: 5,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `def authorize_payments(batch_df, batch_id):\n    # Rule-based validation\n    authorized = batch_df.filter(\n        (col("amount") > 0) & (col("amount") < 50000) &\n        (col("card_status") == "ACTIVE") & (col("fraud_score") < 0.5)\n    ).withColumn("auth_status", lit("APPROVED"))\n    \n    declined = batch_df.subtract(authorized) \\\n        .withColumn("auth_status", lit("DECLINED"))\n    \n    authorized.union(declined) \\\n        .write.format("delta").mode("append") \\\n        .saveAsTable("catalog.secure.payment_auth_log")\n\npayments = spark.readStream.format("delta").table("catalog.bronze.payment_requests")\npayments.writeStream.foreachBatch(authorize_payments) \\\n    .option("checkpointLocation", "/mnt/cp/auth") \\\n    .trigger(processingTime="1 second").start()`,
  },
  {
    id: 26,
    group: 'Real-Time Business',
    title: 'SLA Breach Alerting',
    flow: 'Metrics \u2192 Stream \u2192 Alerts',
    complexity: 3,
    throughput: 3,
    latency: 5,
    reliability: 5,
    governance: 3,
    score: 3.8,
    code: `events = spark.readStream.format("delta").table("catalog.bronze.pipeline_metrics") \\\n    .withWatermark("event_time", "5 minutes")\n\n# Check SLA: pipeline must complete within 30 minutes\nagg = events.groupBy(window("event_time", "30 minutes"), "pipeline_name") \\\n    .agg(max("completion_time").alias("last_completed"))\n\ndef check_sla(batch_df, batch_id):\n    breaches = batch_df.filter(col("last_completed").isNull()) \\\n        .withColumn("alert_type", lit("SLA_BREACH"))\n    if breaches.count() > 0:\n        breaches.write.format("delta").mode("append").saveAsTable("catalog.alerts.sla_breaches")\n\nagg.writeStream.foreachBatch(check_sla) \\\n    .option("checkpointLocation", "/mnt/cp/sla_alert") \\\n    .trigger(processingTime="1 minute").start()`,
  },
  {
    id: 27,
    group: 'Real-Time Business',
    title: 'Customer Activity Tracking',
    flow: 'Events \u2192 Stream \u2192 Behavior Tracking',
    complexity: 4,
    throughput: 5,
    latency: 5,
    reliability: 4,
    governance: 3,
    score: 4.2,
    code: `events = spark.readStream.format("delta").table("catalog.bronze.user_events") \\\n    .withWatermark("event_time", "10 minutes")\n\n# Session-based activity tracking\nactivity = events.groupBy(\n    window("event_time", "30 minutes"), "user_id"\n).agg(\n    count("*").alias("event_count"),\n    countDistinct("page").alias("pages_viewed"),\n    collect_list("action").alias("actions"),\n    min("event_time").alias("session_start"),\n    max("event_time").alias("session_end")\n)\n\nactivity.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/activity") \\\n    .trigger(processingTime="30 seconds") \\\n    .toTable("catalog.silver.user_sessions")`,
  },
  {
    id: 28,
    group: 'Real-Time Business',
    title: 'Supply Chain Tracking',
    flow: 'Logistics \u2192 Stream \u2192 Tracking',
    complexity: 4,
    throughput: 4,
    latency: 5,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `def update_shipments(batch_df, batch_id):\n    from delta.tables import DeltaTable\n    target = DeltaTable.forName(spark, "catalog.silver.shipment_tracking")\n    target.alias("t").merge(batch_df.alias("s"), "t.shipment_id = s.shipment_id") \\\n        .whenMatchedUpdate(set={\n            "current_location": "s.location", "status": "s.status",\n            "eta": "s.estimated_arrival", "last_update": "current_timestamp()"\n        }) \\\n        .whenNotMatchedInsertAll().execute()\n\nlogistics = spark.readStream.format("delta").table("catalog.bronze.logistics_events")\nlogistics.writeStream.foreachBatch(update_shipments) \\\n    .option("checkpointLocation", "/mnt/cp/supply_track") \\\n    .trigger(processingTime="5 seconds").start()`,
  },
  {
    id: 29,
    group: 'Real-Time Business',
    title: 'Energy Usage Monitoring',
    flow: 'IoT \u2192 Stream \u2192 Analytics',
    complexity: 4,
    throughput: 5,
    latency: 5,
    reliability: 4,
    governance: 3,
    score: 4.2,
    code: `meters = spark.readStream.format("delta").table("catalog.bronze.energy_meters") \\\n    .withWatermark("reading_time", "5 minutes")\n\n# 15-minute consumption windows\nconsumption = meters.groupBy(\n    window("reading_time", "15 minutes"), "meter_id", "building_id"\n).agg(\n    avg("kwh").alias("avg_kwh"),\n    max("kwh").alias("peak_kwh"),\n    count("*").alias("readings")\n)\n\nconsumption.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/energy") \\\n    .toTable("catalog.silver.energy_consumption")`,
  },
  {
    id: 30,
    group: 'Real-Time Business',
    title: 'Market Data Ingestion',
    flow: 'Market Feeds \u2192 Stream \u2192 Analytics',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 4,
    score: 4.8,
    code: `# Ultra-low latency market data\ndf = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "market-feed:9092") \\\n    .option("subscribe", "market.ticks") \\\n    .option("maxOffsetsPerTrigger", 500000) \\\n    .load()\n\ntick_schema = "symbol STRING, price DOUBLE, volume LONG, bid DOUBLE, ask DOUBLE, ts LONG"\nparsed = df.select(from_json(col("value").cast("string"), tick_schema).alias("t")).select("t.*") \\\n    .withColumn("tick_time", (col("ts")/1000).cast("timestamp")) \\\n    .withWatermark("tick_time", "1 second")\n\n# 1-second OHLCV bars\nbars = parsed.groupBy(window("tick_time", "1 second"), "symbol").agg(\n    first("price").alias("open"), max("price").alias("high"),\n    min("price").alias("low"), last("price").alias("close"),\n    spark_sum("volume").alias("volume")\n)\n\nbars.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/market") \\\n    .trigger(processingTime="500 milliseconds") \\\n    .toTable("catalog.silver.market_bars_1s")`,
  },

  // ─── 31–40: Streaming + AI/ML/RAG ───
  {
    id: 31,
    group: 'Streaming AI/ML',
    title: 'Real-Time Feature Generation',
    flow: 'Stream \u2192 Features \u2192 Feature Store',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 4,
    score: 4.8,
    code: `def compute_features(batch_df, batch_id):\n    from delta.tables import DeltaTable\n    \n    features = batch_df.groupBy("user_id").agg(\n        count("*").alias("recent_events"),\n        avg("amount").alias("recent_avg_amount"),\n        countDistinct("product_id").alias("unique_products_viewed")\n    )\n    \n    target = DeltaTable.forName(spark, "catalog.ml.realtime_features")\n    target.alias("t").merge(features.alias("s"), "t.user_id = s.user_id") \\\n        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()\n\nevents = spark.readStream.format("delta").table("catalog.bronze.user_events")\nevents.writeStream.foreachBatch(compute_features) \\\n    .option("checkpointLocation", "/mnt/cp/rt_features") \\\n    .trigger(processingTime="10 seconds").start()`,
  },
  {
    id: 32,
    group: 'Streaming AI/ML',
    title: 'Online Model Scoring',
    flow: 'Stream \u2192 Model \u2192 Predictions',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 4,
    score: 4.8,
    code: `import mlflow\nmodel = mlflow.pyfunc.load_model("models:/churn_model/Production")\n\ndef score_stream(batch_df, batch_id):\n    features = batch_df.select("total_orders","avg_amount","days_since_last","unique_products").toPandas()\n    predictions = model.predict(features)\n    result = spark.createDataFrame(batch_df.toPandas().assign(churn_score=predictions))\n    result.write.format("delta").mode("append").saveAsTable("catalog.silver.realtime_predictions")\n\nfeatures_stream = spark.readStream.format("delta").table("catalog.ml.realtime_features")\nfeatures_stream.writeStream.foreachBatch(score_stream) \\\n    .option("checkpointLocation", "/mnt/cp/scoring") \\\n    .trigger(processingTime="5 seconds").start()`,
  },
  {
    id: 33,
    group: 'Streaming AI/ML',
    title: 'Streaming Anomaly Detection',
    flow: 'Stream \u2192 Detect Anomalies \u2192 Alert',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 4,
    score: 4.8,
    code: `from pyspark.sql.functions import stddev, avg, abs as spark_abs\n\n# Calculate rolling stats\nevents = spark.readStream.format("delta").table("catalog.bronze.metrics") \\\n    .withWatermark("event_time", "10 minutes")\n\nstats = events.groupBy(window("event_time", "5 minutes"), "metric_name").agg(\n    avg("value").alias("mean"), stddev("value").alias("std"), count("*").alias("samples")\n)\n\ndef detect_anomalies(batch_df, batch_id):\n    anomalies = batch_df.filter(\n        (spark_abs(col("value") - col("mean")) > 3 * col("std"))  # 3-sigma rule\n    ).withColumn("alert_type", lit("ANOMALY"))\n    \n    if anomalies.count() > 0:\n        anomalies.write.format("delta").mode("append").saveAsTable("catalog.alerts.anomalies")\n\nstats.writeStream.foreachBatch(detect_anomalies) \\\n    .option("checkpointLocation", "/mnt/cp/anomaly") \\\n    .trigger(processingTime="30 seconds").start()`,
  },
  {
    id: 34,
    group: 'Streaming AI/ML',
    title: 'RAG Streaming Ingestion',
    flow: 'Docs/Events \u2192 Stream \u2192 Chunking',
    complexity: 5,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 5,
    score: 4.6,
    code: `def chunk_documents(batch_df, batch_id):\n    from pyspark.sql.functions import udf, explode\n    from pyspark.sql.types import ArrayType, StringType\n    \n    @udf(ArrayType(StringType()))\n    def chunk(text, size=500, overlap=50):\n        if not text: return []\n        return [text[i:i+size] for i in range(0, len(text), size-overlap)]\n    \n    chunked = batch_df.withColumn("chunks", chunk("content")) \\\n        .select("doc_id", "title", explode("chunks").alias("chunk_text")) \\\n        .withColumn("chunk_id", monotonically_increasing_id())\n    \n    chunked.write.format("delta").mode("append").saveAsTable("catalog.ml.doc_chunks")\n\ndocs = spark.readStream.format("delta").table("catalog.bronze.documents")\ndocs.writeStream.foreachBatch(chunk_documents) \\\n    .option("checkpointLocation", "/mnt/cp/rag_chunk") \\\n    .trigger(processingTime="30 seconds").start()`,
  },
  {
    id: 35,
    group: 'Streaming AI/ML',
    title: 'Streaming Embeddings Pipeline',
    flow: 'Text Stream \u2192 Embeddings \u2192 Vector DB',
    complexity: 5,
    throughput: 4,
    latency: 5,
    reliability: 5,
    governance: 5,
    score: 4.8,
    code: `from sentence_transformers import SentenceTransformer\nimport pandas as pd\nmodel = SentenceTransformer("all-MiniLM-L6-v2")\n\ndef embed_batch(batch_df, batch_id):\n    @pandas_udf("array<float>")\n    def embed(texts: pd.Series) -> pd.Series:\n        return pd.Series(model.encode(texts.tolist()).tolist())\n    \n    with_embeddings = batch_df.withColumn("embedding", embed("chunk_text"))\n    with_embeddings.write.format("delta").mode("append").saveAsTable("catalog.ml.embeddings")\n\nchunks = spark.readStream.format("delta").table("catalog.ml.doc_chunks")\nchunks.writeStream.foreachBatch(embed_batch) \\\n    .option("checkpointLocation", "/mnt/cp/embeddings") \\\n    .trigger(processingTime="30 seconds").start()`,
  },
  {
    id: 36,
    group: 'Streaming AI/ML',
    title: 'Conversation Stream Ingestion',
    flow: 'Chat Events \u2192 Stream \u2192 Store/Context',
    complexity: 4,
    throughput: 4,
    latency: 5,
    reliability: 4,
    governance: 5,
    score: 4.4,
    code: `chat = spark.readStream.format("delta").table("catalog.bronze.chat_events")\n\n# Store conversation history\nchat.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/chat_silver") \\\n    .trigger(processingTime="2 seconds") \\\n    .toTable("catalog.silver.conversation_history")`,
  },
  {
    id: 37,
    group: 'Streaming AI/ML',
    title: 'Personalization Pipeline',
    flow: 'Events \u2192 Stream \u2192 User Profile Update',
    complexity: 4,
    throughput: 5,
    latency: 5,
    reliability: 4,
    governance: 4,
    score: 4.4,
    code: `def update_profiles(batch_df, batch_id):\n    from delta.tables import DeltaTable\n    profiles = batch_df.groupBy("user_id").agg(\n        last("page").alias("last_page"), count("*").alias("session_events"),\n        max("event_time").alias("last_active"))\n    target = DeltaTable.forName(spark, "catalog.silver.user_profiles")\n    target.alias("t").merge(profiles.alias("s"), "t.user_id = s.user_id") \\\n        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()\n\nevents = spark.readStream.format("delta").table("catalog.bronze.user_events")\nevents.writeStream.foreachBatch(update_profiles) \\\n    .option("checkpointLocation", "/mnt/cp/profiles") \\\n    .trigger(processingTime="10 seconds").start()`,
  },
  {
    id: 38,
    group: 'Streaming AI/ML',
    title: 'Streaming Feedback Loop',
    flow: 'Predictions \u2192 Feedback \u2192 Retrain Input',
    complexity: 5,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `# Capture prediction outcomes for model retraining\nfeedback = spark.readStream.format("delta").table("catalog.bronze.prediction_feedback")\n\ndef process_feedback(batch_df, batch_id):\n    # Join predictions with actual outcomes\n    predictions = spark.table("catalog.silver.predictions")\n    labeled = batch_df.join(predictions, "prediction_id") \\\n        .select("features.*", "predicted_label", col("actual_outcome").alias("true_label"))\n    \n    labeled.write.format("delta").mode("append").saveAsTable("catalog.ml.training_feedback")\n\nfeedback.writeStream.foreachBatch(process_feedback) \\\n    .option("checkpointLocation", "/mnt/cp/feedback") \\\n    .trigger(processingTime="1 minute").start()`,
  },
  {
    id: 39,
    group: 'Streaming AI/ML',
    title: 'Drift Monitoring Stream',
    flow: 'Model Outputs \u2192 Drift Detection',
    complexity: 5,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `def monitor_drift(batch_df, batch_id):\n    from pyspark.sql.functions import avg, stddev, percentile_approx\n    \n    stats = batch_df.agg(\n        avg("prediction_score").alias("avg_score"),\n        stddev("prediction_score").alias("std_score"),\n        percentile_approx("prediction_score", 0.5).alias("median_score"),\n        count("*").alias("sample_count")\n    ).withColumn("batch_id", lit(batch_id)).withColumn("check_ts", current_timestamp())\n    \n    stats.write.format("delta").mode("append").saveAsTable("catalog.ml.drift_metrics")\n\npredictions = spark.readStream.format("delta").table("catalog.silver.predictions")\npredictions.writeStream.foreachBatch(monitor_drift) \\\n    .option("checkpointLocation", "/mnt/cp/drift") \\\n    .trigger(processingTime="5 minutes").start()`,
  },
  {
    id: 40,
    group: 'Streaming AI/ML',
    title: 'Streaming Knowledge Graph Update',
    flow: 'Events \u2192 Graph DB Updates',
    complexity: 5,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 4.4,
    code: `def update_graph(batch_df, batch_id):\n    # Extract entities and relationships\n    entities = batch_df.select("entity_id", "entity_type", "properties")\n    relationships = batch_df.select("source_id", "target_id", "relation_type")\n    \n    # Merge into graph tables\n    from delta.tables import DeltaTable\n    nodes = DeltaTable.forName(spark, "catalog.silver.graph_nodes")\n    nodes.alias("t").merge(entities.alias("s"), "t.entity_id = s.entity_id") \\\n        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()\n    \n    edges = DeltaTable.forName(spark, "catalog.silver.graph_edges")\n    edges.alias("t").merge(relationships.alias("s"),\n        "t.source_id = s.source_id AND t.target_id = s.target_id") \\\n        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()\n\nevents = spark.readStream.format("delta").table("catalog.bronze.entity_events")\nevents.writeStream.foreachBatch(update_graph) \\\n    .option("checkpointLocation", "/mnt/cp/graph") \\\n    .trigger(processingTime="10 seconds").start()`,
  },

  // ─── 41–50: Governance / Monitoring / Recovery ───
  {
    id: 41,
    group: 'Governance/Recovery',
    title: 'Audit Log Streaming',
    flow: 'Systems \u2192 Stream \u2192 Audit Store',
    complexity: 3,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `audit = spark.readStream.format("delta").table("catalog.bronze.audit_events")\n\ncategorized = audit.withColumn("risk_level",\n    when(col("action").contains("DELETE"), "HIGH")\n    .when(col("action").contains("UPDATE"), "MEDIUM")\n    .otherwise("LOW")\n)\n\ncategorized.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/audit_stream") \\\n    .toTable("catalog.audit.categorized_events")`,
  },
  {
    id: 42,
    group: 'Governance/Recovery',
    title: 'Access Log Monitoring',
    flow: 'IAM \u2192 Stream \u2192 Monitoring',
    complexity: 3,
    throughput: 4,
    latency: 4,
    reliability: 5,
    governance: 5,
    score: 4.2,
    code: `access = spark.readStream.format("delta").table("catalog.bronze.iam_events")\n\n# Detect suspicious access patterns\ndef detect_suspicious(batch_df, batch_id):\n    suspicious = batch_df.groupBy("user_id").agg(count("*").alias("access_count")) \\\n        .filter("access_count > 100")  # >100 accesses in one batch = suspicious\n    if suspicious.count() > 0:\n        suspicious.write.format("delta").mode("append").saveAsTable("catalog.alerts.suspicious_access")\n\naccess.writeStream.foreachBatch(detect_suspicious) \\\n    .option("checkpointLocation", "/mnt/cp/access_monitor") \\\n    .trigger(processingTime="1 minute").start()`,
  },
  {
    id: 43,
    group: 'Governance/Recovery',
    title: 'Cost Monitoring Stream',
    flow: 'Cloud Usage \u2192 Stream \u2192 Cost Analytics',
    complexity: 3,
    throughput: 3,
    latency: 4,
    reliability: 4,
    governance: 4,
    score: 3.6,
    code: `usage = spark.readStream.format("delta").table("catalog.bronze.cloud_usage")\n\ncost = usage.withColumn("estimated_cost",\n    when(col("service")=="compute", col("units") * 0.10)\n    .when(col("service")=="storage", col("units") * 0.023)\n    .otherwise(col("units") * 0.05)\n)\n\ncost.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/cost_stream") \\\n    .trigger(processingTime="5 minutes") \\\n    .toTable("catalog.audit.realtime_costs")`,
  },
  {
    id: 44,
    group: 'Governance/Recovery',
    title: 'Pipeline Health Monitoring',
    flow: 'Jobs \u2192 Stream \u2192 Metrics',
    complexity: 3,
    throughput: 3,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 3.8,
    code: `jobs = spark.readStream.format("delta").table("catalog.bronze.job_events")\n\nhealth = jobs.withColumn("status_category",\n    when(col("state")=="SUCCESS", "HEALTHY")\n    .when(col("state")=="FAILED", "CRITICAL")\n    .when(col("duration_sec") > col("sla_seconds"), "SLA_BREACH")\n    .otherwise("OK")\n)\n\nhealth.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/health") \\\n    .trigger(processingTime="1 minute") \\\n    .toTable("catalog.audit.pipeline_health")`,
  },
  {
    id: 45,
    group: 'Governance/Recovery',
    title: 'SLA Breach Detection',
    flow: 'Events \u2192 Stream \u2192 Alerting',
    complexity: 3,
    throughput: 3,
    latency: 5,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `def check_sla(batch_df, batch_id):\n    breaches = batch_df.filter(col("completion_time") > col("sla_deadline")) \\\n        .withColumn("breach_minutes", (unix_timestamp("completion_time") - unix_timestamp("sla_deadline")) / 60)\n    if breaches.count() > 0:\n        breaches.write.format("delta").mode("append").saveAsTable("catalog.alerts.sla_breaches")\n\nmetrics = spark.readStream.format("delta").table("catalog.bronze.pipeline_metrics")\nmetrics.writeStream.foreachBatch(check_sla) \\\n    .option("checkpointLocation", "/mnt/cp/sla_detect") \\\n    .trigger(processingTime="1 minute").start()`,
  },
  {
    id: 46,
    group: 'Governance/Recovery',
    title: 'Data Lineage Streaming',
    flow: 'Metadata \u2192 Stream \u2192 Lineage Graph',
    complexity: 4,
    throughput: 3,
    latency: 3,
    reliability: 5,
    governance: 5,
    score: 4.0,
    code: `metadata = spark.readStream.format("delta").table("catalog.bronze.etl_metadata")\n\nlineage = metadata.select(\n    col("source_table"), col("target_table"),\n    col("pipeline_name"), col("transform_type"),\n    col("row_count"), col("event_time")\n)\n\nlineage.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/lineage_stream") \\\n    .trigger(processingTime="5 minutes") \\\n    .toTable("catalog.audit.realtime_lineage")`,
  },
  {
    id: 47,
    group: 'Governance/Recovery',
    title: 'Dead Letter Queue Pipeline',
    flow: 'Failed Events \u2192 DLQ \u2192 Reprocess',
    complexity: 4,
    throughput: 3,
    latency: 4,
    reliability: 5,
    governance: 4,
    score: 4.0,
    code: `# Read from dead letter queue\ndlq = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker:9092") \\\n    .option("subscribe", "events.dlq") \\\n    .load()\n\ndef reprocess(batch_df, batch_id):\n    parsed = batch_df.select(from_json(col("value").cast("string"), event_schema).alias("e")).select("e.*")\n    # Attempt reprocessing\n    valid = parsed.filter(col("event_id").isNotNull())\n    still_bad = parsed.filter(col("event_id").isNull())\n    \n    valid.write.format("delta").mode("append").saveAsTable("catalog.bronze.recovered_events")\n    still_bad.write.format("delta").mode("append").saveAsTable("catalog.quarantine.permanent_failures")\n\ndlq.writeStream.foreachBatch(reprocess) \\\n    .option("checkpointLocation", "/mnt/cp/dlq") \\\n    .trigger(processingTime="5 minutes").start()`,
  },
  {
    id: 48,
    group: 'Governance/Recovery',
    title: 'Replay Pipeline',
    flow: 'Kafka Offsets \u2192 Replay Events',
    complexity: 5,
    throughput: 4,
    latency: 3,
    reliability: 5,
    governance: 4,
    score: 4.2,
    code: `# Replay from specific Kafka offset\ndf = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "broker:9092") \\\n    .option("subscribe", "events") \\\n    .option("startingOffsets", '{"events":{"0":1000000,"1":1000000}}') \\\n    .option("endingOffsets", '{"events":{"0":2000000,"1":2000000}}') \\\n    .load()\n\nparsed = df.select(from_json(col("value").cast("string"), event_schema).alias("e")).select("e.*") \\\n    .withColumn("_replay_ts", current_timestamp()) \\\n    .withColumn("_is_replay", lit(True))\n\nparsed.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/cp/replay") \\\n    .trigger(availableNow=True) \\\n    .toTable("catalog.bronze.replayed_events")`,
  },
  {
    id: 49,
    group: 'Governance/Recovery',
    title: 'Multi-Region Replication Stream',
    flow: 'Region A \u2192 Region B Sync',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 5,
    score: 5.0,
    code: `# Active-active replication via Kafka MirrorMaker or Delta Deep Clone\n# Option 1: Stream-based replication\nsource = spark.readStream.format("delta").table("catalog.gold.fact_sales")\n\nsource.writeStream.format("delta") \\\n    .option("checkpointLocation", "s3://dr-region/cp/fact_sales") \\\n    .trigger(processingTime="1 minute") \\\n    .toTable("catalog_dr.gold.fact_sales")\n\n# Option 2: Periodic Deep Clone\n# spark.sql("CREATE OR REPLACE TABLE catalog_dr.gold.fact_sales DEEP CLONE catalog.gold.fact_sales")`,
  },
  {
    id: 50,
    group: 'Governance/Recovery',
    title: 'Disaster Recovery Stream',
    flow: 'Active-Active Streaming Failover',
    complexity: 5,
    throughput: 5,
    latency: 5,
    reliability: 5,
    governance: 5,
    score: 5.0,
    code: `# Active-active DR: both regions consume from Kafka\n# Primary region\nprimary = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "primary-broker:9092") \\\n    .option("subscribe", "events") \\\n    .option("kafka.group.id", "primary-consumer") \\\n    .load()\n\nprimary.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/primary/cp/events") \\\n    .toTable("catalog_primary.bronze.events")\n\n# DR region (same topic, different consumer group)\ndr = spark.readStream.format("kafka") \\\n    .option("kafka.bootstrap.servers", "dr-broker:9092") \\\n    .option("subscribe", "events") \\\n    .option("kafka.group.id", "dr-consumer") \\\n    .load()\n\ndr.writeStream.format("delta") \\\n    .option("checkpointLocation", "/mnt/dr/cp/events") \\\n    .toTable("catalog_dr.bronze.events")\n\n# Failover: switch DNS from primary to DR\n# RPO: 0 (both consume same events), RTO: < 1 minute`,
  },
];

const groups = [...new Set(streamScenarios.map((s) => s.group))];

function ScoreBar({ label, value, max }) {
  const pct = (value / max) * 100;
  const color = value >= 4 ? '#22c55e' : value >= 3 ? '#f59e0b' : '#ef4444';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '0.7rem' }}>
      <span style={{ width: '70px', color: 'var(--text-secondary)' }}>{label}</span>
      <div style={{ flex: 1, height: '8px', background: '#f3f4f6', borderRadius: '4px' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: '4px' }} />
      </div>
      <span style={{ width: '18px', textAlign: 'right', fontWeight: 600 }}>{value}</span>
    </div>
  );
}

function StreamIngestion() {
  const [selectedGroup, setSelectedGroup] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('id');

  const filtered = streamScenarios
    .filter((s) => {
      const matchGroup = selectedGroup === 'All' || s.group === selectedGroup;
      const matchSearch =
        s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
        s.flow.toLowerCase().includes(searchTerm.toLowerCase());
      return matchGroup && matchSearch;
    })
    .sort((a, b) => (sortBy === 'score' ? b.score - a.score : a.id - b.id));

  const downloadScorecard = () => {
    exportToCSV(
      streamScenarios.map((s) => ({
        id: s.id,
        group: s.group,
        title: s.title,
        flow: s.flow,
        complexity: s.complexity,
        throughput: s.throughput,
        latency: s.latency,
        reliability: s.reliability,
        governance: s.governance,
        score: s.score,
      })),
      'streaming-scorecard.csv'
    );
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Streaming Ingestion Scenarios</h1>
          <p>
            50 real-time streaming pipelines \u2014 Kafka, Event Hub, IoT, CDC, AI/ML, Governance
          </p>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon blue">{'\ud83c\udf0a'}</div>
          <div className="stat-info">
            <h4>50</h4>
            <p>Stream Pipelines</p>
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
            <h4>{(streamScenarios.reduce((s, x) => s + x.score, 0) / 50).toFixed(1)}</h4>
            <p>Avg Score</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">{'\ud83c\udfc6'}</div>
          <div className="stat-info">
            <h4>{streamScenarios.filter((s) => s.score >= 4.5).length}</h4>
            <p>Advanced (4.5+)</p>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
          <input
            type="text"
            className="form-input"
            placeholder="Search streaming scenarios..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ maxWidth: '280px' }}
          />
          <select
            className="form-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
            style={{ maxWidth: '230px' }}
          >
            <option value="All">All Groups (50)</option>
            {groups.map((g) => (
              <option key={g} value={g}>
                {g} ({streamScenarios.filter((s) => s.group === g).length})
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

      {filtered.map((s) => {
        const isExpanded = expandedId === s.id;
        return (
          <div key={s.id} className="card" style={{ marginBottom: '0.75rem' }}>
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
                    flexWrap: 'wrap',
                    marginBottom: '0.2rem',
                  }}
                >
                  <span className="badge running">{s.group}</span>
                  <strong>
                    #{s.id} \u2014 {s.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{s.flow}</p>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <span
                  style={{
                    fontSize: '1.1rem',
                    fontWeight: 700,
                    color:
                      s.score >= 4.5
                        ? 'var(--error)'
                        : s.score >= 3.5
                          ? 'var(--warning)'
                          : 'var(--success)',
                  }}
                >
                  {s.score}
                </span>
                <span style={{ color: 'var(--text-secondary)' }}>
                  {isExpanded ? '\u25BC' : '\u25B6'}
                </span>
              </div>
            </div>

            {isExpanded && (
              <div
                style={{
                  marginTop: '1rem',
                  display: 'grid',
                  gridTemplateColumns: '250px 1fr',
                  gap: '1rem',
                }}
              >
                <div style={{ padding: '0.6rem', background: '#f8f9fa', borderRadius: '6px' }}>
                  <div style={{ fontSize: '0.72rem', fontWeight: 700, marginBottom: '0.4rem' }}>
                    Streaming KPI
                  </div>
                  <ScoreBar label="Complexity" value={s.complexity} max={5} />
                  <ScoreBar label="Throughput" value={s.throughput} max={5} />
                  <ScoreBar label="Latency" value={s.latency} max={5} />
                  <ScoreBar label="Reliability" value={s.reliability} max={5} />
                  <ScoreBar label="Governance" value={s.governance} max={5} />
                  <div style={{ marginTop: '0.4rem', fontSize: '0.8rem', fontWeight: 700 }}>
                    Overall:{' '}
                    <span
                      style={{
                        color: s.score >= 4.5 ? '#ef4444' : s.score >= 3.5 ? '#f59e0b' : '#22c55e',
                      }}
                    >
                      {s.score}/5.0
                    </span>
                  </div>
                </div>
                <div>
                  <ScenarioCard scenario={s} />
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default StreamIngestion;
