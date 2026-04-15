import React, { useState } from 'react';

const datasets = [
  {
    id: 1,
    name: 'E-Commerce Orders',
    format: 'CSV',
    size: '50 MB',
    rows: '500K',
    desc: 'Orders with customer_id, product_id, amount, date',
    code: `# Generate sample e-commerce data
from pyspark.sql.functions import rand, randn, floor, date_add, lit, expr
from pyspark.sql.types import *

n = 500000
orders = spark.range(n).select(
    col("id").alias("order_id"),
    (floor(rand() * 10000) + 1).alias("customer_id"),
    (floor(rand() * 500) + 1).alias("product_id"),
    (rand() * 500 + 10).cast("decimal(10,2)").alias("amount"),
    (floor(rand() * 3) + 1).alias("quantity"),
    date_add(lit("2023-01-01"), (rand() * 365).cast("int")).alias("order_date"),
    expr("CASE floor(rand()*4) WHEN 0 THEN 'completed' WHEN 1 THEN 'shipped' WHEN 2 THEN 'pending' ELSE 'cancelled' END").alias("status")
)
orders.write.format("delta").saveAsTable("catalog.bronze.sample_orders")
orders.coalesce(1).write.csv("/Volumes/catalog/landing/csv_files/orders.csv", header=True)`,
  },
  {
    id: 2,
    name: 'Customer Profiles',
    format: 'JSON',
    size: '25 MB',
    rows: '100K',
    desc: 'Customer demographics with addresses',
    code: `from faker import Faker
import json

fake = Faker()
customers = []
for i in range(100000):
    customers.append({
        "customer_id": i + 1,
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "address": {"street": fake.street_address(), "city": fake.city(), "state": fake.state(), "zip": fake.zipcode()},
        "age": fake.random_int(18, 80),
        "segment": fake.random_element(["Premium", "Standard", "Basic"]),
        "created_at": str(fake.date_between(start_date="-3y"))
    })

df = spark.createDataFrame(customers)
df.write.format("delta").saveAsTable("catalog.bronze.sample_customers")`,
  },
  {
    id: 3,
    name: 'IoT Sensor Data',
    format: 'Parquet',
    size: '200 MB',
    rows: '2M',
    desc: 'Temperature, humidity, pressure readings',
    code: `from pyspark.sql.functions import rand, randn, from_unixtime, expr

n = 2000000
sensors = spark.range(n).select(
    (col("id") % 100 + 1).alias("device_id"),
    (randn() * 5 + 22).alias("temperature"),
    (rand() * 40 + 30).alias("humidity"),
    (randn() * 10 + 1013).alias("pressure"),
    from_unixtime(lit(1704067200) + col("id") * 30).alias("timestamp"),
    expr("CASE id % 5 WHEN 0 THEN 'factory_a' WHEN 1 THEN 'factory_b' ELSE 'warehouse' END").alias("location")
)
sensors.write.format("delta").saveAsTable("catalog.bronze.sample_iot")`,
  },
  {
    id: 4,
    name: 'Product Catalog',
    format: 'CSV',
    size: '5 MB',
    rows: '10K',
    desc: 'Products with categories, prices, descriptions',
    code: `from pyspark.sql.functions import rand, expr, floor

products = spark.range(10000).select(
    (col("id") + 1).alias("product_id"),
    expr("concat('Product_', id)").alias("product_name"),
    expr("CASE floor(rand()*6) WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing' WHEN 2 THEN 'Food' WHEN 3 THEN 'Books' WHEN 4 THEN 'Home' ELSE 'Sports' END").alias("category"),
    (rand() * 500 + 5).cast("decimal(10,2)").alias("price"),
    (rand() * 200 + 5).cast("decimal(10,2)").alias("cost"),
    (floor(rand() * 1000)).alias("stock_quantity")
)
products.write.format("delta").saveAsTable("catalog.bronze.sample_products")`,
  },
  {
    id: 5,
    name: 'Web Access Logs',
    format: 'Text',
    size: '100 MB',
    rows: '1M',
    desc: 'Apache-format web server access logs',
    code: `from pyspark.sql.functions import expr, concat, lit, date_format

logs = spark.range(1000000).select(
    concat(
        expr("concat(floor(rand()*255),'.',floor(rand()*255),'.',floor(rand()*255),'.',floor(rand()*255))"),
        lit(" - - ["), date_format(expr("date_add(current_date(), -floor(rand()*30))"), "dd/MMM/yyyy:HH:mm:ss"),
        lit(' +0000] "GET /'),
        expr("CASE floor(rand()*5) WHEN 0 THEN 'index.html' WHEN 1 THEN 'api/data' WHEN 2 THEN 'products' WHEN 3 THEN 'cart' ELSE 'checkout' END"),
        lit(' HTTP/1.1" '),
        expr("CASE floor(rand()*4) WHEN 0 THEN '200' WHEN 1 THEN '301' WHEN 2 THEN '404' ELSE '500' END"),
        lit(" "), expr("floor(rand()*50000 + 100)")
    ).alias("log_line")
)
logs.write.text("/Volumes/catalog/landing/log_files/access_log.txt")`,
  },
  {
    id: 6,
    name: 'Financial Transactions',
    format: 'Parquet',
    size: '300 MB',
    rows: '5M',
    desc: 'Banking transactions for fraud detection',
    code: `from pyspark.sql.functions import rand, randn, expr, when

txns = spark.range(5000000).select(
    col("id").alias("transaction_id"),
    (floor(rand() * 50000) + 1).alias("account_id"),
    (rand() * 10000).cast("decimal(10,2)").alias("amount"),
    expr("CASE floor(rand()*4) WHEN 0 THEN 'purchase' WHEN 1 THEN 'transfer' WHEN 2 THEN 'withdrawal' ELSE 'deposit' END").alias("type"),
    expr("CASE floor(rand()*3) WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END").alias("currency"),
    when(rand() < 0.02, True).otherwise(False).alias("is_fraud"),
    expr("from_unixtime(1704067200 + id * 6)").alias("timestamp")
)
txns.write.format("delta").partitionBy("type").saveAsTable("catalog.bronze.sample_transactions")`,
  },
];

function DownloadData() {
  const [expandedId, setExpandedId] = useState(null);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>Download / Generate Data</h1>
          <p>Sample datasets for testing pipelines</p>
        </div>
      </div>

      <div className="stats-grid" style={{ marginBottom: '1.5rem' }}>
        <div className="stat-card">
          <div className="stat-icon orange">📊</div>
          <div className="stat-info">
            <h4>{datasets.length}</h4>
            <p>Sample Datasets</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon blue">📦</div>
          <div className="stat-info">
            <h4>8.5M+</h4>
            <p>Total Rows</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon green">💾</div>
          <div className="stat-info">
            <h4>~680 MB</h4>
            <p>Total Size</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon purple">📁</div>
          <div className="stat-info">
            <h4>CSV/JSON/Parquet/Text</h4>
            <p>Formats</p>
          </div>
        </div>
      </div>

      {datasets.map((d) => (
        <div key={d.id} className="card" style={{ marginBottom: '0.75rem' }}>
          <div
            onClick={() => setExpandedId(expandedId === d.id ? null : d.id)}
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
                <span className="badge completed">{d.format}</span>
                <strong>{d.name}</strong>
                <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                  ({d.rows} rows, {d.size})
                </span>
              </div>
              <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{d.desc}</p>
            </div>
            <span>{expandedId === d.id ? '▼' : '▶'}</span>
          </div>
          {expandedId === d.id && (
            <div className="code-block" style={{ marginTop: '1rem' }}>
              {d.code}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

export default DownloadData;
