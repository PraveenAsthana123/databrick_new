import React, { useState } from 'react';
import FileFormatRunner from '../components/common/FileFormatRunner';

// ── Real sample data generators per dataset ──────────────────────────
function generateSampleRows(name, count = 20) {
  const n = (name || '').toLowerCase();
  if (n.includes('order') || n.includes('commerce')) {
    return Array.from({ length: count }, (_, i) => ({
      order_id: 1000 + i,
      customer_id: 5000 + Math.floor(Math.random() * 500),
      product_id: 100 + Math.floor(Math.random() * 200),
      amount: +(Math.random() * 500 + 10).toFixed(2),
      quantity: Math.floor(Math.random() * 5) + 1,
      order_date: `2024-${String(Math.floor(Math.random() * 12) + 1).padStart(2, '0')}-${String(Math.floor(Math.random() * 28) + 1).padStart(2, '0')}`,
      status: ['completed', 'shipped', 'pending', 'cancelled'][Math.floor(Math.random() * 4)],
    }));
  }
  if (n.includes('customer') || n.includes('profile')) {
    const firstNames = ['Alice', 'Bob', 'Carol', 'David', 'Emma', 'Frank', 'Grace', 'Henry'];
    const lastNames = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller'];
    const cities = ['New York', 'Chicago', 'Austin', 'Seattle', 'Denver', 'Miami'];
    return Array.from({ length: count }, (_, i) => ({
      customer_id: 1 + i,
      name: `${firstNames[i % firstNames.length]} ${lastNames[i % lastNames.length]}`,
      email: `user${i + 1}@example.com`,
      phone: `+1-555-${String(1000 + i).padStart(4, '0')}`,
      city: cities[i % cities.length],
      age: 18 + Math.floor(Math.random() * 60),
      segment: ['Premium', 'Standard', 'Basic'][Math.floor(Math.random() * 3)],
      created_at: `202${Math.floor(Math.random() * 4) + 1}-01-15`,
    }));
  }
  if (n.includes('iot') || n.includes('sensor')) {
    const locations = ['factory_a', 'factory_b', 'warehouse'];
    return Array.from({ length: count }, (_, i) => ({
      device_id: (i % 100) + 1,
      temperature: +(22 + (Math.random() - 0.5) * 10).toFixed(2),
      humidity: +(30 + Math.random() * 40).toFixed(2),
      pressure: +(1013 + (Math.random() - 0.5) * 20).toFixed(2),
      timestamp: `2024-01-15T${String(Math.floor(i / 6)).padStart(2, '0')}:${String((i * 10) % 60).padStart(2, '0')}:00Z`,
      location: locations[i % 3],
    }));
  }
  if (n.includes('product') || n.includes('catalog')) {
    const cats = ['Electronics', 'Clothing', 'Food', 'Books', 'Home', 'Sports'];
    return Array.from({ length: count }, (_, i) => ({
      product_id: 1 + i,
      product_name: `Product_${1 + i}`,
      category: cats[i % cats.length],
      price: +(Math.random() * 500 + 5).toFixed(2),
      cost: +(Math.random() * 200 + 5).toFixed(2),
      stock_quantity: Math.floor(Math.random() * 1000),
    }));
  }
  if (n.includes('log') || n.includes('web') || n.includes('access')) {
    const paths = ['index.html', 'api/data', 'products', 'cart', 'checkout'];
    const codes = [200, 200, 200, 301, 404, 500];
    return Array.from({ length: count }, (_, i) => ({
      ip: `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
      timestamp: `15/Jan/2024:${String(Math.floor(i / 60) % 24).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}:00 +0000`,
      method: 'GET',
      path: `/${paths[i % paths.length]}`,
      status: codes[i % codes.length],
      bytes: 100 + Math.floor(Math.random() * 50000),
    }));
  }
  if (n.includes('transaction') || n.includes('financial') || n.includes('fraud')) {
    const types = ['purchase', 'transfer', 'withdrawal', 'deposit'];
    const currencies = ['USD', 'EUR', 'GBP'];
    return Array.from({ length: count }, (_, i) => ({
      transaction_id: 1 + i,
      account_id: 1000 + Math.floor(Math.random() * 500),
      amount: +(Math.random() * 10000).toFixed(2),
      type: types[i % 4],
      currency: currencies[i % 3],
      is_fraud: Math.random() < 0.02,
      timestamp: `2024-01-15T${String(Math.floor(i / 60)).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}:00Z`,
    }));
  }
  // Default
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    value: `sample_${i + 1}`,
    amount: +(Math.random() * 1000).toFixed(2),
    status: ['active', 'pending'][i % 2],
  }));
}

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
  const [rowCountById, setRowCountById] = useState({});
  const [generatedById, setGeneratedById] = useState({});

  const handleGenerate = (dataset) => {
    const count = rowCountById[dataset.id] || 20;
    const rows = generateSampleRows(dataset.name, Math.max(1, Math.min(10000, +count)));
    setGeneratedById({ ...generatedById, [dataset.id]: rows });
  };

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
            <div style={{ marginTop: '1rem' }}>
              <div className="code-block">{d.code}</div>

              {/* Generate Data Panel */}
              <div
                style={{
                  marginTop: '1rem',
                  padding: '0.9rem 1rem',
                  background: 'linear-gradient(135deg, #fef3c7 0%, #fde68a 100%)',
                  border: '1px solid #fcd34d',
                  borderLeft: '4px solid #f59e0b',
                  borderRadius: '10px',
                }}
              >
                <div
                  style={{
                    fontSize: '0.75rem',
                    fontWeight: 800,
                    textTransform: 'uppercase',
                    letterSpacing: '0.08em',
                    color: '#92400e',
                    marginBottom: '0.65rem',
                  }}
                >
                  🎲 Generate Sample Data
                </div>
                <div
                  style={{
                    display: 'flex',
                    gap: '0.65rem',
                    alignItems: 'center',
                    flexWrap: 'wrap',
                  }}
                >
                  <label style={{ fontSize: '0.82rem', color: '#78350f', fontWeight: 600 }}>
                    Rows:
                  </label>
                  <input
                    type="number"
                    min="1"
                    max="10000"
                    value={rowCountById[d.id] || 20}
                    onChange={(e) => setRowCountById({ ...rowCountById, [d.id]: e.target.value })}
                    style={{
                      width: '110px',
                      padding: '0.35rem 0.55rem',
                      border: '1px solid #fcd34d',
                      borderRadius: '6px',
                      fontSize: '0.85rem',
                    }}
                  />
                  <button
                    className="btn btn-sm"
                    onClick={() => handleGenerate(d)}
                    style={{
                      background: '#f59e0b',
                      color: '#fff',
                      fontWeight: 600,
                      padding: '0.4rem 1rem',
                    }}
                  >
                    🎲 Generate
                  </button>
                  {generatedById[d.id] && (
                    <span style={{ fontSize: '0.8rem', color: '#78350f' }}>
                      ✓ Generated {generatedById[d.id].length} rows · ready to download / run /
                      schedule
                    </span>
                  )}
                </div>

                {/* Preview first 3 rows */}
                {generatedById[d.id] && (
                  <div
                    style={{
                      marginTop: '0.7rem',
                      padding: '0.55rem 0.75rem',
                      background: '#fff',
                      border: '1px solid #fcd34d',
                      borderRadius: '6px',
                      fontFamily: 'Fira Code, Consolas, monospace',
                      fontSize: '0.72rem',
                      color: '#78350f',
                      overflowX: 'auto',
                    }}
                  >
                    {generatedById[d.id].slice(0, 3).map((row, i) => (
                      <div key={i} style={{ whiteSpace: 'pre', marginBottom: '0.15rem' }}>
                        {JSON.stringify(row)}
                      </div>
                    ))}
                    {generatedById[d.id].length > 3 && (
                      <div style={{ color: '#b45309', marginTop: '0.25rem' }}>
                        ... + {generatedById[d.id].length - 3} more rows
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Download / Run / Schedule */}
              {generatedById[d.id] && (
                <div style={{ marginTop: '0.85rem' }}>
                  <FileFormatRunner
                    data={generatedById[d.id]}
                    slug={`sample-${d.id}-${d.name
                      .toLowerCase()
                      .replace(/[^a-z0-9]+/g, '-')
                      .slice(0, 30)}`}
                    schemaName={d.name.replace(/[^A-Za-z]+/g, '') || 'SampleData'}
                    tableName={`catalog.bronze.sample_${d.name
                      .toLowerCase()
                      .replace(/[^a-z0-9]+/g, '_')
                      .slice(0, 30)}`}
                  />
                </div>
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

export default DownloadData;
