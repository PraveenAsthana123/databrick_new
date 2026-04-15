import React, { useState } from 'react';

const ollamaScenarios = [
  {
    id: 1,
    category: 'Setup',
    title: 'Setup Ollama on Databricks',
    desc: 'Install and configure Ollama server on a Databricks cluster for local LLM inference',
    code: `# Install Ollama on the Databricks cluster driver node
%pip install ollama httpx

# Download and install the Ollama binary
import subprocess, os

subprocess.run(["curl", "-fsSL", "https://ollama.com/install.sh"], capture_output=True)
subprocess.run(["bash", "-c", "curl -fsSL https://ollama.com/install.sh | sh"], check=True)

# Start the Ollama server as a background process
import threading, time

def start_ollama():
    subprocess.run(["ollama", "serve"], env={**os.environ, "OLLAMA_HOST": "0.0.0.0:11434"})

server_thread = threading.Thread(target=start_ollama, daemon=True)
server_thread.start()
time.sleep(5)  # Wait for server to initialize

# Verify the server is running
import httpx
response = httpx.get("http://localhost:11434/api/tags", timeout=10)
print(f"Ollama server status: {response.status_code}")
print(f"Available models: {response.json().get('models', [])}")

# Pull a model for use
subprocess.run(["ollama", "pull", "llama3.1:8b"], check=True)
print("Ollama setup complete with llama3.1:8b model")`,
  },

  {
    id: 2,
    category: 'Inference',
    title: 'Chat Completion with Ollama',
    desc: 'Send chat completion requests to a locally running Ollama model with structured messages',
    code: `import ollama

# Simple chat completion
response = ollama.chat(
    model="llama3.1:8b",
    messages=[
        {"role": "system", "content": "You are a helpful data engineering assistant for Databricks."},
        {"role": "user", "content": "Explain the medallion architecture in 3 sentences."}
    ],
    options={
        "temperature": 0.7,
        "top_p": 0.9,
        "num_predict": 512,
        "seed": 42
    }
)

print(f"Model: {response['model']}")
print(f"Response: {response['message']['content']}")
print(f"Tokens — prompt: {response['prompt_eval_count']}, completion: {response['eval_count']}")
print(f"Duration: {response['total_duration'] / 1e9:.2f}s")

# Multi-turn conversation
conversation = [
    {"role": "system", "content": "You are a Spark SQL expert."},
    {"role": "user", "content": "How do I optimize a shuffle-heavy join?"},
]
reply = ollama.chat(model="llama3.1:8b", messages=conversation)
conversation.append(reply["message"])

# Follow-up
conversation.append({"role": "user", "content": "Can you show a code example?"})
reply2 = ollama.chat(model="llama3.1:8b", messages=conversation)
print(f"Follow-up: {reply2['message']['content'][:200]}...")`,
  },

  {
    id: 3,
    category: 'Embeddings',
    title: 'Generate Embeddings with Ollama',
    desc: 'Create text embeddings using Ollama for vector similarity search and RAG pipelines',
    code: `import ollama
import numpy as np

# Generate embedding for a single text
response = ollama.embeddings(
    model="nomic-embed-text",
    prompt="Delta Lake provides ACID transactions on data lakes"
)
embedding = response["embedding"]
print(f"Embedding dimension: {len(embedding)}")
print(f"First 5 values: {embedding[:5]}")

# Batch embeddings for multiple documents
documents = [
    "Delta Lake supports schema enforcement and evolution",
    "Unity Catalog provides centralized governance for Databricks",
    "MLflow tracks experiments, models, and deployments",
    "Structured Streaming processes real-time data with exactly-once semantics",
    "Photon engine accelerates SQL and DataFrame workloads on Databricks"
]

embeddings = []
for doc in documents:
    resp = ollama.embeddings(model="nomic-embed-text", prompt=doc)
    embeddings.append(resp["embedding"])

embeddings_matrix = np.array(embeddings)
print(f"Embeddings shape: {embeddings_matrix.shape}")

# Compute cosine similarity
from numpy.linalg import norm

query_emb = ollama.embeddings(model="nomic-embed-text", prompt="How does Delta handle transactions?")["embedding"]
query_vec = np.array(query_emb)

similarities = []
for i, emb in enumerate(embeddings):
    emb_vec = np.array(emb)
    cosine_sim = np.dot(query_vec, emb_vec) / (norm(query_vec) * norm(emb_vec))
    similarities.append((i, cosine_sim, documents[i]))

similarities.sort(key=lambda x: x[1], reverse=True)
for idx, sim, doc in similarities[:3]:
    print(f"  Score: {sim:.4f} | {doc}")`,
  },

  {
    id: 4,
    category: 'Management',
    title: 'Model Pull and Management',
    desc: 'Pull, list, copy, and manage Ollama models on the cluster',
    code: `import ollama
import json

# List all available models
models = ollama.list()
for model in models.get("models", []):
    size_gb = model["size"] / (1024 ** 3)
    print(f"  {model['name']} — {size_gb:.1f} GB — Modified: {model['modified_at']}")

# Pull a new model with progress tracking
print("\\nPulling mistral:7b...")
for progress in ollama.pull("mistral:7b", stream=True):
    status = progress.get("status", "")
    completed = progress.get("completed", 0)
    total = progress.get("total", 0)
    if total > 0:
        pct = (completed / total) * 100
        print(f"  {status}: {pct:.1f}%", end="\\r")
print("\\nPull complete!")

# Show model details
model_info = ollama.show("llama3.1:8b")
print(f"\\nModel info for llama3.1:8b:")
print(f"  Parameters: {model_info.get('details', {}).get('parameter_size', 'N/A')}")
print(f"  Quantization: {model_info.get('details', {}).get('quantization_level', 'N/A')}")
print(f"  Format: {model_info.get('details', {}).get('format', 'N/A')}")
print(f"  Family: {model_info.get('details', {}).get('family', 'N/A')}")

# Copy a model to create a variant
ollama.copy("llama3.1:8b", "my-llama:latest")
print("\\nCopied llama3.1:8b -> my-llama:latest")

# Delete a model
ollama.delete("my-llama:latest")
print("Deleted my-llama:latest")`,
  },

  {
    id: 5,
    category: 'Inference',
    title: 'Batch Inference with Ollama',
    desc: 'Run batch inference across a PySpark DataFrame using Ollama for large-scale text processing',
    code: `import ollama
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField
import json

# Sample DataFrame with text to process
df = spark.createDataFrame([
    (1, "Customer reported login failure after password reset"),
    (2, "Application crashes when uploading files larger than 50MB"),
    (3, "Dashboard latency increased 3x after last deployment"),
    (4, "User requests bulk export feature for audit logs"),
    (5, "SSL certificate expiring in 7 days for api.example.com"),
], ["ticket_id", "description"])

# Define a UDF for Ollama inference
def classify_ticket(description):
    try:
        response = ollama.chat(
            model="llama3.1:8b",
            messages=[
                {"role": "system", "content": "Classify the ticket into one of: BUG, FEATURE_REQUEST, PERFORMANCE, SECURITY. Return only the category."},
                {"role": "user", "content": description}
            ],
            options={"temperature": 0.1, "num_predict": 20}
        )
        return response["message"]["content"].strip()
    except Exception as e:
        return f"ERROR: {str(e)}"

classify_udf = F.udf(classify_ticket, StringType())

# Apply classification — use coalesce(1) for driver-only Ollama
result_df = df.coalesce(1).withColumn("category", classify_udf(F.col("description")))
result_df.show(truncate=False)

# For larger datasets, use mapInPandas for better throughput
def classify_batch(iterator):
    import pandas as pd
    for pdf in iterator:
        categories = []
        for desc in pdf["description"]:
            cat = classify_ticket(desc)
            categories.append(cat)
        pdf["category"] = categories
        yield pdf

schema = StructType([
    StructField("ticket_id", StringType()),
    StructField("description", StringType()),
    StructField("category", StringType())
])
result_df2 = df.coalesce(1).mapInPandas(classify_batch, schema=result_df.schema)
result_df2.write.mode("overwrite").saveAsTable("bronze.ticket_classifications")`,
  },

  {
    id: 6,
    category: 'Streaming',
    title: 'Streaming Responses from Ollama',
    desc: 'Stream token-by-token responses from Ollama for real-time output and reduced latency',
    code: `import ollama
import time

# Stream a chat completion token by token
print("Streaming response:\\n")
full_response = ""
token_count = 0
start_time = time.time()

stream = ollama.chat(
    model="llama3.1:8b",
    messages=[
        {"role": "system", "content": "You are a concise data engineering teacher."},
        {"role": "user", "content": "Explain slowly changing dimensions (SCD Type 2) with a brief example."}
    ],
    stream=True,
    options={"temperature": 0.7, "num_predict": 300}
)

for chunk in stream:
    token = chunk["message"]["content"]
    full_response += token
    token_count += 1
    print(token, end="", flush=True)

elapsed = time.time() - start_time
print(f"\\n\\n--- Stream stats ---")
print(f"Tokens generated: {token_count}")
print(f"Time elapsed: {elapsed:.2f}s")
print(f"Tokens/second: {token_count / elapsed:.1f}")

# Streaming with generation progress callback
def stream_with_callback(prompt, on_token=None, on_complete=None):
    tokens = []
    for chunk in ollama.generate(model="llama3.1:8b", prompt=prompt, stream=True):
        token = chunk["response"]
        tokens.append(token)
        if on_token:
            on_token(token, len(tokens))
        if chunk.get("done", False):
            stats = {
                "total_tokens": len(tokens),
                "eval_count": chunk.get("eval_count", 0),
                "eval_duration_s": chunk.get("eval_duration", 0) / 1e9
            }
            if on_complete:
                on_complete("".join(tokens), stats)
    return "".join(tokens)

result = stream_with_callback(
    "List 3 benefits of Delta Lake",
    on_token=lambda t, n: print(t, end="", flush=True),
    on_complete=lambda text, stats: print(f"\\nDone: {stats}")
)`,
  },

  {
    id: 7,
    category: 'Custom',
    title: 'Custom Modelfile for Ollama',
    desc: 'Create a custom Ollama model with a specialized system prompt and parameters for domain-specific tasks',
    code: `import ollama
import subprocess

# Create a custom Modelfile for a Databricks-specialized assistant
modelfile_content = \"\"\"
FROM llama3.1:8b

# Set model parameters
PARAMETER temperature 0.3
PARAMETER top_p 0.85
PARAMETER num_predict 1024
PARAMETER stop "<|end|>"
PARAMETER stop "Human:"

# System prompt baked into the model
SYSTEM You are a Databricks and Apache Spark expert assistant. You provide concise, production-ready code examples. Always use best practices: Delta Lake for storage, Unity Catalog for governance, and structured streaming for real-time pipelines. When writing PySpark, include error handling and logging.

# Custom template
TEMPLATE \"\"\"{{ if .System }}<|system|>
{{ .System }}<|end|>
{{ end }}{{ if .Prompt }}<|user|>
{{ .Prompt }}<|end|>
{{ end }}<|assistant|>
{{ .Response }}<|end|>\"\"\"
\"\"\"

# Write modelfile to disk
modelfile_path = "/tmp/Modelfile.databricks"
with open(modelfile_path, "w") as f:
    f.write(modelfile_content)

# Create the custom model
ollama.create(model="databricks-assistant:latest", modelfile=modelfile_content)
print("Custom model 'databricks-assistant:latest' created successfully!")

# Test the custom model
response = ollama.chat(
    model="databricks-assistant:latest",
    messages=[{"role": "user", "content": "Write a PySpark job that reads JSON, deduplicates, and writes to Delta."}]
)
print(f"Response:\\n{response['message']['content']}")

# Verify model parameters
info = ollama.show("databricks-assistant:latest")
print(f"\\nModel parameters: {info.get('parameters', 'N/A')}")
print(f"System prompt: {info.get('system', 'N/A')[:100]}...")`,
  },

  {
    id: 8,
    category: 'PySpark',
    title: 'PySpark UDF with Ollama',
    desc: 'Create a PySpark UDF that uses Ollama for text enrichment across distributed DataFrame partitions',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import ollama
import json

# Schema for enriched output
enrichment_schema = StructType([
    StructField("summary", StringType()),
    StructField("sentiment", StringType()),
    StructField("confidence", FloatType()),
    StructField("keywords", StringType())
])

# Pandas UDF for batch processing on driver
def enrich_texts(iterator):
    import pandas as pd
    for pdf in iterator:
        summaries, sentiments, confidences, keywords_list = [], [], [], []
        for text in pdf["raw_text"]:
            try:
                resp = ollama.chat(
                    model="llama3.1:8b",
                    messages=[{
                        "role": "system",
                        "content": "Analyze the text. Return JSON: {summary, sentiment, confidence, keywords}"
                    }, {
                        "role": "user",
                        "content": text
                    }],
                    format="json",
                    options={"temperature": 0.1, "num_predict": 256}
                )
                result = json.loads(resp["message"]["content"])
                summaries.append(result.get("summary", ""))
                sentiments.append(result.get("sentiment", "neutral"))
                confidences.append(float(result.get("confidence", 0.0)))
                keywords_list.append(",".join(result.get("keywords", [])))
            except Exception as e:
                summaries.append(f"Error: {str(e)}")
                sentiments.append("unknown")
                confidences.append(0.0)
                keywords_list.append("")

        pdf["summary"] = summaries
        pdf["sentiment"] = sentiments
        pdf["confidence"] = confidences
        pdf["keywords"] = keywords_list
        yield pdf

# Source DataFrame
raw_df = spark.read.table("bronze.customer_feedback")

# Apply enrichment (coalesce to driver where Ollama runs)
enriched_df = raw_df.coalesce(1).mapInPandas(
    enrich_texts,
    schema=StructType([
        StructField("feedback_id", StringType()),
        StructField("raw_text", StringType()),
        StructField("summary", StringType()),
        StructField("sentiment", StringType()),
        StructField("confidence", FloatType()),
        StructField("keywords", StringType())
    ])
)

enriched_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("silver.enriched_feedback")
print(f"Enriched {enriched_df.count()} feedback records")`,
  },
];

const ragScenarios = [
  {
    id: 1,
    category: 'Chunking',
    title: 'Text Chunking — Fixed Size',
    desc: 'Split documents into fixed-size overlapping chunks for RAG ingestion',
    code: `from langchain.text_splitter import CharacterTextSplitter, RecursiveCharacterTextSplitter

# Fixed-size chunking with overlap
text = open("/dbfs/mnt/docs/databricks_guide.txt").read()
print(f"Document length: {len(text)} characters")

# Simple character-based splitting
char_splitter = CharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200,
    separator="\\n",
    length_function=len
)
char_chunks = char_splitter.split_text(text)
print(f"Character chunks: {len(char_chunks)}")

# Recursive splitting (preferred — respects text structure)
recursive_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200,
    separators=["\\n\\n", "\\n", ". ", " ", ""],
    length_function=len
)
recursive_chunks = recursive_splitter.split_text(text)
print(f"Recursive chunks: {len(recursive_chunks)}")

# Token-based splitting (aligns with model context)
from langchain.text_splitter import TokenTextSplitter

token_splitter = TokenTextSplitter(
    chunk_size=256,
    chunk_overlap=50,
    encoding_name="cl100k_base"
)
token_chunks = token_splitter.split_text(text)
print(f"Token chunks: {len(token_chunks)}")

# Display chunk statistics
for i, chunk in enumerate(recursive_chunks[:3]):
    print(f"  Chunk {i}: {len(chunk)} chars | Preview: {chunk[:80]}...")`,
  },

  {
    id: 2,
    category: 'Chunking',
    title: 'Text Chunking — Recursive/Semantic',
    desc: 'Advanced chunking strategies using recursive splitting and semantic similarity boundaries',
    code: `from langchain.text_splitter import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
import numpy as np

# Semantic chunking — split at meaning boundaries
class SemanticChunker:
    def __init__(self, model_name="all-MiniLM-L6-v2", threshold=0.5):
        self.model = SentenceTransformer(model_name)
        self.threshold = threshold

    def chunk(self, text, min_chunk_size=100):
        sentences = [s.strip() for s in text.split(". ") if len(s.strip()) > 20]
        if len(sentences) < 2:
            return [text]

        embeddings = self.model.encode(sentences)
        chunks = []
        current_chunk = [sentences[0]]

        for i in range(1, len(sentences)):
            similarity = np.dot(embeddings[i], embeddings[i - 1]) / (
                np.linalg.norm(embeddings[i]) * np.linalg.norm(embeddings[i - 1])
            )
            if similarity < self.threshold and len(". ".join(current_chunk)) >= min_chunk_size:
                chunks.append(". ".join(current_chunk) + ".")
                current_chunk = [sentences[i]]
            else:
                current_chunk.append(sentences[i])

        if current_chunk:
            chunks.append(". ".join(current_chunk) + ".")
        return chunks

# Usage
chunker = SemanticChunker(threshold=0.45)
text = open("/dbfs/mnt/docs/architecture_doc.txt").read()
semantic_chunks = chunker.chunk(text)

print(f"Semantic chunks created: {len(semantic_chunks)}")
for i, chunk in enumerate(semantic_chunks[:3]):
    print(f"  Chunk {i}: {len(chunk)} chars | {chunk[:100]}...")

# Markdown/code-aware splitting
from langchain.text_splitter import MarkdownTextSplitter

md_splitter = MarkdownTextSplitter(chunk_size=1000, chunk_overlap=100)
md_chunks = md_splitter.split_text(open("/dbfs/mnt/docs/README.md").read())
print(f"\\nMarkdown-aware chunks: {len(md_chunks)}")`,
  },

  {
    id: 3,
    category: 'Embeddings',
    title: 'Generate Embeddings with Sentence-Transformers',
    desc: 'Compute dense vector embeddings using sentence-transformers for semantic search and RAG',
    code: `from sentence_transformers import SentenceTransformer
import numpy as np
import time

# Load embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")
print(f"Model loaded — embedding dimension: {model.get_sentence_embedding_dimension()}")

# Single text embedding
text = "Delta Lake provides ACID transactions on Apache Spark"
embedding = model.encode(text, normalize_embeddings=True)
print(f"Embedding shape: {embedding.shape}, L2 norm: {np.linalg.norm(embedding):.4f}")

# Batch embedding with progress
documents = [
    "Unity Catalog provides centralized data governance",
    "Structured Streaming enables real-time data processing",
    "MLflow manages the full ML lifecycle",
    "Photon engine accelerates Spark SQL queries",
    "Delta Sharing allows secure cross-organization data sharing",
    "Databricks Workflows orchestrate multi-task jobs",
    "Feature Store centralizes feature engineering",
    "Auto Loader incrementally ingests new files from cloud storage"
]

start = time.time()
doc_embeddings = model.encode(
    documents,
    batch_size=32,
    show_progress_bar=True,
    normalize_embeddings=True
)
elapsed = time.time() - start
print(f"Encoded {len(documents)} docs in {elapsed:.2f}s — shape: {doc_embeddings.shape}")

# Semantic search
query = "How do I govern data in Databricks?"
query_emb = model.encode(query, normalize_embeddings=True)

scores = np.dot(doc_embeddings, query_emb)
ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)

print(f"\\nQuery: {query}")
for idx, score in ranked[:3]:
    print(f"  {score:.4f} | {documents[idx]}")`,
  },

  {
    id: 4,
    category: 'VectorDB',
    title: 'Store and Query ChromaDB',
    desc: 'Use ChromaDB as a persistent vector database for RAG document storage and retrieval',
    code: `import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

# Initialize persistent ChromaDB
client = chromadb.PersistentClient(path="/dbfs/mnt/vectordb/chromadb")

# Create or get a collection
collection = client.get_or_create_collection(
    name="databricks_docs",
    metadata={"hnsw:space": "cosine", "hnsw:M": 16, "hnsw:construction_ef": 200}
)

# Prepare documents and embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")
documents = [
    "Delta Lake supports time travel to query historical data versions",
    "Unity Catalog provides fine-grained access control at column level",
    "Auto Loader uses file notification for efficient incremental ingestion",
    "Photon is a C++ vectorized engine that accelerates Spark SQL",
    "Databricks SQL Warehouse provides serverless compute for BI queries"
]

embeddings = model.encode(documents, normalize_embeddings=True).tolist()

# Upsert documents with metadata
collection.upsert(
    ids=[f"doc_{i}" for i in range(len(documents))],
    embeddings=embeddings,
    documents=documents,
    metadatas=[
        {"source": "delta_docs", "topic": "storage"},
        {"source": "unity_docs", "topic": "governance"},
        {"source": "ingestion_docs", "topic": "ingestion"},
        {"source": "performance_docs", "topic": "compute"},
        {"source": "sql_docs", "topic": "analytics"}
    ]
)
print(f"Collection '{collection.name}' has {collection.count()} documents")

# Query with semantic search
query = "How do I access previous versions of my data?"
query_emb = model.encode(query, normalize_embeddings=True).tolist()

results = collection.query(
    query_embeddings=[query_emb],
    n_results=3,
    include=["documents", "metadatas", "distances"]
)

print(f"\\nQuery: {query}")
for i, (doc, meta, dist) in enumerate(zip(
    results["documents"][0], results["metadatas"][0], results["distances"][0]
)):
    print(f"  {i+1}. [{1-dist:.4f}] {doc} (source: {meta['source']})")`,
  },

  {
    id: 5,
    category: 'Pre-Retrieval',
    title: 'Pre-Retrieval: Query Expansion and HyDE',
    desc: 'Improve retrieval with query expansion and Hypothetical Document Embeddings (HyDE)',
    code: `import ollama
from sentence_transformers import SentenceTransformer
import numpy as np

model = SentenceTransformer("all-MiniLM-L6-v2")

# --- Query Expansion ---
def expand_query(original_query, n_expansions=3):
    """Generate multiple query reformulations for better recall."""
    response = ollama.chat(
        model="llama3.1:8b",
        messages=[{
            "role": "system",
            "content": f"Generate {n_expansions} alternative phrasings of the query. Return one per line, no numbering."
        }, {
            "role": "user",
            "content": original_query
        }],
        options={"temperature": 0.8, "num_predict": 200}
    )
    expansions = [q.strip() for q in response["message"]["content"].strip().split("\\n") if q.strip()]
    return [original_query] + expansions[:n_expansions]

query = "How do I handle schema changes in Delta Lake?"
expanded = expand_query(query)
print("Expanded queries:")
for q in expanded:
    print(f"  - {q}")

# Encode all expanded queries and average their embeddings
expanded_embs = model.encode(expanded, normalize_embeddings=True)
fused_embedding = np.mean(expanded_embs, axis=0)
fused_embedding = fused_embedding / np.linalg.norm(fused_embedding)

# --- HyDE (Hypothetical Document Embeddings) ---
def generate_hyde_document(query):
    """Generate a hypothetical answer to use as the search embedding."""
    response = ollama.chat(
        model="llama3.1:8b",
        messages=[{
            "role": "system",
            "content": "Write a short, factual paragraph that directly answers the question."
        }, {
            "role": "user",
            "content": query
        }],
        options={"temperature": 0.3, "num_predict": 200}
    )
    return response["message"]["content"]

hyde_doc = generate_hyde_document(query)
print(f"\\nHyDE document: {hyde_doc[:200]}...")

hyde_embedding = model.encode(hyde_doc, normalize_embeddings=True)

# Use hyde_embedding for vector search instead of raw query embedding
# This often retrieves more relevant documents because the hypothetical
# answer is semantically closer to actual documents than the question is`,
  },

  {
    id: 6,
    category: 'Post-Retrieval',
    title: 'Post-Retrieval: Reranking and MMR',
    desc: 'Rerank retrieved documents and apply Maximal Marginal Relevance for diverse, relevant results',
    code: `import ollama
from sentence_transformers import SentenceTransformer, CrossEncoder
import numpy as np

# --- Cross-Encoder Reranking ---
reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

query = "How does Delta Lake handle concurrent writes?"
retrieved_docs = [
    "Delta Lake uses optimistic concurrency control for concurrent writes",
    "Apache Spark supports parallel processing across cluster nodes",
    "Delta Lake provides ACID transactions using a transaction log",
    "Databricks clusters can auto-scale based on workload",
    "Write conflicts in Delta are resolved using serializable isolation"
]

# Rerank with cross-encoder
pairs = [(query, doc) for doc in retrieved_docs]
scores = reranker.predict(pairs)

reranked = sorted(zip(scores, retrieved_docs), key=lambda x: x[0], reverse=True)
print("Reranked results:")
for score, doc in reranked:
    print(f"  {score:.4f} | {doc}")

# --- Maximal Marginal Relevance (MMR) ---
def mmr_select(query_emb, doc_embs, documents, k=3, lambda_param=0.7):
    """Select k documents balancing relevance and diversity."""
    selected = []
    remaining = list(range(len(documents)))
    query_sims = np.dot(doc_embs, query_emb)

    for _ in range(min(k, len(documents))):
        mmr_scores = []
        for idx in remaining:
            relevance = query_sims[idx]
            diversity = 0
            if selected:
                selected_embs = doc_embs[selected]
                max_sim = np.max(np.dot(selected_embs, doc_embs[idx]))
                diversity = max_sim
            score = lambda_param * relevance - (1 - lambda_param) * diversity
            mmr_scores.append((idx, score))

        best_idx = max(mmr_scores, key=lambda x: x[1])[0]
        selected.append(best_idx)
        remaining.remove(best_idx)

    return [(documents[i], query_sims[i]) for i in selected]

embed_model = SentenceTransformer("all-MiniLM-L6-v2")
query_emb = embed_model.encode(query, normalize_embeddings=True)
doc_embs = embed_model.encode(retrieved_docs, normalize_embeddings=True)

mmr_results = mmr_select(query_emb, doc_embs, retrieved_docs, k=3, lambda_param=0.6)
print("\\nMMR-selected (diverse) results:")
for doc, score in mmr_results:
    print(f"  {score:.4f} | {doc}")`,
  },

  {
    id: 7,
    category: 'Pipeline',
    title: 'Full RAG Pipeline',
    desc: 'End-to-end RAG pipeline: ingest documents, chunk, embed, store, retrieve, and generate answers',
    code: `import ollama
import chromadb
from sentence_transformers import SentenceTransformer
from langchain.text_splitter import RecursiveCharacterTextSplitter
import os, glob

# --- Step 1: Ingest and Chunk ---
splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=150)
all_chunks = []
all_metadatas = []

for filepath in glob.glob("/dbfs/mnt/docs/*.txt"):
    text = open(filepath).read()
    chunks = splitter.split_text(text)
    filename = os.path.basename(filepath)
    for i, chunk in enumerate(chunks):
        all_chunks.append(chunk)
        all_metadatas.append({"source": filename, "chunk_index": i})

print(f"Ingested {len(all_chunks)} chunks from {len(glob.glob('/dbfs/mnt/docs/*.txt'))} files")

# --- Step 2: Embed and Store ---
embed_model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = embed_model.encode(all_chunks, batch_size=64, normalize_embeddings=True, show_progress_bar=True)

client = chromadb.PersistentClient(path="/dbfs/mnt/vectordb/rag_store")
collection = client.get_or_create_collection("rag_knowledge_base", metadata={"hnsw:space": "cosine"})

collection.upsert(
    ids=[f"chunk_{i}" for i in range(len(all_chunks))],
    embeddings=embeddings.tolist(),
    documents=all_chunks,
    metadatas=all_metadatas
)
print(f"Stored {collection.count()} chunks in ChromaDB")

# --- Step 3: Retrieve ---
def retrieve(query, top_k=5):
    query_emb = embed_model.encode(query, normalize_embeddings=True).tolist()
    results = collection.query(query_embeddings=[query_emb], n_results=top_k, include=["documents", "metadatas", "distances"])
    return list(zip(results["documents"][0], results["metadatas"][0], results["distances"][0]))

# --- Step 4: Generate ---
def rag_answer(query, top_k=5):
    retrieved = retrieve(query, top_k)
    context = "\\n\\n".join([f"[Source: {m['source']}] {doc}" for doc, m, _ in retrieved])

    response = ollama.chat(
        model="llama3.1:8b",
        messages=[
            {"role": "system", "content": "Answer based on the provided context. Cite sources. If the context is insufficient, say so."},
            {"role": "user", "content": f"Context:\\n{context}\\n\\nQuestion: {query}"}
        ],
        options={"temperature": 0.3, "num_predict": 512}
    )
    return {
        "answer": response["message"]["content"],
        "sources": [{"source": m["source"], "score": 1 - d, "preview": doc[:100]} for doc, m, d in retrieved]
    }

result = rag_answer("How do I set up incremental ingestion with Auto Loader?")
print(f"Answer: {result['answer']}")
print(f"\\nSources:")
for s in result["sources"]:
    print(f"  [{s['score']:.3f}] {s['source']} — {s['preview']}...")`,
  },

  {
    id: 8,
    category: 'Evaluation',
    title: 'RAG Evaluation: Faithfulness and Relevance',
    desc: 'Evaluate RAG outputs for faithfulness to context, answer relevance, and retrieval quality',
    code: `import ollama
import json

# --- Faithfulness Evaluation ---
def evaluate_faithfulness(question, context, answer):
    """Check if the answer is grounded in the provided context."""
    response = ollama.chat(
        model="llama3.1:8b",
        messages=[{
            "role": "system",
            "content": "Evaluate if the answer is faithful to the context. Return JSON: {faithful: bool, score: 0-1, reason: str}"
        }, {
            "role": "user",
            "content": f"Context: {context}\\n\\nQuestion: {question}\\n\\nAnswer: {answer}"
        }],
        format="json",
        options={"temperature": 0.1}
    )
    return json.loads(response["message"]["content"])

# --- Answer Relevance ---
def evaluate_relevance(question, answer):
    """Check if the answer actually addresses the question."""
    response = ollama.chat(
        model="llama3.1:8b",
        messages=[{
            "role": "system",
            "content": "Rate how well the answer addresses the question. Return JSON: {relevant: bool, score: 0-1, reason: str}"
        }, {
            "role": "user",
            "content": f"Question: {question}\\n\\nAnswer: {answer}"
        }],
        format="json",
        options={"temperature": 0.1}
    )
    return json.loads(response["message"]["content"])

# --- Retrieval Quality ---
def evaluate_retrieval(question, retrieved_docs):
    """Evaluate if the retrieved documents contain information needed to answer."""
    docs_text = "\\n".join([f"Doc {i+1}: {doc[:200]}" for i, doc in enumerate(retrieved_docs)])
    response = ollama.chat(
        model="llama3.1:8b",
        messages=[{
            "role": "system",
            "content": "Evaluate retrieval quality. Return JSON: {precision: 0-1, relevant_docs: [int], reason: str}"
        }, {
            "role": "user",
            "content": f"Question: {question}\\n\\nRetrieved docs:\\n{docs_text}"
        }],
        format="json",
        options={"temperature": 0.1}
    )
    return json.loads(response["message"]["content"])

# Run evaluation suite
question = "How does Delta Lake handle schema evolution?"
context = "Delta Lake supports schema evolution through mergeSchema and overwriteSchema options. New columns can be added automatically during writes."
answer = "Delta Lake handles schema evolution by allowing mergeSchema during writes, which automatically adds new columns."
retrieved = [context, "Spark SQL supports DataFrame operations", "Delta Lake provides ACID transactions"]

faith = evaluate_faithfulness(question, context, answer)
relev = evaluate_relevance(question, answer)
retrieval = evaluate_retrieval(question, retrieved)

print(f"Faithfulness: {faith['score']}/1.0 — {faith['reason']}")
print(f"Relevance:    {relev['score']}/1.0 — {relev['reason']}")
print(f"Retrieval:    {retrieval['precision']}/1.0 — Relevant docs: {retrieval['relevant_docs']}")`,
  },

  {
    id: 9,
    category: 'Multimodal',
    title: 'RAG with Images',
    desc: 'Build a multimodal RAG pipeline that handles both text and image documents',
    code: `import ollama
import chromadb
from sentence_transformers import SentenceTransformer
from PIL import Image
import base64, io, os, glob

# Encode image to base64 for multimodal models
def image_to_base64(image_path):
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

# Generate text description of an image using a vision model
def describe_image(image_path):
    img_b64 = image_to_base64(image_path)
    response = ollama.chat(
        model="llava:13b",
        messages=[{
            "role": "user",
            "content": "Describe this image in detail, focusing on data, charts, architecture diagrams, or technical content.",
            "images": [img_b64]
        }],
        options={"temperature": 0.3, "num_predict": 300}
    )
    return response["message"]["content"]

# Index images by generating text descriptions and embedding them
embed_model = SentenceTransformer("all-MiniLM-L6-v2")
client = chromadb.PersistentClient(path="/dbfs/mnt/vectordb/multimodal_rag")
collection = client.get_or_create_collection("images_and_text", metadata={"hnsw:space": "cosine"})

image_files = glob.glob("/dbfs/mnt/docs/images/*.png")
for i, img_path in enumerate(image_files):
    description = describe_image(img_path)
    embedding = embed_model.encode(description, normalize_embeddings=True).tolist()
    collection.upsert(
        ids=[f"img_{i}"],
        embeddings=[embedding],
        documents=[description],
        metadatas=[{"type": "image", "path": img_path, "filename": os.path.basename(img_path)}]
    )
    print(f"Indexed {os.path.basename(img_path)}: {description[:80]}...")

print(f"\\nTotal indexed items: {collection.count()}")

# Query the multimodal RAG
query = "Show me the data pipeline architecture"
query_emb = embed_model.encode(query, normalize_embeddings=True).tolist()
results = collection.query(query_embeddings=[query_emb], n_results=3, include=["documents", "metadatas", "distances"])

for doc, meta, dist in zip(results["documents"][0], results["metadatas"][0], results["distances"][0]):
    print(f"  [{1-dist:.3f}] {meta.get('filename', 'text')} — {doc[:100]}...")`,
  },

  {
    id: 10,
    category: 'Databricks',
    title: 'RAG with Databricks Vector Search',
    desc: 'Use Databricks Vector Search as the vector store for production RAG workloads',
    code: `from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
import pyspark.sql.functions as F

# Initialize Vector Search client
vsc = VectorSearchClient()

# Create a Vector Search endpoint
vsc.create_endpoint(name="rag_endpoint", endpoint_type="STANDARD")

# Create a Delta Sync index from a Delta table
# First, prepare the source table with embeddings
source_table = "catalog.schema.document_chunks"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {source_table} (
        chunk_id STRING,
        content STRING,
        embedding ARRAY<FLOAT>,
        source_file STRING,
        chunk_index INT
    ) USING DELTA
""")

# Create the vector search index
index = vsc.create_delta_sync_index(
    endpoint_name="rag_endpoint",
    index_name="catalog.schema.doc_chunks_index",
    source_table_name=source_table,
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_dimension=384,
    embedding_vector_column="embedding",
    columns_to_sync=["content", "source_file", "chunk_index"]
)
print(f"Index created: {index.name}")

# Wait for index to be ready
import time
while not vsc.get_index("rag_endpoint", "catalog.schema.doc_chunks_index").describe().get("status", {}).get("ready"):
    print("Waiting for index to sync...")
    time.sleep(30)

# Query the vector search index
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
query_emb = model.encode("How to implement SCD Type 2 in Delta Lake?", normalize_embeddings=True).tolist()

results = vsc.get_index("rag_endpoint", "catalog.schema.doc_chunks_index").similarity_search(
    query_vector=query_emb,
    num_results=5,
    columns=["content", "source_file", "chunk_index"]
)

for row in results.get("result", {}).get("data_array", []):
    print(f"  Score: {row[-1]:.4f} | {row[0][:100]}...")`,
  },

  {
    id: 11,
    category: 'Advanced',
    title: 'Multi-Document and Conversational RAG',
    desc: 'Build RAG that handles multiple document sources with conversation history for follow-up questions',
    code: `import ollama
import chromadb
from sentence_transformers import SentenceTransformer
from datetime import datetime

embed_model = SentenceTransformer("all-MiniLM-L6-v2")
client = chromadb.PersistentClient(path="/dbfs/mnt/vectordb/conv_rag")

# Create separate collections per document source
sources = {
    "delta_docs": client.get_or_create_collection("delta_docs"),
    "spark_docs": client.get_or_create_collection("spark_docs"),
    "mlflow_docs": client.get_or_create_collection("mlflow_docs")
}

# Multi-source retrieval
def multi_source_retrieve(query, top_k_per_source=3):
    query_emb = embed_model.encode(query, normalize_embeddings=True).tolist()
    all_results = []
    for source_name, collection in sources.items():
        if collection.count() == 0:
            continue
        results = collection.query(query_embeddings=[query_emb], n_results=top_k_per_source,
                                   include=["documents", "metadatas", "distances"])
        for doc, meta, dist in zip(results["documents"][0], results["metadatas"][0], results["distances"][0]):
            all_results.append({"doc": doc, "source": source_name, "score": 1 - dist, "meta": meta})

    all_results.sort(key=lambda x: x["score"], reverse=True)
    return all_results[:top_k_per_source * 2]

# Conversational RAG with memory
class ConversationalRAG:
    def __init__(self, model_name="llama3.1:8b", max_history=5):
        self.model_name = model_name
        self.max_history = max_history
        self.history = []

    def _contextualize_query(self, query):
        if not self.history:
            return query
        history_text = "\\n".join([f"Q: {h['q']}\\nA: {h['a'][:200]}" for h in self.history[-3:]])
        resp = ollama.chat(model=self.model_name, messages=[
            {"role": "system", "content": "Rewrite the query to be self-contained using conversation history. Return only the rewritten query."},
            {"role": "user", "content": f"History:\\n{history_text}\\n\\nNew query: {query}"}
        ], options={"temperature": 0.1, "num_predict": 100})
        return resp["message"]["content"].strip()

    def ask(self, query):
        contextualized = self._contextualize_query(query)
        retrieved = multi_source_retrieve(contextualized)
        context = "\\n".join([f"[{r['source']}] {r['doc']}" for r in retrieved])

        messages = [{"role": "system", "content": "Answer using the context. Cite sources in brackets. Maintain conversation continuity."}]
        for h in self.history[-self.max_history:]:
            messages.append({"role": "user", "content": h["q"]})
            messages.append({"role": "assistant", "content": h["a"]})
        messages.append({"role": "user", "content": f"Context:\\n{context}\\n\\nQuestion: {query}"})

        resp = ollama.chat(model=self.model_name, messages=messages, options={"temperature": 0.3, "num_predict": 512})
        answer = resp["message"]["content"]
        self.history.append({"q": query, "a": answer, "ts": datetime.utcnow().isoformat()})
        return {"answer": answer, "sources": [{"source": r["source"], "score": r["score"]} for r in retrieved]}

rag = ConversationalRAG()
r1 = rag.ask("What is Delta Lake?")
print(f"A1: {r1['answer'][:200]}...")
r2 = rag.ask("How does it handle schema changes?")  # uses conversation context
print(f"A2: {r2['answer'][:200]}...")`,
  },

  {
    id: 12,
    category: 'Framework',
    title: 'RAG with LangChain',
    desc: 'Build a complete RAG pipeline using LangChain with Ollama and ChromaDB integration',
    code: `from langchain_community.llms import Ollama
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.vectorstores import Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_community.document_loaders import DirectoryLoader, TextLoader

# --- Step 1: Load documents ---
loader = DirectoryLoader("/dbfs/mnt/docs/", glob="**/*.txt", loader_cls=TextLoader)
documents = loader.load()
print(f"Loaded {len(documents)} documents")

# --- Step 2: Split into chunks ---
splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200, separators=["\\n\\n", "\\n", ". ", " "])
chunks = splitter.split_documents(documents)
print(f"Created {len(chunks)} chunks")

# --- Step 3: Create vector store ---
embeddings = OllamaEmbeddings(model="nomic-embed-text", base_url="http://localhost:11434")
vectorstore = Chroma.from_documents(
    documents=chunks,
    embedding=embeddings,
    persist_directory="/dbfs/mnt/vectordb/langchain_rag",
    collection_name="langchain_docs"
)
print(f"Vector store created with {vectorstore._collection.count()} documents")

# --- Step 4: Build retrieval chain ---
llm = Ollama(model="llama3.1:8b", temperature=0.3, num_predict=512)

prompt_template = PromptTemplate(
    template=\"\"\"Use the following context to answer the question. If you don't know, say so.

Context:
{context}

Question: {question}

Answer:\"\"\",
    input_variables=["context", "question"]
)

qa_chain = RetrievalQA.from_chain_type(
    llm=llm,
    chain_type="stuff",
    retriever=vectorstore.as_retriever(search_type="mmr", search_kwargs={"k": 5, "fetch_k": 10}),
    chain_type_kwargs={"prompt": prompt_template},
    return_source_documents=True
)

# --- Step 5: Query ---
result = qa_chain.invoke({"query": "How do I implement incremental data ingestion?"})

print(f"Answer: {result['result']}")
print(f"\\nSources:")
for doc in result["source_documents"]:
    print(f"  - {doc.metadata.get('source', 'unknown')}: {doc.page_content[:100]}...")`,
  },
];

const mcpScenarios = [
  {
    id: 1,
    category: 'Setup',
    title: 'MCP Server Setup',
    desc: 'Set up a Model Context Protocol server to expose tools and resources to LLM agents',
    code: `from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
import asyncio
import json

# Create an MCP server instance
server = Server("databricks-mcp-server")

# Define the list of available tools
@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="run_sql_query",
            description="Execute a SQL query on Databricks SQL Warehouse and return results",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL query to execute"},
                    "warehouse_id": {"type": "string", "description": "SQL Warehouse ID"},
                    "max_rows": {"type": "integer", "default": 100, "description": "Max rows to return"}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="list_tables",
            description="List all tables in a given catalog and schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "catalog": {"type": "string"},
                    "schema": {"type": "string"}
                },
                "required": ["catalog", "schema"]
            }
        )
    ]

# Handle tool calls
@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name == "run_sql_query":
        query = arguments["query"]
        max_rows = arguments.get("max_rows", 100)
        # Execute via Databricks SQL connector
        from databricks import sql as dbsql
        with dbsql.connect(server_hostname=HOSTNAME, http_path=HTTP_PATH, access_token=TOKEN) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchmany(max_rows)
                result = [dict(zip(columns, row)) for row in rows]
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    elif name == "list_tables":
        catalog = arguments["catalog"]
        schema = arguments["schema"]
        result = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        tables = [{"name": r.tableName, "type": r.tableType} for r in result]
        return [TextContent(type="text", text=json.dumps(tables, indent=2))]

# Run the server
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream)

asyncio.run(main())`,
  },

  {
    id: 2,
    category: 'Tools',
    title: 'MCP Tool Registration',
    desc: 'Register custom tools with MCP for data operations, file management, and pipeline control',
    code: `from mcp.server import Server
from mcp.types import Tool, TextContent
import json

server = Server("data-tools-mcp")

# Register multiple domain-specific tools
TOOLS = {
    "describe_table": {
        "description": "Get schema, row count, and statistics for a Delta table",
        "schema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name (catalog.schema.table)"}
            },
            "required": ["table_name"]
        }
    },
    "run_data_quality_check": {
        "description": "Run data quality checks on a table: nulls, duplicates, value ranges",
        "schema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string"},
                "columns": {"type": "array", "items": {"type": "string"}},
                "checks": {"type": "array", "items": {"type": "string", "enum": ["nulls", "duplicates", "ranges", "patterns"]}}
            },
            "required": ["table_name"]
        }
    },
    "trigger_pipeline": {
        "description": "Trigger a Databricks job or pipeline by name or ID",
        "schema": {
            "type": "object",
            "properties": {
                "pipeline_id": {"type": "string"},
                "parameters": {"type": "object", "description": "Key-value parameters to pass"}
            },
            "required": ["pipeline_id"]
        }
    }
}

@server.list_tools()
async def list_tools():
    return [
        Tool(name=name, description=config["description"], inputSchema=config["schema"])
        for name, config in TOOLS.items()
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name == "describe_table":
        table = arguments["table_name"]
        df = spark.table(table)
        schema_info = [{"name": f.name, "type": str(f.dataType), "nullable": f.nullable} for f in df.schema.fields]
        row_count = df.count()
        result = {"table": table, "columns": schema_info, "row_count": row_count}
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    elif name == "run_data_quality_check":
        table = arguments["table_name"]
        df = spark.table(table)
        checks_result = {}
        columns = arguments.get("columns", [f.name for f in df.schema.fields])
        for col in columns:
            null_count = df.filter(f"{col} IS NULL").count()
            checks_result[col] = {"null_count": null_count, "null_pct": round(null_count / df.count() * 100, 2)}
        return [TextContent(type="text", text=json.dumps(checks_result, indent=2))]

    elif name == "trigger_pipeline":
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        run = w.jobs.run_now(job_id=int(arguments["pipeline_id"]), job_parameters=arguments.get("parameters", {}))
        return [TextContent(type="text", text=f"Pipeline triggered. Run ID: {run.run_id}")]`,
  },

  {
    id: 3,
    category: 'Context',
    title: 'MCP Context Passing',
    desc: 'Pass rich context (resources, prompts, conversation state) between MCP clients and servers',
    code: `from mcp.server import Server
from mcp.types import Resource, TextContent, Prompt, PromptMessage, PromptArgument
import json

server = Server("context-aware-mcp")

# --- Resources: Expose data as context for LLMs ---
@server.list_resources()
async def list_resources():
    return [
        Resource(
            uri="databricks://catalog/main/tables",
            name="Available Tables",
            description="List of all tables in the main catalog",
            mimeType="application/json"
        ),
        Resource(
            uri="databricks://cluster/status",
            name="Cluster Status",
            description="Current cluster compute status and configuration",
            mimeType="application/json"
        ),
        Resource(
            uri="databricks://jobs/recent",
            name="Recent Job Runs",
            description="Last 10 job runs with status and duration",
            mimeType="application/json"
        )
    ]

@server.read_resource()
async def read_resource(uri: str):
    if uri == "databricks://catalog/main/tables":
        tables = spark.sql("SHOW TABLES IN main.default").collect()
        data = [{"table": r.tableName, "database": r.database, "type": r.tableType} for r in tables]
        return json.dumps(data, indent=2)

    elif uri == "databricks://cluster/status":
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        cluster = w.clusters.get(cluster_id)
        return json.dumps({"id": cluster_id, "state": cluster.state.value, "num_workers": cluster.num_workers,
                           "spark_version": cluster.spark_version, "node_type": cluster.node_type_id}, indent=2)

    elif uri == "databricks://jobs/recent":
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        runs = list(w.jobs.list_runs(limit=10))
        data = [{"run_id": r.run_id, "job_id": r.job_id, "state": r.state.result_state.value if r.state.result_state else "RUNNING",
                  "duration_s": (r.end_time - r.start_time) / 1000 if r.end_time else None} for r in runs]
        return json.dumps(data, indent=2, default=str)

# --- Prompts: Reusable prompt templates ---
@server.list_prompts()
async def list_prompts():
    return [
        Prompt(
            name="analyze_table",
            description="Generate a data analysis for a specified table",
            arguments=[PromptArgument(name="table_name", required=True, description="Fully qualified table name")]
        )
    ]

@server.get_prompt()
async def get_prompt(name: str, arguments: dict):
    if name == "analyze_table":
        table = arguments["table_name"]
        return [PromptMessage(role="user", content=TextContent(
            type="text",
            text=f"Analyze the table {table}. Provide: 1) Schema overview 2) Row count and size 3) Data quality issues 4) Optimization suggestions"
        ))]`,
  },

  {
    id: 4,
    category: 'Advanced',
    title: 'Multi-Tool MCP Orchestration',
    desc: 'Orchestrate multiple MCP tools in sequence for complex multi-step data operations',
    code: `from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters
import asyncio
import json

# MCP Client that orchestrates multiple tool calls
class MCPOrchestrator:
    def __init__(self):
        self.session = None

    async def connect(self, server_command, server_args=None):
        params = StdioServerParameters(command=server_command, args=server_args or [])
        transport = await stdio_client(params).__aenter__()
        self.session = await ClientSession(*transport).__aenter__()
        await self.session.initialize()
        tools = await self.session.list_tools()
        print(f"Connected. Available tools: {[t.name for t in tools.tools]}")

    async def call(self, tool_name, arguments):
        result = await self.session.call_tool(tool_name, arguments)
        return json.loads(result.content[0].text) if result.content else None

    async def run_pipeline(self, table_name):
        """Multi-step: describe -> quality check -> profile -> recommend."""
        print(f"\\n--- Pipeline for {table_name} ---")

        # Step 1: Describe table
        schema = await self.call("describe_table", {"table_name": table_name})
        print(f"1. Schema: {len(schema['columns'])} columns, {schema['row_count']} rows")

        # Step 2: Run quality checks
        quality = await self.call("run_data_quality_check", {
            "table_name": table_name,
            "checks": ["nulls", "duplicates"]
        })
        issues = {k: v for k, v in quality.items() if v.get("null_pct", 0) > 5}
        print(f"2. Quality issues: {len(issues)} columns with >5% nulls")

        # Step 3: Profile data
        profile = await self.call("profile_column_stats", {
            "table_name": table_name,
            "columns": [c["name"] for c in schema["columns"][:5]]
        })
        print(f"3. Profile computed for {len(profile)} columns")

        # Step 4: Generate recommendations
        recommendations = await self.call("generate_recommendations", {
            "schema": schema,
            "quality": quality,
            "profile": profile
        })
        print(f"4. Recommendations: {recommendations}")

        return {"schema": schema, "quality": quality, "profile": profile, "recommendations": recommendations}

# Usage
async def main():
    orchestrator = MCPOrchestrator()
    await orchestrator.connect("python", ["mcp_server.py"])
    result = await orchestrator.run_pipeline("catalog.schema.customer_transactions")
    print(json.dumps(result, indent=2))

asyncio.run(main())`,
  },

  {
    id: 5,
    category: 'Databricks',
    title: 'Databricks MCP Server',
    desc: 'Production MCP server exposing Databricks workspace operations: jobs, clusters, notebooks, and SQL',
    code: `from mcp.server import Server
from mcp.types import Tool, TextContent
from databricks.sdk import WorkspaceClient
import json, os

server = Server("databricks-workspace-mcp")
w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"]
)

@server.list_tools()
async def list_tools():
    return [
        Tool(name="list_clusters", description="List all clusters with status", inputSchema={"type": "object", "properties": {}}),
        Tool(name="get_cluster", description="Get cluster details by ID", inputSchema={
            "type": "object", "properties": {"cluster_id": {"type": "string"}}, "required": ["cluster_id"]}),
        Tool(name="list_jobs", description="List all jobs with recent run status", inputSchema={
            "type": "object", "properties": {"limit": {"type": "integer", "default": 20}}}),
        Tool(name="run_job", description="Trigger a job run by job ID", inputSchema={
            "type": "object", "properties": {"job_id": {"type": "integer"}, "params": {"type": "object"}}, "required": ["job_id"]}),
        Tool(name="execute_sql", description="Run SQL on a warehouse", inputSchema={
            "type": "object", "properties": {"query": {"type": "string"}, "warehouse_id": {"type": "string"}}, "required": ["query", "warehouse_id"]}),
        Tool(name="list_notebooks", description="List notebooks in a workspace path", inputSchema={
            "type": "object", "properties": {"path": {"type": "string", "default": "/"}}, "required": []})
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name == "list_clusters":
        clusters = list(w.clusters.list())
        data = [{"id": c.cluster_id, "name": c.cluster_name, "state": c.state.value,
                  "spark_version": c.spark_version, "node_type": c.node_type_id,
                  "num_workers": c.num_workers} for c in clusters]
        return [TextContent(type="text", text=json.dumps(data, indent=2))]

    elif name == "get_cluster":
        c = w.clusters.get(arguments["cluster_id"])
        return [TextContent(type="text", text=json.dumps({
            "id": c.cluster_id, "name": c.cluster_name, "state": c.state.value,
            "driver": c.driver_node_type_id, "workers": c.num_workers,
            "uptime_min": (c.last_activity_time - c.start_time) / 60000 if c.start_time else 0
        }, indent=2, default=str))]

    elif name == "list_jobs":
        jobs = list(w.jobs.list(limit=arguments.get("limit", 20)))
        data = [{"job_id": j.job_id, "name": j.settings.name, "schedule": getattr(j.settings.schedule, "quartz_cron_expression", None)} for j in jobs]
        return [TextContent(type="text", text=json.dumps(data, indent=2))]

    elif name == "run_job":
        run = w.jobs.run_now(job_id=arguments["job_id"], job_parameters=arguments.get("params", {}))
        return [TextContent(type="text", text=f'{{"run_id": {run.run_id}, "status": "TRIGGERED"}}')]

    elif name == "execute_sql":
        stmt = w.statement_execution.execute_statement(
            warehouse_id=arguments["warehouse_id"], statement=arguments["query"], wait_timeout="30s")
        columns = [c.name for c in stmt.manifest.schema.columns]
        rows = [dict(zip(columns, r)) for r in (stmt.result.data_array or [])]
        return [TextContent(type="text", text=json.dumps(rows[:100], indent=2, default=str))]

    elif name == "list_notebooks":
        objects = list(w.workspace.list(arguments.get("path", "/")))
        data = [{"path": o.path, "type": o.object_type.value} for o in objects]
        return [TextContent(type="text", text=json.dumps(data, indent=2))]`,
  },

  {
    id: 6,
    category: 'Integration',
    title: 'MCP with Ollama',
    desc: 'Connect MCP tools to Ollama for agentic workflows where the LLM decides which tools to call',
    code: `import ollama
import json
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters
import asyncio

class OllamaMCPAgent:
    """Agent that uses Ollama to decide tool calls via MCP."""

    def __init__(self, model="llama3.1:8b"):
        self.model = model
        self.session = None
        self.tools = []
        self.max_iterations = 10

    async def connect(self, server_command, server_args=None):
        params = StdioServerParameters(command=server_command, args=server_args or [])
        transport = await stdio_client(params).__aenter__()
        self.session = await ClientSession(*transport).__aenter__()
        await self.session.initialize()
        tools_response = await self.session.list_tools()
        self.tools = tools_response.tools
        print(f"Agent connected. Tools: {[t.name for t in self.tools]}")

    def _build_tool_prompt(self):
        tool_descs = []
        for t in self.tools:
            params = json.dumps(t.inputSchema.get("properties", {}), indent=2)
            tool_descs.append(f"- {t.name}: {t.description}\\n  Parameters: {params}")
        return "\\n".join(tool_descs)

    async def run(self, user_query):
        system_prompt = f\"\"\"You are a data engineering agent with access to these tools:

{self._build_tool_prompt()}

To call a tool, respond with EXACTLY this JSON format:
{{"tool": "tool_name", "arguments": {{...}}}}

To give a final answer, respond normally without JSON.
Think step by step. Call one tool at a time.\"\"\"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ]

        for i in range(self.max_iterations):
            response = ollama.chat(model=self.model, messages=messages, options={"temperature": 0.1, "num_predict": 512})
            content = response["message"]["content"].strip()
            messages.append({"role": "assistant", "content": content})

            # Check if it's a tool call
            try:
                parsed = json.loads(content)
                if "tool" in parsed:
                    tool_name = parsed["tool"]
                    arguments = parsed.get("arguments", {})
                    print(f"  Step {i+1}: Calling {tool_name}({json.dumps(arguments)})")
                    result = await self.session.call_tool(tool_name, arguments)
                    tool_output = result.content[0].text if result.content else "No output"
                    messages.append({"role": "user", "content": f"Tool result:\\n{tool_output}"})
                    continue
            except (json.JSONDecodeError, KeyError):
                pass

            # Final answer
            print(f"\\nFinal answer (after {i+1} steps):")
            print(content)
            return content

        return "Max iterations reached"

async def main():
    agent = OllamaMCPAgent(model="llama3.1:8b")
    await agent.connect("python", ["databricks_mcp_server.py"])
    await agent.run("Check the customer_orders table for data quality issues and suggest optimizations")

asyncio.run(main())`,
  },

  {
    id: 7,
    category: 'Reliability',
    title: 'MCP Error Handling',
    desc: 'Implement robust error handling, retries, and timeouts for MCP tool calls',
    code: `from mcp.server import Server
from mcp.types import Tool, TextContent, ErrorData
import asyncio
import json
import time
import logging

logger = logging.getLogger("mcp-error-handler")
server = Server("robust-mcp-server")

# --- Retry decorator for MCP tool handlers ---
def with_retry(max_retries=3, base_delay=1.0, timeout=30.0):
    def decorator(func):
        async def wrapper(name, arguments):
            last_error = None
            for attempt in range(max_retries):
                try:
                    result = await asyncio.wait_for(func(name, arguments), timeout=timeout)
                    return result
                except asyncio.TimeoutError:
                    last_error = f"Tool '{name}' timed out after {timeout}s"
                    logger.warning(f"Attempt {attempt+1}/{max_retries}: {last_error}")
                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {last_error}")

                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.info(f"Retrying in {delay}s...")
                    await asyncio.sleep(delay)

            logger.error(f"All {max_retries} attempts failed for tool '{name}': {last_error}")
            return [TextContent(type="text", text=json.dumps({
                "error": True,
                "message": f"Tool failed after {max_retries} attempts: {last_error}",
                "tool": name,
                "arguments": arguments
            }))]
        return wrapper
    return decorator

# --- Input validation ---
def validate_arguments(arguments, required_fields, field_types=None):
    errors = []
    for field in required_fields:
        if field not in arguments:
            errors.append(f"Missing required field: {field}")
    if field_types:
        for field, expected_type in field_types.items():
            if field in arguments and not isinstance(arguments[field], expected_type):
                errors.append(f"Field '{field}' must be {expected_type.__name__}, got {type(arguments[field]).__name__}")
    if errors:
        raise ValueError(f"Validation failed: {'; '.join(errors)}")

@server.call_tool()
@with_retry(max_retries=3, base_delay=1.0, timeout=30.0)
async def call_tool(name: str, arguments: dict):
    # Validate inputs
    if name == "run_sql_query":
        validate_arguments(arguments, ["query"], {"query": str, "max_rows": int})
        query = arguments["query"].strip()
        # Prevent dangerous operations
        dangerous = ["DROP", "DELETE", "TRUNCATE", "ALTER"]
        if any(query.upper().startswith(d) for d in dangerous):
            return [TextContent(type="text", text=json.dumps({
                "error": True, "message": f"Dangerous SQL operation blocked: {query[:50]}"
            }))]
        # Execute with timeout
        result = spark.sql(query).limit(arguments.get("max_rows", 100)).toPandas()
        return [TextContent(type="text", text=result.to_json(orient="records"))]

    return [TextContent(type="text", text=json.dumps({"error": True, "message": f"Unknown tool: {name}"}))]`,
  },

  {
    id: 8,
    category: 'Observability',
    title: 'MCP Monitoring and Logging',
    desc: 'Add monitoring, metrics, and structured logging to MCP servers for production observability',
    code: `from mcp.server import Server
from mcp.types import Tool, TextContent
import json, time, logging
from datetime import datetime, timezone
from collections import defaultdict

logger = logging.getLogger("mcp-monitor")

# --- Metrics collector ---
class MCPMetrics:
    def __init__(self):
        self.call_counts = defaultdict(int)
        self.error_counts = defaultdict(int)
        self.latencies = defaultdict(list)
        self.last_call = {}
        self.start_time = datetime.now(timezone.utc)

    def record_call(self, tool_name, duration_ms, success=True):
        self.call_counts[tool_name] += 1
        self.latencies[tool_name].append(duration_ms)
        self.last_call[tool_name] = datetime.now(timezone.utc).isoformat()
        if not success:
            self.error_counts[tool_name] += 1

    def get_stats(self):
        stats = {"uptime_s": (datetime.now(timezone.utc) - self.start_time).total_seconds(), "tools": {}}
        for tool_name in self.call_counts:
            lats = self.latencies[tool_name]
            stats["tools"][tool_name] = {
                "calls": self.call_counts[tool_name],
                "errors": self.error_counts[tool_name],
                "error_rate": self.error_counts[tool_name] / max(self.call_counts[tool_name], 1),
                "avg_latency_ms": sum(lats) / len(lats) if lats else 0,
                "p95_latency_ms": sorted(lats)[int(len(lats) * 0.95)] if len(lats) > 1 else (lats[0] if lats else 0),
                "max_latency_ms": max(lats) if lats else 0,
                "last_call": self.last_call.get(tool_name)
            }
        return stats

metrics = MCPMetrics()
server = Server("monitored-mcp-server")

@server.list_tools()
async def list_tools():
    return [
        Tool(name="run_query", description="Execute SQL", inputSchema={
            "type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}),
        Tool(name="get_metrics", description="Get server metrics", inputSchema={"type": "object", "properties": {}}),
        Tool(name="health_check", description="Server health check", inputSchema={"type": "object", "properties": {}})
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict):
    start = time.time()
    correlation_id = f"mcp-{int(start*1000)}"

    logger.info(json.dumps({
        "event": "tool_call_start", "tool": name, "arguments": arguments,
        "correlation_id": correlation_id, "timestamp": datetime.now(timezone.utc).isoformat()
    }))

    try:
        if name == "get_metrics":
            result = json.dumps(metrics.get_stats(), indent=2, default=str)
        elif name == "health_check":
            result = json.dumps({"status": "healthy", "uptime_s": (datetime.now(timezone.utc) - metrics.start_time).total_seconds(),
                                 "total_calls": sum(metrics.call_counts.values())})
        elif name == "run_query":
            df = spark.sql(arguments["query"]).limit(100).toPandas()
            result = df.to_json(orient="records")
        else:
            raise ValueError(f"Unknown tool: {name}")

        duration_ms = (time.time() - start) * 1000
        metrics.record_call(name, duration_ms, success=True)
        logger.info(json.dumps({
            "event": "tool_call_success", "tool": name, "duration_ms": round(duration_ms, 2),
            "correlation_id": correlation_id
        }))
        return [TextContent(type="text", text=result)]

    except Exception as e:
        duration_ms = (time.time() - start) * 1000
        metrics.record_call(name, duration_ms, success=False)
        logger.error(json.dumps({
            "event": "tool_call_error", "tool": name, "error": str(e),
            "duration_ms": round(duration_ms, 2), "correlation_id": correlation_id
        }))
        return [TextContent(type="text", text=json.dumps({"error": True, "message": str(e)}))]`,
  },
];

const databaseScenarios = [
  {
    id: 1,
    category: 'Vector',
    title: 'ChromaDB Vector Database',
    desc: 'Set up ChromaDB as a persistent vector store with collections, metadata filtering, and HNSW tuning',
    code: `import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
import json

# Persistent ChromaDB with custom settings
client = chromadb.PersistentClient(
    path="/dbfs/mnt/vectordb/chroma_production",
    settings=Settings(
        anonymized_telemetry=False,
        allow_reset=False
    )
)

# Create collection with HNSW index tuning
collection = client.get_or_create_collection(
    name="knowledge_base",
    metadata={
        "hnsw:space": "cosine",        # cosine, l2, or ip
        "hnsw:M": 32,                   # max connections per node (higher = better recall, more memory)
        "hnsw:construction_ef": 200,     # construction-time search depth
        "hnsw:search_ef": 100            # query-time search depth
    }
)

# Batch upsert with embeddings and metadata
model = SentenceTransformer("all-MiniLM-L6-v2")
documents = [
    {"text": "Delta Lake provides ACID transactions", "topic": "storage", "version": "3.0"},
    {"text": "Unity Catalog manages data governance", "topic": "governance", "version": "1.0"},
    {"text": "MLflow tracks ML experiments", "topic": "ml", "version": "2.5"},
    {"text": "Photon accelerates SQL workloads", "topic": "compute", "version": "2.0"},
    {"text": "Auto Loader ingests files incrementally", "topic": "ingestion", "version": "1.5"}
]

embeddings = model.encode([d["text"] for d in documents], normalize_embeddings=True)

collection.upsert(
    ids=[f"doc_{i}" for i in range(len(documents))],
    embeddings=embeddings.tolist(),
    documents=[d["text"] for d in documents],
    metadatas=[{"topic": d["topic"], "version": d["version"]} for d in documents]
)

# Query with metadata filtering
query_emb = model.encode("How to manage data access?", normalize_embeddings=True).tolist()
results = collection.query(
    query_embeddings=[query_emb],
    n_results=3,
    where={"topic": {"$in": ["governance", "storage"]}},
    include=["documents", "metadatas", "distances"]
)

print(f"Collection: {collection.name} ({collection.count()} docs)")
for doc, meta, dist in zip(results["documents"][0], results["metadatas"][0], results["distances"][0]):
    print(f"  [{1-dist:.4f}] {doc} (topic: {meta['topic']})")

# Delete by filter
collection.delete(where={"version": {"$lt": "1.5"}})
print(f"After cleanup: {collection.count()} docs")`,
  },

  {
    id: 2,
    category: 'Vector',
    title: 'FAISS Vector Index Setup',
    desc: 'Build a FAISS index for fast approximate nearest neighbor search at scale',
    code: `import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import pickle, os, time

model = SentenceTransformer("all-MiniLM-L6-v2")
dimension = model.get_sentence_embedding_dimension()  # 384

# Generate sample embeddings
documents = [f"Document about topic {i} in Databricks engineering" for i in range(10000)]
print(f"Encoding {len(documents)} documents...")
start = time.time()
embeddings = model.encode(documents, batch_size=128, show_progress_bar=True, normalize_embeddings=True)
print(f"Encoded in {time.time()-start:.1f}s — shape: {embeddings.shape}")

# --- Flat Index (exact search, good for <100k vectors) ---
flat_index = faiss.IndexFlatIP(dimension)  # Inner product (cosine for normalized vectors)
flat_index.add(embeddings.astype(np.float32))
print(f"Flat index: {flat_index.ntotal} vectors")

# --- IVF Index (approximate, good for 100k-10M vectors) ---
nlist = 100  # number of clusters
quantizer = faiss.IndexFlatIP(dimension)
ivf_index = faiss.IndexIVFFlat(quantizer, dimension, nlist, faiss.METRIC_INNER_PRODUCT)
ivf_index.train(embeddings.astype(np.float32))
ivf_index.add(embeddings.astype(np.float32))
ivf_index.nprobe = 10  # search 10 clusters at query time
print(f"IVF index: {ivf_index.ntotal} vectors, {nlist} clusters")

# --- HNSW Index (best recall/speed tradeoff) ---
hnsw_index = faiss.IndexHNSWFlat(dimension, 32, faiss.METRIC_INNER_PRODUCT)
hnsw_index.hnsw.efConstruction = 200
hnsw_index.hnsw.efSearch = 64
hnsw_index.add(embeddings.astype(np.float32))
print(f"HNSW index: {hnsw_index.ntotal} vectors")

# Query and compare
query = model.encode("Delta Lake ACID transactions", normalize_embeddings=True).reshape(1, -1).astype(np.float32)

for name, index in [("Flat", flat_index), ("IVF", ivf_index), ("HNSW", hnsw_index)]:
    start = time.time()
    scores, indices = index.search(query, 5)
    elapsed = (time.time() - start) * 1000
    print(f"\\n{name} search ({elapsed:.2f}ms):")
    for score, idx in zip(scores[0], indices[0]):
        print(f"  {score:.4f} | {documents[idx][:60]}")

# Save and load index
faiss.write_index(hnsw_index, "/dbfs/mnt/vectordb/faiss_hnsw.index")
loaded_index = faiss.read_index("/dbfs/mnt/vectordb/faiss_hnsw.index")
print(f"\\nLoaded index: {loaded_index.ntotal} vectors")`,
  },

  {
    id: 3,
    category: 'Cache',
    title: 'Redis Cache for RAG',
    desc: 'Use Redis as a caching layer for embeddings, LLM responses, and session state in RAG pipelines',
    code: `import redis
import json
import hashlib
import time
import numpy as np

# Connect to Redis
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# --- Embedding Cache ---
class EmbeddingCache:
    def __init__(self, redis_client, prefix="emb:", ttl=86400):
        self.r = redis_client
        self.prefix = prefix
        self.ttl = ttl

    def _key(self, text):
        return self.prefix + hashlib.sha256(text.encode()).hexdigest()

    def get(self, text):
        cached = self.r.get(self._key(text))
        if cached:
            return np.array(json.loads(cached))
        return None

    def set(self, text, embedding):
        key = self._key(text)
        self.r.setex(key, self.ttl, json.dumps(embedding.tolist()))

    def get_or_compute(self, text, compute_fn):
        cached = self.get(text)
        if cached is not None:
            return cached, True  # (embedding, was_cached)
        embedding = compute_fn(text)
        self.set(text, embedding)
        return embedding, False

emb_cache = EmbeddingCache(r, ttl=3600 * 24)

# --- LLM Response Cache ---
class LLMCache:
    def __init__(self, redis_client, prefix="llm:", ttl=3600):
        self.r = redis_client
        self.prefix = prefix
        self.ttl = ttl

    def _key(self, prompt, model):
        content = f"{model}:{prompt}"
        return self.prefix + hashlib.sha256(content.encode()).hexdigest()

    def get(self, prompt, model="llama3.1:8b"):
        cached = self.r.get(self._key(prompt, model))
        return json.loads(cached) if cached else None

    def set(self, prompt, response, model="llama3.1:8b"):
        self.r.setex(self._key(prompt, model), self.ttl, json.dumps(response))

llm_cache = LLMCache(r, ttl=3600)

# --- Session State ---
class SessionStore:
    def __init__(self, redis_client, prefix="session:", ttl=1800):
        self.r = redis_client
        self.prefix = prefix
        self.ttl = ttl

    def get_history(self, session_id):
        key = self.prefix + session_id
        history = self.r.lrange(key, 0, -1)
        return [json.loads(h) for h in history]

    def add_message(self, session_id, role, content):
        key = self.prefix + session_id
        self.r.rpush(key, json.dumps({"role": role, "content": content, "ts": time.time()}))
        self.r.expire(key, self.ttl)

    def clear(self, session_id):
        self.r.delete(self.prefix + session_id)

sessions = SessionStore(r)
sessions.add_message("user123", "user", "What is Delta Lake?")
sessions.add_message("user123", "assistant", "Delta Lake is an open-source storage layer...")
print(f"Session history: {sessions.get_history('user123')}")

# Cache stats
info = r.info("stats")
print(f"\\nRedis stats: {info['keyspace_hits']} hits, {info['keyspace_misses']} misses")`,
  },

  {
    id: 4,
    category: 'Relational',
    title: 'SQLite Conversation History Store',
    desc: 'Use SQLite to persistently store conversation history, RAG interactions, and audit logs',
    code: `import sqlite3
import json
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager

DB_PATH = "/dbfs/mnt/data/rag_history.db"

@contextmanager
def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# Initialize schema
def init_db():
    with get_connection() as conn:
        conn.executescript(\"\"\"
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                title TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('user', 'assistant', 'system')),
                content TEXT NOT NULL,
                model TEXT,
                tokens_used INTEGER,
                latency_ms REAL,
                sources_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (conversation_id) REFERENCES conversations(id)
            );
            CREATE TABLE IF NOT EXISTS rag_evaluations (
                id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                faithfulness REAL,
                relevance REAL,
                retrieval_precision REAL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (message_id) REFERENCES messages(id)
            );
            CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(conversation_id);
            CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at);
            CREATE INDEX IF NOT EXISTS idx_conversations_user ON conversations(user_id);
        \"\"\")

init_db()

# CRUD operations
def create_conversation(user_id, title=None):
    conv_id = str(uuid.uuid4())
    with get_connection() as conn:
        conn.execute("INSERT INTO conversations (id, user_id, title) VALUES (?, ?, ?)", (conv_id, user_id, title))
    return conv_id

def add_message(conversation_id, role, content, model=None, tokens=None, latency_ms=None, sources=None):
    msg_id = str(uuid.uuid4())
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO messages (id, conversation_id, role, content, model, tokens_used, latency_ms, sources_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (msg_id, conversation_id, role, content, model, tokens, latency_ms, json.dumps(sources) if sources else None)
        )
        conn.execute("UPDATE conversations SET updated_at = datetime('now') WHERE id = ?", (conversation_id,))
    return msg_id

def get_conversation_history(conversation_id, limit=50):
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT role, content, model, tokens_used, latency_ms, created_at FROM messages WHERE conversation_id = ? ORDER BY created_at LIMIT ?",
            (conversation_id, limit)
        ).fetchall()
    return [dict(row) for row in rows]

# Usage
conv_id = create_conversation("user_001", "Delta Lake Questions")
add_message(conv_id, "user", "What is Delta Lake?")
add_message(conv_id, "assistant", "Delta Lake is an open-source storage layer...", model="llama3.1:8b", tokens=150, latency_ms=820.5)
history = get_conversation_history(conv_id)
print(f"Conversation has {len(history)} messages")
for msg in history:
    print(f"  [{msg['role']}] {msg['content'][:60]}...")`,
  },

  {
    id: 5,
    category: 'Cloud',
    title: 'Pinecone Cloud Vector DB',
    desc: 'Use Pinecone managed vector database for serverless vector search at production scale',
    code: `from pinecone import Pinecone, ServerlessSpec
from sentence_transformers import SentenceTransformer
import os, time

# Initialize Pinecone
pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])

# Create a serverless index
index_name = "databricks-rag"
if index_name not in pc.list_indexes().names():
    pc.create_index(
        name=index_name,
        dimension=384,
        metric="cosine",
        spec=ServerlessSpec(cloud="aws", region="us-east-1")
    )
    while not pc.describe_index(index_name).status["ready"]:
        time.sleep(1)

index = pc.Index(index_name)
print(f"Index: {index_name} — {index.describe_index_stats()}")

# Embed and upsert documents
model = SentenceTransformer("all-MiniLM-L6-v2")

documents = [
    {"id": "doc_1", "text": "Delta Lake ACID transactions for data lakes", "topic": "storage"},
    {"id": "doc_2", "text": "Unity Catalog centralized governance platform", "topic": "governance"},
    {"id": "doc_3", "text": "Structured Streaming real-time processing", "topic": "streaming"},
    {"id": "doc_4", "text": "MLflow experiment tracking and model registry", "topic": "ml"},
    {"id": "doc_5", "text": "Photon C++ vectorized query engine", "topic": "compute"}
]

embeddings = model.encode([d["text"] for d in documents], normalize_embeddings=True)

# Batch upsert with metadata
vectors = [
    {"id": d["id"], "values": emb.tolist(), "metadata": {"text": d["text"], "topic": d["topic"]}}
    for d, emb in zip(documents, embeddings)
]
index.upsert(vectors=vectors, namespace="knowledge_base")

# Wait for indexing
time.sleep(2)
stats = index.describe_index_stats()
print(f"Total vectors: {stats['total_vector_count']}")

# Query with metadata filter
query_emb = model.encode("How to enforce data access policies?", normalize_embeddings=True).tolist()
results = index.query(
    vector=query_emb,
    top_k=3,
    namespace="knowledge_base",
    include_metadata=True,
    filter={"topic": {"$in": ["governance", "storage"]}}
)

for match in results["matches"]:
    print(f"  [{match['score']:.4f}] {match['metadata']['text']} (topic: {match['metadata']['topic']})")

# Cleanup
# pc.delete_index(index_name)`,
  },

  {
    id: 6,
    category: 'Vector',
    title: 'Weaviate Vector Database Setup',
    desc: 'Configure Weaviate as a schema-driven vector database with hybrid search capabilities',
    code: `import weaviate
from weaviate.classes.config import Configure, Property, DataType
from sentence_transformers import SentenceTransformer
import json

# Connect to Weaviate (local or cloud)
client = weaviate.connect_to_local(host="localhost", port=8080)

# Define collection with schema
collection_name = "DatabricksDocs"
if client.collections.exists(collection_name):
    client.collections.delete(collection_name)

collection = client.collections.create(
    name=collection_name,
    vectorizer_config=Configure.Vectorizer.none(),  # We provide our own vectors
    properties=[
        Property(name="content", data_type=DataType.TEXT),
        Property(name="source", data_type=DataType.TEXT),
        Property(name="topic", data_type=DataType.TEXT),
        Property(name="chunk_index", data_type=DataType.INT),
    ]
)

# Insert documents with pre-computed embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")
documents = [
    {"content": "Delta Lake provides ACID transactions and time travel", "source": "delta_docs.pdf", "topic": "storage", "chunk_index": 0},
    {"content": "Unity Catalog enforces column-level security policies", "source": "unity_docs.pdf", "topic": "governance", "chunk_index": 0},
    {"content": "Auto Loader watches cloud storage for new files", "source": "ingestion_docs.pdf", "topic": "ingestion", "chunk_index": 0},
    {"content": "Photon engine provides native C++ execution for Spark", "source": "compute_docs.pdf", "topic": "compute", "chunk_index": 0},
]

with collection.batch.dynamic() as batch:
    for doc in documents:
        embedding = model.encode(doc["content"], normalize_embeddings=True)
        batch.add_object(properties=doc, vector=embedding.tolist())

print(f"Collection '{collection_name}' has {collection.aggregate.over_all(total_count=True).total_count} objects")

# Vector search
query_emb = model.encode("How to secure data access?", normalize_embeddings=True).tolist()
results = collection.query.near_vector(
    near_vector=query_emb,
    limit=3,
    return_metadata=weaviate.classes.query.MetadataQuery(distance=True)
)

for obj in results.objects:
    print(f"  [{1-obj.metadata.distance:.4f}] {obj.properties['content']} (source: {obj.properties['source']})")

# Hybrid search (vector + keyword BM25)
hybrid_results = collection.query.hybrid(
    query="ACID transactions Delta",
    vector=query_emb,
    alpha=0.5,  # 0=pure BM25, 1=pure vector
    limit=3
)
print("\\nHybrid results:")
for obj in hybrid_results.objects:
    print(f"  {obj.properties['content'][:80]}...")

client.close()`,
  },

  {
    id: 7,
    category: 'Pattern',
    title: 'Embedding Cache Pattern',
    desc: 'Implement a multi-tier embedding cache (memory + disk + recompute) for efficient RAG pipelines',
    code: `import hashlib
import json
import numpy as np
import os
import time
import sqlite3
from functools import lru_cache
from sentence_transformers import SentenceTransformer
from contextlib import contextmanager

class EmbeddingCacheManager:
    """Multi-tier cache: L1 (memory LRU) -> L2 (SQLite disk) -> L3 (recompute)."""

    def __init__(self, model_name="all-MiniLM-L6-v2", db_path="/dbfs/mnt/cache/embedding_cache.db", memory_size=10000):
        self.model = SentenceTransformer(model_name)
        self.dimension = self.model.get_sentence_embedding_dimension()
        self.db_path = db_path
        self.memory_cache = {}
        self.memory_size = memory_size
        self.stats = {"l1_hits": 0, "l2_hits": 0, "misses": 0, "total": 0}
        self._init_db()

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(\"\"\"
                CREATE TABLE IF NOT EXISTS embeddings (
                    hash TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    embedding BLOB NOT NULL,
                    model TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now')),
                    access_count INTEGER DEFAULT 1
                )
            \"\"\")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_emb_created ON embeddings(created_at)")

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _hash(self, text):
        return hashlib.sha256(text.strip().lower().encode()).hexdigest()

    def encode(self, text):
        self.stats["total"] += 1
        h = self._hash(text)

        # L1: Memory cache
        if h in self.memory_cache:
            self.stats["l1_hits"] += 1
            return self.memory_cache[h]

        # L2: Disk cache
        with self._get_conn() as conn:
            row = conn.execute("SELECT embedding FROM embeddings WHERE hash = ?", (h,)).fetchone()
            if row:
                embedding = np.frombuffer(row[0], dtype=np.float32)
                conn.execute("UPDATE embeddings SET access_count = access_count + 1 WHERE hash = ?", (h,))
                self.memory_cache[h] = embedding
                self.stats["l2_hits"] += 1
                return embedding

        # L3: Compute
        self.stats["misses"] += 1
        embedding = self.model.encode(text, normalize_embeddings=True).astype(np.float32)

        # Store in L2
        with self._get_conn() as conn:
            conn.execute("INSERT OR REPLACE INTO embeddings (hash, text, embedding, model) VALUES (?, ?, ?, ?)",
                         (h, text, embedding.tobytes(), self.model.__class__.__name__))

        # Store in L1 (evict if full)
        if len(self.memory_cache) >= self.memory_size:
            oldest = next(iter(self.memory_cache))
            del self.memory_cache[oldest]
        self.memory_cache[h] = embedding
        return embedding

    def encode_batch(self, texts):
        return np.array([self.encode(t) for t in texts])

    def get_stats(self):
        total = self.stats["total"] or 1
        return {**self.stats, "l1_rate": self.stats["l1_hits"]/total, "l2_rate": self.stats["l2_hits"]/total}

# Usage
cache = EmbeddingCacheManager(db_path="/dbfs/mnt/cache/emb_cache.db")
texts = ["Delta Lake ACID", "Unity Catalog governance", "Delta Lake ACID"]  # duplicate
for t in texts:
    emb = cache.encode(t)
print(f"Stats: {cache.get_stats()}")`,
  },

  {
    id: 8,
    category: 'Pattern',
    title: 'Conversation Memory Store',
    desc: 'Build a persistent conversation memory store with summarization and context window management',
    code: `import ollama
import sqlite3
import json
import uuid
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager

class ConversationMemory:
    """Manages conversation history with summarization and context window management."""

    def __init__(self, db_path="/dbfs/mnt/data/memory.db", max_messages=20, model="llama3.1:8b"):
        self.db_path = db_path
        self.max_messages = max_messages
        self.model = model
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript(\"\"\"
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    summary TEXT,
                    message_count INTEGER DEFAULT 0,
                    created_at TEXT, updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS memory_messages (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    token_count INTEGER,
                    created_at TEXT,
                    FOREIGN KEY (session_id) REFERENCES sessions(id)
                );
                CREATE INDEX IF NOT EXISTS idx_mem_session ON memory_messages(session_id, created_at);
            \"\"\")

    def create_session(self, user_id):
        session_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute("INSERT INTO sessions (id, user_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
                         (session_id, user_id, now, now))
        return session_id

    def add_message(self, session_id, role, content):
        msg_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        token_count = len(content.split()) * 1.3  # rough token estimate
        with self._conn() as conn:
            conn.execute("INSERT INTO memory_messages (id, session_id, role, content, token_count, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                         (msg_id, session_id, role, content, int(token_count), now))
            conn.execute("UPDATE sessions SET message_count = message_count + 1, updated_at = ? WHERE id = ?", (now, session_id))

        # Auto-summarize if exceeding max messages
        self._maybe_summarize(session_id)
        return msg_id

    def _maybe_summarize(self, session_id):
        with self._conn() as conn:
            count = conn.execute("SELECT message_count FROM sessions WHERE id = ?", (session_id,)).fetchone()["message_count"]
            if count <= self.max_messages:
                return

            # Get oldest half of messages to summarize
            half = count // 2
            old_messages = conn.execute(
                "SELECT id, role, content FROM memory_messages WHERE session_id = ? ORDER BY created_at LIMIT ?",
                (session_id, half)
            ).fetchall()

            # Summarize with LLM
            conversation_text = "\\n".join([f"{m['role']}: {m['content']}" for m in old_messages])
            response = ollama.chat(model=self.model, messages=[
                {"role": "system", "content": "Summarize this conversation in 2-3 sentences, preserving key facts and decisions."},
                {"role": "user", "content": conversation_text}
            ], options={"temperature": 0.1, "num_predict": 200})

            summary = response["message"]["content"]
            old_summary = conn.execute("SELECT summary FROM sessions WHERE id = ?", (session_id,)).fetchone()["summary"]
            new_summary = f"{old_summary}\\n{summary}" if old_summary else summary

            # Delete old messages and update summary
            ids = [m["id"] for m in old_messages]
            conn.execute(f"DELETE FROM memory_messages WHERE id IN ({','.join('?'*len(ids))})", ids)
            conn.execute("UPDATE sessions SET summary = ?, message_count = message_count - ? WHERE id = ?",
                         (new_summary, half, session_id))

    def get_context(self, session_id):
        """Get full context: summary + recent messages for LLM context window."""
        with self._conn() as conn:
            session = conn.execute("SELECT summary FROM sessions WHERE id = ?", (session_id,)).fetchone()
            messages = conn.execute(
                "SELECT role, content FROM memory_messages WHERE session_id = ? ORDER BY created_at",
                (session_id,)
            ).fetchall()

        context = []
        if session["summary"]:
            context.append({"role": "system", "content": f"Previous conversation summary: {session['summary']}"})
        for m in messages:
            context.append({"role": m["role"], "content": m["content"]})
        return context

# Usage
memory = ConversationMemory(max_messages=10)
sid = memory.create_session("user_001")
memory.add_message(sid, "user", "What is Delta Lake?")
memory.add_message(sid, "assistant", "Delta Lake is an open-source storage layer providing ACID transactions on data lakes.")
memory.add_message(sid, "user", "How does time travel work?")
context = memory.get_context(sid)
print(f"Context messages: {len(context)}")
for msg in context:
    print(f"  [{msg['role']}] {msg['content'][:80]}...")`,
  },
];

const tabs = [
  { key: 'ollama', label: 'Ollama', scenarios: ollamaScenarios, badgeClass: 'badge badge-blue' },
  { key: 'rag', label: 'RAG Pipelines', scenarios: ragScenarios, badgeClass: 'badge badge-green' },
  {
    key: 'mcp',
    label: 'MCP (Model Context Protocol)',
    scenarios: mcpScenarios,
    badgeClass: 'badge badge-purple',
  },
  {
    key: 'databases',
    label: 'Databases',
    scenarios: databaseScenarios,
    badgeClass: 'badge badge-orange',
  },
];

function RAGIntegration() {
  const [activeTab, setActiveTab] = useState('ollama');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const currentTab = tabs.find((t) => t.key === activeTab);
  const scenarios = currentTab.scenarios;
  const badgeClass = currentTab.badgeClass;

  const filtered = scenarios.filter(
    (s) =>
      s.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.desc.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.category.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const categories = [...new Set(scenarios.map((s) => s.category))];
  const [selectedCategory, setSelectedCategory] = useState('All');

  const displayed = filtered.filter(
    (s) => selectedCategory === 'All' || s.category === selectedCategory
  );

  const totalScenarios = tabs.reduce((sum, t) => sum + t.scenarios.length, 0);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1>RAG Integration — Ollama, RAG, MCP &amp; Databases</h1>
          <p>
            {totalScenarios} scenarios across Ollama LLM inference, RAG pipelines, Model Context
            Protocol, and vector/cache databases for Databricks
          </p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
          {tabs.map((tab) => (
            <button
              key={tab.key}
              className="tab"
              onClick={() => {
                setActiveTab(tab.key);
                setExpandedId(null);
                setSelectedCategory('All');
                setSearchTerm('');
              }}
              style={{
                padding: '0.5rem 1.25rem',
                border:
                  activeTab === tab.key ? '2px solid var(--primary)' : '1px solid var(--border)',
                borderRadius: '6px',
                background: activeTab === tab.key ? 'var(--primary)' : 'transparent',
                color: activeTab === tab.key ? '#fff' : 'var(--text-primary)',
                cursor: 'pointer',
                fontWeight: activeTab === tab.key ? 600 : 400,
                fontSize: '0.9rem',
              }}
            >
              {tab.label} ({tab.scenarios.length})
            </button>
          ))}
        </div>

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
            style={{ maxWidth: '220px' }}
          >
            <option value="All">All Categories ({scenarios.length})</option>
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat} ({scenarios.filter((s) => s.category === cat).length})
              </option>
            ))}
          </select>
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Showing {displayed.length} of {scenarios.length}
          </span>
        </div>
      </div>

      <div className="scenarios-list">
        {displayed.map((scenario) => (
          <div
            key={`${activeTab}-${scenario.id}`}
            className="card scenario-card"
            style={{ marginBottom: '0.75rem' }}
          >
            <div
              className="scenario-header"
              onClick={() =>
                setExpandedId(
                  expandedId === `${activeTab}-${scenario.id}`
                    ? null
                    : `${activeTab}-${scenario.id}`
                )
              }
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
                  <span className={badgeClass}>{scenario.category}</span>
                  <strong>
                    #{scenario.id} — {scenario.title}
                  </strong>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                  {scenario.desc}
                </p>
              </div>
              <span style={{ fontSize: '1.2rem', color: 'var(--text-secondary)' }}>
                {expandedId === `${activeTab}-${scenario.id}` ? '\u25BC' : '\u25B6'}
              </span>
            </div>
            {expandedId === `${activeTab}-${scenario.id}` && (
              <div className="code-block" style={{ marginTop: '1rem' }}>
                {scenario.code}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default RAGIntegration;
