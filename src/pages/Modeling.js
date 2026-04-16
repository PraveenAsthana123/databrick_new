import React, { useState } from 'react';
import ScenarioCard from '../components/common/ScenarioCard';

const modelingScenarios = [
  {
    id: 1,
    category: 'Classification',
    title: 'Logistic Regression',
    desc: 'Binary classification with PySpark MLlib',
    code: `from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

assembler = VectorAssembler(inputCols=["age", "income", "score"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100)
pipeline = Pipeline(stages=[assembler, lr])

train, test = df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)
predictions = model.transform(test)`,
  },
  {
    id: 2,
    category: 'Classification',
    title: 'Random Forest Classifier',
    desc: 'Multi-class classification with Random Forest',
    code: `from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline

indexer = StringIndexer(inputCol="category", outputCol="label")
assembler = VectorAssembler(inputCols=["f1","f2","f3","f4"], outputCol="features")
rf = RandomForestClassifier(numTrees=100, maxDepth=10, seed=42)
pipeline = Pipeline(stages=[indexer, assembler, rf])

model = pipeline.fit(train_df)
predictions = model.transform(test_df)`,
  },
  {
    id: 3,
    category: 'Classification',
    title: 'Gradient Boosted Trees (GBT)',
    desc: 'GBT for binary classification',
    code: `from pyspark.ml.classification import GBTClassifier

gbt = GBTClassifier(maxIter=50, maxDepth=5, stepSize=0.1, seed=42)
pipeline = Pipeline(stages=[assembler, gbt])
model = pipeline.fit(train_df)
predictions = model.transform(test_df)`,
  },
  {
    id: 4,
    category: 'Classification',
    title: 'XGBoost on Spark',
    desc: 'Distributed XGBoost classification',
    code: `from xgboost.spark import SparkXGBClassifier

xgb = SparkXGBClassifier(
    features_col="features", label_col="label",
    num_workers=4, n_estimators=200, max_depth=6,
    learning_rate=0.1, objective="binary:logistic"
)
model = xgb.fit(train_df)
predictions = model.transform(test_df)`,
  },
  {
    id: 5,
    category: 'Classification',
    title: 'Naive Bayes Classifier',
    desc: 'Text classification with Naive Bayes',
    code: `from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import Tokenizer, HashingTF, IDF

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
nb = NaiveBayes(modelType="multinomial")

pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, nb])
model = pipeline.fit(train_df)`,
  },
  {
    id: 6,
    category: 'Classification',
    title: 'Support Vector Machine (SVM)',
    desc: 'Linear SVM for classification',
    code: `from pyspark.ml.classification import LinearSVC

svm = LinearSVC(maxIter=100, regParam=0.01)
pipeline = Pipeline(stages=[assembler, svm])
model = pipeline.fit(train_df)
predictions = model.transform(test_df)`,
  },
  {
    id: 7,
    category: 'Classification',
    title: 'Multilayer Perceptron (MLP)',
    desc: 'Neural network classification',
    code: `from pyspark.ml.classification import MultilayerPerceptronClassifier

layers = [4, 128, 64, 3]  # input, hidden1, hidden2, output
mlp = MultilayerPerceptronClassifier(layers=layers, maxIter=200, blockSize=128, seed=42)
pipeline = Pipeline(stages=[assembler, mlp])
model = pipeline.fit(train_df)`,
  },
  {
    id: 8,
    category: 'Classification',
    title: 'One-vs-Rest Classifier',
    desc: 'Multi-class using binary classifiers',
    code: `from pyspark.ml.classification import OneVsRest, LogisticRegression

lr = LogisticRegression(maxIter=100, regParam=0.01)
ovr = OneVsRest(classifier=lr)
pipeline = Pipeline(stages=[assembler, ovr])
model = pipeline.fit(train_df)`,
  },
  {
    id: 9,
    category: 'Regression',
    title: 'Linear Regression',
    desc: 'Predict continuous values',
    code: `from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="price",
                       maxIter=100, regParam=0.01, elasticNetParam=0.5)
pipeline = Pipeline(stages=[assembler, lr])
model = pipeline.fit(train_df)

# Model summary
summary = model.stages[-1].summary
print(f"RMSE: {summary.rootMeanSquaredError}")
print(f"R2: {summary.r2}")`,
  },
  {
    id: 10,
    category: 'Regression',
    title: 'Random Forest Regressor',
    desc: 'Ensemble regression with Random Forest',
    code: `from pyspark.ml.regression import RandomForestRegressor

rf = RandomForestRegressor(numTrees=100, maxDepth=10, seed=42)
pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(train_df)

# Feature importance
importances = model.stages[-1].featureImportances
print("Feature Importances:", importances)`,
  },
  {
    id: 11,
    category: 'Regression',
    title: 'Gradient Boosted Regression',
    desc: 'GBT for regression problems',
    code: `from pyspark.ml.regression import GBTRegressor

gbt = GBTRegressor(maxIter=100, maxDepth=5, stepSize=0.1, seed=42)
pipeline = Pipeline(stages=[assembler, gbt])
model = pipeline.fit(train_df)`,
  },
  {
    id: 12,
    category: 'Regression',
    title: 'XGBoost Regressor on Spark',
    desc: 'Distributed XGBoost regression',
    code: `from xgboost.spark import SparkXGBRegressor

xgb = SparkXGBRegressor(
    features_col="features", label_col="target",
    num_workers=4, n_estimators=200, max_depth=6,
    learning_rate=0.1, objective="reg:squarederror"
)
model = xgb.fit(train_df)`,
  },
  {
    id: 13,
    category: 'Regression',
    title: 'Decision Tree Regressor',
    desc: 'Single decision tree for regression',
    code: `from pyspark.ml.regression import DecisionTreeRegressor

dt = DecisionTreeRegressor(maxDepth=10, seed=42)
pipeline = Pipeline(stages=[assembler, dt])
model = pipeline.fit(train_df)`,
  },
  {
    id: 14,
    category: 'Regression',
    title: 'Isotonic Regression',
    desc: 'Monotonic regression fitting',
    code: `from pyspark.ml.regression import IsotonicRegression

ir = IsotonicRegression(featuresCol="features", labelCol="target", isotonic=True)
model = ir.fit(train_df)
predictions = model.transform(test_df)`,
  },
  {
    id: 15,
    category: 'Clustering',
    title: 'K-Means Clustering',
    desc: 'Unsupervised K-Means with elbow method',
    code: `from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

costs = []
for k in range(2, 11):
    kmeans = KMeans(k=k, seed=42, maxIter=20)
    model = kmeans.fit(assembled_df)
    cost = model.summary.trainingCost
    costs.append((k, cost))

# Final model
best_k = 5
kmeans = KMeans(k=best_k, seed=42)
model = kmeans.fit(assembled_df)
predictions = model.transform(assembled_df)

evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette: {silhouette}")`,
  },
  {
    id: 16,
    category: 'Clustering',
    title: 'Bisecting K-Means',
    desc: 'Hierarchical divisive clustering',
    code: `from pyspark.ml.clustering import BisectingKMeans

bkm = BisectingKMeans(k=5, seed=42, maxIter=20)
model = bkm.fit(assembled_df)
predictions = model.transform(assembled_df)`,
  },
  {
    id: 17,
    category: 'Clustering',
    title: 'Gaussian Mixture Model',
    desc: 'Soft clustering with GMM',
    code: `from pyspark.ml.clustering import GaussianMixture

gmm = GaussianMixture(k=4, seed=42, maxIter=100)
model = gmm.fit(assembled_df)
predictions = model.transform(assembled_df)

# Cluster probabilities
predictions.select("features", "prediction", "probability").show()`,
  },
  {
    id: 18,
    category: 'Clustering',
    title: 'LDA Topic Modeling',
    desc: 'Discover topics in text data',
    code: `from pyspark.ml.clustering import LDA
from pyspark.ml.feature import CountVectorizer, Tokenizer, StopWordsRemover

tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
cv = CountVectorizer(inputCol="filtered", outputCol="features", maxDF=0.9, minDF=5)
lda = LDA(k=10, maxIter=50, seed=42)

pipeline = Pipeline(stages=[tokenizer, remover, cv, lda])
model = pipeline.fit(documents_df)

topics = model.stages[-1].describeTopics(10)
topics.show(truncate=False)`,
  },
  {
    id: 19,
    category: 'Feature Engineering',
    title: 'VectorAssembler',
    desc: 'Combine multiple columns into feature vector',
    code: `from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["age", "income", "credit_score", "years_employed"],
    outputCol="features",
    handleInvalid="skip"
)
assembled_df = assembler.transform(df)`,
  },
  {
    id: 20,
    category: 'Feature Engineering',
    title: 'StringIndexer + OneHotEncoder',
    desc: 'Encode categorical variables',
    code: `from pyspark.ml.feature import StringIndexer, OneHotEncoder

indexer = StringIndexer(inputCol="color", outputCol="color_idx", handleInvalid="keep")
encoder = OneHotEncoder(inputCol="color_idx", outputCol="color_vec")

pipeline = Pipeline(stages=[indexer, encoder])
encoded_df = pipeline.fit(df).transform(df)`,
  },
  {
    id: 21,
    category: 'Feature Engineering',
    title: 'StandardScaler',
    desc: 'Standardize features to zero mean unit variance',
    code: `from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(inputCol="features", outputCol="scaled_features",
                         withStd=True, withMean=True)
scaler_model = scaler.fit(assembled_df)
scaled_df = scaler_model.transform(assembled_df)`,
  },
  {
    id: 22,
    category: 'Feature Engineering',
    title: 'MinMaxScaler',
    desc: 'Scale features to [0, 1] range',
    code: `from pyspark.ml.feature import MinMaxScaler

scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features", min=0.0, max=1.0)
scaled_df = scaler.fit(assembled_df).transform(assembled_df)`,
  },
  {
    id: 23,
    category: 'Feature Engineering',
    title: 'PCA Dimensionality Reduction',
    desc: 'Reduce feature dimensions with PCA',
    code: `from pyspark.ml.feature import PCA

pca = PCA(k=10, inputCol="features", outputCol="pca_features")
pca_model = pca.fit(assembled_df)
pca_df = pca_model.transform(assembled_df)

# Explained variance
print("Explained variance:", pca_model.explainedVariance)`,
  },
  {
    id: 24,
    category: 'Feature Engineering',
    title: 'Word2Vec Embeddings',
    desc: 'Generate word embeddings',
    code: `from pyspark.ml.feature import Word2Vec

word2vec = Word2Vec(vectorSize=100, minCount=5, inputCol="words", outputCol="word_vectors")
model = word2vec.fit(tokenized_df)
vectorized_df = model.transform(tokenized_df)`,
  },
  {
    id: 25,
    category: 'Feature Engineering',
    title: 'TF-IDF Features',
    desc: 'Text feature extraction with TF-IDF',
    code: `from pyspark.ml.feature import HashingTF, IDF, Tokenizer

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20000)
idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)

pipeline = Pipeline(stages=[tokenizer, hashingTF, idf])
tfidf_df = pipeline.fit(df).transform(df)`,
  },
  {
    id: 26,
    category: 'Feature Engineering',
    title: 'Bucketizer',
    desc: 'Bin continuous variables',
    code: `from pyspark.ml.feature import Bucketizer

splits = [0, 18, 25, 35, 50, 65, float("inf")]
bucketizer = Bucketizer(splits=splits, inputCol="age", outputCol="age_bucket")
bucketed_df = bucketizer.transform(df)`,
  },
  {
    id: 27,
    category: 'Feature Engineering',
    title: 'Imputer',
    desc: 'Handle missing values',
    code: `from pyspark.ml.feature import Imputer

imputer = Imputer(inputCols=["age", "income", "score"],
                   outputCols=["age_imp", "income_imp", "score_imp"],
                   strategy="median")
imputed_df = imputer.fit(df).transform(df)`,
  },
  {
    id: 28,
    category: 'Feature Engineering',
    title: 'QuantileDiscretizer',
    desc: 'Bin features into quantile-based buckets',
    code: `from pyspark.ml.feature import QuantileDiscretizer

discretizer = QuantileDiscretizer(numBuckets=10, inputCol="income", outputCol="income_bucket")
bucketed_df = discretizer.fit(df).transform(df)`,
  },
  {
    id: 29,
    category: 'Feature Engineering',
    title: 'Interaction Features',
    desc: 'Create polynomial interaction features',
    code: `from pyspark.ml.feature import Interaction, VectorAssembler

assembler1 = VectorAssembler(inputCols=["age"], outputCol="age_vec")
assembler2 = VectorAssembler(inputCols=["income"], outputCol="income_vec")
interaction = Interaction(inputCols=["age_vec", "income_vec"], outputCol="age_income_interaction")

pipeline = Pipeline(stages=[assembler1, assembler2, interaction])
result = pipeline.fit(df).transform(df)`,
  },
  {
    id: 30,
    category: 'Feature Engineering',
    title: 'Feature Store',
    desc: 'Databricks Feature Store for ML features',
    code: `from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Create feature table
fe.create_table(
    name="ml_features.customer_features",
    primary_keys=["customer_id"],
    df=feature_df,
    description="Customer features for churn prediction"
)

# Read features for training
training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(table_name="ml_features.customer_features",
                      lookup_key="customer_id")
    ],
    label="churn"
)
training_df = training_set.load_df()`,
  },
  {
    id: 31,
    category: 'Evaluation',
    title: 'Binary Classification Evaluation',
    desc: 'Evaluate binary classifiers (AUC, F1, etc.)',
    code: `from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# AUC
bin_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc = bin_eval.evaluate(predictions)
print(f"AUC-ROC: {auc}")

# F1, Precision, Recall
mc_eval = MulticlassClassificationEvaluator(labelCol="label")
f1 = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})
precision = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedPrecision"})
recall = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedRecall"})
accuracy = mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"})
print(f"F1: {f1}, Precision: {precision}, Recall: {recall}, Accuracy: {accuracy}")`,
  },
  {
    id: 32,
    category: 'Evaluation',
    title: 'Regression Evaluation',
    desc: 'Evaluate regression models (RMSE, R2, MAE)',
    code: `from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction")
rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
print(f"RMSE: {rmse}, R2: {r2}, MAE: {mae}")`,
  },
  {
    id: 33,
    category: 'Evaluation',
    title: 'Confusion Matrix',
    desc: 'Generate confusion matrix for classifiers',
    code: `from pyspark.sql.functions import col

# Confusion matrix
cm = predictions.groupBy("label").pivot("prediction").count().fillna(0)
cm.show()

# Per-class metrics
from pyspark.mllib.evaluation import MulticlassMetrics
pred_labels = predictions.select("prediction", "label").rdd.map(lambda r: (float(r[0]), float(r[1])))
metrics = MulticlassMetrics(pred_labels)
print(f"Confusion Matrix:\\n{metrics.confusionMatrix().toArray()}")`,
  },
  {
    id: 34,
    category: 'Evaluation',
    title: 'Cross-Validation',
    desc: 'K-fold cross-validation',
    code: `from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = ParamGridBuilder() \\
    .addGrid(rf.numTrees, [50, 100, 200]) \\
    .addGrid(rf.maxDepth, [5, 10, 15]) \\
    .build()

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=BinaryClassificationEvaluator(),
    numFolds=5,
    seed=42
)
cv_model = cv.fit(train_df)
best_model = cv_model.bestModel
print(f"Best AUC: {max(cv_model.avgMetrics)}")`,
  },
  {
    id: 35,
    category: 'Evaluation',
    title: 'Train-Validation Split',
    desc: 'Hyperparameter tuning with validation split',
    code: `from pyspark.ml.tuning import TrainValidationSplit

tvs = TrainValidationSplit(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=RegressionEvaluator(metricName="rmse"),
    trainRatio=0.8,
    seed=42
)
tvs_model = tvs.fit(train_df)`,
  },
  {
    id: 36,
    category: 'MLflow',
    title: 'MLflow Experiment Tracking',
    desc: 'Track experiments with MLflow',
    code: `import mlflow
import mlflow.spark

mlflow.set_experiment("/Users/admin/churn_prediction")

with mlflow.start_run(run_name="rf_v1"):
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    auc = BinaryClassificationEvaluator().evaluate(predictions)

    mlflow.log_param("numTrees", 100)
    mlflow.log_param("maxDepth", 10)
    mlflow.log_metric("auc", auc)
    mlflow.spark.log_model(model, "model")

    print(f"Run ID: {mlflow.active_run().info.run_id}")`,
  },
  {
    id: 37,
    category: 'MLflow',
    title: 'MLflow Model Registry',
    desc: 'Register and version models',
    code: `import mlflow

# Register model
model_uri = f"runs:/{run_id}/model"
result = mlflow.register_model(model_uri, "churn_prediction_model")

# Transition to production
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage(
    name="churn_prediction_model",
    version=result.version,
    stage="Production"
)

# Load production model
prod_model = mlflow.spark.load_model("models:/churn_prediction_model/Production")
predictions = prod_model.transform(new_data)`,
  },
  {
    id: 38,
    category: 'MLflow',
    title: 'MLflow AutoLog',
    desc: 'Automatic experiment logging',
    code: `import mlflow
mlflow.autolog()

# Everything gets logged automatically
model = pipeline.fit(train_df)
predictions = model.transform(test_df)
# Params, metrics, model artifacts all tracked automatically`,
  },
  {
    id: 39,
    category: 'MLflow',
    title: 'Model Serving',
    desc: 'Deploy models with Databricks Model Serving',
    code: `# Register model for serving
import mlflow
mlflow.spark.log_model(model, "model", registered_model_name="prod_model")

# Enable serving endpoint (via Databricks UI or REST API)
import requests
endpoint_url = "https://<workspace>.cloud.databricks.com/serving-endpoints/prod_model/invocations"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
data = {"dataframe_records": [{"feature1": 1.0, "feature2": 2.0}]}
response = requests.post(endpoint_url, json=data, headers=headers, timeout=30)
print(response.json())`,
  },
  {
    id: 40,
    category: 'Deep Learning',
    title: 'PyTorch on Spark (Single Node)',
    desc: 'Train PyTorch model on Databricks',
    code: `import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

# Convert Spark DF to PyTorch tensors
pdf = train_df.toPandas()
X = torch.FloatTensor(pdf[feature_cols].values)
y = torch.FloatTensor(pdf["label"].values)

class Net(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, 128)
        self.fc2 = nn.Linear(128, 64)
        self.fc3 = nn.Linear(64, 1)
        self.relu = nn.ReLU()

    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        return torch.sigmoid(self.fc3(x))

model = Net(len(feature_cols))
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
criterion = nn.BCELoss()

for epoch in range(50):
    output = model(X)
    loss = criterion(output.squeeze(), y)
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()`,
  },
  {
    id: 41,
    category: 'Deep Learning',
    title: 'Distributed Training with TorchDistributor',
    desc: 'Multi-node PyTorch training',
    code: `from pyspark.ml.torch.distributor import TorchDistributor

def train_fn():
    import torch
    import torch.distributed as dist
    # Training logic here
    model = Net(input_dim)
    ddp_model = torch.nn.parallel.DistributedDataParallel(model)
    # ... training loop
    return ddp_model.state_dict()

distributor = TorchDistributor(num_processes=4, local_mode=False, use_gpu=True)
state_dict = distributor.run(train_fn)`,
  },
  {
    id: 42,
    category: 'Deep Learning',
    title: 'Hugging Face Transformers',
    desc: 'NLP with Hugging Face on Databricks',
    code: `from transformers import pipeline as hf_pipeline
import mlflow

# Sentiment analysis
classifier = hf_pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

# UDF for Spark
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd

@pandas_udf("string")
def predict_sentiment(texts: pd.Series) -> pd.Series:
    results = classifier(texts.tolist(), batch_size=32)
    return pd.Series([r["label"] for r in results])

result_df = df.withColumn("sentiment", predict_sentiment("text"))`,
  },
  {
    id: 43,
    category: 'Recommendation',
    title: 'ALS Collaborative Filtering',
    desc: 'Matrix factorization for recommendations',
    code: `from pyspark.ml.recommendation import ALS

als = ALS(
    userCol="user_id", itemCol="item_id", ratingCol="rating",
    maxIter=20, regParam=0.01, rank=50,
    coldStartStrategy="drop", seed=42
)
model = als.fit(train_df)
predictions = model.transform(test_df)

# Top-N recommendations
user_recs = model.recommendForAllUsers(10)
item_recs = model.recommendForAllItems(10)`,
  },
  {
    id: 44,
    category: 'Recommendation',
    title: 'Content-Based Filtering',
    desc: 'Similarity-based recommendations',
    code: `from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf

# Build item feature vectors using TF-IDF on descriptions
tokenizer = Tokenizer(inputCol="description", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")

pipeline = Pipeline(stages=[tokenizer, hashingTF, idf])
item_features = pipeline.fit(items_df).transform(items_df)

# Cosine similarity for recommendations
from pyspark.ml.feature import Normalizer
normalizer = Normalizer(inputCol="features", outputCol="norm_features")
normalized = normalizer.transform(item_features)`,
  },
  {
    id: 45,
    category: 'Time Series',
    title: 'Prophet Forecasting on Spark',
    desc: 'Distributed time series forecasting',
    code: `from prophet import Prophet
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType

schema = "ds timestamp, yhat double, yhat_lower double, yhat_upper double, store_id int"

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def forecast_store(pdf):
    model = Prophet(yearly_seasonality=True, weekly_seasonality=True)
    model.fit(pdf[["ds", "y"]])
    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)
    forecast["store_id"] = pdf["store_id"].iloc[0]
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper", "store_id"]]

forecasts = df.groupBy("store_id").apply(forecast_store)`,
  },
  {
    id: 46,
    category: 'Time Series',
    title: 'ARIMA on Spark',
    desc: 'ARIMA time series modeling',
    code: `from statsmodels.tsa.arima.model import ARIMA
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("ds timestamp, forecast double, product_id int")
def arima_forecast(pdf: pd.DataFrame) -> pd.DataFrame:
    model = ARIMA(pdf["sales"], order=(2, 1, 2))
    fitted = model.fit()
    forecast = fitted.forecast(steps=30)
    result = pd.DataFrame({
        "ds": pd.date_range(start=pdf["ds"].max(), periods=30, freq="D"),
        "forecast": forecast.values,
        "product_id": pdf["product_id"].iloc[0]
    })
    return result

forecasts = df.groupBy("product_id").applyInPandas(arima_forecast, schema)`,
  },
  {
    id: 47,
    category: 'Anomaly Detection',
    title: 'Isolation Forest',
    desc: 'Anomaly detection with Isolation Forest',
    code: `from sklearn.ensemble import IsolationForest
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("prediction int")
def detect_anomalies(features: pd.DataFrame) -> pd.Series:
    clf = IsolationForest(contamination=0.05, random_state=42)
    predictions = clf.fit_predict(features)
    return pd.Series(predictions)

# Apply to Spark DataFrame
pdf = assembled_df.select("features").toPandas()
result = detect_anomalies(pdf)`,
  },
  {
    id: 48,
    category: 'Anomaly Detection',
    title: 'Z-Score Anomaly Detection',
    desc: 'Statistical anomaly detection',
    code: `from pyspark.sql.functions import col, mean, stddev, abs as spark_abs

stats = df.select(mean("value").alias("mean_val"), stddev("value").alias("std_val")).collect()[0]
mu, sigma = stats["mean_val"], stats["std_val"]

anomalies = df.withColumn("z_score", spark_abs((col("value") - mu) / sigma)) \\
    .withColumn("is_anomaly", col("z_score") > 3.0)

anomalies.filter("is_anomaly = true").show()`,
  },
  {
    id: 49,
    category: 'Pipeline',
    title: 'Full ML Pipeline',
    desc: 'End-to-end ML pipeline with all stages',
    code: `from pyspark.ml import Pipeline
from pyspark.ml.feature import (StringIndexer, OneHotEncoder, VectorAssembler,
                                 StandardScaler, Imputer)
from pyspark.ml.classification import RandomForestClassifier

# Feature engineering stages
cat_indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
                for c in categorical_cols]
cat_encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec")
                for c in categorical_cols]
imputer = Imputer(inputCols=numeric_cols, outputCols=[f"{c}_imp" for c in numeric_cols])
assembler = VectorAssembler(
    inputCols=[f"{c}_vec" for c in categorical_cols] + [f"{c}_imp" for c in numeric_cols],
    outputCol="raw_features"
)
scaler = StandardScaler(inputCol="raw_features", outputCol="features")
rf = RandomForestClassifier(numTrees=100, maxDepth=10)

pipeline = Pipeline(stages=cat_indexers + cat_encoders + [imputer, assembler, scaler, rf])
model = pipeline.fit(train_df)`,
  },
  {
    id: 50,
    category: 'Pipeline',
    title: 'Save and Load Pipeline',
    desc: 'Persist and reload ML pipelines',
    code: `# Save pipeline model
model.write().overwrite().save("/mnt/models/churn_pipeline_v1")

# Load pipeline model
from pyspark.ml import PipelineModel
loaded_model = PipelineModel.load("/mnt/models/churn_pipeline_v1")
predictions = loaded_model.transform(new_data)`,
  },
  {
    id: 51,
    category: 'Pipeline',
    title: 'AutoML with Databricks',
    desc: 'Automated machine learning',
    code: `from databricks import automl

summary = automl.classify(
    dataset=train_df,
    target_col="churn",
    primary_metric="f1",
    timeout_minutes=30,
    max_trials=50
)

# Best model
print(f"Best trial: {summary.best_trial}")
print(f"Best metric: {summary.best_trial.metrics['f1']}")

# Load best model
best_model = mlflow.pyfunc.load_model(f"runs:/{summary.best_trial.mlflow_run_id}/model")`,
  },
  {
    id: 52,
    category: 'Pipeline',
    title: 'Hyperopt Tuning',
    desc: 'Bayesian hyperparameter optimization',
    code: `from hyperopt import fmin, tpe, hp, SparkTrials, STATUS_OK

def objective(params):
    rf = RandomForestClassifier(
        numTrees=int(params["numTrees"]),
        maxDepth=int(params["maxDepth"])
    )
    pipeline = Pipeline(stages=[assembler, rf])
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)
    auc = BinaryClassificationEvaluator().evaluate(predictions)
    return {"loss": -auc, "status": STATUS_OK}

search_space = {
    "numTrees": hp.quniform("numTrees", 50, 300, 10),
    "maxDepth": hp.quniform("maxDepth", 3, 15, 1)
}

spark_trials = SparkTrials(parallelism=4)
best = fmin(fn=objective, space=search_space, algo=tpe.suggest,
            max_evals=50, trials=spark_trials)`,
  },
  {
    id: 53,
    category: 'NLP',
    title: 'Text Classification Pipeline',
    desc: 'End-to-end NLP classification',
    code: `from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(maxIter=100, regParam=0.01)

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
model = pipeline.fit(train_df)`,
  },
  {
    id: 54,
    category: 'NLP',
    title: 'N-gram Features',
    desc: 'Generate n-gram features for NLP',
    code: `from pyspark.ml.feature import NGram, CountVectorizer

tokenizer = Tokenizer(inputCol="text", outputCol="words")
bigram = NGram(n=2, inputCol="words", outputCol="bigrams")
trigram = NGram(n=3, inputCol="words", outputCol="trigrams")

cv_uni = CountVectorizer(inputCol="words", outputCol="uni_features")
cv_bi = CountVectorizer(inputCol="bigrams", outputCol="bi_features")

pipeline = Pipeline(stages=[tokenizer, bigram, trigram, cv_uni, cv_bi])
result = pipeline.fit(df).transform(df)`,
  },
  {
    id: 55,
    category: 'Graph',
    title: 'GraphFrames - PageRank',
    desc: 'Graph analytics with GraphFrames',
    code: `from graphframes import GraphFrame

vertices = spark.createDataFrame([
    ("1", "Alice"), ("2", "Bob"), ("3", "Charlie")
], ["id", "name"])

edges = spark.createDataFrame([
    ("1", "2", "follows"), ("2", "3", "follows"), ("3", "1", "follows")
], ["src", "dst", "relationship"])

g = GraphFrame(vertices, edges)
results = g.pageRank(resetProbability=0.15, maxIter=20)
results.vertices.select("id", "name", "pagerank").orderBy("pagerank", ascending=False).show()`,
  },
];

const categories = [...new Set(modelingScenarios.map((s) => s.category))];

function Modeling() {
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [expandedId, setExpandedId] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  const filtered = modelingScenarios.filter((s) => {
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
          <h1>Modeling Scenarios</h1>
          <p>{modelingScenarios.length} PySpark ML modeling patterns for Databricks</p>
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
            <option value="All">All Categories ({modelingScenarios.length})</option>
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat} ({modelingScenarios.filter((s) => s.category === cat).length})
              </option>
            ))}
          </select>
          <span style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
            Showing {filtered.length} of {modelingScenarios.length}
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
                  <span className="badge completed">{scenario.category}</span>
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

export default Modeling;
