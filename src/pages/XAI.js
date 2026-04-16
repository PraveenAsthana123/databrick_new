import React, { useState } from 'react';
import ScenarioCard from '../components/common/ScenarioCard';

const explainableScenarios = [
  {
    id: 1,
    category: 'SHAP',
    title: 'SHAP TreeExplainer',
    desc: 'Fast SHAP values for tree-based models (XGBoost, LightGBM, Random Forest)',
    code: `import shap
from xgboost import XGBClassifier

# Train model
model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.1, random_state=42)
model.fit(X_train, y_train)

# TreeExplainer (fast, exact for tree models)
explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)

# Base value (expected model output)
print(f"Base value: {explainer.expected_value}")
print(f"SHAP values shape: {shap_values.shape}")

# Per-instance explanation
instance_idx = 0
feature_contributions = dict(zip(feature_names, shap_values[instance_idx]))
print("Feature contributions:", sorted(feature_contributions.items(), key=lambda x: abs(x[1]), reverse=True))`,
  },
  {
    id: 2,
    category: 'SHAP',
    title: 'SHAP KernelExplainer',
    desc: 'Model-agnostic SHAP values using kernel-based estimation',
    code: `import shap
import numpy as np

# KernelExplainer works with any model (slower but universal)
background = shap.kmeans(X_train, 50)  # summarize background data
explainer = shap.KernelExplainer(model.predict_proba, background)

# Compute SHAP values for a sample
sample = X_test[:100]
shap_values = explainer.shap_values(sample, nsamples=200)

print(f"SHAP values for class 1: {shap_values[1].shape}")
print(f"Expected value: {explainer.expected_value}")

# Verify additivity: sum(shap_values) + base_value ≈ model output
prediction = model.predict_proba(sample[:1])[0][1]
shap_sum = shap_values[1][0].sum() + explainer.expected_value[1]
print(f"Prediction: {prediction:.4f}, SHAP sum: {shap_sum:.4f}")`,
  },
  {
    id: 3,
    category: 'SHAP',
    title: 'SHAP Summary Plot',
    desc: 'Global feature importance with SHAP summary plot',
    code: `import shap
import matplotlib.pyplot as plt

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)

# Summary plot — shows feature importance + distribution of effects
shap.summary_plot(shap_values, X_test, feature_names=feature_names, show=False)
plt.tight_layout()
plt.savefig("/dbfs/tmp/shap_summary.png", dpi=150, bbox_inches="tight")
plt.close()

# Bar plot — mean absolute SHAP value per feature
shap.summary_plot(shap_values, X_test, feature_names=feature_names, plot_type="bar", show=False)
plt.tight_layout()
plt.savefig("/dbfs/tmp/shap_importance_bar.png", dpi=150, bbox_inches="tight")
plt.close()

# Log to MLflow
import mlflow
mlflow.log_artifact("/dbfs/tmp/shap_summary.png")
mlflow.log_artifact("/dbfs/tmp/shap_importance_bar.png")`,
  },
  {
    id: 4,
    category: 'SHAP',
    title: 'SHAP Force Plot',
    desc: 'Individual prediction explanation with force plot',
    code: `import shap

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)

# Force plot for a single prediction
instance_idx = 0
shap.initjs()
force_plot = shap.force_plot(
    explainer.expected_value,
    shap_values[instance_idx],
    X_test.iloc[instance_idx],
    feature_names=feature_names
)

# Save as HTML for Databricks display
shap.save_html("/dbfs/tmp/force_plot.html", force_plot)

# Collective force plot (multiple instances stacked)
collective_plot = shap.force_plot(
    explainer.expected_value,
    shap_values[:50],
    X_test.iloc[:50],
    feature_names=feature_names
)
shap.save_html("/dbfs/tmp/collective_force.html", collective_plot)`,
  },
  {
    id: 5,
    category: 'SHAP',
    title: 'SHAP Dependence Plot',
    desc: 'Feature interaction effects with dependence plots',
    code: `import shap
import matplotlib.pyplot as plt

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test)

# Dependence plot — shows effect of a single feature
shap.dependence_plot("age", shap_values, X_test, feature_names=feature_names, show=False)
plt.savefig("/dbfs/tmp/shap_dependence_age.png", dpi=150, bbox_inches="tight")
plt.close()

# Dependence plot with interaction feature auto-detected
shap.dependence_plot("income", shap_values, X_test,
                     interaction_index="auto",
                     feature_names=feature_names, show=False)
plt.savefig("/dbfs/tmp/shap_dependence_income.png", dpi=150, bbox_inches="tight")
plt.close()

# Explicit interaction: income colored by age
shap.dependence_plot("income", shap_values, X_test,
                     interaction_index="age",
                     feature_names=feature_names, show=False)
plt.savefig("/dbfs/tmp/shap_interaction_income_age.png", dpi=150, bbox_inches="tight")
plt.close()`,
  },
  {
    id: 6,
    category: 'LIME',
    title: 'LIME Tabular Explainer',
    desc: 'Local interpretable model-agnostic explanations for tabular data',
    code: `from lime.lime_tabular import LimeTabularExplainer
import numpy as np

explainer = LimeTabularExplainer(
    training_data=X_train.values,
    feature_names=feature_names,
    class_names=["Not Churn", "Churn"],
    mode="classification",
    discretize_continuous=True
)

# Explain a single prediction
instance = X_test.iloc[0].values
explanation = explainer.explain_instance(
    instance,
    model.predict_proba,
    num_features=10,
    num_samples=5000
)

# Get feature weights
print("Explanation:")
for feature, weight in explanation.as_list():
    print(f"  {feature}: {weight:.4f}")

# Save explanation as HTML
explanation.save_to_file("/dbfs/tmp/lime_explanation.html")

# Prediction probabilities from LIME's local model
print(f"Local prediction: {explanation.local_pred}")
print(f"Intercept: {explanation.intercept}")`,
  },
  {
    id: 7,
    category: 'LIME',
    title: 'LIME Text Explainer',
    desc: 'Explain text classification predictions with LIME',
    code: `from lime.lime_text import LimeTextExplainer

explainer = LimeTextExplainer(class_names=["Negative", "Positive"])

def predict_fn(texts):
    \"\"\"Wrapper for model prediction on text inputs.\"\"\"
    from sklearn.feature_extraction.text import TfidfVectorizer
    vectors = vectorizer.transform(texts)
    return model.predict_proba(vectors)

text_instance = "This product is absolutely wonderful and exceeded my expectations"
explanation = explainer.explain_instance(
    text_instance,
    predict_fn,
    num_features=10,
    num_samples=2000
)

# Words contributing to prediction
print("Text explanation:")
for word, weight in explanation.as_list():
    direction = "positive" if weight > 0 else "negative"
    print(f"  '{word}': {weight:.4f} ({direction})")

explanation.save_to_file("/dbfs/tmp/lime_text.html")`,
  },
  {
    id: 8,
    category: 'LIME',
    title: 'LIME Image Explainer',
    desc: 'Explain image classification predictions with LIME',
    code: `from lime import lime_image
from skimage.segmentation import mark_boundaries
import matplotlib.pyplot as plt
import numpy as np

explainer = lime_image.LimeImageExplainer()

explanation = explainer.explain_instance(
    image,  # numpy array (H, W, C)
    model.predict,
    top_labels=3,
    hide_color=0,
    num_samples=1000
)

# Get image and mask for top prediction
temp, mask = explanation.get_image_and_mask(
    explanation.top_labels[0],
    positive_only=True,
    num_features=5,
    hide_rest=False
)

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
axes[0].imshow(image)
axes[0].set_title("Original")
axes[1].imshow(mark_boundaries(temp / 255.0, mask))
axes[1].set_title("Positive regions")

temp_neg, mask_neg = explanation.get_image_and_mask(
    explanation.top_labels[0], positive_only=False, num_features=10, hide_rest=False
)
axes[2].imshow(mark_boundaries(temp_neg / 255.0, mask_neg))
axes[2].set_title("All contributing regions")
plt.savefig("/dbfs/tmp/lime_image.png", dpi=150, bbox_inches="tight")
plt.close()`,
  },
  {
    id: 9,
    category: 'Feature Importance',
    title: 'Permutation Feature Importance',
    desc: 'Model-agnostic feature importance by permuting features',
    code: `from sklearn.inspection import permutation_importance
import pandas as pd

# Compute permutation importance on test set
perm_result = permutation_importance(
    model, X_test, y_test,
    n_repeats=30,
    random_state=42,
    scoring="roc_auc",
    n_jobs=-1
)

# Build importance DataFrame
importance_df = pd.DataFrame({
    "feature": feature_names,
    "importance_mean": perm_result.importances_mean,
    "importance_std": perm_result.importances_std
}).sort_values("importance_mean", ascending=False)

print("Permutation Feature Importance (AUC decrease):")
for _, row in importance_df.head(15).iterrows():
    print(f"  {row['feature']}: {row['importance_mean']:.4f} +/- {row['importance_std']:.4f}")

# Convert to Spark DataFrame for storage
spark_importance = spark.createDataFrame(importance_df)
spark_importance.write.mode("overwrite").saveAsTable("ml_results.permutation_importance")`,
  },
  {
    id: 10,
    category: 'Feature Importance',
    title: 'Mean Decrease Impurity (MDI)',
    desc: 'Built-in tree-based feature importance using impurity reduction',
    code: `from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import matplotlib.pyplot as plt

rf = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42)
rf.fit(X_train, y_train)

# MDI importance (Gini importance)
mdi_importance = pd.DataFrame({
    "feature": feature_names,
    "importance": rf.feature_importances_
}).sort_values("importance", ascending=False)

print("MDI Feature Importance (Gini):")
for _, row in mdi_importance.head(15).iterrows():
    print(f"  {row['feature']}: {row['importance']:.4f}")

# Plot
plt.figure(figsize=(10, 8))
top_n = mdi_importance.head(20)
plt.barh(top_n["feature"][::-1], top_n["importance"][::-1])
plt.xlabel("Mean Decrease Impurity")
plt.title("Feature Importance (MDI / Gini)")
plt.tight_layout()
plt.savefig("/dbfs/tmp/mdi_importance.png", dpi=150, bbox_inches="tight")
plt.close()

# Note: MDI can be biased toward high-cardinality features
# Always validate with permutation importance`,
  },
  {
    id: 11,
    category: 'Feature Importance',
    title: 'Drop-Column Feature Importance',
    desc: 'Measure importance by retraining without each feature',
    code: `import pandas as pd
from sklearn.model_selection import cross_val_score
import numpy as np

baseline_score = cross_val_score(model, X_train, y_train, cv=5, scoring="roc_auc").mean()
print(f"Baseline AUC: {baseline_score:.4f}")

drop_importances = []
for col in feature_names:
    X_dropped = X_train.drop(columns=[col])
    score = cross_val_score(model, X_dropped, y_train, cv=5, scoring="roc_auc").mean()
    importance = baseline_score - score
    drop_importances.append({"feature": col, "importance": importance})
    print(f"  Drop '{col}': AUC = {score:.4f}, Importance = {importance:.4f}")

drop_df = pd.DataFrame(drop_importances).sort_values("importance", ascending=False)
print("\\nDrop-Column Feature Importance (AUC decrease when removed):")
print(drop_df.head(15).to_string(index=False))

# Save to Delta table
spark_drop = spark.createDataFrame(drop_df)
spark_drop.write.mode("overwrite").saveAsTable("ml_results.drop_column_importance")`,
  },
  {
    id: 12,
    category: 'PDP',
    title: 'Partial Dependence Plots (PDP)',
    desc: 'Visualize marginal effect of features on predictions',
    code: `from sklearn.inspection import PartialDependenceDisplay
import matplotlib.pyplot as plt

# Single feature PDP
fig, ax = plt.subplots(figsize=(10, 6))
PartialDependenceDisplay.from_estimator(
    model, X_test, features=["age", "income", "credit_score"],
    kind="average",  # "average" for PDP
    ax=ax,
    n_cols=3
)
plt.suptitle("Partial Dependence Plots")
plt.tight_layout()
plt.savefig("/dbfs/tmp/pdp_plots.png", dpi=150, bbox_inches="tight")
plt.close()

# 2D PDP (interaction between two features)
fig, ax = plt.subplots(figsize=(8, 6))
PartialDependenceDisplay.from_estimator(
    model, X_test, features=[("age", "income")],
    kind="average",
    ax=ax
)
plt.title("2D Partial Dependence: Age x Income")
plt.tight_layout()
plt.savefig("/dbfs/tmp/pdp_2d.png", dpi=150, bbox_inches="tight")
plt.close()`,
  },
  {
    id: 13,
    category: 'PDP',
    title: 'Individual Conditional Expectation (ICE)',
    desc: 'Per-instance partial dependence curves',
    code: `from sklearn.inspection import PartialDependenceDisplay
import matplotlib.pyplot as plt

# ICE plot — individual curves for each instance
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
features = ["age", "income", "credit_score"]

for i, feature in enumerate(features):
    PartialDependenceDisplay.from_estimator(
        model, X_test, features=[feature],
        kind="both",  # "both" = PDP line + ICE curves
        subsample=50,  # plot 50 individual ICE curves
        ax=axes[i],
        ice_lines_kw={"color": "steelblue", "alpha": 0.1, "linewidth": 0.5},
        pd_line_kw={"color": "red", "linewidth": 2}
    )
    axes[i].set_title(f"ICE + PDP: {feature}")

plt.suptitle("Individual Conditional Expectation Plots")
plt.tight_layout()
plt.savefig("/dbfs/tmp/ice_plots.png", dpi=150, bbox_inches="tight")
plt.close()

# Centered ICE (c-ICE) to remove level effects
fig, ax = plt.subplots(figsize=(8, 5))
PartialDependenceDisplay.from_estimator(
    model, X_test, features=["income"],
    kind="individual", centered=True,
    subsample=100, ax=ax
)
plt.title("Centered ICE: Income")
plt.savefig("/dbfs/tmp/cice_income.png", dpi=150, bbox_inches="tight")
plt.close()`,
  },
  {
    id: 14,
    category: 'Model Agnostic',
    title: 'Attention Visualization for NLP',
    desc: 'Visualize transformer attention weights for text models',
    code: `from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import matplotlib.pyplot as plt
import numpy as np

model_name = "distilbert-base-uncased-finetuned-sst-2-english"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name, output_attentions=True)

text = "The movie was surprisingly good despite the slow start"
inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)

with torch.no_grad():
    outputs = model(**inputs)

# Extract attention weights
attentions = outputs.attentions  # tuple of (batch, heads, seq_len, seq_len)
tokens = tokenizer.convert_ids_to_tokens(inputs["input_ids"][0])

# Average attention across all heads in last layer
last_layer_attn = attentions[-1][0].mean(dim=0).numpy()

# Plot attention heatmap
fig, ax = plt.subplots(figsize=(10, 8))
im = ax.imshow(last_layer_attn, cmap="viridis")
ax.set_xticks(range(len(tokens)))
ax.set_yticks(range(len(tokens)))
ax.set_xticklabels(tokens, rotation=45, ha="right")
ax.set_yticklabels(tokens)
plt.colorbar(im)
plt.title("Attention Weights (Last Layer, Averaged Heads)")
plt.tight_layout()
plt.savefig("/dbfs/tmp/attention_heatmap.png", dpi=150, bbox_inches="tight")
plt.close()`,
  },
  {
    id: 15,
    category: 'Model Agnostic',
    title: 'Counterfactual Explanations',
    desc: 'Generate counterfactual examples that flip the prediction',
    code: `import dice_ml
import pandas as pd

# Prepare DiCE data interface
data_interface = dice_ml.Data(
    dataframe=train_df_pd,
    continuous_features=["age", "income", "credit_score", "account_balance"],
    outcome_name="churn"
)

# Prepare DiCE model interface
model_interface = dice_ml.Model(model=model, backend="sklearn")

# Generate counterfactuals
explainer = dice_ml.Dice(data_interface, model_interface, method="random")

# Instance to explain (predicted as Churn=1)
query_instance = X_test.iloc[[0]]
print(f"Original prediction: {model.predict(query_instance)[0]}")

# Generate 5 counterfactual examples
counterfactuals = explainer.generate_counterfactuals(
    query_instance,
    total_CFs=5,
    desired_class="opposite",
    features_to_vary=["income", "credit_score", "account_balance"]
)

# Display counterfactuals
counterfactuals.visualize_as_dataframe(show_only_changes=True)

# Save results
cf_df = counterfactuals.cf_examples_list[0].final_cfs_df
print("\\nCounterfactual examples (changes needed to flip prediction):")
print(cf_df.to_string())`,
  },
];

const responsibleScenarios = [
  {
    id: 16,
    category: 'Documentation',
    title: 'Model Card Generation',
    desc: 'Generate standardized model documentation following Google Model Cards framework',
    code: `import json
from datetime import datetime

def generate_model_card(model, X_test, y_test, model_name, version):
    \"\"\"Generate a comprehensive model card.\"\"\"
    predictions = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]

    from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                                  f1_score, roc_auc_score)

    model_card = {
        "model_details": {
            "name": model_name,
            "version": version,
            "type": type(model).__name__,
            "created_date": datetime.now().isoformat(),
            "framework": "scikit-learn",
            "license": "Internal Use Only"
        },
        "intended_use": {
            "primary_use": "Customer churn prediction",
            "primary_users": "Data Science and Marketing teams",
            "out_of_scope": "Not for use in credit decisions or hiring"
        },
        "metrics": {
            "accuracy": float(accuracy_score(y_test, predictions)),
            "precision": float(precision_score(y_test, predictions)),
            "recall": float(recall_score(y_test, predictions)),
            "f1_score": float(f1_score(y_test, predictions)),
            "auc_roc": float(roc_auc_score(y_test, proba))
        },
        "training_data": {
            "dataset": "customer_transactions_2024",
            "size": len(X_test) + len(X_test) * 4,
            "features": list(X_test.columns),
            "label_distribution": dict(zip(*np.unique(y_test, return_counts=True)))
        },
        "ethical_considerations": {
            "protected_attributes_used": False,
            "bias_testing_performed": True,
            "fairness_metrics_checked": ["demographic_parity", "equal_opportunity"]
        },
        "caveats_and_recommendations": [
            "Model performance may degrade for customers with < 3 months history",
            "Retrain quarterly with fresh data",
            "Monitor for concept drift using PSI"
        ]
    }
    return model_card

card = generate_model_card(model, X_test, y_test, "churn_predictor", "1.0.0")
with open("/dbfs/tmp/model_card.json", "w") as f:
    json.dump(card, f, indent=2, default=str)
print(json.dumps(card, indent=2, default=str))`,
  },
  {
    id: 17,
    category: 'Documentation',
    title: 'Data Card Documentation',
    desc: 'Document dataset characteristics, collection methods, and known biases',
    code: `import pandas as pd
import json
from datetime import datetime

def generate_data_card(df, dataset_name):
    \"\"\"Generate a comprehensive data card for a dataset.\"\"\"
    data_card = {
        "dataset_details": {
            "name": dataset_name,
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "size_rows": len(df),
            "size_columns": len(df.columns),
            "storage_format": "Delta Lake"
        },
        "schema": {
            col: str(dtype) for col, dtype in df.dtypes.items()
        },
        "statistics": {
            col: {
                "null_count": int(df[col].isnull().sum()),
                "null_pct": float(df[col].isnull().mean() * 100),
                "unique_count": int(df[col].nunique()),
                "dtype": str(df[col].dtype)
            } for col in df.columns
        },
        "numeric_summary": df.describe().to_dict(),
        "collection_method": {
            "source": "Production database ETL pipeline",
            "frequency": "Daily incremental",
            "consent": "User agreement section 4.2",
            "anonymization": "PII fields hashed with SHA-256"
        },
        "known_biases": [
            "Over-representation of urban customers (72%)",
            "Age distribution skewed toward 25-45 range",
            "Income data self-reported, may contain inaccuracies"
        ],
        "sensitive_attributes": {
            "present": ["age", "gender", "zip_code"],
            "removed_before_training": ["gender", "zip_code"],
            "proxy_risk": "age may proxy for other protected attributes"
        },
        "intended_use": "Training and evaluation of churn prediction models",
        "restrictions": "Do not use for credit scoring or hiring decisions"
    }
    return data_card

pdf = df.toPandas()
card = generate_data_card(pdf, "customer_churn_dataset")
print(json.dumps(card, indent=2, default=str))`,
  },
  {
    id: 18,
    category: 'Bias Detection',
    title: 'Training Data Bias Detection',
    desc: 'Detect statistical biases in training data across protected attributes',
    code: `import pandas as pd
import numpy as np
from scipy import stats

def detect_data_bias(df, target_col, protected_cols):
    \"\"\"Detect bias in training data across protected attributes.\"\"\"
    results = {}
    for attr in protected_cols:
        groups = df.groupby(attr)
        group_stats = groups[target_col].agg(["mean", "count", "std"])

        # Label distribution per group
        label_dist = df.groupby([attr, target_col]).size().unstack(fill_value=0)
        label_pct = label_dist.div(label_dist.sum(axis=1), axis=0)

        # Statistical test: chi-square for independence
        contingency = pd.crosstab(df[attr], df[target_col])
        chi2, p_value, dof, expected = stats.chi2_contingency(contingency)

        # Representation ratio
        overall_rate = df[target_col].mean()
        group_rates = groups[target_col].mean()
        disparate_impact = group_rates.min() / group_rates.max() if group_rates.max() > 0 else 0

        results[attr] = {
            "group_sizes": group_stats["count"].to_dict(),
            "positive_rates": group_rates.to_dict(),
            "chi2_statistic": float(chi2),
            "p_value": float(p_value),
            "significant_bias": p_value < 0.05,
            "disparate_impact_ratio": float(disparate_impact),
            "four_fifths_rule_pass": disparate_impact >= 0.8,
            "label_distribution": label_pct.to_dict()
        }

        print(f"\\n=== {attr} ===")
        print(f"  Group sizes: {group_stats['count'].to_dict()}")
        print(f"  Positive rates: {group_rates.round(4).to_dict()}")
        print(f"  Chi-square p-value: {p_value:.6f} ({'BIASED' if p_value < 0.05 else 'OK'})")
        print(f"  Disparate Impact Ratio: {disparate_impact:.4f} ({'FAIL' if disparate_impact < 0.8 else 'PASS'})")

    return results

bias_results = detect_data_bias(train_pdf, "churn", ["gender", "age_group", "region"])`,
  },
  {
    id: 19,
    category: 'Monitoring',
    title: 'Model Drift Monitoring (PSI)',
    desc: 'Detect model drift using Population Stability Index and KS test',
    code: `import numpy as np
from scipy import stats

def calculate_psi(expected, actual, bins=10):
    \"\"\"Calculate Population Stability Index.\"\"\"
    breakpoints = np.quantile(expected, np.linspace(0, 1, bins + 1))
    breakpoints[0] = -np.inf
    breakpoints[-1] = np.inf

    expected_counts = np.histogram(expected, bins=breakpoints)[0] / len(expected)
    actual_counts = np.histogram(actual, bins=breakpoints)[0] / len(actual)

    # Avoid division by zero
    expected_counts = np.clip(expected_counts, 1e-6, None)
    actual_counts = np.clip(actual_counts, 1e-6, None)

    psi = np.sum((actual_counts - expected_counts) * np.log(actual_counts / expected_counts))
    return psi

def monitor_drift(baseline_scores, current_scores, feature_baseline, feature_current, feature_names):
    \"\"\"Comprehensive drift monitoring.\"\"\"
    # Score distribution drift (PSI)
    score_psi = calculate_psi(baseline_scores, current_scores)
    print(f"Score PSI: {score_psi:.4f}")
    print(f"  Interpretation: {'No drift' if score_psi < 0.1 else 'Moderate drift' if score_psi < 0.25 else 'SIGNIFICANT DRIFT'}")

    # KS test on scores
    ks_stat, ks_pval = stats.ks_2samp(baseline_scores, current_scores)
    print(f"\\nKS Test: statistic={ks_stat:.4f}, p-value={ks_pval:.6f}")

    # Chi-square test for categorical features
    # Feature-level drift
    print("\\nFeature-level PSI:")
    for i, feat in enumerate(feature_names):
        feat_psi = calculate_psi(feature_baseline[:, i], feature_current[:, i])
        status = "OK" if feat_psi < 0.1 else "WARN" if feat_psi < 0.25 else "ALERT"
        print(f"  {feat}: PSI={feat_psi:.4f} [{status}]")

    return {"score_psi": score_psi, "ks_stat": ks_stat, "ks_pval": ks_pval}

results = monitor_drift(baseline_scores, current_scores,
                         X_baseline.values, X_current.values, feature_names)`,
  },
  {
    id: 20,
    category: 'Monitoring',
    title: 'A/B Testing for Model Fairness',
    desc: 'Compare fairness metrics between model versions using A/B testing',
    code: `import numpy as np
from scipy import stats

def ab_test_fairness(model_a_preds, model_b_preds, labels, protected_attr):
    \"\"\"A/B test comparing fairness between two models.\"\"\"
    results = {}
    groups = np.unique(protected_attr)

    for model_name, preds in [("Model A", model_a_preds), ("Model B", model_b_preds)]:
        group_tpr = {}
        group_fpr = {}
        group_acceptance = {}

        for group in groups:
            mask = protected_attr == group
            group_labels = labels[mask]
            group_preds = preds[mask]

            tp = ((group_preds == 1) & (group_labels == 1)).sum()
            fn = ((group_preds == 0) & (group_labels == 1)).sum()
            fp = ((group_preds == 1) & (group_labels == 0)).sum()
            tn = ((group_preds == 0) & (group_labels == 0)).sum()

            group_tpr[group] = tp / (tp + fn) if (tp + fn) > 0 else 0
            group_fpr[group] = fp / (fp + tn) if (fp + tn) > 0 else 0
            group_acceptance[group] = group_preds.mean()

        tpr_values = list(group_tpr.values())
        demographic_parity_gap = max(group_acceptance.values()) - min(group_acceptance.values())
        equal_opportunity_gap = max(tpr_values) - min(tpr_values)

        results[model_name] = {
            "demographic_parity_gap": demographic_parity_gap,
            "equal_opportunity_gap": equal_opportunity_gap,
            "group_tpr": group_tpr,
            "group_acceptance_rate": group_acceptance
        }

        print(f"\\n{model_name}:")
        print(f"  Demographic Parity Gap: {demographic_parity_gap:.4f}")
        print(f"  Equal Opportunity Gap: {equal_opportunity_gap:.4f}")

    # Statistical significance of fairness improvement
    print("\\n--- A/B Comparison ---")
    dp_improvement = results["Model A"]["demographic_parity_gap"] - results["Model B"]["demographic_parity_gap"]
    print(f"Demographic Parity improvement: {dp_improvement:.4f} ({'B is fairer' if dp_improvement > 0 else 'A is fairer'})")
    return results

ab_results = ab_test_fairness(preds_a, preds_b, y_test, protected_attr)`,
  },
  {
    id: 21,
    category: 'Privacy',
    title: 'Differential Privacy in ML',
    desc: 'Train models with differential privacy guarantees',
    code: `# Differential Privacy with Opacus (PyTorch) or diffprivlib (sklearn)
from diffprivlib.models import LogisticRegression as DPLogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import numpy as np

# Train with differential privacy
epsilon_values = [0.1, 0.5, 1.0, 5.0, 10.0]
results = []

for epsilon in epsilon_values:
    dp_model = DPLogisticRegression(
        epsilon=epsilon,
        data_norm=10.0,  # L2 norm bound on data
        max_iter=100
    )
    dp_model.fit(X_train, y_train)
    accuracy = accuracy_score(y_test, dp_model.predict(X_test))
    results.append({"epsilon": epsilon, "accuracy": accuracy})
    print(f"Epsilon={epsilon}: Accuracy={accuracy:.4f}")

# Compare with non-private baseline
from sklearn.linear_model import LogisticRegression
baseline = LogisticRegression(max_iter=100)
baseline.fit(X_train, y_train)
baseline_acc = accuracy_score(y_test, baseline.predict(X_test))
print(f"\\nBaseline (no DP): Accuracy={baseline_acc:.4f}")

# Privacy budget analysis
print("\\nPrivacy-Utility Tradeoff:")
for r in results:
    utility_loss = baseline_acc - r["accuracy"]
    print(f"  eps={r['epsilon']}: utility loss = {utility_loss:.4f}")`,
  },
  {
    id: 22,
    category: 'Privacy',
    title: 'Federated Learning Concepts',
    desc: 'Simulate federated learning across data silos on Spark',
    code: `import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

def federated_averaging(client_data, n_rounds=10, n_clients=5):
    \"\"\"Simulate Federated Averaging (FedAvg) across data silos.\"\"\"
    # Initialize global model weights
    global_weights = None
    global_intercept = None

    for round_num in range(n_rounds):
        client_weights = []
        client_intercepts = []
        client_sizes = []

        for client_id in range(n_clients):
            X_client, y_client = client_data[client_id]

            # Local training
            local_model = LogisticRegression(max_iter=50, warm_start=True)
            if global_weights is not None:
                local_model.coef_ = global_weights.copy()
                local_model.intercept_ = global_intercept.copy()
                local_model.classes_ = np.array([0, 1])

            local_model.fit(X_client, y_client)
            client_weights.append(local_model.coef_)
            client_intercepts.append(local_model.intercept_)
            client_sizes.append(len(X_client))

        # Weighted average of client models
        total_size = sum(client_sizes)
        global_weights = sum(w * (s / total_size) for w, s in zip(client_weights, client_sizes))
        global_intercept = sum(b * (s / total_size) for b, s in zip(client_intercepts, client_sizes))

        # Evaluate global model
        eval_model = LogisticRegression()
        eval_model.coef_ = global_weights
        eval_model.intercept_ = global_intercept
        eval_model.classes_ = np.array([0, 1])
        acc = accuracy_score(y_test, eval_model.predict(X_test))
        print(f"Round {round_num + 1}: Global Accuracy = {acc:.4f}")

    return eval_model

# Simulate data silos (e.g., different hospital sites)
client_data = [(X_shard, y_shard) for X_shard, y_shard in zip(
    np.array_split(X_train, 5), np.array_split(y_train, 5)
)]
global_model = federated_averaging(client_data)`,
  },
  {
    id: 23,
    category: 'Governance',
    title: 'Model Governance with MLflow',
    desc: 'Implement model governance workflow with MLflow lifecycle management',
    code: `import mlflow
from mlflow.tracking import MlflowClient
from datetime import datetime

client = MlflowClient()

# 1. Register model with governance metadata
model_name = "churn_prediction_prod"
with mlflow.start_run(run_name="governance_training") as run:
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    # Log comprehensive metadata
    mlflow.log_param("training_data_version", "2024-Q4")
    mlflow.log_param("feature_count", len(feature_names))
    mlflow.log_param("training_samples", train_df.count())
    mlflow.log_param("approved_by", "data_science_lead")
    mlflow.log_param("bias_tested", True)
    mlflow.log_param("fairness_threshold_met", True)

    mlflow.log_metric("auc", 0.87)
    mlflow.log_metric("demographic_parity_gap", 0.03)
    mlflow.log_metric("equal_opportunity_gap", 0.05)

    mlflow.spark.log_model(model, "model", registered_model_name=model_name)

# 2. Model approval workflow
model_version = client.get_latest_versions(model_name, stages=["None"])[0]

# Add governance tags
client.set_model_version_tag(model_name, model_version.version, "review_status", "pending")
client.set_model_version_tag(model_name, model_version.version, "reviewer", "ml_governance_team")
client.set_model_version_tag(model_name, model_version.version, "review_date", datetime.now().isoformat())
client.set_model_version_tag(model_name, model_version.version, "fairness_certified", "true")

# 3. Stage transition (requires approval)
client.transition_model_version_stage(
    name=model_name,
    version=model_version.version,
    stage="Staging",
    archive_existing_versions=False
)
print(f"Model {model_name} v{model_version.version} moved to Staging")

# 4. Promotion to Production (after validation)
client.set_model_version_tag(model_name, model_version.version, "review_status", "approved")
client.transition_model_version_stage(
    name=model_name, version=model_version.version,
    stage="Production", archive_existing_versions=True
)
print(f"Model promoted to Production")`,
  },
  {
    id: 24,
    category: 'Governance',
    title: 'Reproducibility Framework',
    desc: 'Ensure ML experiment reproducibility with seed management and versioning',
    code: `import mlflow
import hashlib
import json
import numpy as np
import random
import os

def set_all_seeds(seed=42):
    \"\"\"Set all random seeds for reproducibility.\"\"\"
    random.seed(seed)
    np.random.seed(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)

    try:
        import torch
        torch.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False
    except ImportError:
        pass

    return seed

def hash_dataset(df):
    \"\"\"Create a deterministic hash of the dataset.\"\"\"
    content = df.toPandas().to_csv(index=False)
    return hashlib.sha256(content.encode()).hexdigest()[:16]

def log_reproducibility_info(spark, df, seed=42):
    \"\"\"Log all information needed to reproduce the experiment.\"\"\"
    set_all_seeds(seed)

    # Log environment
    import pkg_resources
    packages = {p.project_name: p.version for p in pkg_resources.working_set}

    with mlflow.start_run():
        mlflow.log_param("random_seed", seed)
        mlflow.log_param("dataset_hash", hash_dataset(df))
        mlflow.log_param("dataset_rows", df.count())
        mlflow.log_param("spark_version", spark.version)
        mlflow.log_param("python_version", os.sys.version)

        # Log requirements
        with open("/tmp/requirements_snapshot.txt", "w") as f:
            for pkg, ver in sorted(packages.items()):
                f.write(f"{pkg}=={ver}\\n")
        mlflow.log_artifact("/tmp/requirements_snapshot.txt")

        # Log data schema
        schema_info = json.dumps({f.name: str(f.dataType) for f in df.schema.fields})
        mlflow.log_param("data_schema_hash", hashlib.sha256(schema_info.encode()).hexdigest()[:16])

        print(f"Reproducibility info logged. Dataset hash: {hash_dataset(df)}")

log_reproducibility_info(spark, train_df, seed=42)`,
  },
  {
    id: 25,
    category: 'Governance',
    title: 'Human-in-the-Loop Validation',
    desc: 'Implement human review workflow for model predictions',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from datetime import datetime

def create_review_queue(predictions_df, confidence_threshold=0.6):
    \"\"\"Create a human review queue for low-confidence predictions.\"\"\"
    review_queue = predictions_df.withColumn(
        "confidence", F.greatest(F.col("probability_0"), F.col("probability_1"))
    ).withColumn(
        "needs_review", F.col("confidence") < confidence_threshold
    ).withColumn(
        "review_status", F.lit("pending")
    ).withColumn(
        "reviewer", F.lit(None).cast(StringType())
    ).withColumn(
        "review_timestamp", F.lit(None).cast(TimestampType())
    ).withColumn(
        "human_label", F.lit(None).cast(StringType())
    )

    total = review_queue.count()
    needs_review = review_queue.filter("needs_review = true").count()
    print(f"Total predictions: {total}")
    print(f"Needs human review: {needs_review} ({needs_review/total*100:.1f}%)")
    print(f"Auto-approved: {total - needs_review} ({(total-needs_review)/total*100:.1f}%)")

    # Save review queue
    review_queue.write.mode("overwrite").saveAsTable("ml_governance.review_queue")
    return review_queue

def process_review(review_id, reviewer_name, human_label, spark):
    \"\"\"Process a human review decision.\"\"\"
    spark.sql(f\"\"\"
        UPDATE ml_governance.review_queue
        SET review_status = 'reviewed',
            reviewer = '{reviewer_name}',
            review_timestamp = current_timestamp(),
            human_label = '{human_label}'
        WHERE id = '{review_id}'
    \"\"\")
    print(f"Review {review_id} processed by {reviewer_name}: {human_label}")

def compute_agreement_metrics(spark):
    \"\"\"Compute model-human agreement metrics.\"\"\"
    reviewed = spark.sql(\"\"\"
        SELECT prediction, human_label,
               CASE WHEN prediction = human_label THEN 1 ELSE 0 END as agreement
        FROM ml_governance.review_queue
        WHERE review_status = 'reviewed'
    \"\"\")
    agreement_rate = reviewed.agg(F.mean("agreement")).collect()[0][0]
    print(f"Model-Human Agreement Rate: {agreement_rate:.2%}")
    return agreement_rate

review_df = create_review_queue(predictions_df, confidence_threshold=0.65)`,
  },
  {
    id: 26,
    category: 'Ethics',
    title: 'Ethical AI Checklist',
    desc: 'Automated ethical AI assessment checklist for model deployment',
    code: `import json
from datetime import datetime

def run_ethical_ai_checklist(model, X_train, X_test, y_test, protected_attrs, config):
    \"\"\"Run comprehensive ethical AI checklist before deployment.\"\"\"
    checklist = {
        "assessment_date": datetime.now().isoformat(),
        "model_name": config.get("model_name", "unnamed"),
        "assessor": config.get("assessor", "automated"),
        "checks": []
    }

    def add_check(category, name, passed, details=""):
        checklist["checks"].append({
            "category": category, "name": name,
            "passed": passed, "details": details
        })

    # 1. Data Quality
    null_pct = (X_train.isnull().sum() / len(X_train)).max()
    add_check("Data Quality", "Missing values < 10%", null_pct < 0.1, f"Max null: {null_pct:.2%}")

    # 2. Label Balance
    from collections import Counter
    label_counts = Counter(y_test)
    imbalance = min(label_counts.values()) / max(label_counts.values())
    add_check("Data Quality", "Label balance > 20%", imbalance > 0.2, f"Ratio: {imbalance:.2%}")

    # 3. Protected Attributes
    for attr in protected_attrs:
        used = attr in X_train.columns
        add_check("Fairness", f"Protected attr '{attr}' not in features", not used)

    # 4. Performance Threshold
    from sklearn.metrics import roc_auc_score
    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    add_check("Performance", "AUC > 0.7", auc > 0.7, f"AUC: {auc:.4f}")

    # 5. Overfitting Check
    train_auc = roc_auc_score(y_train, model.predict_proba(X_train)[:, 1])
    overfit_gap = train_auc - auc
    add_check("Performance", "Train-Test gap < 0.05", overfit_gap < 0.05, f"Gap: {overfit_gap:.4f}")

    # 6. Explainability Available
    has_feature_importance = hasattr(model, "feature_importances_")
    add_check("Explainability", "Feature importance available", has_feature_importance)

    # Summary
    passed = sum(1 for c in checklist["checks"] if c["passed"])
    total = len(checklist["checks"])
    checklist["summary"] = {
        "total_checks": total, "passed": passed, "failed": total - passed,
        "deployment_approved": passed == total
    }

    print(f"\\nEthical AI Checklist: {passed}/{total} passed")
    for check in checklist["checks"]:
        status = "PASS" if check["passed"] else "FAIL"
        print(f"  [{status}] {check['category']}: {check['name']} — {check['details']}")

    if not checklist["summary"]["deployment_approved"]:
        print("\\n*** DEPLOYMENT BLOCKED: Not all checks passed ***")

    return checklist

result = run_ethical_ai_checklist(model, X_train, X_test, y_test,
    protected_attrs=["gender", "race", "age"],
    config={"model_name": "churn_v2", "assessor": "ml_team"})`,
  },
  {
    id: 27,
    category: 'Monitoring',
    title: 'Model Performance Decay Detection',
    desc: 'Detect gradual model performance decay over time with alerting',
    code: `import numpy as np
from scipy import stats
from datetime import datetime, timedelta

def monitor_performance_decay(daily_metrics, metric_name="auc", window=7, threshold=0.05):
    \"\"\"Detect model performance decay over a rolling window.\"\"\"
    values = [m[metric_name] for m in daily_metrics]
    dates = [m["date"] for m in daily_metrics]

    if len(values) < window * 2:
        print("Insufficient data for decay analysis")
        return None

    # Rolling average
    recent = np.mean(values[-window:])
    baseline = np.mean(values[:window])

    # Trend analysis (linear regression on recent data)
    x = np.arange(len(values))
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, values)

    # Mann-Kendall trend test
    decaying = slope < 0 and abs(recent - baseline) > threshold

    report = {
        "metric": metric_name,
        "baseline_avg": float(baseline),
        "recent_avg": float(recent),
        "change": float(recent - baseline),
        "change_pct": float((recent - baseline) / baseline * 100),
        "trend_slope": float(slope),
        "trend_p_value": float(p_value),
        "is_decaying": decaying,
        "alert_level": "CRITICAL" if abs(recent - baseline) > 2 * threshold else
                       "WARNING" if abs(recent - baseline) > threshold else "OK"
    }

    print(f"Performance Decay Report ({metric_name}):")
    print(f"  Baseline ({window}d): {baseline:.4f}")
    print(f"  Recent ({window}d):   {recent:.4f}")
    print(f"  Change: {report['change_pct']:.2f}%")
    print(f"  Trend slope: {slope:.6f} (p={p_value:.4f})")
    print(f"  Alert: {report['alert_level']}")

    return report

# Example: daily metrics from monitoring table
daily_metrics = spark.sql(\"\"\"
    SELECT date, auc, f1, precision, recall
    FROM ml_monitoring.daily_metrics
    WHERE model_name = 'churn_predictor'
    ORDER BY date
\"\"\").toPandas().to_dict("records")

decay_report = monitor_performance_decay(daily_metrics, "auc")`,
  },
  {
    id: 28,
    category: 'Ethics',
    title: 'Bias Amplification Detection',
    desc: 'Detect if model amplifies existing biases in training data',
    code: `import numpy as np
import pandas as pd

def detect_bias_amplification(train_df, predictions_df, protected_attr, target_col):
    \"\"\"Check if model amplifies biases present in training data.\"\"\"
    results = {}

    # Training data rates
    train_rates = train_df.groupby(protected_attr)[target_col].mean()

    # Prediction rates
    pred_rates = predictions_df.groupby(protected_attr)["prediction"].mean()

    print("Bias Amplification Analysis")
    print("=" * 60)
    print(f"{'Group':<15} {'Train Rate':<15} {'Pred Rate':<15} {'Amplification':<15}")
    print("-" * 60)

    for group in train_rates.index:
        train_rate = train_rates[group]
        pred_rate = pred_rates.get(group, 0)
        amplification = pred_rate - train_rate

        results[group] = {
            "training_rate": float(train_rate),
            "prediction_rate": float(pred_rate),
            "amplification": float(amplification),
            "amplified": abs(amplification) > 0.02
        }

        flag = " *** AMPLIFIED" if abs(amplification) > 0.02 else ""
        print(f"{group:<15} {train_rate:<15.4f} {pred_rate:<15.4f} {amplification:<+15.4f}{flag}")

    # Overall amplification score
    max_train_gap = train_rates.max() - train_rates.min()
    max_pred_gap = pred_rates.max() - pred_rates.min()
    amplification_factor = max_pred_gap / max_train_gap if max_train_gap > 0 else 1.0

    print(f"\\nOverall bias gap — Training: {max_train_gap:.4f}, Predictions: {max_pred_gap:.4f}")
    print(f"Amplification factor: {amplification_factor:.2f}x {'(AMPLIFIED)' if amplification_factor > 1.1 else '(OK)'}")

    return results

amplification = detect_bias_amplification(train_pdf, pred_pdf, "gender", "churn")`,
  },
  {
    id: 29,
    category: 'Documentation',
    title: 'Automated Model Documentation',
    desc: 'Generate comprehensive model documentation with performance visualizations',
    code: `import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import (roc_curve, precision_recall_curve, confusion_matrix,
                              classification_report, roc_auc_score)
import json

def generate_model_report(model, X_test, y_test, feature_names, output_dir="/dbfs/tmp/model_report"):
    \"\"\"Generate full model documentation with plots.\"\"\"
    import os
    os.makedirs(output_dir, exist_ok=True)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    # 1. ROC Curve
    fpr, tpr, _ = roc_curve(y_test, y_proba)
    auc = roc_auc_score(y_test, y_proba)
    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, label=f"AUC = {auc:.4f}")
    plt.plot([0, 1], [0, 1], "k--")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curve")
    plt.legend()
    plt.savefig(f"{output_dir}/roc_curve.png", dpi=150, bbox_inches="tight")
    plt.close()

    # 2. Precision-Recall Curve
    prec, rec, _ = precision_recall_curve(y_test, y_proba)
    plt.figure(figsize=(8, 6))
    plt.plot(rec, prec)
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.title("Precision-Recall Curve")
    plt.savefig(f"{output_dir}/pr_curve.png", dpi=150, bbox_inches="tight")
    plt.close()

    # 3. Confusion Matrix
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(6, 5))
    plt.imshow(cm, cmap="Blues")
    plt.colorbar()
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            plt.text(j, i, str(cm[i, j]), ha="center", va="center")
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.title("Confusion Matrix")
    plt.savefig(f"{output_dir}/confusion_matrix.png", dpi=150, bbox_inches="tight")
    plt.close()

    # 4. Classification Report
    report = classification_report(y_test, y_pred, output_dict=True)

    # 5. Save JSON summary
    summary = {
        "auc_roc": float(auc),
        "classification_report": report,
        "confusion_matrix": cm.tolist(),
        "feature_count": len(feature_names),
        "test_samples": len(y_test)
    }
    with open(f"{output_dir}/model_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"Model report generated at {output_dir}")
    return summary

report = generate_model_report(model, X_test, y_test, feature_names)`,
  },
  {
    id: 30,
    category: 'Privacy',
    title: 'Data Anonymization Pipeline',
    desc: 'Anonymize sensitive data before model training on Spark',
    code: `from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

def anonymize_dataset(df, config):
    \"\"\"Anonymize sensitive columns in a Spark DataFrame.\"\"\"
    result = df

    # 1. Hash PII columns (one-way, deterministic)
    for col_name in config.get("hash_columns", []):
        hash_udf = F.udf(lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:16] if x else None, StringType())
        result = result.withColumn(col_name, hash_udf(F.col(col_name)))

    # 2. Generalize quasi-identifiers
    if "age" in config.get("generalize_columns", []):
        result = result.withColumn("age", (F.floor(F.col("age") / 10) * 10).cast("int"))

    if "zip_code" in config.get("generalize_columns", []):
        result = result.withColumn("zip_code", F.substring(F.col("zip_code"), 1, 3))

    # 3. Suppress rare categories (k-anonymity)
    for col_name in config.get("suppress_rare", []):
        k = config.get("k_threshold", 5)
        counts = result.groupBy(col_name).count()
        rare_values = counts.filter(F.col("count") < k).select(col_name)
        result = result.join(rare_values, col_name, "left_anti")  # remove rare

    # 4. Add noise to numeric columns (differential privacy)
    for col_name in config.get("noise_columns", []):
        epsilon = config.get("noise_epsilon", 1.0)
        noise_scale = 1.0 / epsilon
        result = result.withColumn(
            col_name,
            F.col(col_name) + F.randn() * F.lit(noise_scale)
        )

    # 5. Drop direct identifiers
    for col_name in config.get("drop_columns", []):
        result = result.drop(col_name)

    print(f"Anonymization complete. Rows: {result.count()}, Columns: {len(result.columns)}")
    return result

anon_config = {
    "hash_columns": ["email", "phone"],
    "generalize_columns": ["age", "zip_code"],
    "suppress_rare": ["occupation"],
    "k_threshold": 10,
    "noise_columns": ["income", "account_balance"],
    "noise_epsilon": 1.0,
    "drop_columns": ["name", "ssn", "address"]
}
anon_df = anonymize_dataset(raw_df, anon_config)`,
  },
];

const fairnessScenarios = [
  {
    id: 31,
    category: 'Metric',
    title: 'Demographic Parity Check',
    desc: 'Verify equal positive prediction rates across demographic groups',
    code: `import numpy as np
import pandas as pd

def check_demographic_parity(predictions, protected_attr, threshold=0.1):
    \"\"\"Check if prediction rates are equal across protected groups.

    Demographic Parity: P(Y_hat=1 | A=a) = P(Y_hat=1 | A=b) for all groups a, b
    \"\"\"
    df = pd.DataFrame({"prediction": predictions, "group": protected_attr})
    group_rates = df.groupby("group")["prediction"].mean()

    max_rate = group_rates.max()
    min_rate = group_rates.min()
    parity_gap = max_rate - min_rate

    print("Demographic Parity Analysis")
    print("=" * 50)
    for group, rate in group_rates.items():
        print(f"  {group}: P(Y=1) = {rate:.4f} (n={len(df[df['group']==group])})")

    print(f"\\nParity Gap: {parity_gap:.4f}")
    print(f"Threshold: {threshold}")
    print(f"Result: {'PASS' if parity_gap <= threshold else 'FAIL'}")

    # 80% rule (4/5ths rule from EEOC)
    ratio = min_rate / max_rate if max_rate > 0 else 0
    print(f"\\n4/5ths Rule: {ratio:.4f} ({'PASS' if ratio >= 0.8 else 'FAIL'})")

    return {
        "group_rates": group_rates.to_dict(),
        "parity_gap": float(parity_gap),
        "four_fifths_ratio": float(ratio),
        "passed": parity_gap <= threshold
    }

result = check_demographic_parity(y_pred, gender_labels)`,
  },
  {
    id: 32,
    category: 'Metric',
    title: 'Equal Opportunity Check',
    desc: 'Verify equal true positive rates across groups',
    code: `import numpy as np
import pandas as pd

def check_equal_opportunity(predictions, labels, protected_attr, threshold=0.1):
    \"\"\"Check Equal Opportunity: equal TPR across groups.

    Equal Opportunity: P(Y_hat=1 | Y=1, A=a) = P(Y_hat=1 | Y=1, A=b)
    (True positive rate should be equal across groups)
    \"\"\"
    df = pd.DataFrame({
        "prediction": predictions, "label": labels, "group": protected_attr
    })

    positive_df = df[df["label"] == 1]
    group_tpr = positive_df.groupby("group")["prediction"].mean()

    tpr_gap = group_tpr.max() - group_tpr.min()

    print("Equal Opportunity Analysis (TPR by group)")
    print("=" * 50)
    for group, tpr in group_tpr.items():
        n_pos = len(positive_df[positive_df["group"] == group])
        print(f"  {group}: TPR = {tpr:.4f} (n_positive={n_pos})")

    print(f"\\nTPR Gap: {tpr_gap:.4f}")
    print(f"Result: {'PASS' if tpr_gap <= threshold else 'FAIL'}")

    return {
        "group_tpr": group_tpr.to_dict(),
        "tpr_gap": float(tpr_gap),
        "passed": tpr_gap <= threshold
    }

eo_result = check_equal_opportunity(y_pred, y_test, gender_labels)`,
  },
  {
    id: 33,
    category: 'Metric',
    title: 'Equalized Odds',
    desc: 'Verify equal TPR and FPR across groups simultaneously',
    code: `import numpy as np
import pandas as pd

def check_equalized_odds(predictions, labels, protected_attr, threshold=0.1):
    \"\"\"Check Equalized Odds: equal TPR AND FPR across groups.

    Equalized Odds:
      P(Y_hat=1 | Y=1, A=a) = P(Y_hat=1 | Y=1, A=b)  (equal TPR)
      P(Y_hat=1 | Y=0, A=a) = P(Y_hat=1 | Y=0, A=b)  (equal FPR)
    \"\"\"
    df = pd.DataFrame({
        "prediction": predictions, "label": labels, "group": protected_attr
    })

    results = {}
    for group in df["group"].unique():
        group_df = df[df["group"] == group]
        tp = ((group_df["prediction"] == 1) & (group_df["label"] == 1)).sum()
        fn = ((group_df["prediction"] == 0) & (group_df["label"] == 1)).sum()
        fp = ((group_df["prediction"] == 1) & (group_df["label"] == 0)).sum()
        tn = ((group_df["prediction"] == 0) & (group_df["label"] == 0)).sum()

        tpr = tp / (tp + fn) if (tp + fn) > 0 else 0
        fpr = fp / (fp + tn) if (fp + tn) > 0 else 0
        results[group] = {"tpr": tpr, "fpr": fpr, "n": len(group_df)}

    tpr_values = [v["tpr"] for v in results.values()]
    fpr_values = [v["fpr"] for v in results.values()]
    tpr_gap = max(tpr_values) - min(tpr_values)
    fpr_gap = max(fpr_values) - min(fpr_values)

    print("Equalized Odds Analysis")
    print("=" * 60)
    print(f"{'Group':<15} {'TPR':<10} {'FPR':<10} {'N':<10}")
    print("-" * 45)
    for group, vals in results.items():
        print(f"{group:<15} {vals['tpr']:<10.4f} {vals['fpr']:<10.4f} {vals['n']:<10}")

    print(f"\\nTPR Gap: {tpr_gap:.4f} ({'PASS' if tpr_gap <= threshold else 'FAIL'})")
    print(f"FPR Gap: {fpr_gap:.4f} ({'PASS' if fpr_gap <= threshold else 'FAIL'})")
    print(f"Equalized Odds: {'PASS' if tpr_gap <= threshold and fpr_gap <= threshold else 'FAIL'}")

    return {"tpr_gap": tpr_gap, "fpr_gap": fpr_gap, "group_metrics": results,
            "passed": tpr_gap <= threshold and fpr_gap <= threshold}

eo_result = check_equalized_odds(y_pred, y_test, gender_labels)`,
  },
  {
    id: 34,
    category: 'Metric',
    title: 'Disparate Impact Ratio',
    desc: 'Calculate disparate impact ratio for adverse impact analysis',
    code: `import numpy as np
import pandas as pd

def calculate_disparate_impact(predictions, protected_attr, favorable_outcome=1):
    \"\"\"Calculate Disparate Impact Ratio (DIR).

    DIR = P(favorable | unprivileged) / P(favorable | privileged)
    Legal threshold: DIR >= 0.8 (80% rule / 4/5ths rule)
    \"\"\"
    df = pd.DataFrame({"prediction": predictions, "group": protected_attr})
    group_rates = df.groupby("group").apply(
        lambda x: (x["prediction"] == favorable_outcome).mean()
    )

    # Identify privileged group (highest favorable rate)
    privileged_group = group_rates.idxmax()
    privileged_rate = group_rates.max()

    print("Disparate Impact Analysis")
    print("=" * 60)
    print(f"Privileged group: {privileged_group} (rate: {privileged_rate:.4f})")
    print()

    dir_results = {}
    for group, rate in group_rates.items():
        dir_ratio = rate / privileged_rate if privileged_rate > 0 else 0
        dir_results[group] = {
            "favorable_rate": float(rate),
            "disparate_impact_ratio": float(dir_ratio),
            "adverse_impact": dir_ratio < 0.8
        }

        status = "ADVERSE IMPACT" if dir_ratio < 0.8 else "OK"
        print(f"  {group}: rate={rate:.4f}, DIR={dir_ratio:.4f} [{status}]")

    # Overall assessment
    min_dir = min(d["disparate_impact_ratio"] for d in dir_results.values())
    print(f"\\nMinimum DIR: {min_dir:.4f}")
    print(f"Legal threshold: 0.80")
    print(f"Assessment: {'COMPLIANT' if min_dir >= 0.8 else 'NON-COMPLIANT — remediation required'}")

    return dir_results

dir_results = calculate_disparate_impact(y_pred, gender_labels)`,
  },
  {
    id: 35,
    category: 'Framework',
    title: 'Fairlearn Integration',
    desc: 'Use Microsoft Fairlearn for fairness assessment and mitigation',
    code: `from fairlearn.metrics import (MetricFrame, demographic_parity_difference,
    demographic_parity_ratio, equalized_odds_difference)
from fairlearn.reductions import ExponentiatedGradient, DemographicParity
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# 1. Assess fairness metrics with MetricFrame
metrics = {
    "accuracy": accuracy_score,
    "precision": lambda y, p: precision_score(y, p, zero_division=0),
    "recall": lambda y, p: recall_score(y, p, zero_division=0),
    "f1": lambda y, p: f1_score(y, p, zero_division=0),
    "selection_rate": lambda y, p: p.mean()
}

metric_frame = MetricFrame(
    metrics=metrics,
    y_true=y_test,
    y_pred=y_pred,
    sensitive_features=sensitive_features
)

print("Overall Metrics:")
print(metric_frame.overall)
print("\\nMetrics by Group:")
print(metric_frame.by_group)
print("\\nGroup Differences:")
print(metric_frame.difference())

# 2. Key fairness metrics
dp_diff = demographic_parity_difference(y_test, y_pred, sensitive_features=sensitive_features)
dp_ratio = demographic_parity_ratio(y_test, y_pred, sensitive_features=sensitive_features)
eo_diff = equalized_odds_difference(y_test, y_pred, sensitive_features=sensitive_features)

print(f"\\nDemographic Parity Difference: {dp_diff:.4f}")
print(f"Demographic Parity Ratio: {dp_ratio:.4f}")
print(f"Equalized Odds Difference: {eo_diff:.4f}")

# 3. Mitigate with ExponentiatedGradient
from sklearn.linear_model import LogisticRegression
mitigator = ExponentiatedGradient(
    estimator=LogisticRegression(max_iter=1000),
    constraints=DemographicParity()
)
mitigator.fit(X_train, y_train, sensitive_features=train_sensitive)
fair_preds = mitigator.predict(X_test)

# Compare
print("\\n--- After Mitigation ---")
new_dp = demographic_parity_difference(y_test, fair_preds, sensitive_features=sensitive_features)
new_acc = accuracy_score(y_test, fair_preds)
print(f"DP Difference: {dp_diff:.4f} -> {new_dp:.4f}")
print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f} -> {new_acc:.4f}")`,
  },
  {
    id: 36,
    category: 'Framework',
    title: 'AIF360 (IBM AI Fairness 360)',
    desc: 'Comprehensive fairness assessment with IBM AIF360 toolkit',
    code: `from aif360.datasets import BinaryLabelDataset
from aif360.metrics import BinaryLabelDatasetMetric, ClassificationMetric
from aif360.algorithms.preprocessing import Reweighing
from aif360.algorithms.inprocessing import PrejudiceRemover
import pandas as pd

# Create AIF360 dataset
aif_dataset = BinaryLabelDataset(
    df=train_pdf,
    label_names=["churn"],
    protected_attribute_names=["gender"],
    favorable_label=0,
    unfavorable_label=1
)

# 1. Dataset metrics
dataset_metric = BinaryLabelDatasetMetric(
    aif_dataset,
    unprivileged_groups=[{"gender": 0}],
    privileged_groups=[{"gender": 1}]
)

print("Dataset Fairness Metrics:")
print(f"  Mean difference: {dataset_metric.mean_difference():.4f}")
print(f"  Disparate impact: {dataset_metric.disparate_impact():.4f}")
print(f"  Statistical parity diff: {dataset_metric.statistical_parity_difference():.4f}")
print(f"  Consistency: {dataset_metric.consistency():.4f}")

# 2. Classification metrics
aif_test = BinaryLabelDataset(df=test_pdf, label_names=["churn"],
    protected_attribute_names=["gender"], favorable_label=0, unfavorable_label=1)
aif_pred = aif_test.copy()
aif_pred.labels = y_pred.reshape(-1, 1)

class_metric = ClassificationMetric(
    aif_test, aif_pred,
    unprivileged_groups=[{"gender": 0}],
    privileged_groups=[{"gender": 1}]
)

print("\\nClassification Fairness Metrics:")
print(f"  Equal opportunity diff: {class_metric.equal_opportunity_difference():.4f}")
print(f"  Average odds diff: {class_metric.average_odds_difference():.4f}")
print(f"  Theil index: {class_metric.theil_index():.4f}")
print(f"  Between-group Theil: {class_metric.between_group_theil_index():.4f}")

# 3. Pre-processing: Reweighing
reweigher = Reweighing(unprivileged_groups=[{"gender": 0}], privileged_groups=[{"gender": 1}])
reweighed_dataset = reweigher.fit_transform(aif_dataset)
print(f"\\nReweighed disparate impact: {BinaryLabelDatasetMetric(reweighed_dataset, unprivileged_groups=[{'gender': 0}], privileged_groups=[{'gender': 1}]).disparate_impact():.4f}")`,
  },
  {
    id: 37,
    category: 'Mitigation',
    title: 'Bias Mitigation - Reweighting',
    desc: 'Mitigate bias by adjusting sample weights during training',
    code: `import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

def compute_reweighting_factors(y, protected_attr):
    \"\"\"Compute sample weights to achieve demographic parity.\"\"\"
    df = pd.DataFrame({"y": y, "group": protected_attr})

    # Expected rate under fairness (overall positive rate)
    overall_rate = y.mean()

    weights = np.ones(len(y))
    for group in df["group"].unique():
        mask = df["group"] == group
        group_rate = y[mask].mean()
        group_size = mask.sum()

        for label in [0, 1]:
            label_mask = mask & (y == label)
            expected = overall_rate if label == 1 else (1 - overall_rate)
            actual = y[mask].mean() if label == 1 else (1 - y[mask].mean())

            if actual > 0:
                weights[label_mask] = expected / actual

    # Normalize weights
    weights = weights / weights.mean()

    print("Reweighting Factors:")
    for group in df["group"].unique():
        mask = df["group"] == group
        print(f"  {group}: mean_weight = {weights[mask].mean():.4f}")

    return weights

# Compute weights
sample_weights = compute_reweighting_factors(y_train, train_protected)

# Train with sample weights
model_unweighted = RandomForestClassifier(n_estimators=100, random_state=42)
model_unweighted.fit(X_train, y_train)

model_weighted = RandomForestClassifier(n_estimators=100, random_state=42)
model_weighted.fit(X_train, y_train, sample_weight=sample_weights)

# Compare
for name, m in [("Unweighted", model_unweighted), ("Weighted", model_weighted)]:
    preds = m.predict(X_test)
    acc = accuracy_score(y_test, preds)
    rates = pd.DataFrame({"pred": preds, "group": test_protected}).groupby("group")["pred"].mean()
    parity_gap = rates.max() - rates.min()
    print(f"\\n{name}: Accuracy={acc:.4f}, Parity Gap={parity_gap:.4f}")`,
  },
  {
    id: 38,
    category: 'Mitigation',
    title: 'Bias Mitigation - Resampling',
    desc: 'Mitigate bias by stratified resampling of training data',
    code: `import numpy as np
import pandas as pd
from sklearn.utils import resample

def fair_resample(X, y, protected_attr, strategy="oversample"):
    \"\"\"Resample training data to achieve balanced representation.\"\"\"
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(X.shape[1])])
    df["label"] = y
    df["group"] = protected_attr

    # Create group-label combinations
    df["group_label"] = df["group"].astype(str) + "_" + df["label"].astype(str)

    group_counts = df["group_label"].value_counts()
    print("Original distribution:")
    print(group_counts)

    if strategy == "oversample":
        target_size = group_counts.max()
    elif strategy == "undersample":
        target_size = group_counts.min()
    else:
        target_size = int(group_counts.mean())

    resampled_dfs = []
    for group_label, count in group_counts.items():
        group_df = df[df["group_label"] == group_label]
        resampled = resample(
            group_df,
            replace=(count < target_size),
            n_samples=target_size,
            random_state=42
        )
        resampled_dfs.append(resampled)

    result = pd.concat(resampled_dfs, ignore_index=True)
    result = result.sample(frac=1, random_state=42).reset_index(drop=True)

    print(f"\\nResampled distribution (strategy={strategy}):")
    print(result["group_label"].value_counts())

    feature_cols = [c for c in result.columns if c.startswith("f")]
    X_resampled = result[feature_cols].values
    y_resampled = result["label"].values
    group_resampled = result["group"].values

    return X_resampled, y_resampled, group_resampled

X_fair, y_fair, groups_fair = fair_resample(X_train, y_train, train_protected, strategy="oversample")`,
  },
  {
    id: 39,
    category: 'Mitigation',
    title: 'Adversarial Debiasing',
    desc: 'Use adversarial training to remove protected attribute influence',
    code: `import torch
import torch.nn as nn
import numpy as np
from torch.utils.data import DataLoader, TensorDataset

class AdversarialDebiaser(nn.Module):
    \"\"\"Adversarial debiasing: train predictor to be accurate while
    making it impossible for an adversary to predict the protected attribute.\"\"\"

    def __init__(self, input_dim, hidden_dim=64, n_groups=2):
        super().__init__()
        # Main predictor
        self.predictor = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Linear(hidden_dim // 2, 1),
            nn.Sigmoid()
        )
        # Adversary (tries to predict protected attribute from predictor's hidden state)
        self.adversary = nn.Sequential(
            nn.Linear(hidden_dim // 2, hidden_dim // 4),
            nn.ReLU(),
            nn.Linear(hidden_dim // 4, n_groups),
            nn.Softmax(dim=1)
        )

    def forward(self, x):
        # Get hidden representation
        h = nn.ReLU()(nn.Linear(x.shape[1], 64)(x))
        h = nn.ReLU()(nn.Linear(64, 32)(h))
        prediction = torch.sigmoid(nn.Linear(32, 1)(h))
        adversary_pred = self.adversary(h)
        return prediction, adversary_pred

def train_adversarial(X_train, y_train, protected, epochs=100, adversary_weight=1.0):
    X_t = torch.FloatTensor(X_train)
    y_t = torch.FloatTensor(y_train).unsqueeze(1)
    p_t = torch.LongTensor(protected)

    dataset = TensorDataset(X_t, y_t, p_t)
    loader = DataLoader(dataset, batch_size=256, shuffle=True)

    model = AdversarialDebiaser(X_train.shape[1])
    pred_optimizer = torch.optim.Adam(model.predictor.parameters(), lr=0.001)
    adv_optimizer = torch.optim.Adam(model.adversary.parameters(), lr=0.001)
    pred_loss_fn = nn.BCELoss()
    adv_loss_fn = nn.CrossEntropyLoss()

    for epoch in range(epochs):
        total_pred_loss = 0
        total_adv_loss = 0
        for X_batch, y_batch, p_batch in loader:
            pred, adv_pred = model(X_batch)

            # Train adversary
            adv_loss = adv_loss_fn(adv_pred, p_batch)
            adv_optimizer.zero_grad()
            adv_loss.backward(retain_graph=True)
            adv_optimizer.step()

            # Train predictor (minimize prediction loss + maximize adversary loss)
            pred_loss = pred_loss_fn(pred, y_batch) - adversary_weight * adv_loss
            pred_optimizer.zero_grad()
            pred_loss.backward()
            pred_optimizer.step()

            total_pred_loss += pred_loss.item()
            total_adv_loss += adv_loss.item()

        if (epoch + 1) % 20 == 0:
            print(f"Epoch {epoch+1}: pred_loss={total_pred_loss:.4f}, adv_loss={total_adv_loss:.4f}")

    return model

model = train_adversarial(X_train, y_train, protected_train, adversary_weight=0.5)`,
  },
  {
    id: 40,
    category: 'Analysis',
    title: 'Protected Attribute Analysis',
    desc: 'Comprehensive analysis of model behavior across protected attributes',
    code: `import pandas as pd
import numpy as np
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                              f1_score, roc_auc_score)

def protected_attribute_analysis(y_true, y_pred, y_proba, protected_attrs_dict):
    \"\"\"Analyze model performance across all protected attributes.\"\"\"

    for attr_name, attr_values in protected_attrs_dict.items():
        print(f"\\n{'='*60}")
        print(f"Protected Attribute: {attr_name}")
        print(f"{'='*60}")

        groups = np.unique(attr_values)
        group_metrics = []

        for group in groups:
            mask = attr_values == group
            n = mask.sum()

            if n < 10:
                continue

            metrics = {
                "group": group,
                "n": n,
                "pct": n / len(y_true) * 100,
                "positive_rate": y_pred[mask].mean(),
                "accuracy": accuracy_score(y_true[mask], y_pred[mask]),
                "precision": precision_score(y_true[mask], y_pred[mask], zero_division=0),
                "recall": recall_score(y_true[mask], y_pred[mask], zero_division=0),
                "f1": f1_score(y_true[mask], y_pred[mask], zero_division=0),
            }

            if len(np.unique(y_true[mask])) > 1:
                metrics["auc"] = roc_auc_score(y_true[mask], y_proba[mask])
            else:
                metrics["auc"] = None

            group_metrics.append(metrics)

        metrics_df = pd.DataFrame(group_metrics)
        print(metrics_df.to_string(index=False, float_format="%.4f"))

        # Fairness gaps
        if len(group_metrics) >= 2:
            print(f"\\n  Selection rate gap: {metrics_df['positive_rate'].max() - metrics_df['positive_rate'].min():.4f}")
            print(f"  Accuracy gap: {metrics_df['accuracy'].max() - metrics_df['accuracy'].min():.4f}")
            print(f"  Recall gap: {metrics_df['recall'].max() - metrics_df['recall'].min():.4f}")

protected_attrs = {
    "gender": gender_array,
    "age_group": age_group_array,
    "ethnicity": ethnicity_array
}
protected_attribute_analysis(y_test, y_pred, y_proba, protected_attrs)`,
  },
  {
    id: 41,
    category: 'Training',
    title: 'Fairness Constraints in Training',
    desc: 'Train models with explicit fairness constraints',
    code: `from fairlearn.reductions import (ExponentiatedGradient, DemographicParity,
    EqualizedOdds, TruePositiveRateParity)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

def train_with_fairness_constraint(X_train, y_train, sensitive_train,
                                    X_test, y_test, sensitive_test,
                                    constraint_type="demographic_parity"):
    \"\"\"Train model with different fairness constraints.\"\"\"

    constraints = {
        "demographic_parity": DemographicParity(),
        "equalized_odds": EqualizedOdds(),
        "true_positive_parity": TruePositiveRateParity()
    }

    constraint = constraints[constraint_type]
    base_estimator = LogisticRegression(max_iter=1000, random_state=42)

    # Unconstrained baseline
    base_estimator.fit(X_train, y_train)
    base_preds = base_estimator.predict(X_test)
    base_acc = accuracy_score(y_test, base_preds)

    # Constrained training
    mitigator = ExponentiatedGradient(
        estimator=LogisticRegression(max_iter=1000, random_state=42),
        constraints=constraint,
        eps=0.01,  # fairness violation tolerance
        max_iter=50
    )
    mitigator.fit(X_train, y_train, sensitive_features=sensitive_train)
    fair_preds = mitigator.predict(X_test)
    fair_acc = accuracy_score(y_test, fair_preds)

    # Compare fairness
    from fairlearn.metrics import demographic_parity_difference, equalized_odds_difference

    print(f"Constraint: {constraint_type}")
    print(f"{'Metric':<30} {'Baseline':<15} {'Constrained':<15}")
    print("-" * 60)
    print(f"{'Accuracy':<30} {base_acc:<15.4f} {fair_acc:<15.4f}")

    base_dp = demographic_parity_difference(y_test, base_preds, sensitive_features=sensitive_test)
    fair_dp = demographic_parity_difference(y_test, fair_preds, sensitive_features=sensitive_test)
    print(f"{'Dem. Parity Diff':<30} {base_dp:<15.4f} {fair_dp:<15.4f}")

    base_eo = equalized_odds_difference(y_test, base_preds, sensitive_features=sensitive_test)
    fair_eo = equalized_odds_difference(y_test, fair_preds, sensitive_features=sensitive_test)
    print(f"{'Equalized Odds Diff':<30} {base_eo:<15.4f} {fair_eo:<15.4f}")

    return mitigator

for ct in ["demographic_parity", "equalized_odds", "true_positive_parity"]:
    print(f"\\n{'='*60}")
    train_with_fairness_constraint(X_train, y_train, train_sensitive,
                                    X_test, y_test, test_sensitive, ct)`,
  },
  {
    id: 42,
    category: 'Dashboard',
    title: 'Fairness Dashboard',
    desc: 'Build an interactive fairness dashboard with metrics visualization',
    code: `import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, recall_score, precision_score

def generate_fairness_dashboard(y_true, y_pred, y_proba, sensitive_features,
                                 feature_name="group", output_dir="/dbfs/tmp/fairness"):
    \"\"\"Generate a comprehensive fairness dashboard.\"\"\"
    import os
    os.makedirs(output_dir, exist_ok=True)

    groups = np.unique(sensitive_features)
    metrics_data = []

    for group in groups:
        mask = sensitive_features == group
        metrics_data.append({
            "group": group,
            "selection_rate": y_pred[mask].mean(),
            "accuracy": accuracy_score(y_true[mask], y_pred[mask]),
            "recall": recall_score(y_true[mask], y_pred[mask], zero_division=0),
            "precision": precision_score(y_true[mask], y_pred[mask], zero_division=0),
            "count": mask.sum()
        })

    df = pd.DataFrame(metrics_data)

    # 1. Selection Rate Comparison
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    colors = plt.cm.Set2(np.linspace(0, 1, len(groups)))

    axes[0, 0].bar(df["group"], df["selection_rate"], color=colors)
    axes[0, 0].axhline(y=y_pred.mean(), color="red", linestyle="--", label="Overall rate")
    axes[0, 0].set_title("Selection Rate by Group")
    axes[0, 0].set_ylabel("Positive Prediction Rate")
    axes[0, 0].legend()

    # 2. Accuracy by Group
    axes[0, 1].bar(df["group"], df["accuracy"], color=colors)
    axes[0, 1].axhline(y=accuracy_score(y_true, y_pred), color="red", linestyle="--", label="Overall")
    axes[0, 1].set_title("Accuracy by Group")
    axes[0, 1].set_ylabel("Accuracy")
    axes[0, 1].legend()

    # 3. Recall by Group
    axes[1, 0].bar(df["group"], df["recall"], color=colors)
    axes[1, 0].set_title("Recall (TPR) by Group")
    axes[1, 0].set_ylabel("Recall")

    # 4. Score Distributions
    for i, group in enumerate(groups):
        mask = sensitive_features == group
        axes[1, 1].hist(y_proba[mask], bins=30, alpha=0.5, label=str(group), color=colors[i])
    axes[1, 1].set_title("Score Distribution by Group")
    axes[1, 1].set_xlabel("Predicted Probability")
    axes[1, 1].legend()

    plt.suptitle(f"Fairness Dashboard: {feature_name}", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/fairness_dashboard.png", dpi=150, bbox_inches="tight")
    plt.close()

    print(f"Dashboard saved to {output_dir}/fairness_dashboard.png")
    print(df.to_string(index=False, float_format="%.4f"))

generate_fairness_dashboard(y_test, y_pred, y_proba, gender_labels, "Gender")`,
  },
  {
    id: 43,
    category: 'Analysis',
    title: 'Intersectional Fairness',
    desc: 'Analyze fairness across intersections of protected attributes',
    code: `import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score, f1_score
from itertools import combinations

def intersectional_fairness_analysis(y_true, y_pred, protected_attrs_dict):
    \"\"\"Analyze fairness at intersections of protected attributes.

    Intersectional fairness checks if subgroups defined by combinations
    of protected attributes experience disparate outcomes.
    \"\"\"
    df = pd.DataFrame({
        "y_true": y_true,
        "y_pred": y_pred,
        **protected_attrs_dict
    })

    attr_names = list(protected_attrs_dict.keys())

    # Single attribute analysis
    print("=" * 70)
    print("SINGLE ATTRIBUTE ANALYSIS")
    print("=" * 70)
    for attr in attr_names:
        groups = df.groupby(attr).apply(lambda g: pd.Series({
            "n": len(g),
            "selection_rate": g["y_pred"].mean(),
            "accuracy": accuracy_score(g["y_true"], g["y_pred"])
        }))
        print(f"\\n{attr}:")
        print(groups.to_string(float_format="%.4f"))

    # Intersectional analysis (all pairs)
    print("\\n" + "=" * 70)
    print("INTERSECTIONAL ANALYSIS")
    print("=" * 70)

    for pair in combinations(attr_names, 2):
        col1, col2 = pair
        intersect = df.groupby([col1, col2]).apply(lambda g: pd.Series({
            "n": len(g),
            "selection_rate": g["y_pred"].mean(),
            "accuracy": accuracy_score(g["y_true"], g["y_pred"]) if len(g) >= 5 else None,
            "f1": f1_score(g["y_true"], g["y_pred"], zero_division=0) if len(g) >= 5 else None
        }))

        print(f"\\n{col1} x {col2}:")
        print(intersect.to_string(float_format="%.4f"))

        rates = intersect["selection_rate"].dropna()
        if len(rates) >= 2:
            gap = rates.max() - rates.min()
            ratio = rates.min() / rates.max() if rates.max() > 0 else 0
            print(f"  Max selection rate gap: {gap:.4f}")
            print(f"  Min/Max ratio: {ratio:.4f} ({'PASS' if ratio >= 0.8 else 'FAIL'})")

            # Identify most disadvantaged subgroup
            worst = intersect["selection_rate"].idxmin()
            print(f"  Most disadvantaged subgroup: {worst}")

intersectional_fairness_analysis(y_test, y_pred, {
    "gender": gender_array, "age_group": age_group_array, "region": region_array
})`,
  },
  {
    id: 44,
    category: 'Mitigation',
    title: 'Pre-processing Bias Mitigation',
    desc: 'Apply pre-processing techniques to remove bias before model training',
    code: `import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

def preprocess_for_fairness(X, y, protected_attr, method="disparate_impact_remover"):
    \"\"\"Apply pre-processing bias mitigation techniques.\"\"\"

    if method == "disparate_impact_remover":
        # Transform features to reduce correlation with protected attribute
        from aif360.algorithms.preprocessing import DisparateImpactRemover
        from aif360.datasets import BinaryLabelDataset

        df = pd.DataFrame(X, columns=[f"f{i}" for i in range(X.shape[1])])
        df["label"] = y
        df["protected"] = protected_attr

        aif_ds = BinaryLabelDataset(
            df=df, label_names=["label"],
            protected_attribute_names=["protected"]
        )

        dir_remover = DisparateImpactRemover(repair_level=0.8)
        repaired = dir_remover.fit_transform(aif_ds)
        X_fair = repaired.features[:, :-1]  # exclude protected col

        print(f"Disparate Impact Remover (repair_level=0.8)")

    elif method == "learning_fair_representations":
        # Learn a fair representation using the LFR algorithm
        from aif360.algorithms.preprocessing import LFR
        from aif360.datasets import BinaryLabelDataset

        df = pd.DataFrame(X, columns=[f"f{i}" for i in range(X.shape[1])])
        df["label"] = y
        df["protected"] = protected_attr

        aif_ds = BinaryLabelDataset(
            df=df, label_names=["label"],
            protected_attribute_names=["protected"]
        )

        lfr = LFR(
            unprivileged_groups=[{"protected": 0}],
            privileged_groups=[{"protected": 1}],
            k=5, Ax=0.01, Ay=1.0, Az=50.0
        )
        lfr.fit(aif_ds)
        transformed = lfr.transform(aif_ds)
        X_fair = transformed.features[:, :-1]

        print(f"Learning Fair Representations (LFR)")

    elif method == "correlation_remover":
        # Remove linear correlation between features and protected attribute
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Residualize: regress each feature on protected attribute
        protected_reshaped = protected_attr.reshape(-1, 1)
        from sklearn.linear_model import LinearRegression

        X_fair = np.zeros_like(X_scaled)
        for i in range(X_scaled.shape[1]):
            lr = LinearRegression()
            lr.fit(protected_reshaped, X_scaled[:, i])
            X_fair[:, i] = X_scaled[:, i] - lr.predict(protected_reshaped)

        print(f"Correlation Remover (linear residualization)")

    # Verify reduced correlation
    correlations_before = [np.corrcoef(X[:, i], protected_attr)[0, 1] for i in range(X.shape[1])]
    correlations_after = [np.corrcoef(X_fair[:, i], protected_attr)[0, 1] for i in range(X_fair.shape[1])]

    print(f"  Max |correlation| before: {max(abs(c) for c in correlations_before):.4f}")
    print(f"  Max |correlation| after: {max(abs(c) for c in correlations_after):.4f}")

    return X_fair

X_fair = preprocess_for_fairness(X_train, y_train, protected_train, method="correlation_remover")`,
  },
  {
    id: 45,
    category: 'Mitigation',
    title: 'Post-processing Calibration',
    desc: 'Apply post-processing to calibrate predictions for fairness',
    code: `import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score

def calibrate_for_fairness(y_proba, protected_attr, y_true=None, method="threshold_optimizer"):
    \"\"\"Post-processing calibration to achieve fairness.\"\"\"

    if method == "threshold_optimizer":
        # Find group-specific thresholds that equalize selection rates
        groups = np.unique(protected_attr)
        overall_rate = (y_proba >= 0.5).mean()

        thresholds = {}
        for group in groups:
            mask = protected_attr == group
            group_proba = y_proba[mask]

            # Binary search for threshold that gives target selection rate
            low, high = 0.0, 1.0
            for _ in range(100):
                mid = (low + high) / 2
                rate = (group_proba >= mid).mean()
                if rate > overall_rate:
                    low = mid
                else:
                    high = mid
            thresholds[group] = mid

        # Apply group-specific thresholds
        calibrated_preds = np.zeros(len(y_proba), dtype=int)
        for group in groups:
            mask = protected_attr == group
            calibrated_preds[mask] = (y_proba[mask] >= thresholds[group]).astype(int)

        print("Threshold Optimization:")
        for group, thresh in thresholds.items():
            mask = protected_attr == group
            rate = calibrated_preds[mask].mean()
            print(f"  {group}: threshold={thresh:.4f}, selection_rate={rate:.4f}")

    elif method == "reject_option_classification":
        # Reject Option Classification: flip predictions near decision boundary
        # to favor unprivileged group
        groups = np.unique(protected_attr)
        selection_rates = {}
        for g in groups:
            mask = protected_attr == g
            selection_rates[g] = (y_proba[mask] >= 0.5).mean()

        unprivileged = min(selection_rates, key=selection_rates.get)
        margin = 0.1  # uncertainty band

        calibrated_preds = (y_proba >= 0.5).astype(int)
        uncertain_mask = (y_proba >= 0.5 - margin) & (y_proba <= 0.5 + margin)
        unprivileged_mask = protected_attr == unprivileged

        # Favor unprivileged group in uncertainty band
        calibrated_preds[uncertain_mask & unprivileged_mask] = 1

        print(f"Reject Option Classification (margin={margin}):")
        print(f"  Unprivileged group: {unprivileged}")
        print(f"  Instances adjusted: {(uncertain_mask & unprivileged_mask).sum()}")

    # Evaluate
    if y_true is not None:
        before_acc = accuracy_score(y_true, (y_proba >= 0.5).astype(int))
        after_acc = accuracy_score(y_true, calibrated_preds)
        print(f"\\nAccuracy: {before_acc:.4f} -> {after_acc:.4f}")

    for group in np.unique(protected_attr):
        mask = protected_attr == group
        rate_before = (y_proba[mask] >= 0.5).mean()
        rate_after = calibrated_preds[mask].mean()
        print(f"  {group}: rate {rate_before:.4f} -> {rate_after:.4f}")

    return calibrated_preds

fair_preds = calibrate_for_fairness(y_proba, test_protected, y_test, method="threshold_optimizer")`,
  },
  {
    id: 46,
    category: 'Audit',
    title: 'Fairness Audit Report Generation',
    desc: 'Generate comprehensive fairness audit report for regulatory compliance',
    code: `import json
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.metrics import accuracy_score, roc_auc_score

def generate_fairness_audit_report(model_name, y_true, y_pred, y_proba,
                                    protected_attrs_dict, config=None):
    \"\"\"Generate a comprehensive fairness audit report.\"\"\"
    report = {
        "metadata": {
            "report_title": f"Fairness Audit Report: {model_name}",
            "generated_at": datetime.now().isoformat(),
            "model_name": model_name,
            "total_samples": len(y_true),
            "auditor": config.get("auditor", "Automated") if config else "Automated",
            "framework_version": "1.0"
        },
        "overall_performance": {
            "accuracy": float(accuracy_score(y_true, y_pred)),
            "auc_roc": float(roc_auc_score(y_true, y_proba)),
            "selection_rate": float(y_pred.mean())
        },
        "fairness_analysis": {},
        "compliance": {},
        "recommendations": []
    }

    all_passed = True

    for attr_name, attr_values in protected_attrs_dict.items():
        groups = np.unique(attr_values)
        group_metrics = {}

        for group in groups:
            mask = attr_values == group
            n = mask.sum()
            group_metrics[str(group)] = {
                "n": int(n),
                "selection_rate": float(y_pred[mask].mean()),
                "accuracy": float(accuracy_score(y_true[mask], y_pred[mask])),
                "true_positive_rate": float(
                    y_pred[mask & (y_true == 1)].mean() if (mask & (y_true == 1)).sum() > 0 else 0
                ),
                "false_positive_rate": float(
                    y_pred[mask & (y_true == 0)].mean() if (mask & (y_true == 0)).sum() > 0 else 0
                )
            }

        rates = [m["selection_rate"] for m in group_metrics.values()]
        tprs = [m["true_positive_rate"] for m in group_metrics.values()]

        dp_gap = max(rates) - min(rates)
        eo_gap = max(tprs) - min(tprs)
        dir_ratio = min(rates) / max(rates) if max(rates) > 0 else 0

        attr_passed = dp_gap < 0.1 and dir_ratio >= 0.8
        if not attr_passed:
            all_passed = False

        report["fairness_analysis"][attr_name] = {
            "group_metrics": group_metrics,
            "demographic_parity_gap": float(dp_gap),
            "equal_opportunity_gap": float(eo_gap),
            "disparate_impact_ratio": float(dir_ratio),
            "four_fifths_rule": dir_ratio >= 0.8,
            "passed": attr_passed
        }

        if not attr_passed:
            report["recommendations"].append(
                f"Remediate {attr_name}: DIR={dir_ratio:.3f}, DP gap={dp_gap:.3f}. "
                f"Consider reweighting, threshold adjustment, or adversarial debiasing."
            )

    report["compliance"] = {
        "overall_passed": all_passed,
        "four_fifths_rule": all(
            a["four_fifths_rule"] for a in report["fairness_analysis"].values()
        ),
        "eeoc_compliant": all(
            a["disparate_impact_ratio"] >= 0.8 for a in report["fairness_analysis"].values()
        )
    }

    # Print summary
    print(f"\\n{'='*60}")
    print(f"FAIRNESS AUDIT REPORT: {model_name}")
    print(f"{'='*60}")
    print(f"Date: {report['metadata']['generated_at']}")
    print(f"Samples: {report['metadata']['total_samples']}")
    print(f"\\nOverall: Accuracy={report['overall_performance']['accuracy']:.4f}, "
          f"AUC={report['overall_performance']['auc_roc']:.4f}")

    for attr, analysis in report["fairness_analysis"].items():
        status = "PASS" if analysis["passed"] else "FAIL"
        print(f"\\n  {attr}: [{status}]")
        print(f"    DP Gap: {analysis['demographic_parity_gap']:.4f}")
        print(f"    DIR: {analysis['disparate_impact_ratio']:.4f}")
        for group, m in analysis["group_metrics"].items():
            print(f"    {group}: rate={m['selection_rate']:.4f}, acc={m['accuracy']:.4f}")

    print(f"\\nCompliance: {'PASSED' if report['compliance']['overall_passed'] else 'FAILED'}")

    if report["recommendations"]:
        print("\\nRecommendations:")
        for rec in report["recommendations"]:
            print(f"  - {rec}")

    # Save report
    with open("/dbfs/tmp/fairness_audit_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    return report

audit = generate_fairness_audit_report(
    "churn_predictor_v2", y_test, y_pred, y_proba,
    {"gender": gender_array, "age_group": age_group_array},
    config={"auditor": "ML Governance Team"}
)`,
  },
];

const tabs = [
  {
    key: 'explainable',
    label: 'Explainable AI',
    scenarios: explainableScenarios,
    badgeClass: 'badge pending',
  },
  {
    key: 'responsible',
    label: 'Responsible AI',
    scenarios: responsibleScenarios,
    badgeClass: 'badge running',
  },
  {
    key: 'fairness',
    label: 'Fairness AI',
    scenarios: fairnessScenarios,
    badgeClass: 'badge completed',
  },
];

function XAI() {
  const [activeTab, setActiveTab] = useState('explainable');
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
          <h1>XAI — Explainable, Responsible &amp; Fairness AI</h1>
          <p>
            {totalScenarios} scenarios across Explainability, Responsible AI, and Fairness for
            Databricks / PySpark
          </p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
          {tabs.map((tab) => (
            <button
              key={tab.key}
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
                {expandedId === scenario.id ? '\u25BC' : '\u25B6'}
              </span>
            </div>
            {expandedId === scenario.id && <ScenarioCard scenario={scenario} />}
          </div>
        ))}
      </div>
    </div>
  );
}

export default XAI;
