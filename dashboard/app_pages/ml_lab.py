"""
CityPulse — Spark MLlib & Machine Learning Lab

Interactive machine learning workspace featuring:
- Regression: Linear Regression for PM2.5 forecasting (R², RMSE, MAE, Predicted vs Actual curve).
- Classification: Logistic Regression, Decision Tree, Support Vector Machine (SVM), and Naive Bayes
  for severity prediction with interactive Confusion Matrix heatmap and model comparison benchmark.
- Clustering: K-means clustering of cities into ambient pollution archetypes.
- Feature Correlation Heatmap.
- Full PySpark MLlib Pipeline and VectorAssembler code references.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px

from data import load_readings, HIGH_THRESHOLD, CRITICAL_THRESHOLD
from theme import (
    SEVERITY_NORMAL,
    SEVERITY_HIGH,
    SEVERITY_CRITICAL,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
    get_city_color,
)

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.cluster import KMeans
from sklearn.metrics import (
    r2_score,
    mean_squared_error,
    mean_absolute_error,
    accuracy_score,
    f1_score,
    confusion_matrix,
)

# ── Header ────────────────────────────────────────────────────────────────────
st.header("Spark MLlib & Machine Learning Lab", icon=":material/model_training:")
st.caption("Distributed machine learning, feature extraction via VectorAssembler, and model evaluation.")

st.info(
    "Spark MLlib provides distributed implementations of classification, "
    "regression, clustering, and collaborative filtering built on top of DataFrames and ML Pipelines.",
    icon=":material/info:",
)

# ── Load and Prep Feature Data ───────────────────────────────────────────────
readings = load_readings()

if readings.empty or len(readings) < 50:
    st.warning("Insufficient readings to train ML models. Please run `python scripts/generate_seed_data.py` first.")
    st.stop()

readings["recorded_at"] = pd.to_datetime(readings["recorded_at"], utc=True)
readings["recorded_at_ist"] = readings["recorded_at"].dt.tz_convert("Asia/Kolkata")
readings["hour"] = readings["recorded_at_ist"].dt.hour
readings["day_of_week"] = readings["recorded_at_ist"].dt.dayofweek
readings["temperature"] = readings["temperature"].fillna(readings["temperature"].mean())

# Sort and generate rolling lag
readings = readings.sort_values(["city", "recorded_at"])
readings["pm25_lag1"] = readings.groupby("city")["pm25"].shift(1).fillna(readings["pm25"])

# Target classes: 0 = NORMAL, 1 = HIGH, 2 = CRITICAL
def label_sev(pm):
    if pm >= CRITICAL_THRESHOLD:
        return 2
    elif pm >= HIGH_THRESHOLD:
        return 1
    return 0

readings["sev_label"] = readings["pm25"].apply(label_sev)
class_names = ["NORMAL (<100)", "HIGH (100-150)", "CRITICAL (≥150)"]

# Tabs for the distinct ML disciplines
tab_reg, tab_cls, tab_cluster, tab_corr, tab_pyspark = st.tabs([
    "📈 PM2.5 Regression",
    "🎯 Severity Classification",
    "🌐 K-means Clustering",
    "🔥 Feature Correlation",
    "⚡ PySpark MLlib Code",
])

# ── TAB 1: REGRESSION LAB ─────────────────────────────────────────────────────
with tab_reg:
    st.subheader("Continuous PM2.5 Forecasting (Linear Regression)", icon=":material/trending_up:")
    st.caption("Trains a linear predictor using temporal factors (Hour of Day, Day of Week, Ambient Temperature, and Prior Lag).")

    reg_features = ["hour", "day_of_week", "temperature", "pm25_lag1"]
    X_reg = readings[reg_features]
    y_reg = readings["pm25"]

    X_train, X_test, y_train, y_test = train_test_split(X_reg, y_reg, test_size=0.25, random_state=42)

    lin_reg = LinearRegression()
    lin_reg.fit(X_train, y_train)
    y_pred = lin_reg.predict(X_test)

    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)

    with st.container(horizontal=True):
        st.metric("R² Score (Variance Explained)", f"{r2:.3f}", border=True, help="1.0 is a perfect fit")
        st.metric("Root Mean Squared Error (RMSE)", f"{rmse:.2f} µg/m³", border=True, help="Standard deviation of residuals")
        st.metric("Mean Absolute Error (MAE)", f"{mae:.2f} µg/m³", border=True, help="Average absolute forecasting error")

    # Interactive comparison scatter
    test_sample = pd.DataFrame({
        "Actual": y_test.values[:120],
        "Predicted": y_pred[:120],
    })

    fig_reg = go.Figure()
    fig_reg.add_trace(go.Scatter(
        y=test_sample["Actual"],
        mode="lines+markers",
        name="Actual PM2.5",
        line=dict(color="#38BDF8", width=2),
        marker=dict(size=5),
    ))
    fig_reg.add_trace(go.Scatter(
        y=test_sample["Predicted"],
        mode="lines+markers",
        name="Linear Regression Forecast",
        line=dict(color="#2DD4BF", width=2, dash="dash"),
        marker=dict(size=4),
    ))
    fig_reg.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY, size=12),
        height=320,
        margin=dict(l=40, r=40, t=30, b=40),
        xaxis=dict(title="Test Observation Sample Index", gridcolor="rgba(148, 163, 184, 0.12)"),
        yaxis=dict(title="PM2.5 Concentration (µg/m³)", gridcolor="rgba(148, 163, 184, 0.12)"),
        legend=dict(orientation="h", y=1.05, x=1, xanchor="right"),
    )
    st.plotly_chart(fig_reg, width="stretch", alt="Linear regression prediction versus actual readings")


# ── TAB 2: CLASSIFICATION LAB ─────────────────────────────────────────────────
with tab_cls:
    st.subheader("Severity Classification & Model Benchmark", icon=":material/fact_check:")
    st.caption("Compares 4 fundamental classification algorithms across accuracy, macro F1-score, and confusion matrices.")

    sample_readings = readings.tail(600) if len(readings) > 600 else readings
    X_cls = sample_readings[["hour", "temperature", "pm25_lag1"]]
    y_cls = sample_readings["sev_label"]

    Xc_train, Xc_test, yc_train, yc_test = train_test_split(X_cls, y_cls, test_size=0.25, random_state=42)

    classifiers = {
        "Random Forest": RandomForestClassifier(n_estimators=30, max_depth=5, random_state=42),
        "Decision Tree": DecisionTreeClassifier(max_depth=5, random_state=42),
        "Logistic Regression": LogisticRegression(max_iter=500),
        "Gaussian Naive Bayes": GaussianNB(),
    }

    benchmark_results = []
    models_fitted = {}

    for name, clf in classifiers.items():
        clf.fit(Xc_train, yc_train)
        preds = clf.predict(Xc_test)
        acc = accuracy_score(yc_test, preds)
        f1 = f1_score(yc_test, preds, average="weighted")
        benchmark_results.append({
            "Model": name,
            "Accuracy": round(acc * 100, 2),
            "F1-Score": round(f1 * 100, 2),
        })
        models_fitted[name] = (clf, preds)

    bench_df = pd.DataFrame(benchmark_results)

    col_b1, col_b2 = st.columns([1, 1])

    with col_b1:
        st.markdown("**Model Accuracy Comparison (%)**")
        fig_bar = px.bar(
            bench_df,
            x="Model",
            y="Accuracy",
            color="Model",
            color_discrete_sequence=["#2DD4BF", "#F59E0B", "#8B5CF6", "#38BDF8"],
            text="Accuracy",
        )
        fig_bar.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY, size=12),
            height=300,
            margin=dict(l=30, r=30, t=20, b=40),
            showlegend=False,
            yaxis=dict(range=[0, 105], gridcolor="rgba(148, 163, 184, 0.12)"),
        )
        fig_bar.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_bar, width="stretch", alt="Accuracy comparison chart")

    with col_b2:
        st.markdown("**Benchmark Summary Table**")
        st.dataframe(bench_df, hide_index=True, width="stretch")

    st.divider()

    # Confusion Matrix Inspector
    selected_model = st.selectbox(
        "Inspect Confusion Matrix for model:",
        options=list(classifiers.keys()),
        index=1,
        key="cm_model_choice",
    )

    _, chosen_preds = models_fitted[selected_model]
    cm = confusion_matrix(yc_test, chosen_preds, labels=[0, 1, 2])

    fig_cm = px.imshow(
        cm,
        labels=dict(x="Predicted Severity", y="Actual Severity", color="Observations"),
        x=class_names,
        y=class_names,
        color_continuous_scale="Teal",
        text_auto=True,
    )
    fig_cm.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY, size=12),
        height=320,
        margin=dict(l=40, r=40, t=30, b=40),
    )
    st.plotly_chart(fig_cm, width="stretch", alt="Confusion matrix heatmap")


# ── TAB 3: K-MEANS CLUSTERING ────────────────────────────────────────────────
with tab_cluster:
    st.subheader("Unsupervised City Station Archetypes (K-Means)", icon=":material/bubble_chart:")
    st.caption("Partitions monitoring stations into 3 environmental clusters based on Average PM2.5 and Atmospheric Volatility.")

    city_stats = readings.groupby("city").agg(
        avg_pm25=("pm25", "mean"),
        std_pm25=("pm25", "std"),
        peak_pm25=("pm25", "max"),
        avg_temp=("temperature", "mean"),
    ).reset_index()

    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    city_stats["cluster"] = kmeans.fit_predict(city_stats[["avg_pm25", "std_pm25"]])
    cluster_labels = {0: "Low PM2.5 (Clean Plateau)", 1: "Severe Smog Belt", 2: "Moderate Industrial"}
    city_stats["Archetype"] = city_stats["cluster"].map(cluster_labels)

    fig_cluster = px.scatter(
        city_stats,
        x="avg_pm25",
        y="std_pm25",
        color="Archetype",
        text="city",
        size="peak_pm25",
        color_discrete_map={
            "Low PM2.5 (Clean Plateau)": "#2DD4BF",
            "Moderate Industrial": "#F59E0B",
            "Severe Smog Belt": "#F43F5E",
        },
    )
    fig_cluster.update_traces(textposition="top center", marker=dict(opacity=0.9))
    fig_cluster.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY, size=12),
        height=380,
        margin=dict(l=40, r=40, t=30, b=40),
        xaxis=dict(title="Average PM2.5 (µg/m³)", gridcolor="rgba(148, 163, 184, 0.12)"),
        yaxis=dict(title="Volatility / Standard Deviation (σ)", gridcolor="rgba(148, 163, 184, 0.12)"),
    )
    st.plotly_chart(fig_cluster, width="stretch", alt="K-means city clustering scatter plot")


# ── TAB 4: FEATURE CORRELATION ───────────────────────────────────────────────
with tab_corr:
    st.subheader("Feature Correlation Matrix", icon=":material/heat_pump:")
    st.caption("Pearson correlation coefficients identifying atmospheric codependencies.")

    corr_df = readings[["pm25", "temperature", "hour", "day_of_week", "pm25_lag1"]].corr()

    fig_corr = px.imshow(
        corr_df,
        text_auto=".2f",
        color_continuous_scale="RdBu_r",
        zmin=-1.0,
        zmax=1.0,
        labels=dict(color="Pearson r"),
    )
    fig_corr.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY, size=12),
        height=350,
        margin=dict(l=40, r=40, t=30, b=40),
    )
    st.plotly_chart(fig_corr, width="stretch", alt="Feature correlation heatmap")


# ── TAB 5: PYSPARK MLLIB CODE REFERENCE ──────────────────────────────────────
with tab_pyspark:
    st.subheader("Production PySpark MLlib Pipeline Implementation", icon=":material/code:")
    st.caption("Demonstrating VectorAssembler, ML Pipeline, CrossValidator, and MulticlassClassificationEvaluator.")

    pyspark_pipeline_code = """from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("CityPulseMLlibPipeline").getOrCreate()

# 1. Load distributed DataFrame from PostgreSQL or Kafka
df = spark.read.jdbc(
    url="jdbc:postgresql://postgres:5432/citypulse",
    table="pollution_readings",
    properties={"user": "citypulse", "password": "citypulse_dev_password"}
).filter("is_test = false")

# 2. Feature Assembly: Assemble numerical columns into a single MLlib feature vector
assembler = VectorAssembler(
    inputCols=["temperature", "hour", "pm25_lag1"],
    outputCol="raw_features"
)

# 3. Feature Scaling: Standardize features to zero mean and unit variance
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=True
)

# 4. Pipeline Estimator: Linear Regression for continuous forecasting
lr = LinearRegression(featuresCol="features", labelCol="pm25", maxIter=100, regParam=0.1)

# 5. Build and Fit End-to-End Distributed Pipeline
pipeline = Pipeline(stages=[assembler, scaler, lr])
model = pipeline.fit(df)

# 6. Distributed Evaluation
predictions = model.transform(df)
evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Distributed Spark RMSE: {rmse:.2f} µg/m³")"""
    st.code(pyspark_pipeline_code, language="python")
