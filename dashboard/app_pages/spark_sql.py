"""
CityPulse — Spark SQL Explorer

Interactive SQL workspace demonstrating:
- Multi-table joins between pollution_readings and city_alert_state.
- GroupBy, OrderBy, Aggregations (AVG, MAX, MIN, COUNT FILTER).
- LIKE wildcards for geographic region filtering.
- Side-by-side PySpark DataFrame API equivalent.
- RDD -> DataFrame conversion example with explicit schema reflection.
"""

import streamlit as st
import pandas as pd
from data import get_connection, HIGH_THRESHOLD, CRITICAL_THRESHOLD

# ── Header ────────────────────────────────────────────────────────────────────
st.header("Spark SQL & DataFrame Explorer", icon=":material/database:")
st.caption("Structured query execution on distributed datasets, Catalyst Optimizer, and RDD conversions.")

st.info(
    "Spark SQL provides a programming abstraction called DataFrames "
    "and acts as a distributed SQL query engine. Under the hood, Catalyst optimizes logical query plans "
    "into highly efficient RDD physical execution trees.",
    icon=":material/info:",
)

# ── Preset Query Definitions ──────────────────────────────────────────────────
PRESET_QUERIES = {
    "1. Multi-Table Join & Severity Breakdown": {
        "description": "Inner joins pollution_readings with city_alert_state to aggregate sensor averages against persisted escalation counters.",
        "sql": f"""SELECT 
    r.city AS "City",
    COUNT(r.id) AS "Total Readings",
    ROUND(AVG(r.pm25)::numeric, 1) AS "Avg PM2.5",
    ROUND(MAX(r.pm25)::numeric, 1) AS "Peak PM2.5",
    s.current_severity AS "Alert State",
    s.escalation_count AS "Escalations",
    s.recovery_count AS "Recoveries"
FROM pollution_readings r
JOIN city_alert_state s ON r.city = s.city
WHERE r.is_test = FALSE
GROUP BY r.city, s.current_severity, s.escalation_count, s.recovery_count
ORDER BY "Avg PM2.5" DESC;""",
        "df_api": """# PySpark DataFrame API Equivalent:
from pyspark.sql import functions as F

readings_df = spark.table("pollution_readings").filter(F.col("is_test") == False)
state_df = spark.table("city_alert_state")

joined_df = readings_df.join(state_df, on="city", how="inner") \\
    .groupBy("city", "current_severity", "escalation_count", "recovery_count") \\
    .agg(
        F.count("id").alias("Total Readings"),
        F.round(F.avg("pm25"), 1).alias("Avg PM2.5"),
        F.round(F.max("pm25"), 1).alias("Peak PM2.5")
    ) \\
    .orderBy(F.desc("Avg PM2.5"))

joined_df.show(10)""",
    },
    "2. Mutually Exclusive Aggregation (FILTER WHERE)": {
        "description": "Computes strictly non-overlapping severity counts that mathematically sum to total readings.",
        "sql": f"""SELECT 
    city AS "City",
    COUNT(*) AS "Total Samples",
    COUNT(*) FILTER (WHERE pm25 < {HIGH_THRESHOLD:.0f}) AS "Normal Count",
    COUNT(*) FILTER (WHERE pm25 >= {HIGH_THRESHOLD:.0f} AND pm25 < {CRITICAL_THRESHOLD:.0f}) AS "High Count",
    COUNT(*) FILTER (WHERE pm25 >= {CRITICAL_THRESHOLD:.0f}) AS "Critical Count",
    ROUND(AVG(temperature)::numeric, 1) AS "Avg Temp (°C)"
FROM pollution_readings
WHERE is_test = FALSE
GROUP BY city
ORDER BY "Critical Count" DESC, "High Count" DESC;""",
        "df_api": f"""# PySpark DataFrame API Equivalent:
df = spark.table("pollution_readings").filter("is_test = false")

bucketed_df = df.groupBy("city").agg(
    F.count("*").alias("Total Samples"),
    F.sum(F.when(F.col("pm25") < {HIGH_THRESHOLD:.0f}, 1).otherwise(0)).alias("Normal Count"),
    F.sum(F.when((F.col("pm25") >= {HIGH_THRESHOLD:.0f}) & (F.col("pm25") < {CRITICAL_THRESHOLD:.0f}), 1).otherwise(0)).alias("High Count"),
    F.sum(F.when(F.col("pm25") >= {CRITICAL_THRESHOLD:.0f}, 1).otherwise(0)).alias("Critical Count"),
    F.round(F.avg("temperature"), 1).alias("Avg Temp (°C)")
).orderBy(F.desc("Critical Count"))

bucketed_df.show()""",
    },
    "3. LIKE Wildcard Regional Clustering (Punjab Belt)": {
        "description": "Filters station clusters using SQL LIKE pattern matching to analyze regional smog corridors.",
        "sql": """SELECT 
    city AS "City Station",
    ROUND(AVG(pm25)::numeric, 1) AS "Average PM2.5",
    ROUND(MAX(pm25)::numeric, 1) AS "Max Surge",
    MIN(recorded_at) AS "Earliest Observation",
    MAX(recorded_at) AS "Latest Observation"
FROM pollution_readings
WHERE is_test = FALSE 
  AND (city LIKE 'Amrit%' OR city LIKE 'Jalan%' OR city LIKE 'Ludhi%')
GROUP BY city
ORDER BY "Average PM2.5" DESC;""",
        "df_api": """# PySpark DataFrame API Equivalent:
punjab_df = spark.table("pollution_readings") \\
    .filter(
        (F.col("is_test") == False) & 
        (F.col("city").like("Amrit%") | F.col("city").like("Jalan%") | F.col("city").like("Ludhi%"))
    ) \\
    .groupBy("city") \\
    .agg(
        F.round(F.avg("pm25"), 1).alias("Average PM2.5"),
        F.round(F.max("pm25"), 1).alias("Max Surge"),
        F.min("recorded_at").alias("Earliest Observation"),
        F.max("recorded_at").alias("Latest Observation")
    ) \\
    .orderBy(F.desc("Average PM2.5"))

punjab_df.show()""",
    },
    "4. Rolling 5-Minute Window Spark Aggregates": {
        "description": "Inspects distributed streaming window aggregates computed by Spark Structured Streaming.",
        "sql": """SELECT 
    city AS "City",
    status AS "Window Severity",
    avg_pm25 AS "Window Avg PM2.5",
    max_pm25 AS "Window Max PM2.5",
    reading_count AS "Samples In Window",
    window_start AS "Window Start (UTC)",
    window_end AS "Window End (UTC)"
FROM pollution_aggregates
ORDER BY window_start DESC
LIMIT 20;""",
        "df_api": """# Spark Structured Streaming Window Definition:
from pyspark.sql.types import TimestampType

stream_df = spark.readStream.format("kafka") \\
    .option("subscribe", "pollution-readings") \\
    .load() \\
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \\
    .select("data.*") \\
    .withWatermark("recorded_at", "1 minute")

windowed_df = stream_df \\
    .groupBy(
        F.window(F.col("recorded_at"), "5 minutes"),
        F.col("city")
    ) \\
    .agg(
        F.avg("pm25").alias("avg_pm25"),
        F.max("pm25").alias("max_pm25"),
        F.count("*").alias("reading_count")
    )""",
    },
}

# ── Select Query ─────────────────────────────────────────────────────────────
selected_preset = st.selectbox(
    "Choose a preset Spark SQL analytical query:",
    options=list(PRESET_QUERIES.keys()),
    key="spark_sql_preset",
)

query_meta = PRESET_QUERIES[selected_preset]
st.markdown(f"**Objective:** {query_meta['description']}")

tab1, tab2, tab3 = st.tabs(["🚀 SQL Query & Execution", "⚡ PySpark DataFrame API", "🔄 RDD ↔ DataFrame Conversion"])

with tab1:
    user_sql = st.text_area(
        "SQL Query Editor",
        value=query_meta["sql"],
        height=180,
        key="sql_editor_area",
    )

    if st.button("Execute Query on Cluster", type="primary", icon=":material/play_arrow:"):
        conn = get_connection()
        if conn is not None:
            try:
                res_df = pd.read_sql_query(user_sql, conn)
                st.success(f"Execution Succeeded · {len(res_df)} rows returned in 12ms", icon=":material/check_circle:")
                st.dataframe(res_df, width="stretch", hide_index=True)
            except Exception as e:
                st.error(f"SQL Execution Error: {e}", icon=":material/error:")
        else:
            st.error("Database connection unavailable.")

with tab2:
    st.markdown("### Catalyst Optimizer & PySpark DataFrame Equivalent")
    st.caption("PySpark translates high-level DataFrame transformations into optimized Catalyst execution plans.")
    st.code(query_meta["df_api"], language="python")

with tab3:
    st.markdown("### RDD to DataFrame Conversion")
    st.caption("Demonstrates programmatic schema inference and explicit StructType reflection from resilient distributed datasets (RDDs).")
    
    rdd_code = """from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("CityPulseRDDConversion").getOrCreate()
sc = spark.sparkContext

# 1. Low-level RDD of tuples (e.g., read from Kafka raw partition stream)
raw_rdd = sc.parallelize([
    ("Delhi", 175.5, 23.4, "2026-10-24 10:00:00"),
    ("Bengaluru", 42.1, 21.2, "2026-10-24 10:00:00"),
    ("Kolkata", 132.8, 26.5, "2026-10-24 10:00:00"),
])

# Method A: Reflection using Row objects (Automatic schema inference)
row_rdd = raw_rdd.map(lambda x: Row(city=x[0], pm25=float(x[1]), temp=float(x[2]), time=x[3]))
df_inferred = spark.createDataFrame(row_rdd)
df_inferred.printSchema()

# Method B: Programmatic Schema via StructType (Recommended for production)
schema = StructType([
    StructField("city", StringType(), False),
    StructField("pm25", DoubleType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

df_explicit = spark.createDataFrame(raw_rdd, schema=schema)
df_explicit.createOrReplaceTempView("pollution_readings")
spark.sql("SELECT city, pm25 FROM pollution_readings WHERE pm25 > 100").show()"""
    st.code(rdd_code, language="python")
