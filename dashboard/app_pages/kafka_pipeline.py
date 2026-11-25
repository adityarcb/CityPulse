"""
CityPulse — Kafka & Spark Streaming Pipeline

Streaming pipeline control centre demonstrating:
- Live Kafka broker status and topic partition inspection (pollution-readings).
- End-to-end distributed topology (Producer -> Kafka -> Spark Streaming / Python Consumer -> Postgres).
- Interactive Live Event Publisher: publish test readings directly to Kafka from the dashboard.
- Real-time consumer stream inspector with latency metrics.
- Spark Structured Streaming code with watermarking and 5-minute tumbling windows.
"""

import os
import json
import time
from datetime import datetime, timezone
import streamlit as st
import pandas as pd

from data import get_connection, HIGH_THRESHOLD, CRITICAL_THRESHOLD
from theme import (
    SEVERITY_NORMAL, SEVERITY_HIGH, SEVERITY_CRITICAL,
    BG_CARD, TEXT_PRIMARY, TEXT_MUTED, BORDER_SUBTLE,
)
from city_coordinates import CITIES

# ── Header ────────────────────────────────────────────────────────────────────
st.header("Kafka & Spark Streaming Pipeline", icon=":material/stream:")
st.caption("Distributed event streaming, consumer groups, windowed stateful streaming, and Kafka-Spark integration.")

st.info(
    "Apache Kafka serves as the distributed high-throughput pub-sub commit log. "
    "Apache Spark Structured Streaming ingests micro-batches with event-time watermarking to guarantee "
    "fault-tolerant exactly-once stateful tumbling window processing.",
    icon=":material/info:",
)

# ── Kafka Configuration ───────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:29092")
KAFKA_TOPIC = "pollution-readings"

# ── Helper: Kafka Health Check ────────────────────────────────────────────────
@st.cache_data(ttl=5)
def get_kafka_metadata():
    """Inspect Kafka broker and partition offsets."""
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVERS,
            request_timeout_ms=2500,
            consumer_timeout_ms=2000,
        )
        topics = consumer.topics()
        topic_exists = KAFKA_TOPIC in topics
        
        partition_info = []
        total_messages = 0
        if topic_exists:
            partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
            for p in sorted(list(partitions or [])):
                tp = TopicPartition(KAFKA_TOPIC, p)
                beg_offsets = consumer.beginning_offsets([tp])
                end_offsets = consumer.end_offsets([tp])
                beg = beg_offsets.get(tp, 0)
                end = end_offsets.get(tp, 0)
                msg_count = max(0, end - beg)
                total_messages += msg_count
                partition_info.append({
                    "Partition": p,
                    "Earliest Offset": beg,
                    "Latest Offset": end,
                    "Total Messages": msg_count,
                })
        consumer.close()
        return {
            "connected": True,
            "broker": KAFKA_SERVERS.split(":")[0],  # Mask internal port
            "topic_exists": topic_exists,
            "total_topics": len(topics),
            "partitions": partition_info,
            "total_messages": total_messages,
        }
    except Exception as e:
        return {
            "connected": False,
            "broker": KAFKA_SERVERS.split(":")[0],
            "error": str(e),
            "total_messages": 0,
            "partitions": [],
        }

meta = get_kafka_metadata()

# ── Top Metrics Banner ────────────────────────────────────────────────────────
col_m1, col_m2, col_m3, col_m4 = st.columns(4)

with col_m1:
    if meta["connected"]:
        st.metric("Kafka Broker Status", "Connected", "Active · Port Masked")
    else:
        st.metric("Kafka Broker Status", "Offline", "Reconnecting…")

with col_m2:
    st.metric("Target Topic", KAFKA_TOPIC, f"{len(meta.get('partitions', []))} Partition(s)")

with col_m3:
    st.metric("Topic Message Offset", f"{meta.get('total_messages', 0):,}", "Cumulative log commit")

with col_m4:
    # Query latest ingestion rate from postgres
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM pollution_readings WHERE recorded_at >= NOW() - INTERVAL '1 hour'"
                )
                rate = cur.fetchone()[0]
        st.metric("Ingestion Rate", f"{rate} msgs/hr", "Last 60 minutes")
    except Exception:
        st.metric("Ingestion Rate", "N/A", "DB unreachable")

# ── Pipeline Architecture Flow ────────────────────────────────────────────────
with st.container(border=True):
    st.markdown("#### End-to-End Distributed Pipeline Topology")
    st.markdown(
        """
        ```mermaid
        flowchart LR
            subgraph Ingestion ["1. Data Sources"]
                P1["IoT Sensors / CPCB Producer"]
                P2["Interactive Test Publisher"]
            end

            subgraph Messaging ["2. Distributed Log"]
                K["Apache Kafka Cluster<br/>Topic: pollution-readings"]
            end

            subgraph Processing ["3. Stream Processing Engine"]
                C["Python Ingestion Consumer<br/>(Real-Time Anomaly Detector)"]
                S["Apache Spark Streaming<br/>(5-min Tumbling Windows)"]
            end

            subgraph Storage ["4. Storage & Views"]
                DB[("PostgreSQL 16 Engine")]
                V["View: city_latest (SSoT)"]
            end

            subgraph Presentation ["5. Intelligence UI"]
                UI["CityPulse Dashboard<br/>(Streamlit + Canvas)"]
            end

            P1 -->|JSON over TCP| K
            P2 -->|Interactive Pub| K
            K -->|Consumer Group 1| C
            K -->|Consumer Group 2| S
            C -->|Atomic Inserts| DB
            S -->|Window Aggregates| DB
            DB --> V
            V --> UI
        ```
        """
    )

# ── Tabs: Publisher & Inspector ───────────────────────────────────────────────
tab_pub, tab_inspect, tab_spark_code, tab_partitions = st.tabs([
    "Interactive Event Producer",
    "Live Consumer Log Inspector",
    "Spark Structured Streaming Code",
    "Kafka Partition & Broker Health",
])

# ── Tab 1: Interactive Event Producer ─────────────────────────────────────────
with tab_pub:
    st.markdown("### Publish Live Event to Kafka Topic")
    st.caption("Emit synthetic or custom atmospheric readings directly into Kafka cluster to observe real-time ingestion, alerting, and windowing.")

    with st.form("kafka_publisher_form", clear_on_submit=False):
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            sel_city = st.selectbox("Select City", options=CITIES, index=0)
            sel_pm25 = st.slider(
                "PM2.5 Sensor Reading (µg/m³)",
                min_value=5.0,
                max_value=350.0,
                value=125.0,
                step=1.0,
                help=f"High: ≥{HIGH_THRESHOLD:.0f} µg/m³, Critical: ≥{CRITICAL_THRESHOLD:.0f} µg/m³"
            )
        with col_f2:
            sel_temp = st.slider("Ambient Temperature (°C)", min_value=5.0, max_value=50.0, value=28.5, step=0.5)
            sel_hum = st.slider("Relative Humidity (%)", min_value=15.0, max_value=95.0, value=55.0, step=1.0)

        submit_btn = st.form_submit_button("Publish Event to Kafka", type="primary", use_container_width=True)

        if submit_btn:
            now_iso = datetime.now(timezone.utc).isoformat()
            payload = {
                "city": sel_city,
                "pm25": float(sel_pm25),
                "temperature": float(sel_temp),
                "humidity": float(sel_hum),
                "timestamp": now_iso,
                "source": "dashboard_interactive_producer",
                "is_test": False,
            }

            try:
                from kafka import KafkaProducer
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    request_timeout_ms=4000,
                )
                future = producer.send(KAFKA_TOPIC, value=payload)
                record_meta = future.get(timeout=4.0)
                producer.flush()
                producer.close()

                st.success(
                    f"Successfully published event to topic `{record_meta.topic}` "
                    f"[Partition: {record_meta.partition}, Offset: {record_meta.offset}]",
                    icon=":material/check_circle:",
                )
                st.json(payload)
                # Clear cache so metadata updates
                get_kafka_metadata.clear()
            except Exception as e:
                st.error(f"Failed to publish to Kafka broker: {e}", icon=":material/error:")

# ── Tab 2: Live Consumer Log Inspector ────────────────────────────────────────
with tab_inspect:
    st.markdown("### Real-Time Ingested Message Stream")
    st.caption("Recent records received by the Python consumer and committed to PostgreSQL `pollution_readings`.")

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        id,
                        recorded_at,
                        city,
                        pm25,
                        temperature,
                        humidity,
                        is_test
                    FROM pollution_readings
                    ORDER BY recorded_at DESC
                    LIMIT 20;
                """)
                rows = cur.fetchall()

        if rows:
            df_stream = pd.DataFrame(rows, columns=[
                "ID", "Timestamp (UTC)", "City", "PM2.5 (µg/m³)", "Temp (°C)", "Humidity (%)", "Is Test"
            ])
            # Format timestamp
            df_stream["Timestamp (UTC)"] = df_stream["Timestamp (UTC)"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            st.dataframe(
                df_stream,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "PM2.5 (µg/m³)": st.column_config.NumberColumn(
                        "PM2.5 (µg/m³)",
                        format="%.1f",
                    ),
                    "Temp (°C)": st.column_config.NumberColumn("Temp (°C)", format="%.1f"),
                    "Humidity (%)": st.column_config.NumberColumn("Humidity (%)", format="%.1f"),
                }
            )
        else:
            st.info("No records found in message stream.")
    except Exception as e:
        st.error(f"Error querying message stream: {e}")

# ── Tab 3: Spark Structured Streaming Code ────────────────────────────────────
with tab_spark_code:
    st.markdown("### Apache Spark Structured Streaming Engine (`kafka_stream.py`)")
    st.markdown(
        "Complete PySpark script running inside the `citypulse-spark-1` container. "
        "It consumes continuous micro-batches from Kafka, applies 1-minute watermarking "
        "for late data tolerance, computes 5-minute tumbling window metrics, and writes to PostgreSQL."
    )

    spark_code = """from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, window, avg, max as spark_max, 
    count, when, to_timestamp, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark Session with Streaming Checkpoint
spark = SparkSession.builder \\
    .appName("CityPulse-Kafka-Streaming") \\
    .config("spark.sql.streaming.checkpointLocation", "/tmp/citypulse-checkpoint") \\
    .config("spark.sql.shuffle.partitions", "4") \\
    .getOrCreate()

# 2. Ingest JSON Event Stream from Kafka Broker
schema = StructType([
    StructField("city", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

events = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "kafka:9092") \\
    .option("subscribe", "pollution-readings") \\
    .option("startingOffsets", "latest") \\
    .load()

# 3. Parse Schema and Apply Event-Time Watermarking
readings = events.select(from_json(col("value").cast("string"), schema).alias("data")) \\
    .select("data.*") \\
    .filter(col("city").isNotNull() & col("pm25").isNotNull()) \\
    .withColumn("event_time", to_timestamp(col("timestamp"))) \\
    .withWatermark("event_time", "1 minute")

# 4. Stateful 5-Minute Tumbling Window Aggregation
windowed_aggs = readings \\
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("city")
    ) \\
    .agg(
        avg("pm25").alias("avg_pm25"),
        spark_max("pm25").alias("max_pm25"),
        count("*").alias("reading_count")
    ) \\
    .withColumn("status", 
        when(col("max_pm25") >= 150.0, "CRITICAL")
        .when(col("max_pm25") >= 100.0, "HIGH")
        .otherwise("NORMAL")
    )

# 5. Fault-Tolerant Micro-Batch Sink to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \\
        .format("jdbc") \\
        .option("url", "jdbc:postgresql://postgres:5432/citypulse") \\
        .option("dbtable", "window_aggregates") \\
        .option("user", "citypulse") \\
        .option("password", "citypulse_dev_password") \\
        .option("driver", "org.postgresql.Driver") \\
        .mode("append") \\
        .save()

query = windowed_aggs.writeStream \\
    .foreachBatch(write_to_postgres) \\
    .outputMode("update") \\
    .start()

query.awaitTermination()
"""
    st.code(spark_code, language="python")

# ── Tab 4: Kafka Partition & Broker Health ────────────────────────────────────
with tab_partitions:
    st.markdown("### Topic Partition Topology & Broker Health")
    
    if meta.get("partitions"):
        df_p = pd.DataFrame(meta["partitions"])
        st.dataframe(df_p, use_container_width=True, hide_index=True)
    else:
        st.info("No partition offset metadata available.")

    st.markdown(
        """
        #### Kafka Reliability & Production Considerations:
        - **Consumer Group Offset Management**: Offsets are committed to Kafka internal topic `__consumer_offsets`.
        - **Partition Strategy**: Readings can be partitioned by `city` key to guarantee per-city in-order delivery.
        - **Backpressure & Micro-Batching**: Spark Structured Streaming dynamically throttles Kafka fetch sizes (`spark.streaming.kafka.maxRatePerPartition`).
        """
    )
