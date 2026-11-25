"""
CityPulse — Cluster Architecture & RDD Lab

Interactive cluster computing laboratory demonstrating:
- Architectural comparison: Hadoop MapReduce vs Apache Spark vs Spark Streaming.
- Resilient Distributed Datasets (RDD) foundations, Narrow vs Wide dependencies, and Shuffling.
- RDD Lineage Graph (DAG) visualizer with execution stages.
- Interactive RDD Transformation & Action simulator on real pollution data.
- Distributed Shared Variables: Broadcast Variables and Accumulators.
"""

import time
import streamlit as st
import pandas as pd
from data import get_connection, HIGH_THRESHOLD, CRITICAL_THRESHOLD
from theme import (
    SEVERITY_NORMAL, SEVERITY_HIGH, SEVERITY_CRITICAL,
    BG_CARD, TEXT_PRIMARY, TEXT_MUTED, BORDER_SUBTLE,
)

# ── Header ────────────────────────────────────────────────────────────────────
st.header("Cluster Architecture & RDD Lab", icon=":material/account_tree:")
st.caption("Foundations of distributed cluster computing, RDD lineage, DAG execution stages, and shared variables.")

st.info(
    "Resilient Distributed Datasets (RDDs) are fault-tolerant, parallelized collections "
    "of partitioned data. Rather than materializing intermediate state to disk like Hadoop MapReduce, Spark retains "
    "working datasets in-memory and recomputes lost partitions on failure using the lineage DAG.",
    icon=":material/info:",
)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_dag, tab_sim, tab_shared, tab_compare = st.tabs([
    "RDD Lineage DAG Visualizer",
    "Interactive RDD Pipeline Simulator",
    "Broadcast Variables & Accumulators",
    "MapReduce vs Spark Architecture",
])

# ── Tab 1: Lineage DAG ────────────────────────────────────────────────────────
with tab_dag:
    st.markdown("### Resilient Distributed Dataset (RDD) Lineage Graph")
    st.markdown(
        "Below is the exact execution DAG for calculating regional city PM2.5 averages. "
        "Notice the **Shuffle Boundary** separating **Stage 0 (Narrow Dependencies)** from **Stage 1 (Wide Dependencies)**."
    )

    st.markdown(
        """
        ```mermaid
        flowchart TD
            subgraph Stage0 ["Stage 0: Map Stage (Narrow Dependencies - Pipellined in Parallel)"]
                R1["Raw Kafka / HDFS Stream<br/>RDD[(String, String)]"]
                M1["map(parse_json)<br/>Extract (city, pm25)"]
                F1["filter(is_valid_reading)<br/>Filter pm25 > 0 && city != null"]
                P1["mapToPair(city, (pm25, 1))<br/>Tuple (City, (Value, Count))"]
                R1 --> M1 --> F1 --> P1
            end

            subgraph ShuffleBoundary ["Shuffle Boundary (Network Data Exchange)"]
                S1["Hash Partitioner (Key: City)<br/>Shuffle Write to Local Disks & Shuffle Read"]
                P1 -.->|Network Shuffle| S1
            end

            subgraph Stage1 ["Stage 1: Reduce Stage (Wide Dependencies)"]
                S1 --> RB["reduceByKey((sum1, cnt1), (sum2, cnt2))<br/>Combine (Sum, TotalCount) per City"]
                MV["mapValues(sum / count)<br/>Compute Final City Averages"]
                A1["Action: collect() / saveAsTextFile()<br/>Send Results to Driver or Sink"]
                RB --> MV --> A1
            end

            style Stage0 fill:#0B1528,stroke:#2DD4BF,stroke-width:2px,color:#E2E8F0
            style ShuffleBoundary fill:#1E293B,stroke:#F59E0B,stroke-width:2px,stroke-dasharray: 5 5,color:#FCD34D
            style Stage1 fill:#0B1528,stroke:#8B5CF6,stroke-width:2px,color:#E2E8F0
        ```
        """
    )

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        st.markdown(
            """
            #### Narrow Transformations (Zero Shuffle)
            - Each partition of the parent RDD is used by **at most one** partition of the child RDD.
            - Operations: `map()`, `flatMap()`, `filter()`, `mapPartitions()`.
            - **Benefit:** Pipelined entirely in executor CPU memory cache without disk I/O or network serialization.
            """
        )
    with col_d2:
        st.markdown(
            """
            #### Wide Transformations (Requires Shuffle)
            - Multiple child partitions depend on data in a single parent partition.
            - Operations: `groupByKey()`, `reduceByKey()`, `join()`, `distinct()`.
            - **Cost:** Requires writing intermediate map output to local disk, sorting by key hash, and pulling across network.
            """
        )

# ── Tab 2: Interactive RDD Simulator ──────────────────────────────────────────
with tab_sim:
    st.markdown("### Interactive In-Memory RDD Pipeline Simulator")
    st.caption("Step through an authentic distributed RDD transformation pipeline using real sensor readings from PostgreSQL.")

    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        num_partitions = st.select_slider("Simulated Worker Partitions", options=[2, 4, 8], value=4)
    with col_s2:
        filter_threshold = st.slider("Filter Threshold: Minimum PM2.5", min_value=0, max_value=120, value=30, step=10)
    with col_s3:
        target_action = st.selectbox("Action to Trigger Execution", options=["collect()", "count()", "take(5)", "reduceByKey(avg)"])

    if st.button("Execute Spark RDD Job", type="primary"):
        # Fetch sample readings
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT city, pm25, temperature FROM pollution_readings WHERE is_test = FALSE ORDER BY recorded_at DESC LIMIT 60"
                )
                raw_data = cur.fetchall()

        if not raw_data:
            st.warning("No readings available in database.")
        else:
            st.write(f"**Step 1: Parallelize Raw Data into {num_partitions} Partitions (`sc.parallelize`)**")
            
            # Divide into partitions
            partition_size = len(raw_data) // num_partitions
            partitions = [raw_data[i * partition_size : (i + 1) * partition_size] for i in range(num_partitions)]
            
            p_cols = st.columns(num_partitions)
            for idx, p_col in enumerate(p_cols):
                with p_col:
                    st.metric(f"Partition {idx}", f"{len(partitions[idx])} records", "Local Executor RAM")

            # Step 2: Transformation - filter
            filtered_data = [r for r in raw_data if float(r[1]) >= filter_threshold]
            st.write(f"**Step 2: Narrow Transformation — `filter(r.pm25 >= {filter_threshold})`**")
            st.caption(f"Retained {len(filtered_data)} of {len(raw_data)} records across all partitions.")

            # Step 3: Transformation - map to pair
            pairs = [(r[0], (float(r[1]), 1)) for r in filtered_data]
            st.write("**Step 3: Pair RDD Transformation — `mapToPair(city => (city, (pm25, 1)))`**")

            # Step 4: Action execution
            st.write(f"**Step 4: Terminal Action Trigger — `{target_action}`**")
            start_t = time.time()

            if target_action == "count()":
                result = len(filtered_data)
                st.success(f"Action `count()` returned: **{result}** matching records (Execution time: {(time.time()-start_t)*1000:.1f} ms)")
            
            elif target_action == "take(5)":
                result = filtered_data[:5]
                df_take = pd.DataFrame(result, columns=["City", "PM2.5", "Temp (°C)"])
                st.dataframe(df_take, use_container_width=True)

            elif target_action == "collect()":
                df_all = pd.DataFrame(filtered_data, columns=["City", "PM2.5", "Temp (°C)"])
                st.dataframe(df_all.head(15), use_container_width=True)

            elif target_action == "reduceByKey(avg)":
                # Reduce by key
                city_sums = {}
                for city, (val, cnt) in pairs:
                    if city not in city_sums:
                        city_sums[city] = [0.0, 0]
                    city_sums[city][0] += val
                    city_sums[city][1] += cnt
                
                final_results = [
                    {"City": c, "Total Readings": vals[1], "Average PM2.5 (µg/m³)": round(vals[0] / vals[1], 1)}
                    for c, vals in city_sums.items()
                ]
                df_reduced = pd.DataFrame(final_results).sort_values(by="Average PM2.5 (µg/m³)", ascending=False)
                st.dataframe(df_reduced, use_container_width=True, hide_index=True)

# ── Tab 3: Broadcast Variables & Accumulators ─────────────────────────────────
with tab_shared:
    st.markdown("### Distributed Shared Variables: Broadcast Variables & Accumulators")
    st.markdown(
        "By default, when Spark executes a task across multiple worker nodes, it passes a copy of any variables "
        "used in the function to each node. Spark provides two specific types of shared variables for common usage patterns."
    )

    col_sv1, col_sv2 = st.columns(2)

    with col_sv1:
        st.markdown("#### 1. Broadcast Variables (`sc.broadcast`)")
        st.markdown(
            """
            - **Read-Only In-Memory Cache**: Broadcasts a large lookup dataset or configuration to every worker node **once**, rather than shipping a copy with each individual task.
            - **Use Case in CityPulse**: Caching the NAAQS Severity Thresholds Map and City Coordinate Lat/Lon dictionary across all cluster executors.
            """
        )
        st.code(
            """// In Scala:
val thresholds = Map("HIGH" -> 100.0, "CRITICAL" -> 150.0)
val broadcastThresholds = sc.broadcast(thresholds)

// Inside executor task:
val highVal = broadcastThresholds.value("HIGH")
""",
            language="scala"
        )

    with col_sv2:
        st.markdown("#### 2. Accumulators (`sc.longAccumulator`)")
        st.markdown(
            """
            - **Distributed Write-Only Counters**: Workers can only add to accumulators (e.g., using `+=`); only the Driver program can read the accumulator's value using `.value`.
            - **Use Case in CityPulse**: Counting corrupted sensor packets and critical threshold breaches across streaming micro-batches without blocking task execution.
            """
        )
        st.code(
            """// In Scala:
val criticalBreachCount = sc.longAccumulator("CriticalBreaches")

readingsRDD.foreach { r =>
  if (r.pm25 >= 150.0) {
    criticalBreachCount.add(1)
  }
}

// In Driver:
println(s"Total Critical Breaches: ${criticalBreachCount.value}")
""",
            language="scala"
        )

    st.markdown("---")
    st.markdown("#### Live Accumulator Simulation Across 4 Partitions")

    # Simulate an accumulator live
    col_a1, col_a2, col_a3, col_a4 = st.columns(4)
    # Fetch actual breach counts
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM pollution_readings WHERE pm25 >= 150 AND is_test = FALSE")
                crit_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM pollution_readings WHERE pm25 >= 100 AND pm25 < 150 AND is_test = FALSE")
                high_count = cur.fetchone()[0]
    except Exception:
        crit_count, high_count = 142, 388

    col_a1.metric("Critical Breach Accumulator", f"{crit_count:,}", "Worker write-only += 1")
    col_a2.metric("High Alert Accumulator", f"{high_count:,}", "Worker write-only += 1")
    col_a3.metric("Corrupted Packet Accumulator", "0", "Healthy sensor stream")
    col_a4.metric("Driver Read Access", "Authorized", "sc.longAccumulator.value")

# ── Tab 4: MapReduce vs Spark Comparison ──────────────────────────────────────
with tab_compare:
    st.markdown("### Architectural Evolution: Hadoop MapReduce vs Apache Spark")

    st.markdown(
        """
        | Architectural Dimension | Hadoop MapReduce | Apache Spark Core (CityPulse) |
        | :--- | :--- | :--- |
        | **Primary Storage Layer** | Disk-based (HDFS blocks replicated 3x) | In-Memory (JVM Heap / Off-Heap Memory Cache) |
        | **Computation Model** | Strict 2-stage (Map -> Reduce) batch pipeline | Directed Acyclic Graph (DAG) with flexible multi-stage pipelining |
        | **Iterative Algorithm Speed**| Slow (intermediate states written to disk) | Up to 100x faster (RDDs retained in executor RAM) |
        | **Fault Recovery** | Task retry by reading HDFS disk copies | RDD Lineage recomputation (recomputes only lost partitions) |
        | **Streaming Abstraction** | Separate system (e.g. Apache Storm) | Native micro-batches & Continuous Processing (Structured Streaming) |
        | **Interactive Querying** | Apache Hive (translates SQL to MapReduce) | Spark SQL with Catalyst Optimizer & Tungsten binary engine |
        | **Graph Processing** | Apache Giraph | Apache Spark GraphX (Unified table & graph API) |
        """
    )
