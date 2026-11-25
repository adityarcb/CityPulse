"""
CityPulse — Scala Spark Analytics Job

Scala distributed processing reference:
- Idiomatic Scala features: Case classes, pattern matching, higher-order functions, immutable collections.
- Full source code viewer for CityPulseAnalytics.scala.
- Interactive functional programming evaluation simulator.
- Cluster execution and sbt build configuration breakdown.
"""

import os
import streamlit as st
import pandas as pd
from data import get_latest_readings_all_cities, HIGH_THRESHOLD, CRITICAL_THRESHOLD
from theme import (
    SEVERITY_NORMAL, SEVERITY_HIGH, SEVERITY_CRITICAL,
    BG_CARD, TEXT_PRIMARY, TEXT_MUTED, BORDER_SUBTLE,
)

# ── Header ────────────────────────────────────────────────────────────────────
st.header("Scala Spark Analytics Job", icon=":material/terminal:")
st.caption("Functional programming in Scala, immutable collections, pattern matching, case classes, and Spark Job packaging.")

st.info(
    "Apache Spark is natively implemented in Scala on the Java Virtual Machine (JVM). "
    "Scala combines object-oriented and functional paradigms, offering concise syntax, static type safety, and "
    "first-class functions that enable high-performance parallel execution over distributed datasets.",
    icon=":material/info:",
)

# ── Read Scala source file ────────────────────────────────────────────────────
_scala_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "spark_jobs",
    "CityPulseAnalytics.scala"
)

scala_content = ""
if os.path.exists(_scala_path):
    with open(_scala_path, "r", encoding="utf-8") as f:
        scala_content = f.read()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_code, tab_concepts, tab_sim, tab_build = st.tabs([
    "Scala Job Source Code",
    "Key Functional Concepts",
    "Interactive Scala Function Sandbox",
    "sbt Build & Cluster Execution",
])

# ── Tab 1: Source Code ────────────────────────────────────────────────────────
with tab_code:
    st.markdown("### Production Scala Application (`CityPulseAnalytics.scala`)")
    st.caption("Full type-safe distributed Spark implementation utilizing Scala case classes and RDD/Dataset transformations.")

    col_info1, col_info2, col_info3 = st.columns(3)
    col_info1.metric("Language Version", "Scala 2.12 / 2.13", "Targeted for Spark 3.5")
    col_info2.metric("Total Lines of Code", f"{len(scala_content.splitlines())} lines", "Idiomatic functional style")
    col_info3.metric("Paradigm", "Hybrid FP + OO", "Immutable case classes")

    st.code(scala_content, language="scala")

# ── Tab 2: Key Concepts ───────────────────────────────────────────────────────
with tab_concepts:
    st.markdown("### Five Foundational Scala Paradigms in CityPulse")

    col_c1, col_c2 = st.columns(2)

    with col_c1:
        with st.container(border=True):
            st.markdown("#### 1. Case Classes (Domain Models)")
            st.markdown(
                "Case classes are immutable record representations with compiler-generated "
                "`equals`, `hashCode`, `toString`, and `copy` methods. Ideal for Spark RDD elements."
            )
            st.code(
                """case class SensorReading(
  city: String,
  pm25: Double,
  temperature: Double,
  humidity: Double,
  timestamp: String
)""",
                language="scala"
            )

        with st.container(border=True):
            st.markdown("#### 2. Pattern Matching (Structural Extraction)")
            st.markdown(
                "Replaces imperative `if/else` or `switch` statements with exhaustive pattern matching "
                "supporting value matching, type guards, and case class deconstruction."
            )
            st.code(
                """def classifySeverity(pm25: Double): SeverityGrade = pm25 match {
  case p if p >= 150.0 => CriticalSeverity
  case p if p >= 100.0 => HighSeverity
  case _              => NormalSeverity
}""",
                language="scala"
            )

    with col_c2:
        with st.container(border=True):
            st.markdown("#### 3. Higher-Order Functions")
            st.markdown(
                "Functions that accept other functions as arguments or return functions. "
                "The core mechanism of Spark transformations (`map`, `filter`, `foldLeft`)."
            )
            st.code(
                """// foldLeft accumulates regional macro PM2.5 mean
val total = readings.map(_.pm25).foldLeft(0.0)(_ + _)
val mean = total / readings.length
""",
                language="scala"
            )

        with st.container(border=True):
            st.markdown("#### 4. Type Inference & Immutability (`val`)")
            st.markdown(
                "Scala strongly prefers `val` (immutable value) over `var` (mutable variable). "
                "Immutability eliminates concurrent thread-race conditions in distributed clusters."
            )
            st.code(
                """val readingsRDD = sc.parallelize(rawReadings) // immutable
// Type inferred automatically as RDD[SensorReading]
""",
                language="scala"
            )

# ── Tab 3: Interactive Sandbox ────────────────────────────────────────────────
with tab_sim:
    st.markdown("### Interactive Scala Higher-Order Function Sandbox")
    st.caption("Select a functional transformation to simulate its execution on the 10 canonical cities.")

    readings = get_latest_readings_all_cities()
    city_data = [(r["city"], float(r.get("pm25", 60.0))) for r in readings]

    func_choice = st.selectbox(
        "Choose Functional Expression to Evaluate:",
        options=[
            "1. filter(r => r.pm25 >= 100.0)  // Exceeding NAAQS Threshold",
            "2. map(r => (r.city.toUpperCase, r.pm25 * 1.05))  // Apply 5% Calibration Offset",
            "3. foldLeft(0.0)((acc, r) => acc + r.pm25)  // Sum Total Airshed PM2.5 Load",
            "4. partition(r => r.pm25 >= 100.0)  // Split into (High/Critical, Normal)",
        ]
    )

    if func_choice.startswith("1."):
        filtered = [x for x in city_data if x[1] >= 100.0]
        st.write(f"**Result:** Retained **{len(filtered)}** cities above standard threshold.")
        df_res = pd.DataFrame(filtered, columns=["City", "PM2.5 (µg/m³)"])
        st.dataframe(df_res, use_container_width=True, hide_index=True)

    elif func_choice.startswith("2."):
        mapped = [(x[0].upper(), round(x[1] * 1.05, 1)) for x in city_data]
        st.write("**Result:** Transformed all elements via pure unary projection.")
        df_res = pd.DataFrame(mapped, columns=["City (Transformed)", "Calibrated PM2.5 (µg/m³)"])
        st.dataframe(df_res, use_container_width=True, hide_index=True)

    elif func_choice.startswith("3."):
        total_pm = sum(x[1] for x in city_data)
        st.metric("Accumulated Airshed PM2.5 Mass (`foldLeft`)", f"{total_pm:.1f} µg/m³", f"Mean: {total_pm/len(city_data):.1f} µg/m³")

    elif func_choice.startswith("4."):
        high = [x for x in city_data if x[1] >= 100.0]
        normal = [x for x in city_data if x[1] < 100.0]
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            st.markdown(f"**High/Critical Tuple Partition (`{len(high)}` cities)**")
            st.dataframe(pd.DataFrame(high, columns=["City", "PM2.5"]), use_container_width=True, hide_index=True)
        with col_p2:
            st.markdown(f"**Normal Tuple Partition (`{len(normal)}` cities)**")
            st.dataframe(pd.DataFrame(normal, columns=["City", "PM2.5"]), use_container_width=True, hide_index=True)

# ── Tab 4: Build & Deployment ─────────────────────────────────────────────────
with tab_build:
    st.markdown("### Apache Spark Scala Build Configuration (`build.sbt`)")
    st.markdown("Standard sbt configuration required to compile, package, and deploy `CityPulseAnalytics.jar` to a Spark cluster.")

    sbt_code = """name := "citypulse-analytics"
version := "2.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  "org.postgresql"   %  "postgresql"   % "42.7.2"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
"""
    st.code(sbt_code, language="scala")

    st.markdown("#### Cluster Execution via `spark-submit`")
    st.code(
        """spark-submit \\
  --class com.citypulse.analytics.CityPulseAnalytics \\
  --master spark://spark-master:7077 \\
  --deploy-mode cluster \\
  --executor-memory 2G \\
  --total-executor-cores 4 \\
  target/scala-2.12/citypulse-analytics-assembly-2.0.0.jar
""",
        language="bash"
    )
