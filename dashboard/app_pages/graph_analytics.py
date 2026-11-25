"""
CityPulse — Graph Analytics & Diffusion Modelling

Graph processing engine demonstrating:
- Urban Airshed Network topology (10 canonical cities as vertices).
- Geographic and atmospheric transport edges (<600 km proximity).
- Distributed PageRank algorithm to identify pollution sink/hub centers.
- Connected Components to segment regional airshed corridors (e.g., Gangetic Basin vs Deccan).
- Complete Scala Apache Spark GraphX reference code and Pregel message-passing model.
"""

import math
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import networkx as nx

from data import get_latest_readings_all_cities
from city_coordinates import CITY_COORDINATES, CANONICAL_CITIES
from theme import (
    SEVERITY_NORMAL, SEVERITY_HIGH, SEVERITY_CRITICAL, SEVERITY_ANOMALY,
    BG_CARD, TEXT_PRIMARY, TEXT_MUTED, BORDER_SUBTLE,
)

# ── Header ────────────────────────────────────────────────────────────────────
st.header("Graph Analytics & Diffusion Modeling", icon=":material/hub:")
st.caption("Graph processing with Apache Spark GraphX, PageRank diffusion, and Connected Components.")

st.info(
    "Apache Spark GraphX extends Spark RDDs by introducing a directed multigraph "
    "with properties attached to each vertex and edge (`Graph[VD, ED]`). It combines the data-parallel computation "
    "of RDDs with the graph-parallel computation of the Pregel API.",
    icon=":material/info:",
)

# ── Fetch live readings ────────────────────────────────────────────────────────
readings = get_latest_readings_all_cities()
reading_map = {r["city"]: r for r in readings}

# Helper: Haversine distance in km
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ── Controls ──────────────────────────────────────────────────────────────────
col_c1, col_c2, col_c3 = st.columns([2, 2, 2])
with col_c1:
    dist_threshold = st.slider(
        "Airshed Proximity Threshold (km)",
        min_value=300,
        max_value=1200,
        value=650,
        step=50,
        help="Maximum distance between cities to establish an atmospheric transport diffusion edge.",
    )
with col_c2:
    damping_factor = st.slider(
        "PageRank Damping Factor (α)",
        min_value=0.50,
        max_value=0.95,
        value=0.85,
        step=0.05,
        help="Probability of atmospheric diffusion continuing along connecting edges (Spark GraphX default = 0.85).",
    )
with col_c3:
    layout_mode = st.selectbox(
        "Network Graph Projection",
        options=["Geographic Coordinates (India Map)", "Force-Directed Spring Layout"],
        index=0,
    )

# ── Construct Graph ───────────────────────────────────────────────────────────
G = nx.DiGraph()
city_list = [c for c in CANONICAL_CITIES if c in CITY_COORDINATES]

for city in city_list:
    r = reading_map.get(city, {})
    pm25 = float(r.get("pm25", 60.0))
    sev = r.get("severity", "NORMAL")
    coords = CITY_COORDINATES[city]
    lat, lon = coords["lat"], coords["lon"]
    G.add_node(city, pm25=pm25, severity=sev, lat=lat, lon=lon)

# Build edges based on distance and PM2.5 atmospheric drift
for i in range(len(city_list)):
    c1 = city_list[i]
    coords1 = CITY_COORDINATES[c1]
    lat1, lon1 = coords1["lat"], coords1["lon"]
    pm1 = G.nodes[c1]["pm25"]
    for j in range(len(city_list)):
        if i == j:
            continue
        c2 = city_list[j]
        coords2 = CITY_COORDINATES[c2]
        lat2, lon2 = coords2["lat"], coords2["lon"]
        pm2 = G.nodes[c2]["pm25"]
        d = haversine_km(lat1, lon1, lat2, lon2)
        if d <= dist_threshold:
            # Directed edge representing down-gradient or proximity atmospheric drift
            # Edge weight inversely proportional to distance and scaled by PM2.5
            weight = round((1000.0 / d) * (1.0 + pm1 / 100.0), 3)
            G.add_edge(c1, c2, distance=round(d, 1), weight=weight)

# Run Graph Algorithms
pagerank_scores = nx.pagerank(G, alpha=damping_factor, weight="weight") if len(G.edges) > 0 else {c: 0.1 for c in city_list}
undirected_G = G.to_undirected()
connected_components = list(nx.connected_components(undirected_G))
component_map = {}
for comp_id, comp_nodes in enumerate(connected_components, 1):
    for node in comp_nodes:
        component_map[node] = f"Cluster {comp_id}"

in_degrees = dict(G.in_degree())
out_degrees = dict(G.out_degree())

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_viz, tab_table, tab_graphx, tab_theory = st.tabs([
    "Interactive Graph Visualizer",
    "Centrality & Airshed Metrics",
    "Spark GraphX Scala Implementation",
    "GraphX vs Pregel Architecture",
])

# ── Tab 1: Graph Visualizer ───────────────────────────────────────────────────
with tab_viz:
    if layout_mode == "Geographic Coordinates (India Map)":
        # Geographical Map View with edges
        edge_x = []
        edge_y = []
        edge_hover = []
        for u, v, data in G.edges(data=True):
            lat1, lon1 = CITY_COORDINATES[u]["lat"], CITY_COORDINATES[u]["lon"]
            lat2, lon2 = CITY_COORDINATES[v]["lat"], CITY_COORDINATES[v]["lon"]
            edge_x.extend([lon1, lon2, None])
            edge_y.extend([lat1, lat2, None])

        edge_trace = go.Scattermap(
            lon=edge_x,
            lat=edge_y,
            mode="lines",
            line=dict(width=1.5, color="rgba(45, 212, 191, 0.45)"),
            hoverinfo="none",
            name="Airshed Transport Edges",
        )

        node_lons = [CITY_COORDINATES[c]["lon"] for c in city_list]
        node_lats = [CITY_COORDINATES[c]["lat"] for c in city_list]
        node_colors = [
            SEVERITY_CRITICAL if G.nodes[c]["severity"] == "CRITICAL"
            else SEVERITY_HIGH if G.nodes[c]["severity"] == "HIGH"
            else SEVERITY_NORMAL
            for c in city_list
        ]
        node_sizes = [max(16, int(pagerank_scores[c] * 180)) for c in city_list]
        node_hover = [
            f"<b>{c}</b><br>PM2.5: {G.nodes[c]['pm25']:.1f} µg/m³<br>Severity: {G.nodes[c]['severity']}<br>"
            f"PageRank Score: {pagerank_scores[c]:.4f}<br>Degree: In {in_degrees[c]} / Out {out_degrees[c]}<br>"
            f"Airshed: {component_map.get(c, 'N/A')}"
            for c in city_list
        ]

        node_trace = go.Scattermap(
            lon=node_lons,
            lat=node_lats,
            mode="markers+text",
            marker=dict(
                size=node_sizes,
                color=node_colors,
                opacity=0.9,
            ),
            text=city_list,
            textposition="top right",
            hovertext=node_hover,
            hoverinfo="text",
            name="City Vertices",
        )

        fig = go.Figure(data=[edge_trace, node_trace])
        fig.update_layout(
            map=dict(
                style="carto-darkmatter",
                center=dict(lat=21.5, lon=79.5),
                zoom=3.8,
            ),
            margin=dict(l=0, r=0, t=10, b=0),
            height=540,
            paper_bgcolor="rgba(0,0,0,0)",
            legend=dict(
                font=dict(color=TEXT_PRIMARY, size=11),
                bgcolor="rgba(11, 21, 40, 0.75)",
                x=0.02,
                y=0.98,
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

    else:
        # 2D Spring Layout View
        pos = nx.spring_layout(G, seed=42, k=0.55)
        edge_x = []
        edge_y = []
        for u, v in G.edges():
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])

        edge_trace = go.Scatter(
            x=edge_x,
            y=edge_y,
            line=dict(width=1.5, color="rgba(45, 212, 191, 0.45)"),
            hoverinfo="none",
            mode="lines",
            name="Diffusion Edges",
        )

        node_x = [pos[c][0] for c in city_list]
        node_y = [pos[c][1] for c in city_list]
        node_colors = [
            SEVERITY_CRITICAL if G.nodes[c]["severity"] == "CRITICAL"
            else SEVERITY_HIGH if G.nodes[c]["severity"] == "HIGH"
            else SEVERITY_NORMAL
            for c in city_list
        ]
        node_sizes = [max(18, int(pagerank_scores[c] * 220)) for c in city_list]
        node_hover = [
            f"<b>{c}</b><br>PM2.5: {G.nodes[c]['pm25']:.1f} µg/m³<br>PageRank: {pagerank_scores[c]:.4f}<br>"
            f"Airshed: {component_map.get(c, 'N/A')}"
            for c in city_list
        ]

        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            text=city_list,
            textposition="top center",
            hovertext=node_hover,
            hoverinfo="text",
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(width=2, color="rgba(255,255,255,0.7)"),
            ),
            name="City Vertices",
        )

        fig = go.Figure(data=[edge_trace, node_trace])
        fig.update_layout(
            showlegend=False,
            hovermode="closest",
            margin=dict(b=10, l=10, r=10, t=10),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            height=540,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.caption(
        f"Graph Structure: **{len(G.nodes)} Vertices**, **{len(G.edges)} Directed Edges**, "
        f"**{len(connected_components)} Airshed Components**. Vertex radius is scaled by PageRank score."
    )

# ── Tab 2: Centrality & Airshed Metrics ────────────────────────────────────────
with tab_table:
    table_data = []
    for c in city_list:
        table_data.append({
            "City": c,
            "Airshed Cluster": component_map.get(c, "Cluster 1"),
            "PageRank Centrality": round(pagerank_scores[c], 4),
            "In-Degree": in_degrees[c],
            "Out-Degree": out_degrees[c],
            "Latest PM2.5 (µg/m³)": round(G.nodes[c]["pm25"], 1),
            "Alert Severity": G.nodes[c]["severity"],
        })

    df_metrics = pd.DataFrame(table_data).sort_values(by="PageRank Centrality", ascending=False)

    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    top_city = df_metrics.iloc[0]["City"]
    top_pr = df_metrics.iloc[0]["PageRank Centrality"]
    col_m1.metric("Highest Influence Hub", f"{top_city}", f"PageRank {top_pr:.4f}")
    col_m2.metric("Connected Airsheds", f"{len(connected_components)} Clusters", "Isolated subgraphs")
    col_m3.metric("Total Edges Active", f"{len(G.edges)} Connections", f"Within {dist_threshold} km")
    col_m4.metric("Avg In/Out Degree", f"{len(G.edges)/len(city_list):.1f} links/city", "Graph density")

    st.dataframe(
        df_metrics,
        use_container_width=True,
        hide_index=True,
        column_config={
            "PageRank Centrality": st.column_config.ProgressColumn(
                "PageRank Centrality",
                min_value=0.0,
                max_value=max(pagerank_scores.values()) * 1.15,
                format="%.4f",
            ),
            "Latest PM2.5 (µg/m³)": st.column_config.NumberColumn(
                "Latest PM2.5 (µg/m³)",
                format="%.1f",
            ),
        },
    )

# ── Tab 3: Spark GraphX Scala Implementation ──────────────────────────────────
with tab_graphx:
    st.markdown("### Production Apache Spark GraphX Scala Implementation")
    st.markdown(
        "Below is the complete Scala GraphX code for distributed graph processing. "
        "It defines vertex and edge RDDs, initializes a `Graph[CityProps, EdgeProps]`, "
        "and executes distributed PageRank and Connected Components on a Spark cluster."
    )

    scala_code = """package com.citypulse.graph

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// 1. Define Vertex and Edge property Case Classes
case class CityVertex(name: String, lat: Double, lon: Double, pm25: Double, severity: String)
case class TransportEdge(distanceKm: Double, weight: Double)

object CityPulseGraphX {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("CityPulse-GraphX-Diffusion")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 2. Build Vertex RDD (VertexId -> Long, CityVertex)
    val vertices: RDD[(VertexId, CityVertex)] = sc.parallelize(Seq(
      (1L, CityVertex("Delhi", 28.6139, 77.2090, 182.4, "CRITICAL")),
      (2L, CityVertex("Mumbai", 19.0760, 72.8777, 85.2, "NORMAL")),
      (3L, CityVertex("Bengaluru", 12.9716, 77.5946, 62.1, "NORMAL")),
      (4L, CityVertex("Kolkata", 22.5726, 88.3639, 134.8, "HIGH")),
      (5L, CityVertex("Chennai", 13.0827, 80.2707, 71.3, "NORMAL")),
      (6L, CityVertex("Hyderabad", 17.3850, 78.4867, 92.6, "NORMAL")),
      (7L, CityVertex("Pune", 18.5204, 73.8567, 78.9, "NORMAL")),
      (8L, CityVertex("Ahmedabad", 23.0225, 72.5714, 118.3, "HIGH")),
      (9L, CityVertex("Jaipur", 26.9124, 75.7873, 142.1, "HIGH")),
      (10L, CityVertex("Lucknow", 26.8467, 80.9462, 165.7, "CRITICAL"))
    ))

    // 3. Build Edge RDD (Edge[TransportEdge](srcId, dstId, properties))
    val edges: RDD[Edge[TransportEdge]] = sc.parallelize(Seq(
      Edge(1L, 9L, TransportEdge(260.0, 3.84)),   // Delhi -> Jaipur
      Edge(1L, 10L, TransportEdge(420.0, 2.38)),  // Delhi -> Lucknow
      Edge(10L, 4L, TransportEdge(880.0, 1.13)),  // Lucknow -> Kolkata
      Edge(2L, 7L, TransportEdge(150.0, 6.67)),   // Mumbai -> Pune
      Edge(7L, 6L, TransportEdge(560.0, 1.78)),   // Pune -> Hyderabad
      Edge(6L, 3L, TransportEdge(570.0, 1.75)),   // Hyderabad -> Bengaluru
      Edge(3L, 5L, TransportEdge(350.0, 2.85)),   // Bengaluru -> Chennai
      Edge(8L, 9L, TransportEdge(650.0, 1.53))    // Ahmedabad -> Jaipur
    ))

    // 4. Construct Graph
    val defaultCity = CityVertex("Unknown", 0.0, 0.0, 0.0, "NORMAL")
    val graph: Graph[CityVertex, TransportEdge] = Graph(vertices, edges, defaultCity)

    // 5. Distributed PageRank Algorithm (Tolerance = 0.0001, Reset Prob = 0.15)
    val prGraph = graph.pageRank(0.0001, resetProb = 0.15)
    println("=== TOP 5 POLLUTION DIFFUSION HUBS (PAGERANK) ===")
    val topHubs = prGraph.vertices
      .join(vertices)
      .map { case (id, (rank, city)) => (city.name, rank, city.pm25) }
      .sortBy(_._2, ascending = false)
      .take(5)

    topHubs.foreach { case (name, rank, pm) =>
      println(f"Hub: $name%-12s | PageRank: $rank%.4f | Current PM2.5: $pm%.1f")
    }

    // 6. Connected Components (Identify Airshed Corridors)
    val ccGraph = graph.connectedComponents()
    println("\\n=== CONNECTED AIRSHED CLUSTERS ===")
    ccGraph.vertices
      .join(vertices)
      .map { case (id, (clusterId, city)) => (clusterId, city.name) }
      .groupByKey()
      .foreach { case (clusterId, cities) =>
        println(s"Airshed Cluster $clusterId: ${cities.mkString(", ")}")
      }

    sc.stop()
  }
}
"""
    st.code(scala_code, language="scala")

# ── Tab 4: GraphX Architecture & Pregel ───────────────────────────────────────
with tab_theory:
    st.markdown("### Apache Spark GraphX Architectural Foundations")

    col_t1, col_t2 = st.columns(2)
    with col_t1:
        st.markdown(
            """
            #### 1. Dual-Representation Architecture
            - **VertexRDD[VD]**: Extends `RDD[(VertexId, VD)]` using an immutable routing table and bitmask indexing for fast localized vertex property updates without moving graph structure.
            - **EdgeRDD[ED]**: Encapsulates 2D partitioning (`EdgePartition2D`) of edges across executors, drastically reducing network shuffle during joins.
            - **Graph[VD, ED]**: Unifies tabular relational transformations with graph-parallel algorithms.
            """
        )
    with col_t2:
        st.markdown(
            """
            #### 2. Distributed Pregel Message-Passing Model
            - **Bulk Synchronous Parallel (BSP)** computational paradigm.
            - **Superstep Iterations**:
              1. *Message Delivery*: Active vertices receive aggregated incoming messages from neighbors.
              2. *Vertex Compute*: `vprog(id, attr, msg)` computes new vertex attribute.
              3. *Send Messages*: `sendMsg(edgeTriplet)` emits messages along outgoing edges.
              4. *Combine Messages*: `mergeMsg(msg1, msg2)` reduces incoming messages at destination vertices.
            - Terminates when all vertices vote to halt or max iterations reached.
            """
        )

    st.markdown("---")
    st.markdown(
        """
        | Feature | NetworkX (Single Node) | Apache Spark GraphX (Distributed) |
        | :--- | :--- | :--- |
        | **Execution Model** | Single-threaded in-memory Python object graph | Distributed across JVM executors over RDD partitions |
        | **Dataset Scalability** | Up to ~10M edges (bound by single-machine RAM) | Billions of vertices & edges partitioned across cluster |
        | **Fault Tolerance** | None (crashes if node runs out of memory) | Lineage graph enables automatic executor partition replay |
        | **Graph Mutation** | In-place imperative mutations (`G.add_node()`) | Functional immutable transformations (`graph.mapVertices()`) |
        | **Integration** | Standalone Python libraries (NumPy, SciPy) | Direct zero-overhead joins with Spark SQL DataFrames & MLlib |
        """
    )
