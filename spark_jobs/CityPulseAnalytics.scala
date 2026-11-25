package com.citypulse.analytics

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

/**
 * CityPulse — Urban Air Quality Distributed Analytics Engine
 *
 * Demonstrates idiomatic Scala programming for Apache Spark:
 * - Case classes for type-safe immutable domain modeling.
 * - Functional transformations (map, filter, foldLeft, groupBy).
 * - Pattern matching for environmental severity and state transitions.
 * - Spark SQL Dataset API interoperability.
 */

// 1. Immutable Domain Models (Case Classes)
case class SensorReading(
  city: String,
  pm25: Double,
  temperature: Double,
  humidity: Double,
  timestamp: String
)

case class CitySummary(
  city: String,
  readingCount: Long,
  avgPm25: Double,
  maxPm25: Double,
  severity: String
)

sealed trait SeverityGrade
case object NormalSeverity extends SeverityGrade
case object HighSeverity extends SeverityGrade
case object CriticalSeverity extends SeverityGrade

object CityPulseAnalytics {

  // Pure functional pattern matching for NAAQS severity grading
  def classifySeverity(pm25: Double, highThresh: Double = 100.0, critThresh: Double = 150.0): SeverityGrade = {
    pm25 match {
      case p if p >= critThresh => CriticalSeverity
      case p if p >= highThresh => HighSeverity
      case _                   => NormalSeverity
    }
  }

  def severityToString(grade: SeverityGrade): String = grade match {
    case CriticalSeverity => "CRITICAL"
    case HighSeverity     => "HIGH"
    case NormalSeverity   => "NORMAL"
  }

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("CityPulse-Scala-Analytics")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println("================================================================")
    println("  CityPulse — Scala Distributed Environmental Intelligence Engine")
    println("================================================================")

    // 2. Canonical Sample Readings (Scala Collections)
    val rawReadings = List(
      SensorReading("Delhi", 184.5, 26.2, 58.0, "2026-10-24T12:00:00Z"),
      SensorReading("Delhi", 192.1, 26.5, 56.0, "2026-10-24T12:05:00Z"),
      SensorReading("Mumbai", 74.2, 31.0, 78.0, "2026-10-24T12:00:00Z"),
      SensorReading("Bengaluru", 48.6, 24.1, 62.0, "2026-10-24T12:00:00Z"),
      SensorReading("Kolkata", 138.9, 28.4, 72.0, "2026-10-24T12:00:00Z"),
      SensorReading("Chennai", 68.3, 30.5, 80.0, "2026-10-24T12:00:00Z"),
      SensorReading("Hyderabad", 86.4, 27.8, 65.0, "2026-10-24T12:00:00Z"),
      SensorReading("Pune", 72.1, 25.4, 60.0, "2026-10-24T12:00:00Z"),
      SensorReading("Ahmedabad", 112.5, 32.1, 45.0, "2026-10-24T12:00:00Z"),
      SensorReading("Jaipur", 135.0, 29.3, 40.0, "2026-10-24T12:00:00Z"),
      SensorReading("Lucknow", 168.2, 27.0, 68.0, "2026-10-24T12:00:00Z")
    )

    // 3. Parallelize into Spark RDD
    val readingsRDD: RDD[SensorReading] = sc.parallelize(rawReadings, 4)

    // 4. Functional Transformations: Pair RDD & Aggregation
    // Key: City Name, Value: (Sum of PM2.5, Max PM2.5, Count)
    val cityStatsRDD = readingsRDD
      .map(r => (r.city, (r.pm25, r.pm25, 1L)))
      .reduceByKey { case ((sum1, max1, cnt1), (sum2, max2, cnt2)) =>
        (sum1 + sum2, math.max(max1, max2), cnt1 + cnt2)
      }
      .mapValues { case (totalPm, peakPm, count) =>
        val avgPm = totalPm / count
        val grade = classifySeverity(peakPm)
        CitySummary(
          city = "",
          readingCount = count,
          avgPm25 = math.round(avgPm * 10.0) / 10.0,
          maxPm25 = math.round(peakPm * 10.0) / 10.0,
          severity = severityToString(grade)
        )
      }
      .map { case (cityName, summary) => summary.copy(city = cityName) }

    // 5. Action: Collect and Print
    println("\\n=== DISTRIBUTED CITY AIR QUALITY SUMMARIES ===")
    val results = cityStatsRDD.collect()
    results.sortBy(-_.avgPm25).foreach { s =>
      println(f"City: ${s.city}%-12s | Readings: ${s.readingCount}%2d | " +
              f"Avg PM2.5: ${s.avgPm25}%5.1f | Max PM2.5: ${s.maxPm25}%5.1f | Status: ${s.severity}")
    }

    // 6. Dataset API Conversion (Type-Safe Spark SQL)
    val readingsDS = readingsRDD.toDS()
    println("\\n=== SPARK SQL DATASET SCHEMA ===")
    readingsDS.printSchema()

    val topPolluted = readingsDS
      .groupBy("city")
      .agg(
        round(avg("pm25"), 1).alias("avg_pm25"),
        round(max("pm25"), 1).alias("peak_pm25")
      )
      .filter($"avg_pm25" >= 100.0)
      .sort($"avg_pm25".desc)

    println("\\n=== CITIES EXCEEDING NAAQS STANDARD (>= 100 µg/m³) ===")
    topPolluted.show(false)

    // 7. Higher-Order Functions: foldLeft for Atmospheric Entropy
    val totalRegionalPm25 = rawReadings.map(_.pm25).foldLeft(0.0)(_ + _)
    val regionalMean = totalRegionalPm25 / rawReadings.length
    println(f"\\nRegional Macro Airshed Mean: $regionalMean%.2f µg/m³")

    spark.stop()
  }
}
