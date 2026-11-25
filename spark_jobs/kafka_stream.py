import os
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, window, avg, max as spark_max, 
    count, when, to_timestamp, coalesce, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

HIGH_THRESHOLD = float(os.getenv("HIGH_THRESHOLD", "100"))
CRITICAL_THRESHOLD = float(os.getenv("CRITICAL_THRESHOLD", "150"))
CITYPULSE_DB_PASSWORD = os.getenv("CITYPULSE_DB_PASSWORD", "citypulse_dev_password")

shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


def classify_severity(max_pm25):
    return (
        when(col("max_pm25") >= CRITICAL_THRESHOLD, "CRITICAL")
        .when(col("max_pm25") >= HIGH_THRESHOLD, "HIGH")
        .otherwise("NORMAL")
    )


spark = SparkSession.builder \
    .appName("CityPulse-Kafka-Streaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/citypulse-checkpoint") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

logger.info(f"CityPulse Spark Streaming starting...")
logger.info(f"High threshold: {HIGH_THRESHOLD}, Critical threshold: {CRITICAL_THRESHOLD}")

schema = StructType([
    StructField("city", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

events = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "pollution-readings") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

readings = events.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
    .filter(col("city").isNotNull() & col("pm25").isNotNull()) \
    .filter(col("pm25") >= 0) \
    .withColumn(
        "event_time",
        coalesce(
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
            current_timestamp()
        )
    )

aggregated = readings \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("city")
    ) \
    .agg(
        avg("pm25").alias("avg_pm25"),
        spark_max("pm25").alias("max_pm25"),
        count("*").alias("reading_count"),
        avg("temperature").alias("avg_temperature")
    ) \
    .withColumn("status", classify_severity(col("max_pm25"))) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("avg_pm25"),
        col("max_pm25"),
        col("reading_count"),
        col("avg_temperature"),
        col("status")
    )


def write_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()
    
    if not rows:
        logger.debug(f"Batch {batch_id}: no rows to write")
        return
    
    start_time = time.time()
    
    try:
        jvm = spark._jvm
        
        driver_class = jvm.java.lang.Thread.currentThread() \
            .getContextClassLoader() \
            .loadClass("org.postgresql.Driver")
        
        driver = driver_class.newInstance()
        
        properties = jvm.java.util.Properties()
        properties.setProperty("user", "citypulse")
        properties.setProperty("password", CITYPULSE_DB_PASSWORD)
        properties.setProperty("loginTimeout", "10")
        properties.setProperty("socketTimeout", "30")
        
        connection = driver.connect(
            "jdbc:postgresql://postgres:5432/citypulse",
            properties
        )
        
        if connection is None:
            raise RuntimeError("PostgreSQL JDBC driver could not establish a connection")
        
        sql = """
            INSERT INTO pollution_aggregates
            (city, window_start, window_end, avg_pm25, max_pm25,
             reading_count, avg_temperature, status, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (city, window_start, window_end)
            DO UPDATE SET
                avg_pm25 = EXCLUDED.avg_pm25,
                max_pm25 = EXCLUDED.max_pm25,
                reading_count = EXCLUDED.reading_count,
                avg_temperature = EXCLUDED.avg_temperature,
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP
        """
        
        connection.setAutoCommit(False)
        statement = connection.prepareStatement(sql)
        
        rows_written = 0
        for row in rows:
            try:
                statement.setString(1, row["city"])
                statement.setTimestamp(
                    2, jvm.java.sql.Timestamp.valueOf(str(row["window_start"]))
                )
                statement.setTimestamp(
                    3, jvm.java.sql.Timestamp.valueOf(str(row["window_end"]))
                )
                statement.setDouble(4, float(row["avg_pm25"]))
                statement.setDouble(5, float(row["max_pm25"]))
                statement.setLong(6, int(row["reading_count"]))
                
                if row["avg_temperature"] is None:
                    statement.setNull(7, jvm.java.sql.Types.DOUBLE)
                else:
                    statement.setDouble(7, float(row["avg_temperature"]))
                
                statement.setString(8, row["status"])
                statement.addBatch()
                rows_written += 1
            
            except Exception as row_error:
                logger.error(f"Error preparing row for batch {batch_id}: {row_error}")
        
        statement.executeBatch()
        connection.commit()
        statement.close()
        
        duration = time.time() - start_time
        logger.info(
            f"Batch {batch_id}: saved {rows_written} aggregates to PostgreSQL "
            f"in {duration:.2f}s"
        )
    
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}", exc_info=True)
        try:
            if connection:
                connection.rollback()
        except:
            pass
        raise
    
    finally:
        try:
            if connection:
                connection.close()
        except:
            pass


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

query = aggregated.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/citypulse-postgres-checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

logger.info("Spark streaming query started")

try:
    query.awaitTermination()
except Exception as e:
    logger.error(f"Streaming query terminated with error: {e}", exc_info=True)
finally:
    logger.info("Spark streaming stopped")
