from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark = None


def create_spark_streaming_session():
    global spark
    MINIKUBE_IP = "192.168.58.2"

    os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'

    spark = SparkSession.builder \
        .appName("IoTRealTimeStreaming") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIKUBE_IP}:31000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.streaming.minBatchesToRetain", "1") \
        .config("spark.sql.streaming.fileSource.log.compactInterval", "1") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    return spark


def create_bronze_stream():
    """Đọc từ Bronze layer với micro-batches"""
    global spark
    if spark is None:
        spark = create_spark_streaming_session()

    print("📖 Reading from Bronze layer (MICRO-BATCH)...")

    bronze_df = spark.readStream \
        .format("parquet") \
        .schema(get_bronze_schema()) \
        .option("maxFilesPerTrigger", "10") \
        .option("latestFirst", "true") \
        .load("s3a://bronze/*/*.parquet")

    return bronze_df


def get_bronze_schema():
    """Schema cho dữ liệu Bronze với đầy đủ các cột"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("row_index", LongType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("risk_type", StringType(), True),
        StructField("original_data", StructType([
            StructField("ts", DoubleType(), True),
            StructField("device", StringType(), True),
            StructField("co", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("light", BooleanType(), True),
            StructField("lpg", DoubleType(), True),
            StructField("motion", BooleanType(), True),
            StructField("smoke", DoubleType(), True),
            StructField("temp", DoubleType(), True)
        ]), True),
        StructField("processed_at", StringType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_consumed_at", StringType(), True),
        StructField("_processed_timestamp", LongType(), True)
    ])


def process_silver_stream():
    """Xử lý streaming cho Silver layer - REAL-TIME"""
    bronze_df = create_bronze_stream()

    print("🔄 Processing Bronze → Silver (REAL-TIME)...")

    # Transform data - REAL-TIME
    silver_stream = bronze_df \
        .withColumn("ingestion_timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("ingestion_date", to_date(col("timestamp"))) \
        .withColumn("device", col("original_data.device")) \
        .withColumn("sensor_timestamp",
                    from_unixtime(col("original_data.ts")).cast("timestamp")
                    ) \
        .withColumn("temperature", col("original_data.temp").cast("double")) \
        .withColumn("humidity", col("original_data.humidity").cast("double")) \
        .withColumn("co", col("original_data.co").cast("double")) \
        .withColumn("light", col("original_data.light").cast("boolean")) \
        .withColumn("lpg", col("original_data.lpg").cast("double")) \
        .withColumn("motion", col("original_data.motion").cast("boolean")) \
        .withColumn("smoke", col("original_data.smoke").cast("double")) \
        .filter(col("temperature").isNotNull()) \
        .filter(col("device").isNotNull()) \
        .filter(col("temperature").between(-50, 100)) \
        .filter(col("humidity").between(0, 100))

    # Select final columns
    silver_final = silver_stream.select(
        "ingestion_timestamp",
        "ingestion_date",
        "device",
        "temperature",
        "humidity",
        "co",
        "light",
        "lpg",
        "motion",
        "smoke",
        "risk_type",
        "row_index",
        "sensor_timestamp",
        "processed_at"
    )

    # Write to Silver với micro-batches
    query = silver_final.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/checkpoints/realtime") \
        .option("path", "s3a://silver/cleaned_data") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="2 seconds") \
        .start()

    return query


def update_device_status_realtime(batch_df, batch_id):
    """Update device status REAL-TIME - xử lý từng record"""
    global spark
    if batch_df.count() > 0:
        try:
            print(f"🔄 REAL-TIME: Processing {batch_df.count()} records...")

            # Hiển thị record đang xử lý
            batch_df.select("device", "temperature", "risk_type", "ingestion_timestamp").show(5, False)

            # SỬA: Cập nhật device status với đầy đủ thông tin
            latest_status = batch_df \
                .orderBy(col("ingestion_timestamp").desc()) \
                .groupBy("device") \
                .agg(
                first("ingestion_timestamp").alias("last_active"),
                avg("temperature").alias("avg_temp"),  # SỬA: avg thay vì first
                count("*").alias("total_readings"),  # SỬA: đổi tên từ batch_count
                sum(when(col("risk_type") != "NORMAL", 1).otherwise(0)).alias("total_alerts")  # THÊM: số lượng alerts
            ) \
                .withColumn("status", lit("ONLINE")) \
                .select(
                "device",
                "status",
                "avg_temp",  # SỬA: từ current_temp → avg_temp
                "total_readings",  # SỬA: từ batch_count → total_readings
                "last_active",
                "total_alerts"  # THÊM: tổng số alerts
            )

            print("📱 REAL-TIME DEVICE STATUS:")
            latest_status.show(truncate=False)

            # MERGE vào device_status
            from delta.tables import DeltaTable

            delta_path = "s3a://gold/device_status"

            # Tạo table nếu chưa tồn tại
            if not DeltaTable.isDeltaTable(spark, delta_path):
                latest_status.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(delta_path)
                print("✅ Created new device_status table")
            else:
                # MERGE real-time
                delta_table = DeltaTable.forPath(spark, delta_path)
                delta_table.alias("target") \
                    .merge(latest_status.alias("source"), "target.device = source.device") \
                    .whenMatchedUpdate(set={
                    "status": "source.status",
                    "avg_temp": "source.avg_temp",  # SỬA
                    "total_readings": "source.total_readings",  # SỬA
                    "last_active": "source.last_active",
                    "total_alerts": "source.total_alerts"  # THÊM
                }) \
                    .whenNotMatchedInsertAll() \
                    .execute()
                print("✅ Updated existing device_status table")

            print(f"✅ REAL-TIME: Updated {latest_status.count()} devices")

        except Exception as e:
            print(f"❌ REAL-TIME Error: {e}")
            import traceback
            traceback.print_exc()


def update_daily_summary_realtime(batch_df, batch_id):
    """Update Daily Summary REAL-TIME - xử lý từng record"""
    global spark
    if batch_df.count() > 0:
        try:
            print(f"📊 REAL-TIME: Updating Daily Summary with {batch_df.count()} records...")

            # SỬA: Tạo summary với đầy đủ metrics
            current_summary = batch_df \
                .groupBy("ingestion_date", "device") \
                .agg(
                avg("temperature").alias("avg_temp"),
                max("temperature").alias("max_temp"),
                min("temperature").alias("min_temp"),
                avg("humidity").alias("avg_humidity"),  # THÊM: độ ẩm trung bình
                count("*").alias("total_records"),  # SỬA: từ batch_records
                sum(when(col("risk_type") != "NORMAL", 1).otherwise(0)).alias("high_temp_alerts"),
                # SỬA: từ batch_alerts
                sum(when(col("motion") == True, 1).otherwise(0)).alias("motion_events"),  # THÊM: sự kiện motion
                sum(when(col("light") == True, 1).otherwise(0)).alias("light_events")  # THÊM: sự kiện light
            ) \
                .select(
                "ingestion_date",
                "device",
                "avg_temp",
                "max_temp",
                "min_temp",
                "avg_humidity",  # THÊM
                "total_records",  # SỬA
                "high_temp_alerts",  # SỬA
                "motion_events",  # THÊM
                "light_events"  # THÊM
            )

            print("📈 REAL-TIME DAILY SUMMARY:")
            current_summary.show(truncate=False)

            # APPEND vào daily_summary
            current_summary.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save("s3a://gold/daily_summary")

            print(f"✅ REAL-TIME: Added {current_summary.count()} summary records")

        except Exception as e:
            print(f"❌ REAL-TIME Summary Error: {e}")
            import traceback
            traceback.print_exc()


def create_alerts_summary_realtime(batch_df, batch_id):
    """Tạo alerts summary REAL-TIME"""
    global spark
    if batch_df.count() > 0:
        try:
            print(f"🚨 REAL-TIME: Creating Alerts Summary with {batch_df.count()} records...")

            # Tạo alerts summary
            alerts_summary = batch_df \
                .filter(col("risk_type") != "NORMAL") \
                .groupBy("device", "ingestion_date") \
                .agg(
                count("*").alias("alert_count"),
                first("risk_type").alias("alert_severity")
            ) \
                .select(
                "device",
                "alert_count",
                "alert_severity",
                "ingestion_date"
            )

            if alerts_summary.count() > 0:
                print("🔴 REAL-TIME ALERTS SUMMARY:")
                alerts_summary.show(truncate=False)

                # Ghi vào alerts_summary
                alerts_summary.write \
                    .format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save("s3a://gold/alerts_summary")

                print(f"✅ REAL-TIME: Added {alerts_summary.count()} alert records")

        except Exception as e:
            print(f"❌ REAL-TIME Alerts Error: {e}")


def process_gold_stream_realtime():
    """Xử lý streaming cho Gold layer - REAL-TIME"""
    global spark

    if spark is None:
        spark = create_spark_streaming_session()

    print("🌟 Processing Silver → Gold (REAL-TIME)...")

    try:
        # Đọc từ Silver layer dạng stream
        silver_stream = spark.readStream \
            .format("delta") \
            .load("s3a://silver/cleaned_data")

        print(f"✅ Silver stream ready: {silver_stream.isStreaming}")

        # 1. Device Status - REAL-TIME (mỗi record)
        query_status = silver_stream.writeStream \
            .outputMode("update") \
            .option("checkpointLocation", "s3a://gold/checkpoints/status_realtime") \
            .trigger(processingTime="3 seconds") \
            .foreachBatch(update_device_status_realtime) \
            .start()

        print(f"✅ REAL-TIME Device Status started")

        # 2. Daily Summary - REAL-TIME (mỗi record)
        query_summary = silver_stream.writeStream \
            .outputMode("update") \
            .option("checkpointLocation", "s3a://gold/checkpoints/summary_realtime") \
            .trigger(processingTime="5 seconds") \
            .foreachBatch(update_daily_summary_realtime) \
            .start()

        print(f"✅ REAL-TIME Daily Summary started")

        # 3. Alerts Summary - REAL-TIME (mỗi record)
        query_alerts = silver_stream.writeStream \
            .outputMode("update") \
            .option("checkpointLocation", "s3a://gold/checkpoints/alerts_realtime") \
            .trigger(processingTime="5 seconds") \
            .foreachBatch(create_alerts_summary_realtime) \
            .start()

        print(f"✅ REAL-TIME Alerts Summary started")

        return [query_status, query_summary, query_alerts]

    except Exception as e:
        print(f"❌ REAL-TIME Gold streaming error: {e}")
        import traceback
        traceback.print_exc()
        return []


def start_realtime_pipeline():
    """Khởi chạy REAL-TIME pipeline"""
    print("🚀 STARTING REAL-TIME STREAMING PIPELINE!")
    print("   ⚡ MICRO-BATCH MODE: 1 file per trigger")
    print("   ⚡ PROCESSING: 3-5 seconds intervals")
    print("   ⚡ OUTPUT: Real-time Dashboard updates")

    # Cleanup trước
    cleanup_old_tables()

    # Bronze → Silver
    silver_query = process_silver_stream()
    print("✅ Silver streaming started...")

    # Chờ Silver có data (ngắn hơn)
    import time
    print("⏳ Waiting for Silver data (10 seconds)...")
    time.sleep(10)

    # Silver → Gold REAL-TIME
    try:
        gold_queries = process_gold_stream_realtime()
        print("✅ Gold REAL-TIME streaming started...")

        print("🎯 REAL-TIME PIPELINE RUNNING!")
        print("   📖 Source: Bronze layer (s3a://bronze/)")
        print("   ⚡ Processing: MICRO-BATCH (1 file/trigger)")
        print("   📊 Output: REAL-TIME Dashboard")
        print("   🕐 Updates: Every 3-5 seconds")
        print("Press Ctrl+C to stop...")

        silver_query.awaitTermination()
        for query in gold_queries:
            query.awaitTermination()

    except Exception as e:
        print(f"❌ REAL-TIME Error: {e}")
        silver_query.stop()


def cleanup_old_tables():
    """Cleanup tables cũ"""
    global spark
    if spark is None:
        spark = create_spark_streaming_session()

    paths = [
        "s3a://silver/cleaned_data",
        "s3a://gold/daily_summary",
        "s3a://gold/device_status",
        "s3a://gold/alerts_summary"  # THÊM: cleanup alerts table
    ]

    for path in paths:
        try:
            from delta.tables import DeltaTable
            if DeltaTable.isDeltaTable(spark, path):
                spark.sql(f"DROP TABLE IF EXISTS delta.`{path}`")
                print(f"✅ Cleaned: {path}")
        except Exception as e:
            print(f"⚠️ Could not clean {path}: {e}")


if __name__ == "__main__":
    start_realtime_pipeline()