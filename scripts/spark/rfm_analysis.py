import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_db = os.getenv("POSTGRES_DB", "ecommerce_analytics")
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "root")
    jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_db}"

    spark = SparkSession.builder \
        .appName("EcommerceRFMAnalysis") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://ecommerce-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("--> Чтение raw_events из MinIO (Data Lake)...")
    try:
        events_df = spark.read.json("s3a://raw-events/events/*/*.json")
    except Exception as e:
        print(f"--> Ошибка при чтении из MinIO (папка пуста или бакет отсутствует): {e}")
        spark.stop()
        return

    if events_df.rdd.isEmpty():
        print("--> В MinIO нет файлов для обработки. Завершение.")
        spark.stop()
        return

    events_df = events_df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))

    print("--> Выполнение Data Quality Check...")
    null_users = events_df.filter(F.col("user_id").isNull()).count()
    invalid_prices = events_df.filter(F.col("price") < 0).count()

    print(f"[DATA QUALITY] Битых пользователей (null): {null_users}, Отрицательных цен: {invalid_prices}")

    if null_users > 0 or invalid_prices > 0:
        spark.stop()
        raise ValueError(f"Data Quality Failed! Null users: {null_users}, Invalid prices: {invalid_prices}")

    buys_df = events_df.filter((F.col("action") == "buy") & F.col("user_id").isNotNull())

    if buys_df.count() == 0:
        print("--> Нет событий покупок 'buy'. Завершение.")
        spark.stop()
        return

    max_timestamp = buys_df.select(F.max("timestamp")).collect()[0][0]

    print("--> Расчет метрик R, F, M...")
    rfm_raw = buys_df.groupBy("user_id").agg(
        F.datediff(F.lit(max_timestamp), F.max("timestamp")).alias("recency_days"),
        F.count("event_id").alias("frequency"),
        F.sum("price").alias("monetary")
    )

    r_window = Window.orderBy(F.col("recency_days").desc())
    f_window = Window.orderBy(F.col("frequency").asc())
    m_window = Window.orderBy(F.col("monetary").asc())

    rfm_scores = rfm_raw \
        .withColumn("r_score", F.ntile(5).over(r_window)) \
        .withColumn("f_score", F.ntile(5).over(f_window)) \
        .withColumn("m_score", F.ntile(5).over(m_window))

    rfm_segmented = rfm_scores.withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "Champions")
         .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Loyal Customers")
         .when((F.col("r_score") >= 3) & (F.col("f_score") < 3), "Promising / Recent")
         .when((F.col("r_score") < 3) & (F.col("f_score") >= 3), "At Risk")
         .otherwise("Lost / Hibernating")
    ).withColumn("calculated_at", F.current_timestamp())

    print("--> Запись результатов в PostgreSQL (user_rfm_segments)...")
    rfm_segmented.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "user_rfm_segments") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("--> RFM-анализ из MinIO успешно завершен!")
    spark.stop()

if __name__ == "__main__":
    main()
