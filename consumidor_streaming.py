from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("category", StringType(), True),
    StructField("device_type", StringType(), True)
])

spark = SparkSession.builder \
    .appName("Monitoreo_Ecommerce_Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce-transactions-stream") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

alerts_df = parsed_df.withColumn(
    "alerta_fraude", 
    when(col("amount") > 1000.0, "ALERTA: TRANSACCIÓN DE ALTO RIESGO").otherwise("NORMAL")
)

query = alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
