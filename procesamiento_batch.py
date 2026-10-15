from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count, when

# 1. Iniciar sesión de Spark
spark = SparkSession.builder \
    .appName("Monitoreo_Ecommerce_Batch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== 1. CARGA DE DATOS DESDE HDFS ===")
df = spark.read.csv("hdfs://localhost:9000/data/ecommerce/ecommerce_transactions.csv", header=True, inferSchema=True)
df.show(5)

print("=== 2. ANÁLISIS EXPLORATORIO Y DETECCIÓN EN BATCH ===")
stats = df.select(avg("amount").alias("mean_amount"), stddev("amount").alias("stddev_amount")).collect()[0]
mean_val = stats["mean_amount"]
stddev_val = stats["stddev_amount"]
threshold = mean_val + (3 * stddev_val)

print(f"\nMonto Promedio: ${mean_val:.2f} | Desviación Estándar: ${stddev_val:.2f}")
print(f"Umbral de Anomalía (>3 stddev): ${threshold:.2f}\n")

anomalous_txns = df.filter(col("amount") > threshold)
print(f"Total de Transacciones Anómalas Detectadas: {anomalous_txns.count()}")
anomalous_txns.show(10)

print("=== 3. RESUMEN DE TRANSACCIONES Y FRAUDES POR CATEGORÍA ===")
summary_df = df.groupBy("category") \
    .agg(
        count("transaction_id").alias("total_txns"),
        avg("amount").alias("avg_amount"),
        count(when(col("is_fraud") == 1, 1)).alias("total_frauds")
    ) \
    .orderBy(col("total_frauds").desc())

summary_df.show()

spark.stop()
