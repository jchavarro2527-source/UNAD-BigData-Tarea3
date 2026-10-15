# Monitoreo y Detección de Patrones Anómalos en Transacciones Financieras de E-commerce

**Curso:** Big Data - UNAD  
**Tarea 3:** Procesamiento de Datos con Apache Spark  

## 📌 Descripción del Problema
Los entornos transaccionales de comercio electrónico requieren análisis en **Batch** para consolidar métricas contables históricas y análisis en **Streaming** para detectar transacciones atípicas o fraudulentas en tiempo real antes de que sean autorizadas.

---

## 🛠️ Tecnologías Utilizadas
* **Lenguaje:** Python 3
* **Procesamiento Distribuido:** Apache Spark (PySpark - DataFrames y Structured Streaming)
* **Almacenamiento Distribuido:** Hadoop HDFS
* **Ingesta de Datos en Tiempo Real:** Apache Kafka & Zookeeper

---

## 📂 Estructura del Repositorio
* `generar_dataset_batch.py`: Genera un conjunto de 10.000 datos sintéticos con anomalías simuladas.
* `procesamiento_batch.py`: Carga el dataset desde HDFS, calcula métricas globales ($3\sigma$) y resume las anomalías por categoría.
* `productor_kafka.py`: Simulador de transacciones e-commerce transmitiendo eventos JSON en tiempo real a Kafka.
* `consumidor_streaming.py`: Aplicación Spark Streaming que consume del topic de Kafka y clasifica transacciones de alto riesgo.

---

## 🚀 Instrucciones de Ejecución

### 1. Procesamiento Batch
1. Generar el dataset y cargarlo en HDFS:
   ```bash
   python3 generar_dataset_batch.py
   hdfs dfs -mkdir -p /data/ecommerce/
   hdfs dfs -put -f ecommerce_transactions.csv /data/ecommerce/
