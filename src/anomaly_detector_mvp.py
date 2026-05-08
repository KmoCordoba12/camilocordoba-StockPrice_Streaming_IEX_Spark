"""
Motor de Detección de Anomalías con Apache Spark Structured Streaming

Lee mensajes de transacciones desde data/raw/, calcula el Z-Score
adaptativo sobre ventanas deslizantes y escribe las anomalías
detectadas en data/alerts/ usando foreachBatch.

Comportamiento:
  - Solo crea archivos CSV cuando detecta anomalías reales.
  - Imprime resumen de cada batch para diagnóstico.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    avg,
    stddev,
    count,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Configuración
INPUT_DIR = "data/raw"
ALERTS_DIR = "data/alerts"
CHECKPOINT_DIR = "data/checkpoints"
WINDOW_DURATION = "60 seconds"
SLIDE_DURATION = "10 seconds"
VOLATILITY_THRESHOLD = 0.00001  # 0.001% de volatilidad mínima

# Asegurar que las carpetas existen
os.makedirs(ALERTS_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def crear_spark_session():
    """Crea y configura una sesión de Spark."""
    return (
        SparkSession.builder
        .appName("AnomalyDetector")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


def procesar_batch(batch_df, batch_id):
    """
    Procesa cada micro-batch de Spark.

    Comportamiento:
      - Solo crea archivos CSV cuando detecta anomalías reales.
      - Imprime resumen de cada batch para diagnóstico.
    """
    if batch_df.isEmpty():
        return

    # Convertir a Pandas para procesamiento más simple
    batch_pd = batch_df.toPandas()

    if batch_pd.empty:
        return

    # Filtrar ventanas con datos válidos
    ventanas_validas = batch_pd[
        (batch_pd["count_trades"] >= 2)
        & (batch_pd["stddev_price"].notna())
        & (batch_pd["stddev_price"] > 0)
    ].copy()

    total_ventanas = len(ventanas_validas)

    if total_ventanas == 0:
        print(f"📊 Batch {batch_id}: sin ventanas válidas (insuficientes datos).")
        return

    # Calcular volatility_ratio
    ventanas_validas["volatility_ratio"] = (
        ventanas_validas["stddev_price"] / ventanas_validas["mean_price"]
    )

    # Filtrar anomalías (volatility por encima del umbral)
    anomalies = ventanas_validas[
        ventanas_validas["volatility_ratio"] > VOLATILITY_THRESHOLD
    ].copy()

    if anomalies.empty:
        max_vol = ventanas_validas["volatility_ratio"].max()
        print(
            f"📊 Batch {batch_id}: {total_ventanas} ventanas analizadas, "
            f"0 anomalías (max volatility = {max_vol:.2e}, umbral = {VOLATILITY_THRESHOLD:.2e})."
        )
        return

    # Si hay anomalías, las guardamos
    # Extraer window_start y window_end (la columna 'window' es un struct)
    anomalies["window_start"] = anomalies["window"].apply(lambda w: w["start"])
    anomalies["window_end"] = anomalies["window"].apply(lambda w: w["end"])

    # Seleccionar y ordenar columnas
    anomalies_export = anomalies[[
        "window_start",
        "window_end",
        "symbol",
        "count_trades",
        "mean_price",
        "stddev_price",
        "volatility_ratio",
    ]]

    # Guardar como CSV con nombre único
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{ALERTS_DIR}/alerts_batch_{batch_id}_{timestamp}.csv"
    anomalies_export.to_csv(filename, index=False)

    print(
        f"🚨 Batch {batch_id}: {total_ventanas} ventanas analizadas, "
        f"{len(anomalies_export)} anomalías guardadas en {filename}"
    )


def main():
    print("🚀 Iniciando motor de detección de anomalías\n")

    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"📂 Leyendo archivos de: {INPUT_DIR}")
    print(f"⏱️  Ventana deslizante: {WINDOW_DURATION} (avance cada {SLIDE_DURATION})")
    print(f"🎯 Umbral de volatilidad: {VOLATILITY_THRESHOLD}")
    print(f"💾 Alertas se guardarán en: {ALERTS_DIR}\n")

    # Esquema de los mensajes JSON
    schema = StructType([
        StructField("s", StringType(), True),
        StructField("p", DoubleType(), True),
        StructField("v", DoubleType(), True),
        StructField("t", LongType(), True),
        StructField("c", StringType(), True),
    ])

    # Leer la carpeta como stream
    raw_stream = (
        spark.readStream
        .schema(schema)
        .option("multiLine", "false")
        .option("maxFilesPerTrigger", 10)
        .option("latestFirst", "false")
        .json(INPUT_DIR)
    )

    # Renombrar campos
    transactions = raw_stream.select(
        col("s").alias("symbol"),
        col("p").alias("price"),
        col("v").alias("volume"),
        (col("t") / 1000).cast("timestamp").alias("event_time"),
    )

    # Watermark
    transactions_with_watermark = transactions.withWatermark("event_time", "30 seconds")

    # Calcular estadísticas por ventana y símbolo
    statistics = (
        transactions_with_watermark
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            col("symbol"),
        )
        .agg(
            avg("price").alias("mean_price"),
            stddev("price").alias("stddev_price"),
            count("price").alias("count_trades"),
        )
    )

    # Usar foreachBatch para procesar cada micro-batch como DataFrame
    query = (
        statistics.writeStream
        .outputMode("update")
        .foreachBatch(procesar_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("✅ Pipeline iniciado. Procesando datos...")
    print("Presiona Ctrl+C para detener.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()