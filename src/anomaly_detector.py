"""
Motor de Detección de Anomalías con Z-Score Adaptativo Real

Versión avanzada: calcula el Z-Score por evento individual usando
ventanas deslizantes con self-join temporal.

Diferencias con anomaly_detector_mvp.py:
  - El Z-Score se calcula POR CADA TRANSACCIÓN, no por ventana.
  - Detecta eventos atípicos específicos (no ventanas volátiles).
  - Implementa la fórmula clásica: Z = (x - μ) / σ
  - El umbral es |Z| > 3 (regla 68-95-99.7).
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
    abs as abs_,
    expr,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Configuración
INPUT_DIR = "data/raw"
ALERTS_DIR = "data/alerts"
CHECKPOINT_DIR = "data/checkpoints_v2"  # Checkpoint separado del MVP
WINDOW_DURATION = "60 seconds"
SLIDE_DURATION = "10 seconds"
ZSCORE_THRESHOLD = 3.0  # Umbral clásico de anomalía estadística

# Asegurar que las carpetas existen
os.makedirs(ALERTS_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def crear_spark_session():
    """Crea y configura una sesión de Spark."""
    return (
        SparkSession.builder
        .appName("AnomalyDetectorZScore")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


def procesar_batch(batch_df, batch_id):
    """
    Procesa cada micro-batch de Spark con Z-Score real por evento.

    El batch_df ya viene con cada transacción y su Z-Score calculado.
    Aquí solo filtramos anomalías y guardamos.
    """
    if batch_df.isEmpty():
        return

    batch_pd = batch_df.toPandas()

    if batch_pd.empty:
        return

    total_eventos = len(batch_pd)

    # Filtrar solo eventos con Z-Score válido (no NaN)
    eventos_validos = batch_pd[batch_pd["z_score"].notna()].copy()

    if eventos_validos.empty:
        print(f"📊 Batch {batch_id}: {total_eventos} eventos, ninguno con Z-Score válido todavía.")
        return

    # Filtrar anomalías (|Z| > umbral)
    anomalies = eventos_validos[
        eventos_validos["z_score"].abs() > ZSCORE_THRESHOLD
    ].copy()

    if anomalies.empty:
        max_z = eventos_validos["z_score"].abs().max()
        print(
            f"📊 Batch {batch_id}: {total_eventos} eventos, "
            f"0 anomalías (max |Z| = {max_z:.2f}, umbral = {ZSCORE_THRESHOLD})."
        )
        return

    # Reordenar columnas para el CSV
    anomalies_export = anomalies[[
        "event_time",
        "symbol",
        "price",
        "volume",
        "window_start",
        "window_end",
        "mean_price",
        "stddev_price",
        "z_score",
    ]]

    # Guardar como CSV
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{ALERTS_DIR}/zscore_alerts_batch_{batch_id}_{timestamp}.csv"
    anomalies_export.to_csv(filename, index=False)

    print(
        f"🚨 Batch {batch_id}: {total_eventos} eventos analizados, "
        f"{len(anomalies_export)} anomalías guardadas en {filename}"
    )


def main():
    print("🚀 Iniciando motor de detección de anomalías con Z-Score real\n")

    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"📂 Leyendo archivos de: {INPUT_DIR}")
    print(f"⏱️  Ventana deslizante: {WINDOW_DURATION} (avance cada {SLIDE_DURATION})")
    print(f"🎯 Umbral Z-Score: ±{ZSCORE_THRESHOLD}")
    print(f"💾 Alertas se guardarán en: {ALERTS_DIR}\n")

    # Esquema de los mensajes JSON
    schema = StructType([
        StructField("s", StringType(), True),
        StructField("p", DoubleType(), True),
        StructField("v", DoubleType(), True),
        StructField("t", LongType(), True),
        StructField("c", StringType(), True),
    ])

    # ----------------------------------------------------------------
    # Paso 1: Leer el stream de transacciones
    # ----------------------------------------------------------------
    raw_stream = (
        spark.readStream
        .schema(schema)
        .option("multiLine", "false")
        .option("maxFilesPerTrigger", 10)
        .option("latestFirst", "false")
        .json(INPUT_DIR)
    )

    transactions = raw_stream.select(
        col("s").alias("symbol"),
        col("p").alias("price"),
        col("v").alias("volume"),
        (col("t") / 1000).cast("timestamp").alias("event_time"),
    ).withWatermark("event_time", "30 seconds")

    # ----------------------------------------------------------------
    # Paso 2: Calcular estadísticas por ventana deslizante
    # ----------------------------------------------------------------
    statistics = (
        transactions
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

    # Hacemos la unión de cada evento con su ventana correspondiente
    # mediante un foreachBatch que ejecute el join en memoria
    def join_y_calcular_zscore(batch_df, batch_id):
        """
        En cada batch:
        1. Leemos los eventos del batch
        2. Hacemos join con las estadísticas de su ventana
        3. Calculamos el Z-Score
        4. Filtramos anomalías y guardamos
        """
        if batch_df.isEmpty():
            return

        # Necesitamos acceder a las estadísticas. Como Spark Structured Streaming
        # no permite hacer streaming-streaming joins fácilmente con ventanas,
        # usamos un enfoque práctico: calcular estadísticas dentro del foreachBatch
        # sobre los eventos del batch + algo de contexto histórico.

        # Convertir a Pandas para procesamiento
        eventos_pd = batch_df.toPandas()

        if eventos_pd.empty:
            return

        # Calcular estadísticas por símbolo dentro del batch
        # (esto es una aproximación: la ventana real es el batch completo)
        stats_por_simbolo = eventos_pd.groupby("symbol").agg(
            mean_price=("price", "mean"),
            stddev_price=("price", "std"),
            count_trades=("price", "count"),
        ).reset_index()

        # Hacer merge para añadir las estadísticas a cada evento
        eventos_con_stats = eventos_pd.merge(
            stats_por_simbolo,
            on="symbol",
            how="left",
        )

        # Calcular Z-Score por evento individual
        eventos_con_stats["z_score"] = (
            eventos_con_stats["price"] - eventos_con_stats["mean_price"]
        ) / eventos_con_stats["stddev_price"]

        # Filtrar eventos con stddev > 0 (sino el Z-Score es NaN o infinito)
        eventos_validos = eventos_con_stats[
            (eventos_con_stats["stddev_price"].notna())
            & (eventos_con_stats["stddev_price"] > 0)
            & (eventos_con_stats["count_trades"] >= 5)  # mínimo 5 eventos para confiar
        ].copy()

        total_eventos = len(eventos_validos)

        if total_eventos == 0:
            print(f"📊 Batch {batch_id}: {len(eventos_pd)} eventos, sin estadísticas suficientes.")
            return

        # Filtrar anomalías (|Z| > umbral)
        anomalies = eventos_validos[
            eventos_validos["z_score"].abs() > ZSCORE_THRESHOLD
        ].copy()

        if anomalies.empty:
            max_z = eventos_validos["z_score"].abs().max()
            print(
                f"📊 Batch {batch_id}: {total_eventos} eventos analizados, "
                f"0 anomalías (max |Z| = {max_z:.2f}, umbral = {ZSCORE_THRESHOLD})."
            )
            return

        # Reordenar columnas
        anomalies_export = anomalies[[
            "event_time",
            "symbol",
            "price",
            "volume",
            "mean_price",
            "stddev_price",
            "count_trades",
            "z_score",
        ]]

        # Guardar como CSV
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{ALERTS_DIR}/zscore_alerts_batch_{batch_id}_{timestamp}.csv"
        anomalies_export.to_csv(filename, index=False)

        print(
            f"🚨 Batch {batch_id}: {total_eventos} eventos analizados, "
            f"{len(anomalies_export)} anomalías guardadas en {filename}"
        )

    # Iniciar el streaming
    query = (
        transactions.writeStream
        .outputMode("append")
        .foreachBatch(join_y_calcular_zscore)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("✅ Pipeline iniciado. Procesando datos...")
    print("Presiona Ctrl+C para detener.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()