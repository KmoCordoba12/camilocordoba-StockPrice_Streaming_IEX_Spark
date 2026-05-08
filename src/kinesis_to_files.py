"""
Relay: Kinesis Data Stream → Archivos JSON locales

Este script lee continuamente mensajes desde Kinesis y los escribe
como archivos JSON pequeños en data/raw/. Spark consumirá esos archivos como fuente de datos.
"""

import os
import json
import time
import base64
from datetime import datetime
import boto3
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
STREAM_NAME = "finnhub-stock-trades"
OUTPUT_DIR = "data/raw"
POLL_INTERVAL = 1.0  # Segundos entre lecturas

# Cliente de Kinesis
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

# Asegurar que la carpeta de salida existe
os.makedirs(OUTPUT_DIR, exist_ok=True)


def obtener_shard_ids():
    """Obtiene los IDs de todos los shards del stream."""
    response = kinesis_client.describe_stream(StreamName=STREAM_NAME)
    return [shard["ShardId"] for shard in response["StreamDescription"]["Shards"]]


def obtener_iterator(shard_id, iterator_type="LATEST"):
    """
    Obtiene un shard iterator para empezar a leer.

    iterator_type:
      - LATEST: Solo mensajes nuevos desde ahora.
      - TRIM_HORIZON: Todos los mensajes desde el inicio del shard.
    """
    response = kinesis_client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType=iterator_type,
    )
    return response["ShardIterator"]


def guardar_lote(records, batch_index):
    """Guarda un lote de mensajes como un archivo JSON."""
    if not records:
        return

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{OUTPUT_DIR}/batch_{timestamp}_{batch_index}.json"

    # Escribir cada record en una línea (formato JSON Lines)
    with open(filename, "w") as f:
        for record in records:
            # Decodificar el data desde base64 (Kinesis lo entrega así)
            data_bytes = record["Data"]
            data_str = data_bytes.decode("utf-8") if isinstance(data_bytes, bytes) else data_bytes

            try:
                trade = json.loads(data_str)
                f.write(json.dumps(trade) + "\n")
            except json.JSONDecodeError:
                # Si no es JSON válido (mensajes antiguos de prueba), saltar
                continue

    print(f"💾 Guardado: {filename} ({len(records)} mensajes)")


def main():
    print("🚀 Iniciando relay: Kinesis → Archivos locales")
    print(f"📥 Stream: {STREAM_NAME}")
    print(f"📁 Carpeta de salida: {OUTPUT_DIR}\n")

    # Obtener todos los shards del stream
    shard_ids = obtener_shard_ids()
    print(f"🔍 Shards encontrados: {shard_ids}\n")

    # Inicializar un iterator por cada shard (solo mensajes nuevos)
    iterators = {shard_id: obtener_iterator(shard_id, "LATEST") for shard_id in shard_ids}

    batch_index = 0

    print("⏳ Esperando nuevos mensajes... (Ctrl+C para detener)\n")

    try:
        while True:
            for shard_id, iterator in list(iterators.items()):
                if iterator is None:
                    continue

                # Leer registros del shard
                response = kinesis_client.get_records(
                    ShardIterator=iterator,
                    Limit=100,  # Máximo 100 mensajes por llamada
                )

                records = response.get("Records", [])

                if records:
                    guardar_lote(records, batch_index)
                    batch_index += 1

                # Actualizar el iterator para la próxima lectura
                iterators[shard_id] = response.get("NextShardIterator")

            # Esperar antes de volver a consultar
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n🛑 Relay detenido manualmente por el usuario.")
        print(f"📊 Total de archivos generados: {batch_index}")


if __name__ == "__main__":
    main()