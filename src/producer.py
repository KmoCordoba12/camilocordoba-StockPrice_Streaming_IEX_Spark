"""
Productor: Finnhub WebSocket → AWS Kinesis Data Streams

Este script se conecta al WebSocket de Finnhub, recibe transacciones bursátiles en tiempo real y las publica en un stream de Kinesis para
ser consumidas luego por el motor de análisis (Apache Spark).
"""

import os
import ssl
import json
import certifi
import boto3
import websocket
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración del proyecto
API_KEY = os.getenv("FINNHUB_API_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
STREAM_NAME = "finnhub-stock-trades"
# Accciones y ETFs seleccionados para monitorear (diversificación sectorial y geográfica)
SYMBOLS = [
    # Criptomonedas (24/7)
    "BINANCE:BTCUSDT",   # Bitcoin
    "BINANCE:ETHUSDT",   # Ethereum
    "BINANCE:SOLUSDT",   # Solana

    # Tecnología
    "AAPL",   # Apple
    "NVDA",   # NVIDIA - líder de IA
    "MSFT",   # Microsoft - mayor inversor de OpenAI
    "GOOGL",  # Alphabet (Google) Class A
    "AMZN",   # Amazon

    # Sectores diversos
    "PNR",    # Pentair - Industrial (agua)
    "SMR",    # NuScale Power - Energía nuclear modular
    "TTWO",   # Take-Two Interactive - Entretenimiento

    # Sectores no-tech tradicionales
    "JPM",    # JPMorgan Chase - Banca
    "XOM",    # Exxon Mobil - Energía / Petróleo
    "JNJ",    # Johnson & Johnson - Salud
    "KO",     # Coca-Cola - Consumo defensivo
    "GM",     # General Motors - Automotor

    # Holdings y FinTech
    "BRK.B",  # Berkshire Hathaway Class B - Holding diversificado
    "NU",     # Nu Holdings (Nubank) - FinTech LATAM

    # Sector inmobiliario
    "LEN",    # Lennar - Constructora de viviendas
    "O",      # Realty Income - REIT comercial

    # ETFs para diversificación
    "VOO",    # Vanguard S&P 500 (mercado USA)
    "VWO",    # Vanguard Emerging Markets (mercados emergentes)
]

# Validación de credenciales
if not API_KEY:
    print("❌ ERROR: No se encontró FINNHUB_API_KEY en el archivo .env")
    exit(1)

# Cliente de Kinesis
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

# Contador de mensajes enviados (monitoreo)
mensajes_enviados = 0


def enviar_a_kinesis(trade):
    """
    Envía una transacción individual a Kinesis Data Stream.

    Args:
        trade (dict): Diccionario con los campos de la transacción de Finnhub.
                      Esperado: {'s': symbol, 'p': price, 'v': volume, 't': timestamp}
    """
    global mensajes_enviados

    try:
        # Convertir el diccionario a JSON
        payload = json.dumps(trade)

        # Enviar el mensaje a Kinesis
        # El partition_key (símbolo) garantiza que todos los eventos de na misma acción vayan al mismo shard, manteniendo el orden.
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=payload,
            PartitionKey=trade["s"],
        )

        mensajes_enviados += 1

        # Imprimir cada 10 mensajes para no saturar la consola
        if mensajes_enviados % 10 == 0:
            print(f"📤 [{mensajes_enviados}] Enviado: {trade['s']} @ ${trade['p']:.2f}")

    except Exception as e:
        print(f"❌ Error enviando a Kinesis: {e}")


def on_message(ws, message):
    """Callback que se ejecuta cuando llega un mensaje del WebSocket."""
    data = json.loads(message)

    # Solo procesamos mensajes de tipo 'trade' (transacciones reales)
    if data.get("type") == "trade":
        for trade in data.get("data", []):
            enviar_a_kinesis(trade)
    else:
        print(f"ℹ️  Mensaje del servidor: {data}")


def on_error(ws, error):
    print(f"❌ Error en WebSocket: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"\n🔌 Conexión WebSocket cerrada (código: {close_status_code})")
    print(f"📊 Total de mensajes enviados a Kinesis: {mensajes_enviados}")


def on_open(ws):
    print(f"🔗 Conexión WebSocket establecida con Finnhub")
    print(f"📊 Suscribiendo a símbolos: {', '.join(SYMBOLS)}")
    print(f"🌊 Stream destino: {STREAM_NAME} (región {AWS_REGION})\n")

    for symbol in SYMBOLS:
        mensaje_suscripcion = json.dumps({"type": "subscribe", "symbol": symbol})
        ws.send(mensaje_suscripcion)


if __name__ == "__main__":
    websocket_url = f"wss://ws.finnhub.io?token={API_KEY}"

    print("🚀 Iniciando productor: Finnhub → Kinesis\n")

    ws = websocket.WebSocketApp(
        websocket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )

    # Configuración SSL explícita
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    try:
        ws.run_forever(sslopt={"context": ssl_context})
    except KeyboardInterrupt:
        print("\n\n🛑 Productor detenido manualmente por el usuario.")
        ws.close()