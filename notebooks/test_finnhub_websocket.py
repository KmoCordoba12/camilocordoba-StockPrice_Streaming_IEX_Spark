"""
Script de prueba: Validación de conexión WebSocket a Finnhub
Objetivo: Recibir un flujo de transacciones en tiempo real
"""

import os
import json
import websocket
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

# Obtener la API Key
API_KEY = os.getenv("FINNHUB_API_KEY")

if not API_KEY:
    print("❌ ERROR: No se encontró FINNHUB_API_KEY en el archivo .env")
    exit(1)

# Símbolos a monitorear (puedes añadir más)
SYMBOLS = ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN"]
# SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT"]

# Contador de mensajes recibidos
mensajes_recibidos = 0


def on_message(ws, message):
    """Se ejecuta cada vez que llega un mensaje del servidor."""
    global mensajes_recibidos
    mensajes_recibidos += 1

    data = json.loads(message)

    # Solo procesamos mensajes de tipo "trade" (transacciones reales)
    if data.get("type") == "trade":
        for trade in data.get("data", []):
            symbol = trade.get("s")
            price = trade.get("p")
            volume = trade.get("v")
            timestamp = trade.get("t")

            print(f"📈 [{symbol}] Precio: ${price:.2f} | Volumen: {volume} | Timestamp: {timestamp}")

    # Otros tipos de mensajes (ping, error, etc.)
    else:
        print(f"ℹ️  Mensaje del servidor: {data}")

    # Después de recibir 20 mensajes, cerramos la conexión para no saturar la consola
    if mensajes_recibidos >= 20:
        print(f"\n✅ Recibidos {mensajes_recibidos} mensajes. Cerrando conexión...")
        ws.close()


def on_error(ws, error):
    """Se ejecuta si hay un error en la conexión."""
    print(f"❌ Error: {error}")


def on_close(ws, close_status_code, close_msg):
    """Se ejecuta cuando se cierra la conexión."""
    print(f"\n🔌 Conexión cerrada (código: {close_status_code})")


def on_open(ws):
    """Se ejecuta cuando se abre la conexión exitosamente."""
    print(f"🔗 Conexión WebSocket establecida con Finnhub")
    print(f"📊 Suscribiendo a símbolos: {', '.join(SYMBOLS)}\n")

    # Suscribirse a cada símbolo
    for symbol in SYMBOLS:
        mensaje_suscripcion = json.dumps({"type": "subscribe", "symbol": symbol})
        ws.send(mensaje_suscripcion)


if __name__ == "__main__":
    # URL del WebSocket de Finnhub con la API Key
    websocket_url = f"wss://ws.finnhub.io?token={API_KEY}"

    print("🚀 Iniciando conexión WebSocket a Finnhub...\n")

    # Crear la conexión WebSocket
    ws = websocket.WebSocketApp(
        websocket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )

    # Mantener la conexión activa hasta que se cierre
    ws.run_forever()