"""
Script de prueba: Validación de conexión REST a Finnhub
"""

import os
import requests
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

# Obtener la API Key desde el archivo .env
API_KEY = os.getenv("FINNHUB_API_KEY")

if not API_KEY:
    print("❌ ERROR: No se encontró FINNHUB_API_KEY en el archivo .env")
    exit(1)

# Símbolo a consultar
SYMBOL = "AAPL"

# Endpoint REST de Finnhub para obtener cotización actual
url = f"https://finnhub.io/api/v1/quote?symbol={SYMBOL}&token={API_KEY}"

print(f"🔍 Consultando precio actual de {SYMBOL}...")

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Lanza error si el código HTTP no es 2xx

    data = response.json()

    print("\n✅ Respuesta exitosa de Finnhub:")
    print(f"   Símbolo:           {SYMBOL}")
    print(f"   Precio actual (c): ${data.get('c')}")
    print(f"   Precio anterior:   ${data.get('pc')}")
    print(f"   Apertura:          ${data.get('o')}")
    print(f"   Máximo del día:    ${data.get('h')}")
    print(f"   Mínimo del día:    ${data.get('l')}")
    print(f"   Variación:         {data.get('d')} ({data.get('dp')}%)")

    print("\n📦 JSON completo recibido:")
    print(data)

except requests.exceptions.HTTPError as e:
    print(f"❌ Error HTTP: {e}")
    print(f"Respuesta: {response.text}")
except requests.exceptions.RequestException as e:
    print(f"❌ Error de conexión: {e}")