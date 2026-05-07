# Detección de Anomalías en Cotizaciones Bursátiles en Tiempo Real

Trabajo de Fin de Máster (TFM) — Máster Universitario en Ciencia de Datos
Universitat Oberta de Catalunya (UOC)

**Autor:** Camilo Córdoba Patarroyo
**Asesor:** Rafael Luque Ocaña

## Descripción

Este proyecto implementa una arquitectura de *streaming* escalable para la detección
de anomalías en las cotizaciones de mercados financieros en tiempo real, utilizando
Finnhub.io como fuente de datos, AWS Kinesis como capa de transporte y Apache Spark
Structured Streaming como motor de análisis.

## Arquitectura
Finnhub WebSocket  →  Productor (Python)  →  AWS Kinesis  →  Spark Structured Streaming  →  CSV + Dashboard

## Estructura del proyecto
camilocordoba-StockPrice_Streaming_IEX_Spark/
├── src/                    # Código fuente del pipeline
├── data/
│   └── alerts/             # Archivos CSV con las alertas generadas
├── notebooks/              # Notebooks de validación y experimentación
├── requirements.txt        # Dependencias de Python
├── LICENSE                 # GNU AGPL v3.0
└── README.md               # Este archivo

## Requisitos

- macOS, Linux o Windows
- Python 3.10 o superior
- Cuenta de AWS (capa gratuita disponible)
- Cuenta de Finnhub.io (capa gratuita disponible)
- Apache Spark 3.x (instalación local)

## Instalación

```bash
# Clonar el repositorio
git clone https://github.com/KmoCordoba12/camilocordoba-StockPrice_Streaming_IEX_Spark.git
cd camilocordoba-StockPrice_Streaming_IEX_Spark

# Crear y activar entorno virtual
python3 -m venv venv
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

## Licencia

Este proyecto está licenciado bajo GNU Affero General Public License v3.0.
Consulta el archivo `LICENSE` para más detalles.

## Estado del proyecto

🚧 En desarrollo — Trabajo de Fin de Máster en curso.