"""
Orquestador del Pipeline de Detección de Anomalías Financieras

Inicia y detiene de forma coordinada los 4 procesos del sistema:
  1. Productor    (Finnhub WebSocket → AWS Kinesis)
  2. Relay        (Kinesis → archivos JSON locales)
  3. Motor Spark  (MVP o avanzado con Z-Score real)
  4. Dashboard    (Streamlit)

Adicionalmente gestiona el ciclo de vida de AWS Kinesis:
  - Crea el stream al iniciar
  - Espera a que esté ACTIVE
  - Lo elimina al cerrar el orquestador (para ahorro de costos)

Uso:
    python run_pipeline.py

Detención:
    Ctrl+C (detiene todos los procesos y elimina Kinesis automáticamente)
"""

import os
import sys
import time
import shutil
import signal
import subprocess
from datetime import datetime
from pathlib import Path

# -------------------------------------------------------------------
# Configuración
# -------------------------------------------------------------------

# Rutas
PROJECT_ROOT = Path(__file__).parent.resolve()
SRC_DIR = PROJECT_ROOT / "src"
LOGS_DIR = PROJECT_ROOT / "logs"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# AWS / Kinesis
STREAM_NAME = "finnhub-stock-trades"
SHARD_COUNT = 1
KINESIS_TIMEOUT = 120  # segundos máximos de espera

# Streamlit
DASHBOARD_PORT_MVP = 8501
DASHBOARD_PORT_ZSCORE = 8502

# -------------------------------------------------------------------
# Utilidades de presentación
# -------------------------------------------------------------------

def banner(texto, char="=", ancho=70):
    """Imprime un banner estilizado."""
    print(f"\n{char * ancho}")
    print(f"{texto.center(ancho)}")
    print(f"{char * ancho}\n")


def info(mensaje):
    print(f"ℹ️  {mensaje}")


def ok(mensaje):
    print(f"✅ {mensaje}")


def warn(mensaje):
    print(f"⚠️  {mensaje}")


def error(mensaje):
    print(f"❌ {mensaje}")


# -------------------------------------------------------------------
# Gestión de Kinesis
# -------------------------------------------------------------------

def kinesis_existe():
    """Verifica si el stream de Kinesis ya existe."""
    result = subprocess.run(
        ["aws", "kinesis", "describe-stream-summary",
         "--stream-name", STREAM_NAME],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def crear_kinesis():
    """Crea el stream de Kinesis y espera a que esté ACTIVE."""
    if kinesis_existe():
        info(f"Stream '{STREAM_NAME}' ya existe. Saltando creación.")
        return True

    info(f"Creando stream '{STREAM_NAME}' con {SHARD_COUNT} shard(s)...")
    result = subprocess.run(
        ["aws", "kinesis", "create-stream",
         "--stream-name", STREAM_NAME,
         "--shard-count", str(SHARD_COUNT)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        error(f"Fallo al crear el stream: {result.stderr}")
        return False

    info("Esperando a que el stream esté ACTIVE (puede tardar 30-60 segundos)...")
    inicio = time.time()

    while time.time() - inicio < KINESIS_TIMEOUT:
        result = subprocess.run(
            ["aws", "kinesis", "describe-stream-summary",
             "--stream-name", STREAM_NAME,
             "--query", "StreamDescriptionSummary.StreamStatus",
             "--output", "text"],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0 and result.stdout.strip() == "ACTIVE":
            ok(f"Stream '{STREAM_NAME}' está ACTIVE")
            return True

        time.sleep(3)

    error(f"Timeout esperando que el stream esté ACTIVE")
    return False


def eliminar_kinesis():
    """Elimina el stream de Kinesis."""
    if not kinesis_existe():
        info("Stream ya no existe. Nada que eliminar.")
        return

    info(f"Eliminando stream '{STREAM_NAME}'...")
    result = subprocess.run(
        ["aws", "kinesis", "delete-stream",
         "--stream-name", STREAM_NAME],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        ok(f"Stream '{STREAM_NAME}' eliminado")
    else:
        warn(f"No se pudo eliminar el stream: {result.stderr}")


# -------------------------------------------------------------------
# Gestión de procesos
# -------------------------------------------------------------------

def lanzar_proceso(nombre, comando, log_file):
    """Lanza un proceso en background con su salida redirigida a un archivo (sin buffering)."""
    info(f"Iniciando: {nombre} (logs en {log_file.name})")
    log_handle = open(log_file, "w")

    # Variables de entorno: forzar salida sin buffer en Python
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    proceso = subprocess.Popen(
        comando,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        cwd=PROJECT_ROOT,
        env=env,
        bufsize=0,  # sin buffering del subproceso
    )
    return {"nombre": nombre, "proceso": proceso, "log_handle": log_handle}


def detener_proceso(p):
    """Detiene un proceso de forma ordenada."""
    nombre = p["nombre"]
    proceso = p["proceso"]

    if proceso.poll() is None:  # Todavía corriendo
        info(f"Deteniendo: {nombre}")
        proceso.terminate()
        try:
            proceso.wait(timeout=10)
        except subprocess.TimeoutExpired:
            warn(f"{nombre} no respondió, forzando cierre.")
            proceso.kill()
            proceso.wait()

    p["log_handle"].close()


# -------------------------------------------------------------------
# Limpieza opcional
# -------------------------------------------------------------------

def limpiar_data_raw():
    """Elimina los archivos JSON acumulados en data/raw/."""
    if not RAW_DIR.exists():
        return

    archivos = list(RAW_DIR.glob("batch_*.json"))
    if not archivos:
        return

    info(f"Eliminando {len(archivos)} archivos de data/raw/...")
    for archivo in archivos:
        archivo.unlink()
    ok("Carpeta data/raw/ limpia")


# -------------------------------------------------------------------
# Lógica principal
# -------------------------------------------------------------------

def preguntar_motor():
    """Pregunta al usuario qué motor desea ejecutar."""
    banner("SELECCIÓN DE MOTOR")
    print("¿Qué versión del motor de detección quieres ejecutar?\n")
    print("  [1] MVP        - Detección por volatilidad de ventana")
    print("  [2] Z-Score    - Detección por evento individual (recomendado)")

    while True:
        respuesta = input("\nElige una opción (1/2): ").strip()
        if respuesta == "1":
            return {
                "nombre": "MVP",
                "script": "src/anomaly_detector_mvp.py",
                "dashboard": "src/dashboard_mvp.py",
                "puerto": DASHBOARD_PORT_MVP,
            }
        elif respuesta == "2":
            return {
                "nombre": "Z-Score Real",
                "script": "src/anomaly_detector.py",
                "dashboard": "src/dashboard.py",
                "puerto": DASHBOARD_PORT_ZSCORE,
            }
        else:
            warn("Opción inválida. Escribe 1 o 2.")


def preguntar_limpieza():
    """Pregunta si limpiar la carpeta data/raw/."""
    banner("LIMPIEZA DE DATOS PREVIOS")
    archivos_existentes = list(RAW_DIR.glob("batch_*.json")) if RAW_DIR.exists() else []

    if not archivos_existentes:
        info("No hay archivos previos en data/raw/. Saltando.")
        return False

    print(f"Hay {len(archivos_existentes)} archivos previos en data/raw/.")
    print("Si los mantienes, Spark los reprocesará junto con los nuevos.\n")

    while True:
        respuesta = input("¿Limpiar data/raw/ y empezar desde cero? (s/n): ").strip().lower()
        if respuesta in ("s", "si", "sí", "y", "yes"):
            return True
        elif respuesta in ("n", "no"):
            return False
        else:
            warn("Respuesta inválida. Escribe s o n.")


# -------------------------------------------------------------------
# Punto de entrada
# -------------------------------------------------------------------

def main():
    banner("ORQUESTADOR DEL PIPELINE DE DETECCIÓN DE ANOMALÍAS", char="█")

    # 1. Selección del motor
    motor = preguntar_motor()
    print(f"\n→ Motor seleccionado: {motor['nombre']}")

    # 2. Limpieza opcional
    if preguntar_limpieza():
        limpiar_data_raw()

    # 3. Preparar carpeta de logs
    LOGS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_session_dir = LOGS_DIR / f"session_{timestamp}"
    log_session_dir.mkdir()
    info(f"Logs de esta sesión: {log_session_dir}")

    # 4. Crear el stream de Kinesis
    banner("PASO 1/4 — AWS KINESIS")
    if not crear_kinesis():
        error("No se pudo configurar Kinesis. Abortando.")
        sys.exit(1)

    # 5. Lanzar los 4 procesos
    procesos = []

    try:
        # Productor
        banner("PASO 2/4 — PRODUCTOR")
        procesos.append(lanzar_proceso(
            nombre="Productor",
            comando=[sys.executable, str(SRC_DIR / "producer.py")],
            log_file=log_session_dir / "producer.log",
        ))
        time.sleep(2)  # Pequeña pausa para que el productor empiece

        # Relay
        banner("PASO 3/4 — RELAY")
        procesos.append(lanzar_proceso(
            nombre="Relay (Kinesis → archivos)",
            comando=[sys.executable, str(SRC_DIR / "kinesis_to_files.py")],
            log_file=log_session_dir / "relay.log",
        ))
        time.sleep(2)

        # Motor Spark
        banner("PASO 4/4 — MOTOR SPARK + DASHBOARD")
        procesos.append(lanzar_proceso(
            nombre=f"Motor Spark ({motor['nombre']})",
            comando=[sys.executable, str(PROJECT_ROOT / motor["script"])],
            log_file=log_session_dir / "spark.log",
        ))
        time.sleep(3)

        # Dashboard
        procesos.append(lanzar_proceso(
            nombre=f"Dashboard (puerto {motor['puerto']})",
            comando=[
                sys.executable, "-m", "streamlit", "run",
                str(PROJECT_ROOT / motor["dashboard"]),
                "--server.port", str(motor["puerto"]),
                "--server.headless", "true",
            ],
            log_file=log_session_dir / "dashboard.log",
        ))

        # Resumen final
        banner("✅ PIPELINE EN EJECUCIÓN", char="█")
        print("Procesos activos:")
        for p in procesos:
            print(f"  • {p['nombre']} (PID {p['proceso'].pid})")
        print()
        print(f"📊 Dashboard disponible en: http://localhost:{motor['puerto']}")
        print(f"📁 Logs en tiempo real:     tail -f {log_session_dir}/*.log")
        print()
        print("Presiona Ctrl+C para detener todos los procesos y limpiar Kinesis.\n")

        # Esperar indefinidamente hasta Ctrl+C
        while True:
            time.sleep(5)
            # Verificar si algún proceso murió inesperadamente
            for p in procesos:
                if p["proceso"].poll() is not None:
                    warn(
                        f"El proceso '{p['nombre']}' terminó inesperadamente. "
                        f"Revisa el log: {log_session_dir / (p['nombre'].split()[0].lower() + '.log')}"
                    )

    except KeyboardInterrupt:
        banner("🛑 DETENIENDO PIPELINE", char="█")

    finally:
        # Detener todos los procesos
        for p in procesos:
            detener_proceso(p)

        # Eliminar Kinesis para no acumular costos
        banner("LIMPIEZA DE AWS")
        eliminar_kinesis()

        banner("✅ ORQUESTADOR FINALIZADO", char="█")
        print(f"📁 Logs preservados en: {log_session_dir}\n")


if __name__ == "__main__":
    main()