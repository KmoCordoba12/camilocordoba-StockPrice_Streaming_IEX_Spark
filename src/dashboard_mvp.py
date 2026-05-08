"""
Dashboard de Detección de Anomalías Financieras
Visualización web en tiempo real de las alertas generadas por Spark.
"""

import os
import glob
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px

# Configuración
ALERTS_DIR = "data/alerts"
RAW_DIR = "data/raw"

# Configurar la página
st.set_page_config(
    page_title="TFM | Detección de Anomalías Bursátiles",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


# -------------------------------------------------------------------
# Funciones auxiliares
# -------------------------------------------------------------------

@st.cache_data(ttl=5)  # Cache de 5 segundos para refrescar datos
def cargar_alertas():
    """Lee todos los archivos CSV de alertas y los combina en un solo DataFrame."""
    archivos = glob.glob(f"{ALERTS_DIR}/alerts_batch_*.csv")

    if not archivos:
        return pd.DataFrame()

    dataframes = []
    for archivo in archivos:
        try:
            df = pd.read_csv(archivo)
            if not df.empty:
                dataframes.append(df)
        except Exception as e:
            st.warning(f"Error leyendo {archivo}: {e}")

    if not dataframes:
        return pd.DataFrame()

    # Combinar todos los DataFrames
    alertas = pd.concat(dataframes, ignore_index=True)

    # Convertir fechas
    alertas["window_start"] = pd.to_datetime(alertas["window_start"])
    alertas["window_end"] = pd.to_datetime(alertas["window_end"])

    # Eliminar duplicados (las ventanas deslizantes pueden generar registros similares)
    alertas = alertas.drop_duplicates(
        subset=["window_start", "window_end", "symbol", "mean_price"],
        keep="first",
    )

    # Ordenar por fecha (más reciente primero)
    alertas = alertas.sort_values("window_start", ascending=False)

    return alertas


@st.cache_data(ttl=5)
def contar_archivos_raw():
    """Cuenta cuántos archivos hay en data/raw/ para mostrar la salud del pipeline."""
    archivos = glob.glob(f"{RAW_DIR}/batch_*.json")
    return len(archivos)


# -------------------------------------------------------------------
# Sidebar — Filtros y configuración
# -------------------------------------------------------------------

st.sidebar.title("⚙️ Configuración")

st.sidebar.markdown("---")

# Auto-refresh
auto_refresh = st.sidebar.checkbox("🔄 Auto-actualizar (cada 5s)", value=True)

if auto_refresh:
    # Streamlit re-ejecutará el script cada 5 segundos
    import time
    time.sleep(5)
    st.rerun()

st.sidebar.markdown("---")

# Botón para refrescar manualmente
if st.sidebar.button("🔁 Refrescar datos"):
    st.cache_data.clear()
    st.rerun()


# -------------------------------------------------------------------
# Cargar datos
# -------------------------------------------------------------------

alertas = cargar_alertas()
total_archivos_raw = contar_archivos_raw()


# -------------------------------------------------------------------
# Header principal
# -------------------------------------------------------------------

st.title("📊 Detección de Anomalías Bursátiles en Tiempo Real")
st.markdown(
    "**TFM — Máster en Ciencia de Datos | UOC**  \n"
    "_Pipeline: Finnhub → AWS Kinesis → Apache Spark → Dashboard_"
)

st.markdown("---")


# -------------------------------------------------------------------
# Métricas principales (KPIs)
# -------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="🚨 Anomalías totales detectadas",
        value=len(alertas) if not alertas.empty else 0,
    )

with col2:
    simbolos_unicos = alertas["symbol"].nunique() if not alertas.empty else 0
    st.metric(
        label="📈 Símbolos analizados",
        value=simbolos_unicos,
    )

with col3:
    st.metric(
        label="📁 Archivos procesados",
        value=total_archivos_raw,
    )

with col4:
    if not alertas.empty:
        ultima_alerta = alertas["window_end"].max()
        ahora = pd.Timestamp.now(tz=ultima_alerta.tz)
        delta = (ahora - ultima_alerta).total_seconds()

        if delta < 60:
            tiempo_str = f"{int(delta)}s"
        elif delta < 3600:
            tiempo_str = f"{int(delta / 60)}min"
        else:
            tiempo_str = f"{int(delta / 3600)}h"
    else:
        tiempo_str = "—"

    st.metric(
        label="⏱️ Última alerta",
        value=f"hace {tiempo_str}" if tiempo_str != "—" else "—",
    )

st.markdown("---")


# -------------------------------------------------------------------
# Si no hay datos, mostrar mensaje
# -------------------------------------------------------------------

if alertas.empty:
    st.info(
        "📭 No hay alertas detectadas todavía.\n\n"
        "**Pasos para generar datos:**\n"
        "1. Ejecuta el productor: `python src/producer.py`\n"
        "2. Ejecuta el relay: `python src/kinesis_to_files.py`\n"
        "3. Ejecuta el motor: `python src/anomaly_detector_mvp.py`\n\n"
        "Las alertas aparecerán automáticamente en este dashboard."
    )
    st.stop()


# -------------------------------------------------------------------
# Filtros
# -------------------------------------------------------------------

st.subheader("🔍 Filtros")

col_f1, col_f2 = st.columns(2)

with col_f1:
    simbolos_disponibles = sorted(alertas["symbol"].unique().tolist())
    simbolos_seleccionados = st.multiselect(
        "Selecciona símbolos a visualizar:",
        options=simbolos_disponibles,
        default=simbolos_disponibles,
    )

with col_f2:
    rango_volatilidad = st.slider(
        "Rango de volatility ratio:",
        min_value=float(alertas["volatility_ratio"].min()),
        max_value=float(alertas["volatility_ratio"].max()),
        value=(
            float(alertas["volatility_ratio"].min()),
            float(alertas["volatility_ratio"].max()),
        ),
        format="%.6f",
    )

# Aplicar filtros
alertas_filtradas = alertas[
    (alertas["symbol"].isin(simbolos_seleccionados))
    & (alertas["volatility_ratio"] >= rango_volatilidad[0])
    & (alertas["volatility_ratio"] <= rango_volatilidad[1])
]

st.markdown(f"**Mostrando {len(alertas_filtradas)} alertas** (de {len(alertas)} totales)")

st.markdown("---")


# -------------------------------------------------------------------
# Gráficos
# -------------------------------------------------------------------

st.subheader("📈 Visualizaciones")

tab1, tab2, tab3 = st.tabs(["Volatilidad por tiempo", "Distribución por símbolo", "Tabla detallada"])

with tab1:
    if not alertas_filtradas.empty:
        fig = px.scatter(
            alertas_filtradas.sort_values("window_start"),
            x="window_start",
            y="volatility_ratio",
            color="symbol",
            size="mean_price",
            hover_data=["mean_price", "stddev_price", "count_trades"],
            title="Volatility ratio detectado a lo largo del tiempo",
        )
        fig.update_layout(
            xaxis_title="Tiempo",
            yaxis_title="Volatility ratio",
            hovermode="closest",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No hay datos para los filtros seleccionados.")

with tab2:
    if not alertas_filtradas.empty:
        col_g1, col_g2 = st.columns(2)

        with col_g1:
            conteo_simbolos = alertas_filtradas["symbol"].value_counts().reset_index()
            conteo_simbolos.columns = ["symbol", "count"]
            fig_bar = px.bar(
                conteo_simbolos,
                x="symbol",
                y="count",
                color="symbol",
                title="Cantidad de alertas por símbolo",
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_g2:
            fig_box = px.box(
                alertas_filtradas,
                x="symbol",
                y="volatility_ratio",
                color="symbol",
                title="Distribución de volatility ratio por símbolo",
            )
            st.plotly_chart(fig_box, use_container_width=True)
    else:
        st.warning("No hay datos para los filtros seleccionados.")

with tab3:
    st.dataframe(
        alertas_filtradas,
        use_container_width=True,
        hide_index=True,
    )

st.markdown("---")


# -------------------------------------------------------------------
# Footer
# -------------------------------------------------------------------

st.markdown(
    "<div style='text-align: center; color: gray; font-size: 0.8em;'>"
    "Camilo Córdoba Patarroyo | TFM 2026 | UOC<br>"
    f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    "</div>",
    unsafe_allow_html=True,
)