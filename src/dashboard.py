"""
Dashboard de Detección de Anomalías Financieras (Z-Score Real)

Visualización web en tiempo real de las alertas generadas por el motor
de Z-Score adaptativo (anomaly_detector.py).

Lee solo archivos zscore_alerts_batch_*.csv para mantener separación
clara con la versión MVP.
"""

import os
import glob
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Configuración
ALERTS_DIR = "data/alerts"
RAW_DIR = "data/raw"
ALERT_PATTERN = "zscore_alerts_batch_*.csv"  # Solo lee alertas del Z-Score real

# Configurar la página
st.set_page_config(
    page_title="TFM | Detección de Anomalías Bursátiles (Z-Score)",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


# -------------------------------------------------------------------
# Funciones auxiliares
# -------------------------------------------------------------------

@st.cache_data(ttl=5)
def cargar_alertas():
    """Lee todos los archivos CSV de alertas Z-Score y los combina."""
    archivos = glob.glob(f"{ALERTS_DIR}/{ALERT_PATTERN}")

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

    alertas = pd.concat(dataframes, ignore_index=True)

    # Convertir fechas
    if "event_time" in alertas.columns:
        alertas["event_time"] = pd.to_datetime(alertas["event_time"])

    # Eliminar duplicados (basados en evento + símbolo + precio)
    columnas_dedup = [c for c in ["event_time", "symbol", "price"] if c in alertas.columns]
    if columnas_dedup:
        alertas = alertas.drop_duplicates(subset=columnas_dedup, keep="first")

    # Ordenar por fecha (más reciente primero)
    if "event_time" in alertas.columns:
        alertas = alertas.sort_values("event_time", ascending=False)

    return alertas


@st.cache_data(ttl=5)
def contar_archivos_raw():
    """Cuenta los archivos JSON en data/raw/ para mostrar la salud del pipeline."""
    archivos = glob.glob(f"{RAW_DIR}/batch_*.json")
    return len(archivos)


# -------------------------------------------------------------------
# Sidebar — Configuración
# -------------------------------------------------------------------

st.sidebar.title("⚙️ Configuración")

st.sidebar.markdown("---")
st.sidebar.markdown("**Motor activo:** `anomaly_detector.py`")
st.sidebar.markdown(f"**Patrón de archivos:** `{ALERT_PATTERN}`")

st.sidebar.markdown("---")

# Auto-refresh
auto_refresh = st.sidebar.checkbox("🔄 Auto-actualizar (cada 5s)", value=True)

if auto_refresh:
    import time
    time.sleep(5)
    st.rerun()

st.sidebar.markdown("---")

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

st.title("📊 Detección de Anomalías Bursátiles — Z-Score Adaptativo")
st.markdown(
    "**TFM — Máster en Ciencia de Datos | UOC**  \n"
    "_Motor: Z-Score real por evento individual sobre ventanas deslizantes_  \n"
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
    if not alertas.empty and "z_score" in alertas.columns:
        max_z_abs = alertas["z_score"].abs().max()
        st.metric(
            label="📊 |Z-Score| máximo",
            value=f"{max_z_abs:.2f}",
        )
    else:
        st.metric(label="📊 |Z-Score| máximo", value="—")

st.markdown("---")


# -------------------------------------------------------------------
# Si no hay datos, mostrar mensaje
# -------------------------------------------------------------------

if alertas.empty:
    st.info(
        "📭 No hay alertas Z-Score detectadas todavía.\n\n"
        "**Pasos para generar datos:**\n"
        "1. Ejecuta el productor: `python src/producer.py`\n"
        "2. Ejecuta el relay: `python src/kinesis_to_files.py`\n"
        "3. Ejecuta el motor Z-Score: `python src/anomaly_detector.py`\n\n"
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
    z_min = float(alertas["z_score"].abs().min())
    z_max = float(alertas["z_score"].abs().max())

    rango_z = st.slider(
        "Rango de |Z-Score|:",
        min_value=z_min,
        max_value=z_max,
        value=(z_min, z_max),
        step=0.1,
        format="%.2f",
    )

# Aplicar filtros
alertas_filtradas = alertas[
    (alertas["symbol"].isin(simbolos_seleccionados))
    & (alertas["z_score"].abs() >= rango_z[0])
    & (alertas["z_score"].abs() <= rango_z[1])
]

st.markdown(f"**Mostrando {len(alertas_filtradas)} alertas** (de {len(alertas)} totales)")

st.markdown("---")


# -------------------------------------------------------------------
# Visualizaciones
# -------------------------------------------------------------------

st.subheader("📈 Visualizaciones")

tab1, tab2, tab3, tab4 = st.tabs([
    "Z-Score por tiempo",
    "Precios y anomalías",
    "Distribución por símbolo",
    "Tabla detallada",
])


# Tab 1 — Z-Score por tiempo
with tab1:
    if not alertas_filtradas.empty:
        fig = px.scatter(
            alertas_filtradas.sort_values("event_time"),
            x="event_time",
            y="z_score",
            color="symbol",
            size=alertas_filtradas["z_score"].abs(),
            hover_data=["price", "mean_price", "stddev_price"],
            title="Z-Score detectado a lo largo del tiempo",
        )

        # Líneas de referencia para los umbrales (+3 y -3)
        fig.add_hline(
            y=3,
            line_dash="dash",
            line_color="red",
            annotation_text="Umbral +3σ",
            annotation_position="top right",
        )
        fig.add_hline(
            y=-3,
            line_dash="dash",
            line_color="red",
            annotation_text="Umbral -3σ",
            annotation_position="bottom right",
        )
        fig.add_hline(y=0, line_dash="dot", line_color="gray", line_width=1)

        fig.update_layout(
            xaxis_title="Tiempo del evento",
            yaxis_title="Z-Score",
            hovermode="closest",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No hay datos para los filtros seleccionados.")


# Tab 2 — Precios reales vs media móvil
with tab2:
    if not alertas_filtradas.empty:
        # Para cada símbolo, mostrar el precio real y la media de su ventana
        simbolos_a_graficar = alertas_filtradas["symbol"].unique()

        for simbolo in simbolos_a_graficar:
            datos_simbolo = alertas_filtradas[
                alertas_filtradas["symbol"] == simbolo
            ].sort_values("event_time")

            fig = go.Figure()

            # Línea de la media de la ventana
            fig.add_trace(go.Scatter(
                x=datos_simbolo["event_time"],
                y=datos_simbolo["mean_price"],
                mode="lines",
                name="Media móvil",
                line=dict(color="lightblue", width=2),
            ))

            # Banda de ±3σ
            fig.add_trace(go.Scatter(
                x=datos_simbolo["event_time"],
                y=datos_simbolo["mean_price"] + 3 * datos_simbolo["stddev_price"],
                mode="lines",
                line=dict(color="rgba(255,0,0,0.3)", dash="dot"),
                showlegend=False,
                name="+3σ",
            ))
            fig.add_trace(go.Scatter(
                x=datos_simbolo["event_time"],
                y=datos_simbolo["mean_price"] - 3 * datos_simbolo["stddev_price"],
                mode="lines",
                line=dict(color="rgba(255,0,0,0.3)", dash="dot"),
                fill="tonexty",
                fillcolor="rgba(255,0,0,0.05)",
                showlegend=False,
                name="-3σ",
            ))

            # Anomalías como puntos rojos
            fig.add_trace(go.Scatter(
                x=datos_simbolo["event_time"],
                y=datos_simbolo["price"],
                mode="markers",
                name="Anomalía",
                marker=dict(
                    color="red",
                    size=10,
                    symbol="x",
                    line=dict(color="white", width=1),
                ),
            ))

            fig.update_layout(
                title=f"{simbolo} — Precio vs banda de ±3σ",
                xaxis_title="Tiempo",
                yaxis_title="Precio (USD)",
                hovermode="x unified",
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No hay datos para los filtros seleccionados.")


# Tab 3 — Distribución por símbolo
with tab3:
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
                title="Cantidad de anomalías por símbolo",
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_g2:
            fig_hist = px.histogram(
                alertas_filtradas,
                x="z_score",
                color="symbol",
                nbins=30,
                title="Distribución de Z-Scores detectados",
            )
            fig_hist.add_vline(x=3, line_dash="dash", line_color="red")
            fig_hist.add_vline(x=-3, line_dash="dash", line_color="red")
            st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.warning("No hay datos para los filtros seleccionados.")


# Tab 4 — Tabla detallada
with tab4:
    columnas_mostrar = [
        c for c in [
            "event_time",
            "symbol",
            "price",
            "mean_price",
            "stddev_price",
            "z_score",
            "volume",
        ] if c in alertas_filtradas.columns
    ]
    st.dataframe(
        alertas_filtradas[columnas_mostrar],
        use_container_width=True,
        hide_index=True,
    )

    # Botón para descargar como CSV
    csv = alertas_filtradas.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Descargar alertas filtradas como CSV",
        data=csv,
        file_name=f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
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