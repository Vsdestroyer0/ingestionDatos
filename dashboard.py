import json
import random
import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import streamlit as st

# Set page configuration with a custom title and wide layout
st.set_page_config(
    page_title="Steam Analytics Dashboard",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom premium styling using CSS
st.markdown("""
<style>
    /* Main container styling */
    .reportview-container {
        background: #0f111a;
    }
    
    /* Title styling */
    .main-title {
        font-family: 'Outfit', 'Inter', sans-serif;
        background: linear-gradient(90deg, #00f2fe 0%, #4facfe 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        font-size: 3rem;
        margin-bottom: 0.5rem;
    }
    
    .subtitle {
        font-family: 'Inter', sans-serif;
        color: #8f9cae;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }

    /* Metric card styling */
    .metric-card {
        background-color: #161925;
        border-radius: 10px;
        padding: 1.5rem;
        border: 1px solid #23283d;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        transition: transform 0.3s ease, border-color 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        border-color: #00f2fe;
    }
    
    .metric-title {
        color: #8f9cae;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 600;
    }
    
    .metric-value {
        color: #ffffff;
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0.5rem 0;
        font-family: 'Courier New', monospace;
    }
    
    .metric-delta {
        font-size: 0.85rem;
        font-weight: 500;
    }
    
    .delta-up {
        color: #00e676;
    }
    
    .delta-down {
        color: #ff1744;
    }
    
    /* Live indicator styling */
    .live-pulse {
        display: inline-block;
        width: 10px;
        height: 10px;
        background-color: #00e676;
        border-radius: 50%;
        margin-right: 8px;
        box-shadow: 0 0 10px #00e676;
        animation: pulse 1.5s infinite;
    }
    
    @keyframes pulse {
        0% {
            transform: scale(0.95);
            box-shadow: 0 0 0 0 rgba(0, 230, 118, 0.7);
        }
        70% {
            transform: scale(1);
            box-shadow: 0 0 0 10px rgba(0, 230, 118, 0);
        }
        100% {
            transform: scale(0.95);
            box-shadow: 0 0 0 0 rgba(0, 230, 118, 0);
        }
    }
</style>
""", unsafe_allow_html=True)

# Configuration & Database connection
DB_URL = "postgresql+pg8000://admin:password@localhost:5432/steam_analytics"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "steam_players"

@st.cache_resource
def get_db_engine():
    return create_engine(DB_URL)

engine = get_db_engine()

# Initialize session state for real-time tracking
if "live_history" not in st.session_state:
    st.session_state.live_history = {}  # {game_name: [list of (timestamp, ccu)]}
if "last_ccu" not in st.session_state:
    st.session_state.last_ccu = {}      # {game_name: ccu}
if "previous_ccu" not in st.session_state:
    st.session_state.previous_ccu = {}  # {game_name: ccu}

# Function to fetch data from PostgreSQL
def load_historical_data():
    query = """
        SELECT 
            f.fact_id,
            g.title,
            g.developer,
            g.publisher,
            t.full_date,
            t.hour,
            t.day,
            t.month,
            t.year,
            f.ccu
        FROM fact_players f
        JOIN dim_games g ON f.game_id = g.game_id
        JOIN dim_time t ON f.time_id = t.time_id
        ORDER BY t.full_date DESC
    """
    try:
        df = pd.read_sql(query, con=engine)
        # Convert full_date to datetime if not already
        df['full_date'] = pd.to_datetime(df['full_date'])
        return df
    except Exception as e:
        st.error(f"Error loading historical data: {e}")
        return pd.DataFrame()

def load_genre_data():
    query = """
        SELECT 
            g.title,
            tag.tag_name,
            tag.tag_priority,
            f.ccu
        FROM fact_players f
        JOIN dim_games g ON f.game_id = g.game_id
        JOIN dim_game_tags tag ON g.game_id = tag.game_id
        WHERE tag.tag_priority = 1 -- Principal genre
    """
    try:
        return pd.read_sql(query, con=engine)
    except Exception as e:
        st.error(f"Error loading genre data: {e}")
        return pd.DataFrame()

# Main dashboard layout
st.markdown('<div class="main-title">🎮 STEAM ANALYTICS</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Plataforma de monitoreo y análisis en tiempo real de jugadores concurrentes</div>', unsafe_allow_html=True)

# Tabs configuration
tab1, tab2, tab3 = st.tabs(["📈 Análisis Histórico (PostgreSQL)", "⚡ Tiempo Real (Kafka)", "🗄️ Explorador de BD"])

# TAB 1: Historical Analysis
with tab1:
    st.subheader("Tendencias y Análisis de Datos Históricos")
    
    # Reload button
    if st.button("🔄 Actualizar Datos Históricos"):
        st.cache_data.clear()
        
    df_hist = load_historical_data()
    
    if df_hist.empty:
        st.warning("No hay datos históricos registrados aún. Inicia la ingesta de datos para poblar PostgreSQL.")
    else:
        # Metrics cards row
        unique_games = df_hist['title'].nunique()
        avg_ccu = int(df_hist['ccu'].mean())
        peak_ccu = int(df_hist['ccu'].max())
        total_records = len(df_hist)
        
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Juegos Monitoreados</div>
                <div class="metric-value">{unique_games}</div>
                <div class="metric-delta"><span class="delta-up">▲ Activos</span> en base</div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Jugadores Promedio</div>
                <div class="metric-value">{avg_ccu:,}</div>
                <div class="metric-delta">Histórico global</div>
            </div>
            """, unsafe_allow_html=True)
        with c3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Pico de Jugadores</div>
                <div class="metric-value">{peak_ccu:,}</div>
                <div class="metric-delta"><span class="delta-up">▲ Máximo</span> registrado</div>
            </div>
            """, unsafe_allow_html=True)
        with c4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Registros en BD</div>
                <div class="metric-value">{total_records:,}</div>
                <div class="metric-delta">Métricas en fact_players</div>
            </div>
            """, unsafe_allow_html=True)
            
        st.write("")
        st.write("")
        
        # Sidebar/Filters inside columns for cleaner layout
        st.markdown("### Filtros de Análisis")
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            selected_games = st.multiselect(
                "Seleccionar Juegos:", 
                options=df_hist['title'].unique(), 
                default=df_hist['title'].unique()[:3]
            )
        with col_f2:
            date_range = st.date_input(
                "Rango de Fechas:",
                value=(df_hist['full_date'].min().date(), df_hist['full_date'].max().date())
            )
            
        # Filter dataframe
        filtered_df = df_hist[df_hist['title'].isin(selected_games)]
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_df = filtered_df[
                (filtered_df['full_date'].dt.date >= start_date) & 
                (filtered_df['full_date'].dt.date <= end_date)
            ]
            
        if filtered_df.empty:
            st.warning("No hay datos para los filtros seleccionados.")
        else:
            # Main Historical Chart
            st.markdown("### Tendencia Temporal de Jugadores Concurrentes")
            
            # Sort data for line chart
            chart_df = filtered_df.sort_values(by='full_date')
            
            fig_line = px.line(
                chart_df,
                x='full_date',
                y='ccu',
                color='title',
                title="Jugadores en línea (CCU) vs. Tiempo",
                labels={"full_date": "Fecha y Hora", "ccu": "Jugadores (CCU)", "title": "Juego"},
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            
            # Premium styling for plotly
            fig_line.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#8f9cae',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis=dict(gridcolor='#1e2230', showgrid=True),
                yaxis=dict(gridcolor='#1e2230', showgrid=True),
                margin=dict(l=0, r=0, t=50, b=0)
            )
            st.plotly_chart(fig_line, use_container_width=True)
            
            # Secondary Charts Row
            st.write("")
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                st.markdown("### Comparativa: Promedio por Juego")
                df_avg = filtered_df.groupby('title')['ccu'].mean().reset_index()
                df_avg = df_avg.sort_values(by='ccu', ascending=False)
                
                fig_bar = px.bar(
                    df_avg,
                    x='ccu',
                    y='title',
                    orientation='h',
                    color='title',
                    labels={"ccu": "Promedio CCU", "title": "Juego"},
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                fig_bar.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#8f9cae',
                    showlegend=False,
                    xaxis=dict(gridcolor='#1e2230', showgrid=True),
                    yaxis=dict(gridcolor='rgba(0,0,0,0)'),
                    margin=dict(l=0, r=0, t=20, b=0)
                )
                st.plotly_chart(fig_bar, use_container_width=True)
                
            with col_chart2:
                st.markdown("### Participación por Géneros (Dominante)")
                df_genres = load_genre_data()
                if not df_genres.empty:
                    df_genre_sum = df_genres.groupby('tag_name')['ccu'].mean().reset_index()
                    df_genre_sum = df_genre_sum.sort_values(by='ccu', ascending=False)
                    
                    fig_donut = px.pie(
                        df_genre_sum,
                        names='tag_name',
                        values='ccu',
                        hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Bold
                    )
                    fig_donut.update_layout(
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        font_color='#8f9cae',
                        legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1),
                        margin=dict(l=0, r=0, t=20, b=0)
                    )
                    st.plotly_chart(fig_donut, use_container_width=True)
                else:
                    st.info("No hay datos de géneros/etiquetas cargados en la dimensión.")

# TAB 2: Real-Time Stream (Kafka)
with tab2:
    st.subheader("Monitoreo en Tiempo Real (Kafka Stream)")
    
    col_ctrl, col_desc = st.columns([1, 3])
    with col_ctrl:
        live_active = st.checkbox("🔌 Activar Consumidor Kafka en Vivo", value=False)
        st.write("")
        st.markdown("**Configuración:**")
        bootstrap_srv = st.text_input("Kafka Broker", KAFKA_BOOTSTRAP_SERVERS)
        topic_name = st.text_input("Kafka Topic", KAFKA_TOPIC)
        
    with col_desc:
        st.markdown(f"""
        ### Canal en Vivo
        Cuando el interruptor de la izquierda está **activo**, Streamlit se conectará al Broker de Kafka en `{bootstrap_srv}` 
        y escuchará eventos en el tópico `{topic_name}`.
        
        - Se graficarán los datos conforme lleguen en tiempo real.
        - La tabla y las tarjetas de métricas se actualizarán instantáneamente.
        """)
        if live_active:
            st.markdown('<div style="display: flex; align-items: center;"><span class="live-pulse"></span><span style="color:#00e676; font-weight:bold;">Escuchando eventos en tiempo real de Kafka...</span></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#ff1744; font-weight:bold;">🔌 Consumidor inactivo.</div>', unsafe_allow_html=True)
            
    st.write("")
    
    # Placeholders for dynamic content
    live_metrics_placeholder = st.empty()
    live_chart_placeholder = st.empty()
    
    # If live mode is selected, run a loop that polls Kafka
    if live_active:
        from confluent_kafka import Consumer, KafkaError
        
        # Configure unique consumer group to always get latest events
        group_id = f"streamlit-live-{random.randint(1000, 9999)}"
        conf = {
            'bootstrap.servers': bootstrap_srv,
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False
        }
        
        try:
            kafka_consumer = Consumer(conf)
            kafka_consumer.subscribe([topic_name])
            
            # Run for a session loop before reloading
            # A loop that polls Kafka and updates elements
            for iteration in range(60):
                # Poll Kafka for messages
                msg = kafka_consumer.poll(0.5)
                
                # Check for message
                if msg is not None and not msg.error():
                    try:
                        # Decode JSON
                        event = json.loads(msg.value().decode('utf-8'))
                        game_id = event["game_id"]
                        ccu = event["ccu"]
                        timestamp_str = event["timestamp"]
                        dt_obj = datetime.fromisoformat(timestamp_str)
                        time_str = dt_obj.strftime("%H:%M:%S")
                        
                        # Try to resolve title from local database or fallback
                        with engine.connect() as conn:
                            res = conn.execute(
                                text("SELECT title FROM dim_games WHERE game_id = :game_id"), 
                                {"game_id": game_id}
                            ).fetchone()
                            game_name = res[0] if res else f"AppID {game_id}"
                            
                        # Update session state values
                        if game_name not in st.session_state.live_history:
                            st.session_state.live_history[game_name] = []
                            
                        # Save current state
                        if game_name in st.session_state.last_ccu:
                            st.session_state.previous_ccu[game_name] = st.session_state.last_ccu[game_name]
                        else:
                            st.session_state.previous_ccu[game_name] = ccu
                            
                        st.session_state.last_ccu[game_name] = ccu
                        
                        # Append history (keep last 30 points)
                        st.session_state.live_history[game_name].append((time_str, ccu))
                        if len(st.session_state.live_history[game_name]) > 30:
                            st.session_state.live_history[game_name].pop(0)
                            
                    except Exception as parse_err:
                        print(f"Error parsing kafka message: {parse_err}")
                
                # Update UI elements
                if st.session_state.last_ccu:
                    # 1. Metric cards rendering
                    with live_metrics_placeholder.container():
                        st.markdown("#### Métricas Recientes")
                        cols = st.columns(min(len(st.session_state.last_ccu), 4))
                        
                        for i, (g_name, curr_ccu) in enumerate(st.session_state.last_ccu.items()):
                            col_idx = i % len(cols)
                            prev_ccu = st.session_state.previous_ccu.get(g_name, curr_ccu)
                            diff = curr_ccu - prev_ccu
                            
                            if diff > 0:
                                delta_html = f'<span class="delta-up">▲ +{diff:,}</span>'
                            elif diff < 0:
                                delta_html = f'<span class="delta-down">▼ {diff:,}</span>'
                            else:
                                delta_html = '<span style="color:#8f9cae;">■ Estable</span>'
                                
                            with cols[col_idx]:
                                st.markdown(f"""
                                <div class="metric-card">
                                    <div class="metric-title">{g_name}</div>
                                    <div class="metric-value">{curr_ccu:,}</div>
                                    <div class="metric-delta">Flujo en vivo: {delta_html}</div>
                                </div>
                                """, unsafe_allow_html=True)
                                
                    # 2. Live chart rendering
                    with live_chart_placeholder.container():
                        st.write("")
                        st.markdown("#### Gráfica de CCU en Tiempo Real (Últimos 30 eventos)")
                        
                        # Build DataFrame for plotting
                        chart_rows = []
                        for g_name, pts in st.session_state.live_history.items():
                            for t_val, c_val in pts:
                                chart_rows.append({"Juego": g_name, "Hora": t_val, "Jugadores (CCU)": c_val})
                                
                        if chart_rows:
                            df_live_chart = pd.DataFrame(chart_rows)
                            fig_live = px.line(
                                df_live_chart,
                                x="Hora",
                                y="Jugadores (CCU)",
                                color="Juego",
                                title="CCU en vivo desde stream de Kafka",
                                markers=True,
                                color_discrete_sequence=px.colors.qualitative.Safe
                            )
                            fig_live.update_layout(
                                paper_bgcolor='rgba(0,0,0,0)',
                                plot_bgcolor='rgba(0,0,0,0)',
                                font_color='#8f9cae',
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                xaxis=dict(gridcolor='#1e2230', showgrid=True),
                                yaxis=dict(gridcolor='#1e2230', showgrid=True),
                                margin=dict(l=0, r=0, t=50, b=0)
                            )
                            st.plotly_chart(fig_live, use_container_width=True)
                            
                # Sleep briefly
                time.sleep(0.5)
                
            # After finishing iterations, rerun Streamlit to restart consumer cycle
            st.rerun()
            
        except Exception as conn_err:
            st.error(f"Error en la conexión a Kafka: {conn_err}. Asegúrate de que el contenedor de Kafka esté en línea en {bootstrap_srv}.")
        finally:
            try:
                kafka_consumer.close()
            except:
                pass
    else:
        # Show message when inactive but have history in session
        if st.session_state.last_ccu:
            st.info("Mostrando los últimos datos guardados antes de desactivar el Stream en vivo.")
            
            # Show static metric cards
            st.markdown("#### Métricas Recientes (Guardadas)")
            cols = st.columns(min(len(st.session_state.last_ccu), 4))
            for i, (g_name, curr_ccu) in enumerate(st.session_state.last_ccu.items()):
                col_idx = i % len(cols)
                with cols[col_idx]:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-title">{g_name}</div>
                        <div class="metric-value">{curr_ccu:,}</div>
                        <div class="metric-delta"><span style="color:#8f9cae;">■ Detenido</span></div>
                    </div>
                    """, unsafe_allow_html=True)
                    
            # Show static chart
            chart_rows = []
            for g_name, pts in st.session_state.live_history.items():
                for t_val, c_val in pts:
                    chart_rows.append({"Juego": g_name, "Hora": t_val, "Jugadores (CCU)": c_val})
                    
            if chart_rows:
                st.write("")
                df_live_chart = pd.DataFrame(chart_rows)
                fig_live = px.line(
                    df_live_chart,
                    x="Hora",
                    y="Jugadores (CCU)",
                    color="Juego",
                    title="CCU en vivo desde stream de Kafka",
                    markers=True,
                    color_discrete_sequence=px.colors.qualitative.Safe
                )
                fig_live.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font_color='#8f9cae',
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    xaxis=dict(gridcolor='#1e2230', showgrid=True),
                    yaxis=dict(gridcolor='#1e2230', showgrid=True)
                )
                st.plotly_chart(fig_live, use_container_width=True)
        else:
            st.info("Activa el Consumidor de Kafka en Vivo para ver datos y gráficos en tiempo real.")

# TAB 3: Database Explorer
with tab3:
    st.subheader("Explorador de Tablas de Base de Datos")
    st.markdown("Consulta en tiempo real del esquema analítico relacional en PostgreSQL.")
    
    table_option = st.selectbox(
        "Seleccionar Tabla:",
        ["dim_games", "dim_game_tags", "dim_time", "fact_players"]
    )
    
    query = f"SELECT * FROM {table_option} ORDER BY 1 DESC LIMIT 100"
    
    try:
        df_table = pd.read_sql(query, con=engine)
        if df_table.empty:
            st.warning(f"La tabla `{table_option}` está vacía.")
        else:
            st.write(f"Mostrando los últimos 100 registros de `{table_option}`:")
            st.dataframe(df_table, use_container_width=True)
    except Exception as e:
        st.error(f"Error al explorar la tabla `{table_option}`: {e}")
