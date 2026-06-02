import json
import random
import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import streamlit as st

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Steam Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Premium CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .reportview-container { background: #0f111a; }

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

    .metric-card {
        background-color: #161925;
        border-radius: 10px;
        padding: 1.5rem;
        border: 1px solid #23283d;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
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
    .metric-delta { font-size: 0.85rem; font-weight: 500; }
    .delta-up   { color: #00e676; }
    .delta-down { color: #ff1744; }

    .live-pulse {
        display: inline-block;
        width: 10px; height: 10px;
        background-color: #00e676;
        border-radius: 50%;
        margin-right: 8px;
        box-shadow: 0 0 10px #00e676;
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse {
        0%   { transform: scale(0.95); box-shadow: 0 0 0 0   rgba(0,230,118,0.7); }
        70%  { transform: scale(1);    box-shadow: 0 0 0 10px rgba(0,230,118,0);   }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0   rgba(0,230,118,0);   }
    }
</style>
""", unsafe_allow_html=True)

# ─── Config & DB ──────────────────────────────────────────────────────────────
DB_URL                = "postgresql+pg8000://admin:password@localhost:5432/steam_analytics"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC           = "steam_players"

_CHART_BG = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font_color='#8f9cae',
)
_GRID = dict(gridcolor='#1e2230', showgrid=True)


@st.cache_resource
def get_db_engine():
    return create_engine(DB_URL)

engine = get_db_engine()

# ─── Session state ────────────────────────────────────────────────────────────
for key, default in [("live_history", {}), ("last_ccu", {}), ("previous_ccu", {})]:
    if key not in st.session_state:
        st.session_state[key] = default

# ─── Data loaders ─────────────────────────────────────────────────────────────
def load_historical_data() -> pd.DataFrame:
    query = """
        SELECT
            f.fact_id, g.game_id, g.title, g.developer, g.publisher,
            t.full_date, t.hour, t.day, t.month, t.year, f.ccu
        FROM fact_players f
        JOIN dim_games g  ON f.game_id  = g.game_id
        JOIN dim_time  t  ON f.time_id  = t.time_id
        ORDER BY t.full_date DESC
    """
    try:
        df = pd.read_sql(query, con=engine)
        df['full_date'] = pd.to_datetime(df['full_date'])
        return df
    except Exception as e:
        st.error(f"Error loading historical data: {e}")
        return pd.DataFrame()


def get_genre_hierarchy(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """
    Dynamically loads all tags from dim_game_tags for the filtered games,
    and constructs a clean 3-level hierarchy:
        Level 1 (genero): Primary genre (tag_priority = 1)
        Level 2 (subgenero): Subgenres (tag_priority > 1)
        Level 3 (detalle): Game title (unique to avoid parent-child conflicts)

    This includes all tags (e.g. Action, FPS, Strategy, Shooter) without flattening them,
    making it completely stable and deterministic.
    """
    if df_filtered.empty:
        return pd.DataFrame()

    game_ids = df_filtered['game_id'].unique().tolist()
    if not game_ids:
        return pd.DataFrame()

    # Load tags dynamically from PostgreSQL
    query_tags = text("""
        SELECT game_id, tag_name, tag_priority
        FROM   dim_game_tags
        WHERE  game_id = ANY(:ids)
        ORDER  BY game_id, tag_priority ASC
    """)
    try:
        with engine.connect() as conn:
            df_tags = pd.read_sql(query_tags, con=conn, params={"ids": game_ids})
    except Exception as e:
        st.error(f"Error cargando etiquetas de base de datos: {e}")
        return pd.DataFrame()

    if df_tags.empty:
        return pd.DataFrame()

    # Map game_id to its title
    game_map = dict(zip(df_filtered['game_id'], df_filtered['title']))

    rows = []
    for gid in game_ids:
        title = game_map.get(gid, f"AppID {gid}")
        df_g = df_tags[df_tags['game_id'] == gid]
        if df_g.empty:
            continue

        # Level 1: Primary tags (priority = 1)
        primaries = df_g[df_g['tag_priority'] == 1]['tag_name'].tolist()
        if not primaries:
            primaries = [df_g.iloc[0]['tag_name']]

        # Level 2: Secondary tags (priority > 1)
        secondaries = df_g[df_g['tag_priority'] > 1]['tag_name'].tolist()
        if not secondaries:
            secondaries = primaries  # Fallback to keep hierarchy consistent

        # Build paths
        for p in primaries:
            for s in secondaries:
                # Avoid parent-child name identity
                sub_label = s if s != p else f"{s} Sub"
                rows.append({
                    'genero': p,
                    'subgenero': sub_label,
                    'detalle': title,
                    'count': 1
                })

    return pd.DataFrame(rows)



# ─── Header ───────────────────────────────────────────────────────────────────
st.markdown('<div class="main-title">Steamspy Analytics</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Plataforma de monitoreo y análisis en tiempo real de jugadores concurrentes</div>',
    unsafe_allow_html=True
)

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs([
    "Análisis Histórico",
    "Tiempo Real",
    "Explorador BD",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Historical Analysis
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Tendencias y Análisis de Datos Históricos")

    if st.button("Actualizar Datos Históricos"):
        st.cache_data.clear()

    df_hist = load_historical_data()

    if df_hist.empty:
        st.warning("No hay datos históricos registrados aún. Inicia la ingesta de datos para poblar PostgreSQL.")
    else:
        # ── KPI cards ─────────────────────────────────────────────────────────
        unique_games  = df_hist['title'].nunique()
        avg_ccu       = int(df_hist['ccu'].mean())
        peak_ccu      = int(df_hist['ccu'].max())
        total_records = len(df_hist)

        c1, c2, c3, c4 = st.columns(4)
        for col, title_label, value, delta in [
            (c1, "Juegos Monitoreados", str(unique_games),    '<span class="delta-up">Activos</span> en base'),
            (c2, "Jugadores Promedio",  f"{avg_ccu:,}",       "Histórico global"),
            (c3, "Pico de Jugadores",   f"{peak_ccu:,}",      '<span class="delta-up">Máximo</span> registrado'),
            (c4, "Registros en BD",     f"{total_records:,}", "Métricas en fact_players"),
        ]:
            with col:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">{title_label}</div>
                    <div class="metric-value">{value}</div>
                    <div class="metric-delta">{delta}</div>
                </div>""", unsafe_allow_html=True)

        st.write("")
        st.write("")

        # ── Filters ───────────────────────────────────────────────────────────
        st.markdown("### Filtros de Análisis")
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            selected_games = st.multiselect(
                "Seleccionar Juegos (deja vacío para mostrar todos):",
                options=df_hist['title'].unique(),
                default=[]
            )
        with col_f2:
            date_range = st.date_input(
                "Rango de Fechas:",
                value=(df_hist['full_date'].min().date(), df_hist['full_date'].max().date())
            )

        # Apply game filter: empty selection means all games
        filtered_df = df_hist[df_hist['title'].isin(selected_games)] if selected_games else df_hist.copy()

        # Apply date filter
        if not filtered_df.empty and len(date_range) == 2:
            s, e = date_range
            filtered_df = filtered_df[
                (filtered_df['full_date'].dt.date >= s) &
                (filtered_df['full_date'].dt.date <= e)
            ]

        if filtered_df.empty:
            st.warning("No hay datos para los filtros seleccionados.")
        else:
            # ── Chart 2: Real Top-10 — queried from fact_players globally ──────
            # Independent of the game-selection filter so it always shows the
            # actual 10 highest-CCU games tracked by the streaming consumer.
            st.markdown("### Top 10 Juegos con Más Jugadores (Streaming)")
            try:
                df_top10 = pd.read_sql("""
                    SELECT g.title, AVG(f.ccu) AS avg_ccu, MAX(f.ccu) AS peak_ccu
                    FROM   fact_players f
                    JOIN   dim_games g ON g.game_id = f.game_id
                    GROUP  BY g.title
                    ORDER  BY avg_ccu DESC
                    LIMIT  10
                """, con=engine)
            except Exception as e:
                df_top10 = pd.DataFrame()
                st.error(f"Error cargando Top 10: {e}")

            if not df_top10.empty:
                fig_bar = px.bar(
                    df_top10, x='avg_ccu', y='title', orientation='h', color='title',
                    labels={"avg_ccu": "Promedio CCU", "title": "Juego"},
                    color_discrete_sequence=px.colors.qualitative.Plotly,
                    height=max(300, len(df_top10) * 38),
                    custom_data=['peak_ccu'],
                )
                fig_bar.update_traces(
                    hovertemplate="<b>%{y}</b><br>Promedio CCU: %{x:,.0f}<br>Pico CCU: %{customdata[0]:,.0f}<extra></extra>"
                )
                fig_bar.update_layout(
                    **_CHART_BG,
                    showlegend=False,
                    xaxis=_GRID,
                    yaxis=dict(gridcolor='rgba(0,0,0,0)', categoryorder='total ascending'),
                    margin=dict(l=0, r=0, t=20, b=0),
                )
                st.plotly_chart(fig_bar, use_container_width=True)

            st.markdown("---")

            # ── Chart 3: Genre and Subgenre distribution ──────────────────────
            st.markdown("### Participación y Distribución de Géneros")

            df_hier = get_genre_hierarchy(filtered_df)

            if df_hier.empty:
                st.info("No hay datos de géneros/etiquetas para el conjunto de filtros actual.")
            else:
                col_ctrl_viz, col_viz_title = st.columns([2, 3])
                with col_ctrl_viz:
                    viz_mode = st.radio(
                        "Seleccionar tipo de gráfico:",
                        ["Pastel (Simple - Agrupa <1%)", "Sunburst (Jerárquico - 3 Niveles)"],
                        horizontal=True
                    )

                if viz_mode == "Pastel (Simple - Agrupa <1%)":
                    # Group by genero
                    df_pie = df_hier.groupby('genero')['count'].sum().reset_index()
                    total_c = df_pie['count'].sum()
                    df_pie['pct'] = df_pie['count'] / total_c

                    # Group anything < 1% into "Otros"
                    df_pie.loc[df_pie['pct'] < 0.01, 'genero'] = 'Otros'
                    df_pie = df_pie.groupby('genero')['count'].sum().reset_index()

                    fig_pie = px.pie(
                        df_pie,
                        names='genero',
                        values='count',
                        color='genero',
                        color_discrete_sequence=px.colors.qualitative.Bold,
                        title="Distribución de Géneros Principales",
                        hole=0.4
                    )
                    fig_pie.update_layout(
                        **_CHART_BG,
                        margin=dict(l=0, r=0, t=40, b=0),
                        height=550,
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)

                else:
                    # Sunburst with breadcrumb and navigation panel
                    if 'sun_genre'    not in st.session_state: st.session_state.sun_genre    = None
                    if 'sun_subgenero' not in st.session_state: st.session_state.sun_subgenero = None

                    # Navigation panel | Chart panel
                    col_nav, col_sun = st.columns([1, 4])

                    with col_nav:
                        st.markdown("**Navegación**")
                        # Placeholder for breadcrumb so it renders at top
                        bread_ph = st.empty()
                        st.write("")

                        # Genre selector using explicit index to avoid key collision
                        genres_available = sorted(df_hier['genero'].unique().tolist())
                        options_genre = ["(Todos)"] + genres_available
                        idx_genre = options_genre.index(st.session_state.sun_genre) if st.session_state.sun_genre in options_genre else 0

                        genre_sel = st.selectbox(
                            "Filtrar por género:",
                            options_genre,
                            index=idx_genre
                        )

                        if genre_sel != "(Todos)":
                            if st.session_state.sun_genre != genre_sel:
                                st.session_state.sun_genre = genre_sel
                                st.session_state.sun_subgenero = None
                                st.rerun()
                                
                            # Subgenre selector
                            subs = sorted(df_hier[df_hier['genero'] == genre_sel]['subgenero'].unique().tolist())
                            options_sub = ["(Todos)"] + subs
                            idx_sub = options_sub.index(st.session_state.sun_subgenero) if st.session_state.sun_subgenero in options_sub else 0
                            
                            sub_sel = st.selectbox(
                                "Filtrar por subgénero:",
                                options_sub,
                                index=idx_sub
                            )
                            if sub_sel != "(Todos)":
                                if st.session_state.sun_subgenero != sub_sel:
                                    st.session_state.sun_subgenero = sub_sel
                                    st.rerun()
                            else:
                                if st.session_state.sun_subgenero is not None:
                                    st.session_state.sun_subgenero = None
                                    st.rerun()
                        else:
                            if st.session_state.sun_genre is not None:
                                st.session_state.sun_genre = None
                                st.session_state.sun_subgenero = None
                                st.rerun()

                        st.write("")
                        if st.button("Subir un nivel", use_container_width=True):
                            if st.session_state.sun_subgenero:
                                st.session_state.sun_subgenero = None
                                st.rerun()
                            elif st.session_state.sun_genre:
                                st.session_state.sun_genre = None
                                st.rerun()

                        if st.button("Resetear vista", use_container_width=True):
                            st.session_state.sun_genre     = None
                            st.session_state.sun_subgenero = None
                            st.rerun()

                    # Now compute breadcrumb after all state is updated
                    breadcrumb = "Todos los géneros"
                    if st.session_state.sun_genre:
                        breadcrumb = st.session_state.sun_genre
                    if st.session_state.sun_subgenero:
                        breadcrumb = f"{st.session_state.sun_genre} / {st.session_state.sun_subgenero}"
                        
                    bread_ph.caption(f"Vista: **{breadcrumb}**")

                    # Filter hierarchy dataframe based on nav state
                    df_sun = df_hier.copy()
                    if st.session_state.sun_genre:
                        df_sun = df_sun[df_sun['genero'] == st.session_state.sun_genre]
                    if st.session_state.sun_subgenero:
                        df_sun = df_sun[df_sun['subgenero'] == st.session_state.sun_subgenero]

                    # Determine dynamic path to show levels incrementally (prevents hair-thin cluttered rings)
                    if not st.session_state.sun_genre:
                        current_path = ['genero']
                    elif not st.session_state.sun_subgenero:
                        current_path = ['genero', 'subgenero']
                    else:
                        current_path = ['genero', 'subgenero', 'detalle']

                    with col_sun:
                        fig_sun = px.sunburst(
                            df_sun,
                            path=current_path,
                            values='count',
                            color='genero',
                            color_discrete_sequence=px.colors.qualitative.Bold,
                            title=f"Distribución Jerárquica: {breadcrumb}"
                        )
                        fig_sun.update_layout(
                            **_CHART_BG,
                            margin=dict(l=0, r=0, t=40, b=0),
                            height=640,
                            clickmode='event+select',
                        )
                        fig_sun.update_traces(
                            textinfo="label+percent parent",
                            insidetextorientation='radial',
                        )
                        st.plotly_chart(fig_sun, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Real-Time Kafka Stream
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Monitoreo en Tiempo Real (Kafka Stream)")

    col_ctrl, col_desc = st.columns([1, 3])
    with col_ctrl:
        live_active   = st.checkbox("Activar Consumidor Kafka en Vivo", value=False)
        st.write("")
        st.markdown("**Configuración:**")
        bootstrap_srv = st.text_input("Kafka Broker", KAFKA_BOOTSTRAP_SERVERS)
        topic_name    = st.text_input("Kafka Topic", KAFKA_TOPIC)

    with col_desc:
        st.markdown(f"""
        ### Canal en Vivo
        Cuando el interruptor de la izquierda está **activo**, Streamlit se conectará al Broker
        de Kafka en `{bootstrap_srv}` y escuchará eventos en el tópico `{topic_name}`.

        - Se graficarán los datos conforme lleguen en tiempo real.
        - Las tarjetas de métricas se actualizarán instantáneamente.
        """)
        if live_active:
            st.markdown(
                '<div style="display:flex;align-items:center;">'
                '<span class="live-pulse"></span>'
                '<span style="color:#00e676;font-weight:bold;">Escuchando eventos en tiempo real de Kafka...</span>'
                '</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown('<div style="color:#ff1744;font-weight:bold;">Consumidor inactivo.</div>', unsafe_allow_html=True)

    st.write("")

    live_metrics_ph = st.empty()
    live_chart_ph   = st.empty()

    if live_active:
        from confluent_kafka import Consumer

        conf = {
            'bootstrap.servers': bootstrap_srv,
            'group.id':          f"streamlit-live-{random.randint(1000, 9999)}",
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
        }
        try:
            kafka_consumer = Consumer(conf)
            kafka_consumer.subscribe([topic_name])

            for _ in range(60):
                msg = kafka_consumer.poll(0.5)
                if msg is not None and not msg.error():
                    try:
                        event       = json.loads(msg.value().decode('utf-8'))
                        game_id     = event["game_id"]
                        ccu         = event["ccu"]
                        time_str    = datetime.fromisoformat(event["timestamp"]).strftime("%H:%M:%S")

                        with engine.connect() as conn:
                            res = conn.execute(
                                text("SELECT title FROM dim_games WHERE game_id = :gid"),
                                {"gid": game_id}
                            ).fetchone()
                            game_name = res[0] if res else f"AppID {game_id}"

                        if game_name not in st.session_state.live_history:
                            st.session_state.live_history[game_name] = []

                        st.session_state.previous_ccu[game_name] = st.session_state.last_ccu.get(game_name, ccu)
                        st.session_state.last_ccu[game_name] = ccu

                        history = st.session_state.live_history[game_name]
                        history.append((time_str, ccu))
                        if len(history) > 30:
                            history.pop(0)

                    except Exception as parse_err:
                        print(f"Kafka parse error: {parse_err}")

                if st.session_state.last_ccu:
                    with live_metrics_ph.container():
                        st.markdown("#### Métricas Recientes")
                        cols = st.columns(min(len(st.session_state.last_ccu), 4))
                        for i, (g_name, curr_ccu) in enumerate(st.session_state.last_ccu.items()):
                            prev  = st.session_state.previous_ccu.get(g_name, curr_ccu)
                            diff  = curr_ccu - prev
                            arrow = (f'<span class="delta-up">▲ +{diff:,}</span>' if diff > 0
                                     else f'<span class="delta-down">▼ {diff:,}</span>' if diff < 0
                                     else '<span style="color:#8f9cae;">■ Estable</span>')
                            with cols[i % len(cols)]:
                                st.markdown(f"""
                                <div class="metric-card">
                                    <div class="metric-title">{g_name}</div>
                                    <div class="metric-value">{curr_ccu:,}</div>
                                    <div class="metric-delta">Flujo en vivo: {arrow}</div>
                                </div>""", unsafe_allow_html=True)

                    with live_chart_ph.container():
                        st.markdown("#### CCU en Tiempo Real (Últimos 30 eventos)")
                        chart_rows = [
                            {"Juego": g, "Hora": t, "Jugadores (CCU)": c}
                            for g, pts in st.session_state.live_history.items()
                            for t, c in pts
                        ]
                        if chart_rows:
                            fig_live = px.line(
                                pd.DataFrame(chart_rows),
                                x="Hora", y="Jugadores (CCU)", color="Juego",
                                title="CCU en vivo — Kafka stream",
                                markers=True,
                                color_discrete_sequence=px.colors.qualitative.Safe,
                            )
                            fig_live.update_layout(
                                **_CHART_BG,
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                xaxis=_GRID, yaxis=_GRID,
                                margin=dict(l=0, r=0, t=50, b=0),
                            )
                            st.plotly_chart(fig_live, use_container_width=True)

                time.sleep(0.5)

            st.rerun()

        except Exception as conn_err:
            st.error(f"Error de conexión a Kafka: {conn_err}")
        finally:
            try:
                kafka_consumer.close()
            except Exception:
                pass

    else:
        if st.session_state.last_ccu:
            st.info("Mostrando los últimos datos guardados antes de desactivar el Stream en vivo.")
            cols = st.columns(min(len(st.session_state.last_ccu), 4))
            for i, (g_name, curr_ccu) in enumerate(st.session_state.last_ccu.items()):
                with cols[i % len(cols)]:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-title">{g_name}</div>
                        <div class="metric-value">{curr_ccu:,}</div>
                        <div class="metric-delta"><span style="color:#8f9cae;">■ Detenido</span></div>
                    </div>""", unsafe_allow_html=True)

            chart_rows = [
                {"Juego": g, "Hora": t, "Jugadores (CCU)": c}
                for g, pts in st.session_state.live_history.items()
                for t, c in pts
            ]
            if chart_rows:
                fig_live = px.line(
                    pd.DataFrame(chart_rows),
                    x="Hora", y="Jugadores (CCU)", color="Juego",
                    markers=True,
                    color_discrete_sequence=px.colors.qualitative.Safe,
                )
                fig_live.update_layout(
                    **_CHART_BG,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    xaxis=_GRID, yaxis=_GRID,
                )
                st.plotly_chart(fig_live, use_container_width=True)
        else:
            st.info("Activa el Consumidor de Kafka en Vivo para ver datos y gráficos en tiempo real.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Database Explorer
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Explorador de Tablas de Base de Datos")
    st.markdown("Consulta en tiempo real del esquema analítico relacional en PostgreSQL.")

    table_option = st.selectbox(
        "Seleccionar Tabla:",
        ["dim_games", "dim_game_tags", "dim_time", "fact_players"],
    )

    try:
        df_table = pd.read_sql(
            f"SELECT * FROM {table_option} ORDER BY 1 DESC",
            con=engine,
        )
        if df_table.empty:
            st.warning(f"La tabla `{table_option}` está vacía.")
        else:
            st.write(f"Mostrando los registros de `{table_option}`:")
            st.dataframe(df_table, use_container_width=True)
    except Exception as e:
        st.error(f"Error al explorar la tabla `{table_option}`: {e}")
