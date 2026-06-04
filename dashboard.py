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

    # Load global tag frequencies from PostgreSQL
    query_freq = text("""
        SELECT tag_name, COUNT(*) as freq
        FROM   dim_game_tags
        GROUP  BY tag_name
    """)
    try:
        with engine.connect() as conn:
            df_freq = pd.read_sql(query_freq, con=conn)
            tag_freqs = dict(zip(df_freq['tag_name'], df_freq['freq']))
    except Exception:
        tag_freqs = {}

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
                count_val = tag_freqs.get(s, 1)
                rows.append({
                    'genero': p,
                    'subgenero': sub_label,
                    'detalle': title,
                    'count': count_val
                })

    return pd.DataFrame(rows)


def get_genre_hierarchy_json(file_path: str, df_filtered: pd.DataFrame) -> pd.DataFrame:
    """
    Dynamically loads tags from a daily JSON snapshot (in data/batch/DD-MM-YY.json)
    and constructs a clean 3-level hierarchy identical to the database hierarchy:
        Level 1 (genero): Primary genre (first tag in the JSON list)
        Level 2 (subgenero): Subgenres (remaining tags)
        Level 3 (detalle): Game title
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        st.error(f"Error cargando archivo histórico JSON: {e}")
        return pd.DataFrame()

    # Pre-calculate global tag frequencies from this JSON snapshot
    tag_freqs = {}
    for item in data:
        for t in item.get('tags', []):
            tag_freqs[t] = tag_freqs.get(t, 0) + 1

    # Map game titles and ids from the filtered dataset
    game_ids = set(df_filtered['game_id'].unique().tolist()) if not df_filtered.empty else set()

    rows = []
    for item in data:
        gid = item.get('game_id')
        # If filtering is active, only include games in the current selection
        if game_ids and gid not in game_ids:
            continue

        title = item.get('title', f"AppID {gid}")
        tags = item.get('tags', [])
        if not tags:
            continue

        # Level 1: Primary tags (first tag)
        primaries = [tags[0]]

        # Level 2: Secondary tags (all subsequent tags)
        secondaries = tags[1:]
        if not secondaries:
            secondaries = primaries  # Fallback

        for p in primaries:
            for s in secondaries:
                sub_label = s if s != p else f"{s} Sub"
                count_val = tag_freqs.get(s, 1)
                rows.append({
                    'genero': p,
                    'subgenero': sub_label,
                    'detalle': title,
                    'count': count_val
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
            date_range = st.date_input(
                "Rango de Fechas:",
                value=(df_hist['full_date'].min().date(), df_hist['full_date'].max().date())
            )
        with col_f2:
            import os
            from datetime import datetime
            os.makedirs("data/batch", exist_ok=True)
            all_files = [f for f in os.listdir("data/batch") if f.endswith(".json")]
            
            # Parse files to date objects
            valid_files = []
            for f in all_files:
                try:
                    date_part = f.replace(".json", "")
                    file_date = datetime.strptime(date_part, "%d-%m-%y").date()
                    valid_files.append((f, file_date))
                except ValueError:
                    continue
            
            # Filter by selected date range
            if len(date_range) == 2:
                s, e = date_range
                valid_files = [vf for vf in valid_files if s <= vf[1] <= e]
            
            # Sort descending by date
            valid_files = sorted(valid_files, key=lambda x: x[1], reverse=True)
            json_files = [vf[0] for vf in valid_files]
            
            options_source = ["Base de Datos (Actual)"] + [f.replace(".json", "") for f in json_files]
            selected_source = st.selectbox(
                "Origen de Géneros (Batch):",
                options_source
            )

        # Apply date filter directly on df_hist
        filtered_df = df_hist.copy()
        if not filtered_df.empty and len(date_range) == 2:
            s, e = date_range
            filtered_df = filtered_df[
                (filtered_df['full_date'].dt.date >= s) &
                (filtered_df['full_date'].dt.date <= e)
            ]

        if filtered_df.empty:
            st.warning("No hay datos para los filtros seleccionados.")
        else:
            # ── Chart 3: Genre and Subgenre distribution ──────────────────────
            st.markdown("### Participación y Distribución de Géneros")

            if selected_source == "Base de Datos (Actual)":
                df_hier = get_genre_hierarchy(filtered_df)
            else:
                file_path = f"data/batch/{selected_source}.json"
                df_hier = get_genre_hierarchy_json(file_path, filtered_df)

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
with tab2:
    st.subheader("Panel de Control de Ingestión y Monitoreo (Kafka)")

    # Initialize background processes if not exist
    if 'producer_proc' not in st.session_state:
        st.session_state.producer_proc = None
    if 'consumer_proc' not in st.session_state:
        st.session_state.consumer_proc = None

    # Check status
    prod_active = st.session_state.producer_proc is not None and st.session_state.producer_proc.poll() is None
    cons_active = st.session_state.consumer_proc is not None and st.session_state.consumer_proc.poll() is None

    st.markdown("### Control del Pipeline de Streaming")
    col_p1, col_p2 = st.columns(2)
    
    with col_p1:
        st.info("Productor: Obtiene CCU en tiempo real de Steam API y publica en Kafka.")
        prod_toggle = st.toggle("Activar Productor", value=prod_active, key="prod_toggle")
        
        if prod_toggle and not prod_active:
            import sys
            import subprocess
            try:
                st.session_state.producer_proc = subprocess.Popen([sys.executable, "ingesta/ingest_stream.py"])
                st.rerun()
            except Exception as e:
                st.error(f"Error al iniciar productor: {e}")
        elif not prod_toggle and prod_active:
            st.session_state.producer_proc.terminate()
            st.session_state.producer_proc = None
            st.rerun()
            
    with col_p2:
        st.info("Consumidor: Lee eventos de Kafka y los persiste en PostgreSQL (fact_players).")
        cons_toggle = st.toggle("Activar Consumidor", value=cons_active, key="cons_toggle")
        
        if cons_toggle and not cons_active:
            import sys
            import subprocess
            try:
                st.session_state.consumer_proc = subprocess.Popen([sys.executable, "consumer.py"])
                st.rerun()
            except Exception as e:
                st.error(f"Error al iniciar consumidor: {e}")
        elif not cons_toggle and cons_active:
            st.session_state.consumer_proc.terminate()
            st.session_state.consumer_proc = None
            st.rerun()

    st.markdown("---")
    
    @st.fragment(run_every="5s")
    def render_streaming_chart():
        st.markdown("### Top 15 Juegos con Más Jugadores (Streaming en Vivo)")
        try:
            df_top15 = pd.read_sql("""
                WITH RankedPlayers AS (
                    SELECT 
                        g.title, 
                        f.ccu,
                        MAX(f.ccu) OVER (PARTITION BY f.game_id) as peak_ccu,
                        ROW_NUMBER() OVER (PARTITION BY f.game_id ORDER BY f.time_id DESC) as rn
                    FROM fact_players f
                    JOIN dim_games g ON g.game_id = f.game_id
                )
                SELECT title, ccu AS current_ccu, peak_ccu
                FROM RankedPlayers
                WHERE rn = 1
                ORDER BY current_ccu DESC
                LIMIT 15
            """, con=engine)
        except Exception as e:
            df_top15 = pd.DataFrame()
            st.error(f"Error cargando Top 15: {e}")

        if not df_top15.empty:
            fig_bar = px.bar(
                df_top15, x='current_ccu', y='title', orientation='h', color='title',
                labels={"current_ccu": "Jugadores Actuales", "title": "Juego"},
                color_discrete_sequence=px.colors.qualitative.Plotly,
                height=max(350, len(df_top15) * 38),
                custom_data=['peak_ccu'],
            )
            fig_bar.update_traces(
                hovertemplate="<b>%{y}</b><br>Actual: %{x:,.0f}<br>Pico Histórico: %{customdata[0]:,.0f}<extra></extra>"
            )
            fig_bar.update_layout(
                **_CHART_BG,
                showlegend=False,
                xaxis=_GRID,
                yaxis=dict(gridcolor='rgba(0,0,0,0)', categoryorder='total ascending'),
                margin=dict(l=0, r=0, t=20, b=0),
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No hay datos de streaming guardados en `fact_players` todavía. Inicia el Productor y Consumidor para registrar datos.")

    render_streaming_chart()



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
