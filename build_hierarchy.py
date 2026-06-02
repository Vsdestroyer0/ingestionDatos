#!/usr/bin/env python3
"""
build_hierarchy.py
──────────────────
Pre-processing batch script that materialises the 3-level genre hierarchy
from dim_game_tags into a dedicated table: dim_game_hierarchy.

Run once after every batch ingestion (ingest_batch.py) to refresh the table:

    python build_hierarchy.py

The dashboard then reads dim_game_hierarchy with a simple JOIN — no
Python-level looping, fully deterministic per game_id.

Schema created:
    dim_game_hierarchy (
        game_id   INT PRIMARY KEY REFERENCES dim_games(game_id),
        genero    VARCHAR(100),   -- tag_priority = 1 (lowest number)
        subgenero VARCHAR(100),   -- tag_priority = 2
        detalle   VARCHAR(100)    -- tag_priority = 3, or game title as fallback
    )
"""

import logging
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hierarchy.builder")

# ─── Config ───────────────────────────────────────────────────────────────────
DB_URL = os.getenv(
    "DB_URL",
    "postgresql+pg8000://admin:password@localhost:5432/steam_analytics",
)


def create_hierarchy_table() -> int:
    """
    Read dim_game_tags, pivot the first 3 priority levels into columns,
    and write the result to dim_game_hierarchy (overwrite if exists).

    Returns the number of rows written.
    """
    log.info("Connecting to database…")
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))   # quick connectivity check
        log.info("Connection OK.")
    except Exception as exc:
        log.error("Cannot connect to the database: %s", exc)
        sys.exit(1)

    # ── 1. Load tags + game titles ─────────────────────────────────────────────
    log.info("Loading dim_game_tags and dim_games…")
    try:
        df_tags = pd.read_sql(
            """
            SELECT t.game_id, g.title, t.tag_name, t.tag_priority
            FROM   dim_game_tags t
            JOIN   dim_games     g ON g.game_id = t.game_id
            ORDER  BY t.game_id, t.tag_priority ASC
            """,
            con=engine,
        )
    except Exception as exc:
        log.error("Failed to load tags: %s", exc)
        sys.exit(1)

    if df_tags.empty:
        log.warning("dim_game_tags is empty — nothing to build.")
        return 0

    log.info("  Loaded %d tag rows for %d games.", len(df_tags), df_tags['game_id'].nunique())

    # ── 2. Pivot: keep only the first 3 priority levels per game ──────────────
    # For each game, rank tags by priority (ascending) and take positions 1, 2, 3.
    df_tags['rank'] = (
        df_tags.groupby('game_id')['tag_priority']
        .rank(method='first', ascending=True)
        .astype(int)
    )

    def _pivot_game(grp: pd.DataFrame) -> pd.Series:
        title = grp['title'].iloc[0]
        tags  = grp.sort_values('rank')['tag_name'].tolist()

        l1 = tags[0] if len(tags) > 0 else "Sin Género"
        l2 = tags[1] if len(tags) > 1 else l1
        l3 = tags[2] if len(tags) > 2 else title   # fallback: game title

        return pd.Series({'genero': l1, 'subgenero': l2, 'detalle': l3})

    log.info("Building 3-level hierarchy…")
    df_hier = (
        df_tags
        .groupby('game_id', group_keys=True)
        .apply(_pivot_game)
        .reset_index()
    )

    log.info("  Hierarchy built: %d games × 3 levels.", len(df_hier))

    # ── 3. Write to dim_game_hierarchy (replace) ───────────────────────────────
    log.info("Writing to dim_game_hierarchy (replace)…")
    try:
        with engine.begin() as conn:
            # Drop & recreate so we own the schema precisely
            conn.execute(text("DROP TABLE IF EXISTS dim_game_hierarchy"))
            conn.execute(text("""
                CREATE TABLE dim_game_hierarchy (
                    game_id   INT PRIMARY KEY REFERENCES dim_games(game_id),
                    genero    VARCHAR(150),
                    subgenero VARCHAR(150),
                    detalle   VARCHAR(255)
                )
            """))

        # Insert using pandas (fast path)
        df_hier.to_sql(
            "dim_game_hierarchy",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        n = len(df_hier)
        log.info("  Done — %d rows written to dim_game_hierarchy.", n)
        return n

    except Exception as exc:
        log.error("Failed to write dim_game_hierarchy: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    rows_written = create_hierarchy_table()
    log.info("=== Hierarchy table ready (%d games). ===", rows_written)
