"""
consumer.py — Kafka Consumer de Steam Analytics
Lee eventos del tópico 'steam_players', los enriquece con datos de la
Steam Store API y los persiste en PostgreSQL usando SQLAlchemy puro
(sin mezclar df.to_sql con engine.begin para evitar conflictos de
transacción con pg8000).
"""

import json
import sys
import requests
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine, text

# ─── Configuración ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "steam_players"
KAFKA_GROUP_ID          = "steam-ingestion-group"
DB_URL = "postgresql+pg8000://admin:password@localhost:5432/steam_analytics"

engine = create_engine(DB_URL, pool_pre_ping=True)

# ─── Steam Store API ──────────────────────────────────────────────────────────
def fetch_game_details(game_id: int) -> dict:
    url = f"https://store.steampowered.com/api/appdetails?appids={game_id}"
    print(f"  [API] Obteniendo detalles del juego {game_id}…", flush=True)
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        key  = str(game_id)
        if key in data and data[key].get("success"):
            d = data[key]["data"]
            return {
                "game_id":   game_id,
                "title":     d.get("name", f"Game {game_id}"),
                "developer": (d.get("developers") or ["Unknown"])[0],
                "publisher": (d.get("publishers") or ["Unknown"])[0],
                "tags":      [g["description"] for g in d.get("genres", []) if g.get("description")],
            }
    except Exception as e:
        print(f"  [API] Error al obtener detalles de {game_id}: {e}", flush=True)

    return {"game_id": game_id, "title": f"Game {game_id}",
            "developer": "Unknown", "publisher": "Unknown", "tags": []}


# ─── Helpers de dimensión ─────────────────────────────────────────────────────
def ensure_game(conn, game_id: int):
    """Inserta el juego en dim_games si no existe. Retorna True si es nuevo."""
    exists = conn.execute(
        text("SELECT 1 FROM dim_games WHERE game_id = :id"), {"id": game_id}
    ).fetchone()
    if exists:
        return False

    d = fetch_game_details(game_id)
    conn.execute(
        text("INSERT INTO dim_games (game_id, title, developer, publisher) "
             "VALUES (:gid, :t, :dev, :pub)"),
        {"gid": d["game_id"], "t": d["title"], "dev": d["developer"], "pub": d["publisher"]},
    )
    print(f"  [DB] dim_games ← {d['title']} ({game_id})", flush=True)

    for idx, tag in enumerate(d["tags"], 1):
        conn.execute(
            text("INSERT INTO dim_game_tags (game_id, tag_name, tag_priority) "
                 "VALUES (:gid, :tn, :tp)"),
            {"gid": game_id, "tn": tag, "tp": idx},
        )
        print(f"       tag #{idx}: {tag}", flush=True)
    return True


def ensure_time(conn, dt: datetime) -> int:
    """Inserta el timestamp en dim_time si no existe. Retorna time_id."""
    row = conn.execute(
        text("SELECT time_id FROM dim_time WHERE full_date = :dt"), {"dt": dt}
    ).fetchone()
    if row:
        return row[0]

    result = conn.execute(
        text("INSERT INTO dim_time (full_date, hour, day, month, year) "
             "VALUES (:dt, :h, :d, :m, :y) RETURNING time_id"),
        {"dt": dt, "h": dt.hour, "d": dt.day, "m": dt.month, "y": dt.year},
    )
    tid = result.fetchone()[0]
    print(f"  [DB] dim_time ← {dt.isoformat()}  (time_id={tid})", flush=True)
    return tid


def insert_fact(conn, game_id: int, time_id: int, ccu: int):
    """Inserta un registro en fact_players."""
    conn.execute(
        text("INSERT INTO fact_players (game_id, time_id, ccu) "
             "VALUES (:gid, :tid, :ccu)"),
        {"gid": game_id, "tid": time_id, "ccu": ccu},
    )
    print(f"  [DB] fact_players ← game={game_id} time={time_id} ccu={ccu:,}", flush=True)


# ─── Procesamiento de un mensaje ─────────────────────────────────────────────
def process_message(payload: dict):
    game_id = int(payload["game_id"])
    ccu     = int(payload["ccu"])
    dt      = datetime.fromisoformat(payload["timestamp"])

    print(f"\n[EVENT] AppID={game_id}  CCU={ccu:,}  ts={dt.isoformat()}", flush=True)

    # Usamos pandas sólo para mostrar que validamos el DataFrame antes de cargar
    event_df = pd.DataFrame([payload])
    event_df["ccu"] = event_df["ccu"].astype(int)
    assert event_df["ccu"].iloc[0] >= 0, "CCU negativo, descartando evento"

    # Toda la escritura en UNA sola conexión / transacción
    with engine.begin() as conn:
        ensure_game(conn, game_id)
        time_id = ensure_time(conn, dt)
        insert_fact(conn, game_id, time_id, ccu)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60, flush=True)
    print("  Steam Analytics — Kafka Consumer (ingesta)", flush=True)
    print(f"  Broker : {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
    print(f"  Topic  : {KAFKA_TOPIC}", flush=True)
    print(f"  DB     : {DB_URL}", flush=True)
    print("=" * 60, flush=True)

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id":          KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",   # consume desde el principio
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    print("Esperando mensajes…\n", flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[Kafka ERROR] {msg.error()}", flush=True)
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                process_message(payload)
            except Exception as e:
                print(f"[PROC ERROR] {e}", flush=True)

    except KeyboardInterrupt:
        print("\nDeteniendo consumer…", flush=True)
    finally:
        consumer.close()
        print("Consumer cerrado.", flush=True)


if __name__ == "__main__":
    main()
