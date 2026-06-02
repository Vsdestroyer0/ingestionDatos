"""
consumer.py — Steam Analytics | Kafka Consumer (Streaming → PostgreSQL)
=======================================================================
Consume mensajes del tópico 'steam_players', los deserializa y los persiste
en PostgreSQL siguiendo el esquema dimensional:

    dim_time     ← upsert idempotente por full_date
    fact_players ← insert por event (game_id, time_id, ccu)

Características:
- Logging estructurado (no print)
- Manejo robusto de mensajes corruptos / JSON inválido
- Idempotencia en dim_time via ON CONFLICT DO NOTHING + SELECT
- Commit manual de offsets sólo al confirmar la inserción en BD
- Reconexión automática a BD ante caídas transitorias
- Servicio de larga duración con while True

Uso:
    python consumer.py

Variables de entorno opcionales:
    KAFKA_BOOTSTRAP  → por defecto "localhost:9092"
    KAFKA_TOPIC      → por defecto "steam_players"
    KAFKA_GROUP      → por defecto "steam-ingestion-group"
    DB_URL           → por defecto "postgresql+pg8000://admin:password@localhost:5432/steam_analytics"
"""

import json
import logging
import os
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("steam.consumer")

# ─── Configuración ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "steam_players")
KAFKA_GROUP     = os.getenv("KAFKA_GROUP", "steam-ingestion-group")
DB_URL          = os.getenv(
    "DB_URL",
    "postgresql+pg8000://admin:password@localhost:5432/steam_analytics",
)

# ─── Motor de BD (pool de conexiones) ─────────────────────────────────────────
engine = create_engine(DB_URL, pool_pre_ping=True, pool_size=2, max_overflow=2)


# ─── Operaciones de base de datos ─────────────────────────────────────────────

def get_or_create_time_id(conn, dt: datetime) -> int:
    """
    Inserta el slot de tiempo en dim_time si no existe (idempotente via
    ON CONFLICT DO NOTHING) y devuelve el time_id correspondiente.

    La unicidad está garantizada por el índice UNIQUE en full_date.
    """
    # 1. Intentar insertar (ignorar conflicto si ya existe)
    conn.execute(
        text("""
            INSERT INTO dim_time (full_date, hour, day, month, year)
            VALUES (:dt, :h, :d, :m, :y)
            ON CONFLICT (full_date) DO NOTHING
        """),
        {"dt": dt, "h": dt.hour, "d": dt.day, "m": dt.month, "y": dt.year},
    )
    # 2. Obtener el time_id (ya sea recién insertado o preexistente)
    row = conn.execute(
        text("SELECT time_id FROM dim_time WHERE full_date = :dt"),
        {"dt": dt},
    ).fetchone()

    if row is None:
        raise RuntimeError(f"No se encontró time_id para full_date={dt!r}")

    return row[0]


def insert_fact(conn, game_id: int, time_id: int, ccu: int) -> None:
    """Inserta un registro de CCU en fact_players."""
    conn.execute(
        text("""
            INSERT INTO fact_players (game_id, time_id, ccu)
            VALUES (:gid, :tid, :ccu)
        """),
        {"gid": game_id, "tid": time_id, "ccu": ccu},
    )


def process_event(payload: dict) -> None:
    """
    Persiste un evento en PostgreSQL dentro de una única transacción.
    Levanta excepción si algo falla (el caller la captura y no hace commit
    del offset de Kafka).
    """
    game_id = int(payload["game_id"])
    ccu     = int(payload["ccu"])
    dt      = datetime.fromisoformat(payload["timestamp"])

    with engine.begin() as conn:                    # transacción atómica
        time_id = get_or_create_time_id(conn, dt)
        insert_fact(conn, game_id, time_id, ccu)

    log.info(
        "✓ game_id=%-10d  ccu=%8s  time_id=%d  ts=%s",
        game_id, f"{ccu:,}", time_id, dt.isoformat(),
    )


# ─── Validación del mensaje ────────────────────────────────────────────────────
REQUIRED_KEYS = {"game_id", "timestamp", "ccu"}

def parse_message(raw_value: bytes) -> dict | None:
    """
    Deserializa el JSON y valida los campos requeridos.
    Retorna None si el mensaje es inválido (no se procesa ni commitea).
    """
    try:
        payload = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        log.error("Mensaje corrupto (no es JSON válido): %s | raw=%r", exc, raw_value[:120])
        return None

    missing = REQUIRED_KEYS - payload.keys()
    if missing:
        log.error("Mensaje incompleto — faltan campos: %s | payload=%r", missing, payload)
        return None

    # Validar tipos básicos
    try:
        int(payload["game_id"])
        int(payload["ccu"])
        datetime.fromisoformat(str(payload["timestamp"]))
    except (ValueError, TypeError) as exc:
        log.error("Tipos de datos inválidos en payload: %s | payload=%r", exc, payload)
        return None

    return payload


# ─── Núcleo del consumidor ────────────────────────────────────────────────────
def run_consumer() -> None:
    log.info("=== Steam Consumer iniciado ===")
    log.info("Broker: %s | Topic: %s | Group: %s", KAFKA_BOOTSTRAP, KAFKA_TOPIC, KAFKA_GROUP)
    log.info("DB: %s", DB_URL)

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":           KAFKA_GROUP,
        "auto.offset.reset":  "earliest",
        # Commit manual para garantizar at-least-once con procesamiento confirmado
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Suscrito a tópico '%s'. Esperando mensajes…\n", KAFKA_TOPIC)

    processed = 0
    skipped   = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            # Sin mensajes en este poll
            if msg is None:
                continue

            # Error de Kafka
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("Fin de partición %s [%d] @ offset %d",
                              msg.topic(), msg.partition(), msg.offset())
                    continue
                # Error fatal
                log.error("Error de Kafka: %s", msg.error())
                raise KafkaException(msg.error())

            # ── Procesar mensaje ──────────────────────────────────────────
            payload = parse_message(msg.value())

            if payload is None:
                # Mensaje inválido: commitear de todas formas para no quedarse atascado
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                log.warning("Offset %d commiteado (mensaje inválido, skipped=%d).",
                             msg.offset(), skipped)
                continue

            try:
                process_event(payload)
                # Commit sólo después de confirmar la escritura en BD
                consumer.commit(message=msg, asynchronous=False)
                processed += 1

                if processed % 50 == 0:
                    log.info("── Resumen: %d procesados | %d omitidos ──", processed, skipped)

            except SQLAlchemyError as exc:
                log.error("Error de BD al procesar game_id=%s: %s",
                          payload.get("game_id"), exc)
                # No commitear: el mensaje se reintentará al reiniciar
                time.sleep(2)

            except Exception as exc:
                log.error("Error inesperado procesando payload=%r: %s", payload, exc)
                time.sleep(1)

    except KeyboardInterrupt:
        log.info("Consumer detenido por el usuario.")
    finally:
        consumer.close()
        log.info("Consumer cerrado. Total procesados=%d | omitidos=%d", processed, skipped)


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_consumer()
