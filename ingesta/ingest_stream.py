"""
ingest_stream.py — Steam Analytics | Kafka Producer (Streaming)
================================================================
Consulta el CCU (jugadores concurrentes) de los top-10 juegos vía la
Steam Web API y publica cada evento como mensaje JSON en un tópico Kafka.

Características:
- Logging estructurado (no print)
- Timeout de 10 s por request
- Excepciones manejadas por juego: un fallo no detiene el batch completo
- Servicio de larga duración: while True + sleep configurable
- Los IDs se leen del archivo top_games.txt generado por get_top_ids.py

Uso:
    python ingesta/ingest_stream.py

Variables de entorno opcionales (o editar las constantes):
    KAFKA_BOOTSTRAP  → por defecto "localhost:9092"
    KAFKA_TOPIC      → por defecto "steam_players"
    POLL_INTERVAL    → segundos entre batches (por defecto 60)
    TOP_N            → cuántos juegos del top usar (por defecto 10)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Producer, KafkaException

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("steam.producer")

# ─── Configuración ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "steam_players")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "60"))   # segundos
TOP_N           = int(os.getenv("TOP_N", "300"))

# top_games.txt está un nivel arriba del directorio ingesta/
TOP_GAMES_FILE = Path(__file__).resolve().parent.parent / "top_games.txt"

# IDs de respaldo si el archivo no existe
FALLBACK_IDS = [730, 570, 578080, 271590, 1240440, 440, 1172470, 1063730, 105600, 252490]

STEAM_CCU_URL = (
    "https://api.steampowered.com/ISteamUserStats/"
    "GetNumberOfCurrentPlayers/v1/?appid={appid}"
)


# ─── Helpers ──────────────────────────────────────────────────────────────────
def load_top_ids(n: int = TOP_N) -> list[int]:
    """Lee los primeros N AppIDs de top_games.txt; usa fallback si no existe."""
    if TOP_GAMES_FILE.exists():
        ids = []
        with TOP_GAMES_FILE.open() as fh:
            for line in fh:
                stripped = line.strip()
                if stripped.isdigit():
                    ids.append(int(stripped))
                if len(ids) >= n:
                    break
        log.info("IDs cargados desde %s: %d juegos", TOP_GAMES_FILE.name, len(ids))
        return ids

    log.warning("Archivo %s no encontrado. Usando lista de respaldo.", TOP_GAMES_FILE.name)
    return FALLBACK_IDS[:n]


def fetch_ccu(appid: int) -> int | None:
    """
    Obtiene el CCU de un juego vía Steam Web API.
    Retorna None si el request falla o la respuesta es inválida.
    """
    url = STEAM_CCU_URL.format(appid=appid)
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        ccu = data.get("response", {}).get("player_count")
        if ccu is None:
            log.warning("Respuesta sin player_count para appid=%d", appid)
        return ccu
    except requests.exceptions.Timeout:
        log.error("Timeout al consultar CCU de appid=%d", appid)
    except requests.exceptions.HTTPError as exc:
        log.error("HTTP %s al consultar appid=%d", exc.response.status_code, appid)
    except requests.exceptions.RequestException as exc:
        log.error("Error de red para appid=%d: %s", appid, exc)
    except (KeyError, ValueError) as exc:
        log.error("Error al parsear respuesta para appid=%d: %s", appid, exc)
    return None


def delivery_report(err, msg):
    """Callback de confluent-kafka al confirmar (o fallar) la entrega."""
    if err:
        log.error("Entrega FALLIDA | topic=%s offset=%s | %s",
                  msg.topic(), msg.offset(), err)
    else:
        log.debug("Entrega OK | topic=%s partition=%d offset=%d",
                  msg.topic(), msg.partition(), msg.offset())


# ─── Núcleo del productor ─────────────────────────────────────────────────────
def run_producer():
    log.info("=== Steam Producer iniciado ===")
    log.info("Broker: %s | Topic: %s | Intervalo: %ds | Top-N: %d",
             KAFKA_BOOTSTRAP, KAFKA_TOPIC, POLL_INTERVAL, TOP_N)

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "steam-stream-producer",
        # Reintenta hasta 5 veces ante errores transitorios de Kafka
        "retries": 5,
        "retry.backoff.ms": 300,
    })

    game_ids = load_top_ids(TOP_N)

    batch_num = 0
    while True:
        batch_num += 1
        ts = datetime.now(timezone.utc).isoformat()
        log.info("── Batch #%d | %s ──────────────────", batch_num, ts)

        ok, failed = 0, 0
        for appid in game_ids:
            ccu = fetch_ccu(appid)
            if ccu is None:
                log.warning("  [SKIP] appid=%d — sin CCU disponible", appid)
                failed += 1
                continue

            event = {"game_id": appid, "timestamp": ts, "ccu": ccu}
            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")

            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=str(appid).encode("utf-8"),
                    value=payload,
                    callback=delivery_report,
                )
                log.info("  [PROD] appid=%-10d CCU=%s", appid, f"{ccu:,}")
                ok += 1
            except KafkaException as exc:
                log.error("  [KAFKA ERR] appid=%d | %s", appid, exc)
                failed += 1

        # Espera que todos los mensajes encolados sean enviados
        pending = producer.flush(timeout=15)
        if pending:
            log.warning("Flush timeout: %d mensajes sin confirmar.", pending)

        log.info("Batch #%d completado — OK=%d | Fallidos=%d", batch_num, ok, failed)
        log.info("Durmiendo %d segundos…\n", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        log.info("Producer detenido por el usuario.")