"""
ingest_stream.py — Steam Analytics | Kafka Producer (Streaming)
================================================================
Consulta el Top 15 de juegos más jugados en tiempo real utilizando la API
oficial de SteamCharts (GetGamesByConcurrentPlayers). Si un juego nuevo entra 
al top, consulta dinámicamente su nombre mediante appdetails.
Publica los eventos como mensajes JSON en un tópico Kafka.

Uso:
    python ingesta/ingest_stream.py

Variables de entorno opcionales:
    KAFKA_BOOTSTRAP  → por defecto "localhost:9092"
    KAFKA_TOPIC      → por defecto "steam_players"
    POLL_INTERVAL    → segundos entre batches (por defecto 60)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

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

STEAM_CHARTS_URL = "https://api.steampowered.com/ISteamChartsService/GetGamesByConcurrentPlayers/v1/"
STEAM_DETAILS_URL = "https://store.steampowered.com/api/appdetails?appids={appid}"

# Caché local de nombres de juegos para no llamar a la API de details constantemente
TITLE_CACHE = {}

def get_game_title(appid: int) -> str:
    """Obtiene el nombre del juego de la API de Steam si no está en caché."""
    if appid in TITLE_CACHE:
        return TITLE_CACHE[appid]
        
    try:
        url = STEAM_DETAILS_URL.format(appid=appid)
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        str_appid = str(appid)
        if data and str_appid in data and data[str_appid].get("success"):
            title = data[str_appid]["data"].get("name", f"AppID {appid}")
            TITLE_CACHE[appid] = title
            return title
    except Exception as e:
        log.warning("No se pudo obtener el nombre para appid=%d: %s", appid, e)
        
    return f"AppID {appid}"

def delivery_report(err, msg):
    if err:
        log.error("Entrega FALLIDA | topic=%s offset=%s | %s", msg.topic(), msg.offset(), err)
    else:
        log.debug("Entrega OK | topic=%s partition=%d offset=%d", msg.topic(), msg.partition(), msg.offset())

def run_producer():
    log.info("=== Steam Producer iniciado (Top 15 Most Played) ===")
    log.info("Broker: %s | Topic: %s | Intervalo: %ds", KAFKA_BOOTSTRAP, KAFKA_TOPIC, POLL_INTERVAL)

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "steam-stream-producer",
        "retries": 5,
        "retry.backoff.ms": 300,
    })

    batch_num = 0
    while True:
        batch_num += 1
        ts = datetime.now(timezone.utc).isoformat()
        log.info("── Batch #%d | %s ──────────────────", batch_num, ts)

        try:
            resp = requests.get(STEAM_CHARTS_URL, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            ranks = data.get("response", {}).get("ranks", [])
            
            # Tomar sólo el top 15
            top_15 = ranks[:15]
            
            ok, failed = 0, 0
            for rank_item in top_15:
                appid = rank_item.get("appid")
                ccu = rank_item.get("concurrent_in_game")
                
                if not appid or ccu is None:
                    continue
                    
                title = get_game_title(appid)
                
                event = {
                    "game_id": appid,
                    "title": title,
                    "timestamp": ts,
                    "ccu": ccu,
                    "rank": rank_item.get("rank")
                }
                
                payload = json.dumps(event, ensure_ascii=False).encode("utf-8")

                try:
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=str(appid).encode("utf-8"),
                        value=payload,
                        callback=delivery_report,
                    )
                    log.info("  [PROD] Rank %02d | %-25s | CCU=%s", rank_item.get("rank"), title[:25], f"{ccu:,}")
                    ok += 1
                except KafkaException as exc:
                    log.error("  [KAFKA ERR] appid=%d | %s", appid, exc)
                    failed += 1

            pending = producer.flush(timeout=15)
            if pending:
                log.warning("Flush timeout: %d mensajes sin confirmar.", pending)

            log.info("Batch #%d completado — OK=%d | Fallidos=%d", batch_num, ok, failed)
            
        except Exception as e:
            log.error("Error al obtener el Top 15 de SteamCharts: %s", e)

        log.info("Durmiendo %d segundos…\n", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        log.info("Producer detenido por el usuario.")