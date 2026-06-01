"""
producer.py — Steam API → Kafka Producer
Obtiene los AppIDs más populares desde SteamSpy, luego consulta el CCU
(jugadores concurrentes) de cada juego vía la Steam Web API y publica
los eventos en el tópico Kafka 'steam_players'.

El módulo Ingesta/get_top_ids.py se usa para construir la lista dinámica.
Si SteamSpy no está disponible, se cae al conjunto curado por defecto.
"""

import json
import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Producer

# ─── Configuración ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "steam_players"
POLL_INTERVAL_SECONDS   = 30
MAX_GAMES               = 30          # cuántos juegos top vigilar

# Juegos de respaldo en caso de que SteamSpy falle
FALLBACK_GAMES = {
    730:     "Counter-Strike 2",
    570:     "Dota 2",
    440:     "Team Fortress 2",
    105600:  "Terraria",
    271590:  "Grand Theft Auto V",
    1091500: "Cyberpunk 2077",
    1245620: "Elden Ring",
    1172470: "Apex Legends",
    346110:  "ARK: Survival Evolved",
    252490:  "Rust",
    578080:  "PUBG: BATTLEGROUNDS",
    1172620: "Sea of Thieves",
}


# ─── SteamSpy: obtener top AppIDs ─────────────────────────────────────────────
def get_top_ids_from_steamspy(limit: int = MAX_GAMES) -> dict[int, str]:
    """
    Consulta SteamSpy para obtener los juegos más jugados.
    Devuelve {appid: nombre}. Retorna None si falla.
    """
    url = "https://steamspy.com/api.php?request=top100in2weeks"
    print(f"[SteamSpy] Consultando top {limit} juegos…", flush=True)
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()           # dict keyed by appid (string)
        result = {}
        for appid_str, info in list(data.items())[:limit]:
            result[int(appid_str)] = info.get("name", f"Game {appid_str}")
        print(f"[SteamSpy] {len(result)} juegos obtenidos.", flush=True)
        return result
    except Exception as e:
        print(f"[SteamSpy] Error: {e}. Usando lista de respaldo.", flush=True)
        return None


# ─── Steam Web API: CCU ───────────────────────────────────────────────────────
def fetch_ccu(app_id: int) -> int | None:
    url = (
        f"https://api.steampowered.com/ISteamUserStats/"
        f"GetNumberOfCurrentPlayers/v1/?appid={app_id}"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        d = r.json()
        return d.get("response", {}).get("player_count")
    except Exception as e:
        print(f"  [API] Error CCU para {app_id}: {e}", flush=True)
        return None


# ─── Callback de entrega ──────────────────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        print(f"  [Kafka] FALLO entrega: {err}", flush=True)
    else:
        print(
            f"  [Kafka] offset={msg.offset()}  "
            f"partition={msg.partition()}  topic={msg.topic()}",
            flush=True,
        )


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60, flush=True)
    print("  Steam Analytics — Kafka Producer", flush=True)
    print(f"  Broker : {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
    print(f"  Topic  : {KAFKA_TOPIC}", flush=True)
    print(f"  Intervalo: {POLL_INTERVAL_SECONDS}s  |  Max juegos: {MAX_GAMES}", flush=True)
    print("=" * 60, flush=True)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                         "client.id": "steam-producer"})

    # Intentar obtener lista dinámica de SteamSpy
    games = get_top_ids_from_steamspy(MAX_GAMES) or FALLBACK_GAMES
    print(f"\nMonitoreando {len(games)} juegos:\n"
          + "\n".join(f"  {aid}: {name}" for aid, name in list(games.items())[:10])
          + ("\n  …" if len(games) > 10 else ""),
          flush=True)

    try:
        while True:
            ts = datetime.now(timezone.utc).isoformat()
            print(f"\n{'─'*50}", flush=True)
            print(f"[BATCH] {ts}", flush=True)

            for app_id, game_name in games.items():
                ccu = fetch_ccu(app_id)
                if ccu is None:
                    print(f"  ⚠  {game_name} ({app_id}) — sin datos", flush=True)
                    continue

                event = {"game_id": app_id, "timestamp": ts, "ccu": ccu}
                print(f"  ▶ {game_name} ({app_id})  CCU={ccu:,}", flush=True)

                producer.produce(
                    KAFKA_TOPIC,
                    key=str(app_id).encode(),
                    value=json.dumps(event).encode(),
                    callback=delivery_report,
                )

            producer.flush()
            print(f"\n[SLEEP] {POLL_INTERVAL_SECONDS}s hasta el próximo batch…", flush=True)
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nDeteniendo producer…", flush=True)
    finally:
        producer.flush()
        print("Producer cerrado.", flush=True)


if __name__ == "__main__":
    main()
