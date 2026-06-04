import requests
import json
import psycopg2

conn = psycopg2.connect(
    dbname="steam_analytics", user="admin", password="password", host="localhost", port="5432"
)
cur = conn.cursor()

import os
from datetime import datetime


with open("top_games.txt", "r") as f:
    game_ids = [line.strip() for line in f.readlines()]

fetched_records = []

def fetch_and_save(appid):
    url = f"https://steamspy.com/api.php?request=appdetails&appid={appid}"
    try:
        response = requests.get(url).json()
    except Exception as e:
        print(f"Error fetching detail for appid {appid}: {e}")
        return
    
    name = response.get('name', f"AppID {appid}")
    developer = response.get('developer', 'N/A')
    publisher = response.get('publisher', 'N/A')
    tags = response.get('tags', {})

    # Insertar en dim_games
    cur.execute("INSERT INTO dim_games (game_id, title, developer, publisher) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                (appid, name, developer, publisher))
    
    # Procesar tags
    for i, tag in enumerate(tags.keys()):
        priority = i + 1
        # To avoid duplicating tags when run multiple times, let's delete existing first or ignore
        cur.execute("DELETE FROM dim_game_tags WHERE game_id = %s AND tag_name = %s", (appid, tag))
        cur.execute("INSERT INTO dim_game_tags (game_id, tag_name, tag_priority) VALUES (%s, %s, %s)", 
                    (appid, tag, priority))
    
    conn.commit()
    print(f"Procesado: {name}")

    # Accumulate record for daily snapshot
    fetched_records.append({
        "game_id": int(appid),
        "title": name,
        "developer": developer,
        "publisher": publisher,
        "tags": list(tags.keys())  # Just a clean list of ordered tags
    })

for gid in game_ids:
    fetch_and_save(gid)

# Save historical daily snapshot file
os.makedirs("data/batch", exist_ok=True)
daily_filename = f"data/batch/{datetime.now().strftime('%d-%m-%y')}.json"
try:
    with open(daily_filename, "w", encoding="utf-8") as f:
        json.dump(fetched_records, f, indent=4, ensure_ascii=False)
    print(f"Historial local guardado correctamente en {daily_filename}")
except Exception as e:
    print(f"Error guardando archivo histórico: {e}")

cur.close()
conn.close()