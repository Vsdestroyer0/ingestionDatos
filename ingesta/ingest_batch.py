import requests
import json
import psycopg2 # Librería para conectar a Postgres

# Configuración de conexión (Ajusta si es necesario)
conn = psycopg2.connect(
    dbname="steam_analytics", user="admin", password="password", host="localhost", port="5432"
)
cur = conn.cursor()

# Lista de los 250 IDs (Puedes extraerlos del endpoint de SteamSpy que trae el top)
# Por ahora, un pequeño extracto de los principales
# Dentro de tu ingest_batch.py, reemplaza la lista manual por esto:
with open("top_games.txt", "r") as f:
    game_ids = [line.strip() for line in f.readlines()]


def fetch_and_save(appid):
    url = f"https://steamspy.com/api.php?request=appdetails&appid={appid}"
    response = requests.get(url).json()
    
    # Insertar en dim_games
    cur.execute("INSERT INTO dim_games (game_id, title, developer, publisher) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", 
                (appid, response['name'], response['developer'], response['publisher']))
    
    # Procesar tags
    tags = response.get('tags', {})
    for i, tag in enumerate(tags.keys()):
        priority = i + 1
        cur.execute("INSERT INTO dim_game_tags (game_id, tag_name, tag_priority) VALUES (%s, %s, %s)", 
                    (appid, tag, priority))
    
    conn.commit()
    print(f"Procesado: {response['name']}")

for gid in game_ids:
    fetch_and_save(gid)

cur.close()
conn.close()