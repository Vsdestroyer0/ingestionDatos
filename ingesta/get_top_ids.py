import requests
import json

def get_top_ids(limit=200):
    # Usamos el endpoint 'all' que devuelve la lista completa
    url = "https://steamspy.com/api.php?request=all"
    print("Consultando lista completa de SteamSpy...")
    response = requests.get(url).json()
    
    # SteamSpy devuelve un diccionario donde las llaves son los appids
    # Los juegos vienen ordenados internamente por popularidad
    all_games = list(response.values())
    
    # Tomamos los primeros 'limit' juegos
    top_games = all_games[:limit]
    
    # Extraemos solo los IDs y los guardamos en un TXT
    with open("top_games.txt", "w") as f:
        for game in top_games:
            f.write(str(game['appid']) + "\n")
            
    print(f"Éxito: Se han guardado {len(top_games)} IDs en top_games.txt")

if __name__ == "__main__":
    get_top_ids(500)