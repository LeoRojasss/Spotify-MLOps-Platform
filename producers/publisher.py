from kafka import KafkaProducer
import json, time, random, requests, base64, os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("SPOTIFY_CLIENT_ID y SPOTIFY_CLIENT_SECRET deben estar configurados en .env")

# ===============================================
# Spotify Real-Time Producer for Kafka
# ===============================================
# Fuente: Spotify Web API (no user auth needed)
# - Mezcla: new releases / recommendations / search
# - Genera eventos reales para analítica
# ===============================================

def get_spotify_token():
    """Obtiene un token de acceso usando client_credentials"""
    auth = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    data = {"grant_type": "client_credentials"}
    r = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)
    if r.status_code != 200:
        raise Exception(f"Error al obtener token: {r.text}")
    return r.json().get("access_token")

ACCESS_TOKEN = get_spotify_token()
headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
print("Kafka producer conectado")

def fetch_new_releases(limit=5):
    """Obtiene álbumes nuevos y sus tracks"""
    url = "https://api.spotify.com/v1/browse/new-releases"
    params = {"limit": limit, "country": random.choice(["US", "CO", "BR", "MX", "ES"])}
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        print(f"Error new releases: {r.status_code} {r.text}")
        return []
    albums = r.json().get("albums", {}).get("items", [])
    all_tracks = []
    for album in albums:
        album_id = album["id"]
        t_url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
        t_r = requests.get(t_url, headers=headers, params={"limit": 5})
        if t_r.status_code == 200:
            for t in t_r.json().get("items", []):
                all_tracks.append({
                    "track_id": t["id"],
                    "track_name": t["name"],
                    "artist": t["artists"][0]["name"],
                    "album": album["name"],
                    "event_type": "new_release",
                    "country": params["country"],
                    "timestamp": time.time(),
                })
    return all_tracks

def fetch_recommendations(limit=10):
    """Recomendaciones por género"""
    url = "https://api.spotify.com/v1/recommendations"
    params = {
        "limit": limit,
        "seed_genres": random.choice(["pop", "latin", "reggaeton", "trap", "rock", "hip-hop"]),
        "market": random.choice(["US", "CO", "MX", "ES", "BR"])
    }
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        print(f"⚠️ Error recommendations: {r.status_code} {r.text}")
        return []
    return [
        {
            "track_id": t["id"],
            "track_name": t["name"],
            "artist": t["artists"][0]["name"],
            "album": t["album"]["name"],
            "popularity": t["popularity"],
            "event_type": "recommendation",
            "country": params["market"],
            "timestamp": time.time(),
        }
        for t in r.json().get("tracks", [])
    ]

def fetch_tracks_from_search():
    """Busca canciones según palabras aleatorias"""
    terms = ["duki", "rosalia", "bad bunny", "karol g", "fiesta", "chill", "sad", "latin", "trap", "pop"]
    term = random.choice(terms)
    url = "https://api.spotify.com/v1/search"
    params = {"q": term, "type": "track", "limit": 5}
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        print(f"Error search: {r.status_code} {r.text}")
        return []
    data = r.json().get("tracks", {}).get("items", [])
    return [
        {
            "track_id": t["id"],
            "track_name": t["name"],
            "artist": t["artists"][0]["name"],
            "album": t["album"]["name"],
            "popularity": t["popularity"],
            "event_type": "search_result",
            "query": term,
            "country": random.choice(["CO", "AR", "US", "MX", "ES"]),
            "timestamp": time.time(),
        }
        for t in data
    ]

while True:
    try:
        mode = random.choice(["new_releases", "recommendations", "search"])
        print(f"Fetching mode: {mode}")

        if mode == "new_releases":
            tracks = fetch_new_releases()
        elif mode == "recommendations":
            tracks = fetch_recommendations()
        else:
            tracks = fetch_tracks_from_search()

        if not tracks:
            print("No se obtuvieron resultados.")
            continue

        for track in tracks:
            producer.send(TOPIC, value=track)
            print(f"Sent: {track['track_name']} by {track['artist']} ({track['event_type']})")
            time.sleep(0.3)

        time.sleep(5)

    except KeyboardInterrupt:
        print("Detenido por el usuario.")
        break

    except Exception as e:
        print(f"Error general: {e}")
        time.sleep(10)
