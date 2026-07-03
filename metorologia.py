import requests
from google.cloud import bigquery
import pandas as pd
import time
from datetime import datetime

# Configuración
PROJECT_ID = "bachincendios"
DATASET_ID = "Incendios_Historicos"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.Clima_Establecimientos"

client = bigquery.Client(project=PROJECT_ID)

def obtener_puntos_monitoreo():
    # Solo traemos los nombres y coordenadas de tus postas
    query = f"""
        SELECT nombre, latitud, longitud
        FROM `{PROJECT_ID}.{DATASET_ID}.Establecimientos_Salud`
        WHERE latitud IS NOT NULL AND longitud IS NOT NULL
        LIMIT 20  -- Empecemos con 20 puntos críticos para asegurar estabilidad
    """
    return list(client.query(query).result())

def consultar_clima(lote, session):
    lats = ",".join(str(round(p.latitud, 4)) for p in lote)
    lons = ",".join(str(round(p.longitud, 4)) for p in lote)
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lats, "longitude": lons,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m",
        "timezone": "America/Santiago"
    }
    
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else [data]

def job_meteorologia():
    puntos = obtener_puntos_monitoreo()
    datos_clima = []
    
    with requests.Session() as session:
        session.headers.update({'User-Agent': 'MonitorEducativo/1.0'})
        tamano_lote = 5 
        
        for i in range(0, len(puntos), tamano_lote):
            lote = puntos[i:i + tamano_lote]
            try:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Consultando lote {i//tamano_lote + 1}...")
                resultados = consultar_clima(lote, session)
                
                for punto, clima in zip(lote, resultados):
                    curr = clima.get("current", {})
                    datos_clima.append({
                        "nombre_establecimiento": punto.nombre,
                        "temperatura": curr.get("temperature_2m"),
                        "humedad": curr.get("relative_humidity_2m"),
                        "viento_vel": curr.get("wind_speed_10m"),
                        "viento_dir": curr.get("wind_direction_10m"),
                        "ultima_actualizacion": curr.get("time")
                    })
                time.sleep(2) # Respiro para la API
            except Exception as e:
                print(f"⚠️ Error en lote: {e}")

    if datos_clima:
        df = pd.DataFrame(datos_clima)
        # WRITE_TRUNCATE para que el tablero siempre muestre el clima "actual"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
        print(f"✅ Datos de clima actualizados en BigQuery.")

if __name__ == "__main__":
    print("🚀 Iniciando servicio de Meteorología (Cada 5 min). Ctrl+C para salir.")
    while True:
        job_meteorologia()
        time.sleep(300) # Espera 5 minutos exactos
