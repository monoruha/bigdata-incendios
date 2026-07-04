import os
import time
import requests
from google.cloud import bigquery
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ID = "bachincendios"
DATASET_ID = "Incendios_Historicos"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.Clima_Establecimientos"

client = bigquery.Client(project=PROJECT_ID)


def crear_sesion():
    session = requests.Session()
    session.headers.update({"User-Agent": "ProyectoBigData-BIY7131/1.0"})
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def obtener_puntos_monitoreo():
    query = f"""
        SELECT nombre, latitud, longitud
        FROM `{PROJECT_ID}.{DATASET_ID}.Establecimientos_Salud`
        WHERE latitud IS NOT NULL AND longitud IS NOT NULL
    """
    return list(client.query(query).result())


def consultar_clima(lote, session):
    lats = ",".join(str(round(p.latitud, 4)) for p in lote)
    lons = ",".join(str(round(p.longitud, 4)) for p in lote)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lats,
        "longitude": lons,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m",
        "timezone": "America/Santiago",
    }
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else [data]


def job_meteorologia():
    puntos = obtener_puntos_monitoreo()
    print(f"Total de postas a consultar: {len(puntos)}")

    datos_clima = []
    tamano_lote = 200  # ahora que la conexión está estable, subimos el lote
    errores = 0

    with crear_sesion() as session:
        for i in range(0, len(puntos), tamano_lote):
            lote = puntos[i:i + tamano_lote]
            try:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Lote {i // tamano_lote + 1} "
                      f"({len(lote)} postas)...")
                resultados = consultar_clima(lote, session)

                for punto, clima in zip(lote, resultados):
                    curr = clima.get("current", {})
                    datos_clima.append({
                        "nombre_establecimiento": punto.nombre,
                        "temperatura": curr.get("temperature_2m"),
                        "humedad": curr.get("relative_humidity_2m"),
                        "viento_vel": curr.get("wind_speed_10m"),
                        "viento_dir": curr.get("wind_direction_10m"),
                        "ultima_actualizacion": curr.get("time"),
                    })
                time.sleep(1)
            except Exception as e:
                print(f"⚠️ Error en lote {i // tamano_lote + 1}: {e}")
                errores += 1
                continue

    print(f"Consulta terminada. Postas con clima: {len(datos_clima)}, lotes fallidos: {errores}")

    if datos_clima:
        import pandas as pd
        df = pd.DataFrame(datos_clima)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
        print(f"✅ BigQuery actualizado con {len(df)} registros.")
    else:
        print("❌ No se obtuvo ningún dato de clima, no se actualizó la tabla.")


if __name__ == "__main__":
    job_meteorologia()
