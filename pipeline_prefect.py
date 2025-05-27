from prefect import flow, task
import pandas as pd
from pymongo import MongoClient

# --- Tareas ---
@task(retries=2, retry_delay_seconds=10)
def cargar_googlesheets():
    sheet_id = "1HBwyjBG0zH1AGpIXnKyFC26rGhSsIOF744vc6aSjfLE"
    base_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet="
    sheets = {
        'cronograma': "Cronograma",
        'rutas': "Rutas",
        'buses': "buses",
        'falta': "Falta",
        'inspector': "Inspector",
        'operador': "Operador",
        'paradero': "Paradero",
        'sanciones': "Sanciones",
        'sentido': "Sentido"
    }

    dfs = {}
    for key, sheet_name in sheets.items():
        url = base_url + sheet_name
        print(f"Cargando hoja: {sheet_name}")
        dfs[key] = pd.read_csv(url)
        print(f"Columnas {sheet_name}:", dfs[key].columns.tolist())
    return dfs

@task(retries=2, retry_delay_seconds=10)
def cargar_mongodb():
    uri = "mongodb+srv://benja15mz:n0gbGQKX7AkJB1vE@cluster0.a0drvta.mongodb.net/"
    client = MongoClient(uri)
    db = client["Gps"]
    collection = db["bd_gps"]
    df_mongo = pd.DataFrame(list(collection.find()))
    print("Columnas MongoDB:", df_mongo.columns.tolist())
    return df_mongo

@task
def unir_datos(dfs, df_mongo):
    # Renombrar columna para merge
    df_mongo.rename(columns={'vehiculo_id': 'bus_id'}, inplace=True)

    df_final = pd.merge(df_mongo, dfs['buses'], on='bus_id', how='left')
    print("Merge MongoDB + buses OK")

    if 'operador_id' in df_final.columns:
        df_final = pd.merge(df_final, dfs['cronograma'], on='operador_id', how='left')
        print("Merge con cronograma OK")
        df_final = pd.merge(df_final, dfs['operador'], on='operador_id', how='left')
        print("Merge con operador OK")
    else:
        print("No existe 'operador_id' en df_final, no se pudo hacer merge con cronograma ni operador")

    print("Dataset final columnas:", df_final.columns.tolist())
    return df_final

@task
def guardar_csv(df_final):
    df_final.to_csv("dataset_final_prefect.csv", index=False)
    print("Archivo guardado: dataset_final_prefect.csv")

# --- Flow ---
@flow(name="Pipeline_DataOps")
def pipeline_dataops():
    dfs = cargar_googlesheets()
    df_mongo = cargar_mongodb()
    df_final = unir_datos(dfs, df_mongo)
    guardar_csv(df_final)

if __name__ == "__main__":
    pipeline_dataops()
