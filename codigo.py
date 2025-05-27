import pandas as pd
from pymongo import MongoClient
import json

# 1. Cargar datos desde MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["mi_base_de_datos"]
collection = db["mi_coleccion"]

# Cargar datos desde MongoDB
data = list(collection.find())
df = pd.DataFrame(data)
print("✅ Datos cargados desde MongoDB:")
print(df.head())

# 2. Cargar Excel si existe (ignorar si no)
try:
    df_excel = pd.read_excel("archivo_datos.xlsx")
    df = pd.concat([df, df_excel], ignore_index=True)
    print("✅ Datos de Excel combinados.")
except FileNotFoundError:
    print("⚠️ Archivo Excel no encontrado. Solo se usará MongoDB.")

# 3. Limpieza básica
# Convertir dicts en strings para que sean serializables
for col in df.columns:
    if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
        df[col] = df[col].astype(str)
        print(f"🛠 Columna '{col}' convertida a string por contener dict o list.")

# 4. Convertir columnas de fecha
for col in df.columns:
    try:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        if df[col].notna().any():
            print(f"📆 Columna '{col}' convertida a datetime.")
    except Exception:
        pass

# 5. Guardar resultados
try:
    with open("datos_limpios.json", "w", encoding="utf-8") as f:
        df.to_json(f, orient="records", lines=True, force_ascii=False)
    print("💾 Archivo 'datos_limpios.json' guardado correctamente.")
except Exception as e:
    print(f"❌ Error al guardar JSON: {e}")

# Guardar esquema de columnas
try:
    df.dtypes.to_frame(name="tipo_dato").to_csv("esquema_columnas.csv", encoding="utf-8")
    print("📄 Archivo 'esquema_columnas.csv' guardado correctamente.")
except Exception as e:
    print(f"❌ Error al guardar esquema de columnas: {e}")
