import os
import sys
import subprocess
from datetime import datetime, timedelta, time

# Instalar 'pytz', 'pandas', 'psycopg2-binary', 'requests' si no están disponibles
# Render usará requirements.txt, esto es más para pruebas locales si las necesitaras.
try:
    import pytz
    import pandas as pd
    import psycopg2
    import requests
except ImportError:
    print("Instalando librerías necesarias...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pytz", "pandas", "psycopg2-binary", "requests"])
    import pytz
    import pandas as pd
    import psycopg2
    import requests

# --- CONFIGURACIÓN HIBOT ---
HIBOT_APP_ID = "6749f162ea4755c8d8df65f8"
HIBOT_APP_SECRET = "260903b7-bdbb-44d7-acaf-bad9decea3a8"
BASE_URL = "https://pdn.api.hibot.us/api_external"

# --- CONFIGURACIÓN DE LA BASE DE DATOS ---
# Render nos dará esta URL automáticamente en el Paso 3
DATABASE_URL = os.environ.get('DATABASE_URL') 
ARGENTINA_TZ = pytz.timezone('America/Argentina/Buenos_Aires')

# --- LÓGICA DE HIBOT ---

def get_hibot_token():
    """Obtiene el token de autenticación de HiBot."""
    login_url = f"{BASE_URL}/login"
    payload = {"appId": HIBOT_APP_ID, "appSecret": HIBOT_APP_SECRET}
    print("🤖 Obteniendo token de HiBot...")
    try:
        response = requests.post(login_url, json=payload)
        response.raise_for_status()
        print("✅ Token de HiBot obtenido.")
        return response.json().get('token')
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener el token de HiBot: {e}")
        return None

def get_current_month_date_range():
    """Calcula el rango de fechas para el mes en curso (horario Argentina)."""
    today = datetime.now(ARGENTINA_TZ)
    end_date = today
    start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Ajuste por si el primer día es domingo (weekday() == 6)
    if start_date.weekday() == 6: 
        start_date += timedelta(days=1)
    print(f"🗓️  Rango de fechas a procesar: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    return start_date, end_date

def get_hibot_conversations(token, start_date, end_date):
    """Descarga conversaciones de HiBot día por día para evitar timeouts."""
    print(f"📥 Iniciando descarga de conversaciones...")
    all_conversations = []
    current_day = start_date
    while current_day.date() <= end_date.date():
        start_of_day = current_day.replace(hour=0, minute=0, second=0)
        end_of_day = current_day.replace(hour=23, minute=59, second=59)
        
        print(f"   ...obteniendo día {current_day.strftime('%Y-%m-%d')}")
        conversations_url = f"{BASE_URL}/conversations"
        payload = {"from": int(start_of_day.timestamp() * 1000), "to": int(end_of_day.timestamp() * 1000)}
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        
        try:
            response = requests.post(conversations_url, headers=headers, json=payload)
            response.raise_for_status()
            conversations = response.json()
            if conversations:
                all_conversations.extend(conversations)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Error al descargar día {current_day.strftime('%Y-%m-%d')}: {e}")
            
        current_day += timedelta(days=1)
    
    print(f"✨ Descarga finalizada. Total de conversaciones acumuladas: {len(all_conversations)}")
    return all_conversations

# --- LÓGICA DE BASE DE DATOS (POSTGRESQL) ---

def create_schema(conn):
    """
    Asegura que la tabla 'conversaciones' exista en la base de datos
    con todas las columnas que necesitamos.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS conversaciones (
        id VARCHAR(255) PRIMARY KEY,
        tipificacion VARCHAR(255),
        fecha_creacion TIMESTAMP WITH TIME ZONE,
        fecha_cierre TIMESTAMP WITH TIME ZONE,
        fecha_delegado TIMESTAMP WITH TIME ZONE,
        fecha_asignado TIMESTAMP WITH TIME ZONE,
        hora_atencion TIMESTAMP WITH TIME ZONE,
        duracion_ms BIGINT,
        nota TEXT,
        estado VARCHAR(255),
        tiempo_espera_ms BIGINT,
        tiempo_respuesta_ms BIGINT,
        nombre_agente VARCHAR(255),
        tipo_canal VARCHAR(255),
        campaña VARCHAR(255),
        dinamico VARCHAR(255),
        numero_ov VARCHAR(255)
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
        print("✅ Esquema de base de datos verificado/creado.")
    except Exception as e:
        print(f"❌ Error al crear el esquema: {e}")
        conn.rollback()

def process_and_clean_data(conversations_json):
    """
    Convierte el JSON de HiBot en un DataFrame de Pandas limpio,
    listo para ser insertado en la base de datos.
    """
    if not conversations_json:
        return pd.DataFrame()

    print("🧹 Procesando y limpiando datos...")
    df = pd.json_normalize(conversations_json, sep='.')
    
    # Renombrar columnas anidadas (ej. 'status.name' -> 'status')
    df.columns = [col.replace('fields.', '').replace('.value', '') for col in df.columns]

    # Definir el mapeo final de columnas
    column_map = {
        'id': 'id',
        'typing': 'tipificacion',
        'created': 'fecha_creacion',
        'closed': 'fecha_cierre',
        'delegated': 'fecha_delegado',
        'assigned': 'fecha_asignado',
        'attentionHour': 'hora_atencion',
        'duration': 'duracion_ms',
        'note': 'nota',
        'status.name': 'estado_obj', # Columna temporal
        'status': 'estado_simple', # Columna temporal
        'waitTime': 'tiempo_espera_ms',
        'answerTime': 'tiempo_respuesta_ms',
        'agent.name': 'nombre_agente',
        'channel.type': 'tipo_canal',
        'campaign.name': 'campaña',
        'Dinamico': 'dinamico',
        'numeroov': 'numero_ov'
    }
    
    # Crear un DataFrame final solo con las columnas que existen
    final_df = pd.DataFrame()
    for original_col, new_name in column_map.items():
        if original_col in df:
            final_df[new_name] = df[original_col]

    # Lógica inteligente para la columna 'estado'
    if 'estado_obj' in final_df:
        final_df['estado'] = final_df['estado_obj']
    elif 'estado_simple' in final_df:
        final_df['estado'] = final_df['estado_simple']
    
    # Eliminar columnas temporales
    final_df = final_df.drop(columns=['estado_obj', 'estado_simple'], errors='ignore')

    # Convertir fechas (milisegundos) a Timestamps de Pandas (con zona horaria UTC)
    date_columns = ['fecha_creacion', 'fecha_cierre', 'fecha_delegado', 'fecha_asignado', 'hora_atencion']
    for col in date_columns:
        if col in final_df:
            final_df[col] = pd.to_datetime(final_df[col], unit='ms', errors='coerce', utc=True)

    # Asegurarnos de que solo tengamos las columnas que están en la BBDD
    final_db_columns = [
        'id', 'tipificacion', 'fecha_creacion', 'fecha_cierre', 'fecha_delegado',
        'fecha_asignado', 'hora_atencion', 'duracion_ms', 'nota', 'estado',
        'tiempo_espera_ms', 'tiempo_respuesta_ms', 'nombre_agente', 'tipo_canal',
        'campaña', 'dinamico', 'numero_ov'
    ]
    
    # Añadir columnas que faltan (ej. 'dinamico') si no vinieron en el JSON
    for col in final_db_columns:
        if col not in final_df:
            final_df[col] = pd.NaT if 'fecha' in col else None # Asigna nulo apropiado

    # Reordenar y filtrar el DataFrame final
    final_df = final_df[final_db_columns]
    
    print(f"✅ Datos procesados. {len(final_df)} filas listas para insertar.")
    return final_df

def upsert_data(conn, df):
    """
    Inserta o actualiza los datos en la base de datos.
    Usa "INSERT ... ON CONFLICT" (Upsert) para que los
    registros existentes (misma ID) se actualicen en lugar de duplicarse.
    """
    if df.empty:
        print("ℹ️ No hay datos para sincronizar.")
        return

    print(f"🔄 Sincronizando {len(df)} filas con la base de datos...")
    
    # Convertir DataFrame a una lista de tuplas
    # Reemplazar NaT/NaN de Pandas por None (SQL NULL)
    data_tuples = [tuple(row) for row in df.where(pd.notnull(df), None).values]
    
    # Crear la lista de columnas (ej: "id", "tipificacion", ...)
    cols = ','.join([f'"{col}"' for col in df.columns])
    
    # Crear los placeholders (ej: %s, %s, ...)
    placeholders = ','.join(['%s'] * len(df.columns))
    
    # Crear la parte de "ON CONFLICT" para actualizar
    update_cols = ','.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != 'id'])
    
    query = f"""
    INSERT INTO conversaciones ({cols})
    VALUES ({placeholders})
    ON CONFLICT (id) DO UPDATE SET
        {update_cols};
    """
    
    try:
        with conn.cursor() as cur:
            # psycopg2 puede ejecutar muchas inserciones a la vez
            cur.executemany(query, data_tuples)
            conn.commit()
        print(f"🚀 Sincronización de datos completada.")
    except Exception as e:
        print(f"❌ Error durante el 'UPSERT' a la base de datos: {e}")
        conn.rollback()


# --- LÓGICA DE HORARIOS ---

def is_within_business_hours():
    """
    Verifica si la hora actual está dentro del horario laboral.
    L-V: 9:00 a 17:59:59 (para incluir las 17:xx)
    Sáb: 9:00 a 12:59:59 (para incluir las 12:xx)
    """
    now = datetime.now(ARGENTINA_TZ)
    current_time = now.time()
    current_day = now.weekday() # Lunes=0, Domingo=6

    # Lunes (0) a Viernes (4) de 9:00 a 17:59:59
    if 0 <= current_day <= 4:
        if current_time >= time(9, 0) and current_time < time(18, 0):
            return True
            
    # Sábado (5) de 9:00 a 12:59:59
    if current_day == 5:
        if current_time >= time(9, 0) and current_time < time(13, 0):
            return True
            
    return False

# --- EJECUCIÓN PRINCIPAL ---
def main():
    """
    Función principal del script.
    """
    if not is_within_business_hours():
        print(f"[{datetime.now(ARGENTINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}] Fuera de horario laboral. No se ejecuta nada.")
        return

    print(f"[{datetime.now(ARGENTINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}] Iniciando sincronización...")

    if not DATABASE_URL:
        print("❌ Error fatal: La variable de entorno DATABASE_URL no está definida.")
        print("   Este script debe ser ejecutado por Render (Paso 3) para funcionar.")
        return

    conn = None
    hibot_token = get_hibot_token()
    
    if not hibot_token:
        print("No se pudo obtener el token de HiBot. Abortando.")
        return
        
    try:
        # 1. Conectar y preparar la BBDD
        conn = psycopg2.connect(DATABASE_URL)
        create_schema(conn)
        
        # 2. Obtener rango de fechas (mes en curso)
        start_month, end_today = get_current_month_date_range()
        
        # 3. Descargar datos de HiBot
        json_data = get_hibot_conversations(hibot_token, start_month, end_today)
        
        # 4. Procesar datos
        df_cleaned = process_and_clean_data(json_data)
        
        # 5. Subir datos a PostgreSQL
        upsert_data(conn, df_cleaned)
        
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado en el proceso: {e}")
    finally:
        if conn:
            conn.close()
            print("⏹️ Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()

