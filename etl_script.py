import requests
import json
from datetime import datetime, timedelta, time
import os
import pandas as pd
import sys
import subprocess
import psycopg2 # Nueva librería para PostgreSQL

# Instalar 'pytz' si no está disponible (Render lo hará usando requirements.txt)
try:
    import pytz
except ImportError:
    print("Instalando librería 'pytz'...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pytz"])
    import pytz

# --- CONFIGURACIÓN HIBOT ---
HIBOT_APP_ID = "6749f162ea4755c8d8df65f8"
HIBOT_APP_SECRET = "260903b7-bdbb-44d7-acaf-bad9decea3a8"
BASE_URL = "https://pdn.api.hibot.us/api_external"

# --- CONFIGURACIÓN DE LA BASE DE DATOS ---
# Render nos dará esta URL y la pondremos como variable de entorno
DATABASE_URL = os.environ.get('DATABASE_URL')
ARGENTINA_TZ = pytz.timezone('America/Argentina/Buenos_Aires')

# --- FUNCIONES DE HIBOT (Sin cambios) ---
def get_hibot_token():
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
    today = datetime.now(ARGENTINA_TZ)
    end_date = today
    start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start_date.weekday() == 6: # Si es Domingo
        start_date += timedelta(days=1)
    print(f"🗓️  Rango de fechas a procesar: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    return start_date, end_date

def get_hibot_conversations(token, start_date, end_date):
    conversations_url = f"{BASE_URL}/conversations"
    payload = {"from": int(start_date.timestamp() * 1000), "to": int(end_date.timestamp() * 1000)}
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    print(f"📥 Descargando conversaciones de HiBot para el {start_date.strftime('%Y-%m-%d')}...")
    try:
        response = requests.post(conversations_url, headers=headers, json=payload)
        response.raise_for_status()
        conversations = response.json()
        print(f"✅ Se encontraron {len(conversations)} conversaciones.")
        return conversations
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error al descargar conversaciones para el {start_date.strftime('%Y-%m-%d')}: {e}")
        try:
            print(f"   Detalle del servidor: {response.text}")
        except:
            pass
        return []

# --- ¡NUEVAS FUNCIONES DE BASE DE DATOS! ---

def create_table(conn):
    """Crea la tabla 'conversations' si no existe."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS conversations (
        id_conversacion VARCHAR(255) PRIMARY KEY,
        tipificacion TEXT,
        fecha_creacion TIMESTAMP,
        fecha_cierre TIMESTAMP,
        fecha_delegado TIMESTAMP,
        fecha_asignado TIMESTAMP,
        hora_atencion TIMESTAMP,
        duracion_ms VARCHAR(50),
        nota TEXT,
        estado TEXT,
        tiempo_espera_ms VARCHAR(50),
        tiempo_respuesta_ms VARCHAR(50),
        nombre_agente TEXT,
        tipo_canal TEXT,
        campaña TEXT,
        dinamico TEXT,
        numero_ov TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
        print("✅ Tabla 'conversations' verificada/creada exitosamente.")
    except Exception as e:
        print(f"❌ Error al crear la tabla: {e}")
        raise e

def clear_and_insert_data(conn, data_df):
    """Borra los datos del mes actual e inserta los nuevos."""
    
    # Asegurarnos de que las columnas coinciden
    # Convertimos los nombres del DataFrame a los nombres de la BDD
    column_mapping = {
        'ID_Conversacion': 'id_conversacion',
        'Tipificacion': 'tipificacion',
        'Fecha_Creacion': 'fecha_creacion',
        'Fecha_Cierre': 'fecha_cierre',
        'Fecha_Delegado': 'fecha_delegado',
        'Fecha_Asignado': 'fecha_asignado',
        'Hora_Atencion': 'hora_atencion',
        'Duracion_ms': 'duracion_ms',
        'Nota': 'nota',
        'Estado': 'estado',
        'Tiempo_Espera_ms': 'tiempo_espera_ms',
        'Tiempo_Respuesta_ms': 'tiempo_respuesta_ms',
        'Nombre_Agente': 'nombre_agente',
        'Tipo_Canal': 'tipo_canal',
        'Campaña': 'campaña',
        'Dinamico': 'dinamico',
        'Numero_OV': 'numero_ov'
    }
    df_renamed = data_df.rename(columns=column_mapping)
    
    # Obtener el primer y último día del mes para el query DELETE
    start_date = df_renamed['fecha_creacion'].min().replace(day=1, hour=0, minute=0, second=0)
    # Convertimos a string en formato que PostgreSQL entiende
    start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')

    try:
        with conn.cursor() as cur:
            # 1. Borrar solo los datos del mes en curso
            # (Usamos 'fecha_creacion' que ya está convertida a Timestamp)
            print(f"🧹 Limpiando datos del mes en curso (desde {start_date_str})...")
            cur.execute("DELETE FROM conversations WHERE fecha_creacion >= %s", (start_date_str,))
            
            # 2. Preparar e Insertar los nuevos datos
            print(f"✍️  Insertando {len(df_renamed)} filas en la base de datos...")
            
            # Convertir DataFrame a una lista de tuplas
            # Reemplazamos NaT/NaN por None (que se convertirá en NULL en la BDD)
            df_cleaned = df_renamed.where(pd.notnull(df_renamed), None)
            tuples = [tuple(x) for x in df_cleaned.to_numpy()]
            
            # Nombres de columnas como string: "col1, col2, col3"
            cols = ','.join(list(df_cleaned.columns))
            # String de valores: "%s, %s, %s"
            vals = ','.join(['%s'] * len(df_cleaned.columns))
            
            # Query final
            insert_query = f"INSERT INTO conversations ({cols}) VALUES ({vals})"
            
            # Ejecutar la inserción masiva
            cur.executemany(insert_query, tuples)
            
            conn.commit()
            
        print("🚀 ¡Proceso completado! Base de datos actualizada.")
        
    except Exception as e:
        print(f"❌ Error durante la limpieza/inserción: {e}")
        conn.rollback() # Revertir cambios en caso de error
        raise e

# --- LÓGICA DE HORARIOS ---
def is_within_business_hours():
    now = datetime.now(ARGENTINA_TZ)
    current_time = now.time()
    current_day = now.weekday()
    if 0 <= current_day <= 4:
        if current_time >= time(9, 0) and current_time <= time(18, 0):
            return True
    if current_day == 5:
        if current_time >= time(9, 0) and current_time <= time(13, 0):
            return True
    return False

# --- FUNCIÓN DE SINCRONIZACIÓN PRINCIPAL (MODIFICADA) ---
def run_sync_process():
    print(f"[{datetime.now(ARGENTINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}] Iniciando sincronización...")
    hibot_token = get_hibot_token()
    if hibot_token:
        start_month, end_today = get_current_month_date_range()
        
        all_conversations = []
        current_day = start_month
        while current_day.date() <= end_today.date():
            start_of_day = current_day.replace(hour=0, minute=0, second=0)
            end_of_day = current_day.replace(hour=23, minute=59, second=59)
            daily_conversations = get_hibot_conversations(hibot_token, start_of_day, end_of_day)
            if daily_conversations:
                all_conversations.extend(daily_conversations)
            current_day += timedelta(days=1)
        
        print(f"\n✨ Descarga diaria finalizada. Total de conversaciones acumuladas: {len(all_conversations)}")
        
        if all_conversations:
            df_normalized = pd.json_normalize(all_conversations, sep='.')
            df_normalized.columns = [col.replace('fields.', '').replace('.value', '') for col in df_normalized.columns]

            final_columns_map = {
                'id': 'ID_Conversacion', 'typing': 'Tipificacion', 'created': 'Fecha_Creacion',
                'closed': 'Fecha_Cierre', 'delegated': 'Fecha_Delegado', 'assigned': 'Fecha_Asignado',
                'attentionHour': 'Hora_Atencion', 'duration': 'Duracion_ms', 'note': 'Nota',
                'status': 'Estado', 'waitTime': 'Tiempo_Espera_ms', 'answerTime': 'Tiempo_Respuesta_ms',
                'agent.name': 'Nombre_Agente', 'channel.type': 'Tipo_Canal', 'campaign.name': 'Campaña',
                'Dinamico': 'Dinamico', 'numeroov': 'Numero_OV'
            }
            final_df = pd.DataFrame()
            for original_col, new_name in final_columns_map.items():
                if new_name == 'Estado':
                    if 'status.name' in df_normalized:
                        final_df[new_name] = df_normalized['status.name']
                    elif 'status' in df_normalized:
                        final_df[new_name] = df_normalized['status']
                    else:
                        final_df[new_name] = ""
                elif original_col in df_normalized:
                    final_df[new_name] = df_normalized[original_col]
                else:
                    final_df[new_name] = ""
            
            date_columns = ['Fecha_Creacion', 'Fecha_Cierre', 'Fecha_Delegado', 'Fecha_Asignado', 'Hora_Atencion']
            for col in date_columns:
                if col in final_df.columns:
                    # Convertir de milisegundos a datetime
                    final_df[col] = pd.to_datetime(final_df[col], unit='ms', errors='coerce')
                    # Localizar en UTC y convertir a zona horaria de Argentina
                    final_df.loc[final_df[col].notna(), col] = final_df.loc[final_df[col].notna(), col].dt.tz_localize('UTC').dt.tz_convert(ARGENTINA_TZ)

            # --- ¡NUEVA LÓGICA DE CARGA A LA BDD! ---
            conn = None
            try:
                if not DATABASE_URL:
                    print("❌ Error: La variable de entorno DATABASE_URL no está configurada.")
                    return

                # Conectar a la base de datos PostgreSQL
                conn = psycopg2.connect(DATABASE_URL)
                print("✅ Conexión a la base de datos PostgreSQL exitosa.")
                
                # 1. Asegurar que la tabla exista
                create_table(conn)
                
                # 2. Limpiar datos del mes actual e insertar los nuevos
                clear_and_insert_data(conn, final_df)
                
            except Exception as e:
                print(f"❌ Error en la operación de base de datos: {e}")
            finally:
                if conn:
                    conn.close() # Siempre cerrar la conexión
                    print("🔌 Conexión a la base de datos cerrada.")

        else:
            print("No se encontraron conversaciones para actualizar en todo el período.")
    print(f"--- Sincronización completada a las {datetime.now(ARGENTINA_TZ).strftime('%H:%M:%S')} ---")

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    # Render (como Cron Job) no se preocupa por el horario. Simplemente
    # le diremos que ejecute el script cada 15 min DENTRO del horario laboral.
    # El script mismo no necesita verificar la hora, solo ejecutarse.
    
    print("Iniciando el Sincronizador de HiBot (Modo Cron Job)...")
    try:
        run_sync_process()
    except Exception as e:
        print(f"Error inesperado al ejecutar el script: {e}")
        # Salir con un código de error para que Render sepa que falló
        sys.exit(1)
