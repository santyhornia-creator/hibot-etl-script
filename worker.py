import requests
import json
from datetime import datetime, timedelta, time
import os
import pandas as pd
import psycopg2
from psycopg2 import extras
import sys
import subprocess
import numpy as np
import time as time_sleep

# Importar el programador de tareas
try:
    import pytz
    from apscheduler.schedulers.blocking import BlockingScheduler
except ImportError:
    print("Instalando librerías 'pytz' y 'apscheduler'...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pytz", "apscheduler"])
    import pytz
    from apscheduler.schedulers.blocking import BlockingScheduler

# --- CONFIGURACIÓN HIBOT ---
HIBOT_APP_ID = os.environ.get('HIBOT_APP_ID')
HIBOT_APP_SECRET = os.environ.get('HIBOT_APP_SECRET')
BASE_URL = "https://api.hibot.us/api_external"

# --- CONFIGURACIÓN DE LA BASE DE DATOS ---
ARGENTINA_TZ_STR = 'America/Argentina/Buenos_Aires'
ARGENTINA_TZ = pytz.timezone(ARGENTINA_TZ_STR)
DATABASE_URL = os.environ.get('DATABASE_URL') 

# --- FUNCIONES DE HIBOT ---
def get_hibot_token():
    login_url = f"{BASE_URL}/login"
    payload = {"appId": HIBOT_APP_ID, "appSecret": HIBOT_APP_SECRET}
    print("🤖 Obteniendo token de HiBot...")
    try:
        response = requests.post(login_url, json=payload, timeout=10) # Añadido timeout
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
    if start_date.weekday() == 6: 
        start_date += timedelta(days=1)
    return start_date, end_date

def get_hibot_conversations(token, start_date, end_date):
    conversations_url = f"{BASE_URL}/conversations"
    payload = {"from": int(start_date.timestamp() * 1000), "to": int(end_date.timestamp() * 1000)}
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    print(f"📥 Descargando conversaciones de HiBot para el {start_date.strftime('%Y-%m-%d')}...")
    try:
        response = requests.post(conversations_url, headers=headers, json=payload, timeout=30) # Añadido timeout
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

# --- FUNCIONES DE BASE DE DATOS ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("🔌 Conexión a la base de datos PostgreSQL exitosa.")
        return conn
    except Exception as e:
        print(f"❌ No se pudo conectar a la base de datos: {e}")
        return None

def create_conversations_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS conversations (
        hibot_id VARCHAR(255) PRIMARY KEY,
        created TIMESTAMPTZ,
        closed TIMESTAMPTZ,
        delegated TIMESTAMPTZ,
        assigned TIMESTAMPTZ,
        attentionHour TIMESTAMPTZ,
        duration BIGINT,
        waitTime BIGINT,
        answerTime BIGINT,
        typing VARCHAR(255),
        note TEXT,
        status VARCHAR(255),
        agent_name VARCHAR(255),
        channel_type VARCHAR(255),
        campaign_name VARCHAR(255),
        dinamico VARCHAR(255),
        numeroov VARCHAR(255),
        last_updated TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("✔️ Tabla 'conversations' asegurada (creada o ya existente).")
    except Exception as e:
        print(f"❌ Error al crear la tabla: {e}")
        conn.rollback()

def upsert_conversations(conn, df):
    column_order = [
        'hibot_id', 'created', 'closed', 'delegated', 'assigned', 'attentionHour',
        'duration', 'waitTime', 'answerTime', 'typing', 'note', 'status',
        'agent_name', 'channel_type', 'campaign_name', 'dinamico', 'numeroov'
    ]
    
    print("🧹 Procesando y limpiando datos...")
    df_normalized = pd.json_normalize(df, sep='.')
    df_renamed = df_normalized.rename(columns={
        'id': 'hibot_id', 'agent.name': 'agent_name', 'channel.type': 'channel_type',
        'campaign.name': 'campaign_name', 'status.name': 'status_obj',
    })
    
    date_columns = ['created', 'closed', 'delegated', 'assigned', 'attentionHour']
    for col in date_columns:
        if col in df_renamed.columns:
            df_renamed[col] = pd.to_numeric(df_renamed[col], errors='coerce')
            df_renamed[col] = pd.to_datetime(df_renamed[col], unit='ms', errors='coerce')
            df_renamed[col] = df_renamed[col].dt.tz_localize('UTC').dt.tz_convert(ARGENTINA_TZ_STR)
        else:
            df_renamed[col] = pd.NaT
            
    for col in column_order:
        if col not in df_renamed.columns:
            if col in date_columns:
                 df_renamed[col] = pd.NaT
            else:
                df_renamed[col] = None
    
    if 'status_obj' in df_renamed.columns:
        df_renamed['status'] = df_renamed['status_obj']
    elif 'status' in df_renamed.columns:
        df_renamed['status'] = df_renamed['status']
    else:
        df_renamed['status'] = None

    df_final = df_renamed[column_order]
    df_final = df_final.replace({pd.NaT: None, np.nan: None})
    
    print(f"✔️ Datos procesados. {len(df_final)} filas listas para insertar.")
    data_tuples = [tuple(x) for x in df_final.to_numpy()]

    print(f"🔄 Sincronizando {len(data_tuples)} filas con la base de datos...")
    insert_query = f"""
    INSERT INTO conversations ({', '.join(column_order)})
    VALUES %s
    ON CONFLICT (hibot_id) DO UPDATE SET
        created = EXCLUDED.created, closed = EXCLUDED.closed, delegated = EXCLUDED.delegated,
        assigned = EXCLUDED.assigned, attentionHour = EXCLUDED.attentionHour, duration = EXCLUDED.duration,
        waitTime = EXCLUDED.waitTime, answerTime = EXCLUDED.answerTime, typing = EXCLUDED.typing,
        note = EXCLUDED.note, status = EXCLUDED.status, agent_name = EXCLUDED.agent_name,
        channel_type = EXCLUDED.channel_type, campaign_name = EXCLUDED.campaign_name,
        dinamico = EXCLUDED.dinamico, numeroov = EXCLUDED.numeroov,
        last_updated = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');
    """
    try:
        with conn.cursor() as cur:
            extras.execute_values(cur, insert_query, data_tuples)
            conn.commit()
            print(f"🚀 Sincronización de {len(data_tuples)} filas completada.")
    except Exception as e:
        print(f"❌ Error durante el 'UPSERT' a la base de datos: {e}")
        conn.rollback()

# --- FUNCIÓN PRINCIPAL DE SINCRONIZACIÓN ---
def job_sincronizacion():
    """
    Esta es la función que será llamada por el programador (apscheduler).
    """
    print(f"--- INICIANDO EJECUCIÓN PROGRAMADA: {datetime.now(ARGENTINA_TZ)} ---")
    
    # 1. Verificar si estamos en horario laboral
    now = datetime.now(ARGENTINA_TZ)
    current_time = now.time()
    current_day = now.weekday() # Lunes=0, Domingo=6

    is_weekday = 0 <= current_day <= 4 and time(9, 0) <= current_time <= time(18, 0)
    is_saturday = current_day == 5 and time(9, 0) <= current_time <= time(13, 0)
    
    if not (is_weekday or is_saturday):
        print("... Fuera de horario laboral. Omitiendo ejecución.")
        return

    print("... Dentro del horario laboral. Procediendo con la sincronización.")
    
    # 2. Ejecutar el proceso
    conn = get_db_connection()
    if not conn:
        return

    create_conversations_table(conn)
    
    hibot_token = get_hibot_token()
    if not hibot_token:
        conn.close()
        return
        
    start_date, end_date = get_current_month_date_range()
    
    all_conversations = []
    current_day = start_date
    while current_day.date() <= end_date.date():
        day_start = current_day.replace(hour=0, minute=0, second=0)
        day_end = current_day.replace(hour=23, minute=59, second=59)
        daily_conversations = get_hibot_conversations(hibot_token, day_start, day_end)
        if daily_conversations:
            all_conversations.extend(daily_conversations)
        current_day += timedelta(days=1)
        
    if all_conversations:
        print(f"\n✨ Descarga diaria finalizada. Total de conversaciones acumuladas: {len(all_conversations)}")
        upsert_conversations(conn, all_conversations)
    else:
        print("No se encontraron conversaciones en el período.")

    conn.close()
    print("🔌 Conexión a la base de datos cerrada.")
    print(f"--- EJECUCIÓN PROGRAMADA FINALIZADA: {datetime.now(ARGENTINA_TZ)} ---")


# --- EJECUCIÓN DEL "CAMILLERO" (WORKER) ---
if __name__ == "__main__":
    
    # Verificamos que las variables de entorno cruciales estén presentes
    if not all([DATABASE_URL, HIBOT_APP_ID, HIBOT_APP_SECRET]):
        print("❌ FALTAN VARIABLES DE ENTORNO. (DATABASE_URL, HIBOT_APP_ID, HIBOT_APP_SECRET)")
        print("   Por favor, configúralas en la pestaña 'Environment' de Render.")
        sys.exit(1) # Detiene el script si faltan variables

    print("Iniciando el 'Background Worker' (Camillero)...")
    
    # 1. Ejecutar el job una vez al arrancar (para no esperar 15 min)
    try:
        print("Ejecutando la primera sincronización al arrancar...")
        job_sincronizacion()
    except Exception as e:
        print(f"Error durante la ejecución inicial: {e}")

    # 2. Configurar el programador (scheduler)
    scheduler = BlockingScheduler(timezone=ARGENTINA_TZ_STR)
    
    # Programar la tarea para que se ejecute cada 15 minutos
    scheduler.add_job(job_sincronizacion, 'interval', minutes=15)
    
    print(f"Tarea programada. Próxima ejecución en 15 minutos.")
    print("El 'Camillero' está despierto y escuchando. (Presiona Ctrl+C para detener localmente)")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
