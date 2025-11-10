import pandas as pd
import requests 
import sqlite3 
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


from utils import banco_mexico
from utils import inegi_pro

load_dotenv()



#CONEXIÓN A LA BASE DE DATOS
db_name = "economiaMex.db"
conectar = sqlite3.connect(db_name)
cursor = conectar.cursor()
engine = create_engine(f"sqlite:///{db_name}")

print(f"Conectado a la base de datos: {db_name}")

#CREACIÓN DE TABLAS 

cursor.execute("""
CREATE TABLE IF NOT EXISTS tasa_objetivo (
    fecha TEXT PRIMARY KEY,
    dato REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tipo_de_cambio (
    fecha TEXT PRIMARY KEY,
    dato REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS inflacion (
    fecha TEXT PRIMARY KEY,
    dato REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS desempleo (
    fecha TEXT PRIMARY KEY,
    dato REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS confianza_consumidor (
    fecha TEXT PRIMARY KEY,
    dato REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS pib (
    fecha TEXT PRIMARY KEY,
    dato REAL
)
""")

conectar.commit()
print("Tablas 'crudas' creadas/verificadas con éxito.\n")


#EXTRACCIÓN Y CARGA

fecha_inicio = "2008-01-01"
fecha_fin = "2025-12-31"

#BANXICO (DIARIOS)
API_TOKEN_BX = os.environ.get("API_TOKENBX")

#Tasa Objetivo
print("--- Jalando Tasa Objetivo (Banxico)...")
ID_tasa_objetivo = "SF61745"
df_tasa_objetivo = banco_mexico(API_TOKEN_BX, ID_tasa_objetivo, fecha_inicio, fecha_fin)

if df_tasa_objetivo is not None:
    try:
        df_tasa_objetivo.to_sql('tasa_objetivo', con=engine, if_exists="replace", index=False)
        print(f"¡Éxito! Se agregaron {len(df_tasa_objetivo)} filas a la tabla 'tasa_objetivo'.\n")
    except Exception as e:
        print(f"Ocurrió un error al insertar Tasa Objetivo: {e}\n")


#Tipo de Cambio
print("--- Jalando Tipo de Cambio (Banxico)...")
ID_tipo_cambio = "SF43718"
df_tipo_cambio = banco_mexico(API_TOKEN_BX, ID_tipo_cambio, fecha_inicio, fecha_fin)

if df_tipo_cambio is not None:
    try:
        df_tipo_cambio.to_sql('tipo_de_cambio', con=engine, if_exists="replace", index=False)
        print(f"¡Éxito! Se agregaron {len(df_tipo_cambio)} filas a la tabla 'tipo_de_cambio'.\n")
    except Exception as e:
        print(f"Ocurrió un error al insertar Tipo de Cambio: {e}\n")


# --- INEGI (MENSUALES Y TRIMESTRALES) ---
API_TOKEN_IN = os.environ.get("API_TOKENIN")

#Inflación
print("--- Jalando Inflación (INEGI)...")
ID_inflacion = "628217"
df_inflacion = inegi_pro(API_TOKEN_IN, ID_inflacion)

if df_inflacion is not None:
    try:
        df_inflacion.to_sql('inflacion', con=engine, if_exists="replace", index=False)
        print(f"¡Éxito! Se agregaron {len(df_inflacion)} filas a la tabla 'inflacion'.\n")
    except Exception as e:
        print(f"Ocurrió un error al insertar Inflación: {e}\n")


#Desempleo
print("--- Jalando Desempleo (INEGI)...")
ID_desempleo = "444666"
df_desempleo = inegi_pro(API_TOKEN_IN, ID_desempleo)

if df_desempleo is not None:
    try:
        df_desempleo.to_sql('desempleo', con=engine, if_exists="replace", index=False)
        print(f"¡Éxito! Se agregaron {len(df_desempleo)} filas a la tabla 'desempleo'.\n")
    except Exception as e:
        print(f"Ocurrió un error al insertar Desempleo: {e}\n")


#Confianza del Consumidor
print("--- Jalando Confianza del Consumidor (INEGI)...")
ID_confianzacons = "454168"
df_confianzacons = inegi_pro(API_TOKEN_IN, ID_confianzacons)

if df_confianzacons is not None:
    try:
        df_confianzacons.to_sql('confianza_consumidor', con=engine, if_exists="replace", index=False)
        print(f"¡Éxito! Se agregaron {len(df_confianzacons)} filas a la tabla 'confianza_consumidor'.\n")
    except Exception as e:
        print(f"Ocurrió un error al insertar Confianza: {e}\n")


#PIB
print("--- Jalando PIB (INEGI)...")
ID_pib = "910399"
df_pib = inegi_pro(API_TOKEN_IN, ID_pib)

if df_pib is not None:
    try:
        df_pib.to_sql('pib', con=engine, if_exists="replace", index=False)
        print(f"¡Éxito! Se agregaron {len(df_pib)} filas a la tabla 'pib'.\n")
    except Exception as e:
        print(f"Ocurrió un error al insertar PIB: {e}\n")


# --- 4. CERRAR CONEXIÓN ---
conectar.close()
print("--- SCRIPT DE ETL TERMINADO. Conexión cerrada. ---")