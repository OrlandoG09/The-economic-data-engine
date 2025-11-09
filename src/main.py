import pandas as pd
import requests 
import sqlite3 
from sqlalchemy import create_engine
import config 
from utils import banco_mexico

import os
from dotenv import load_dotenv
load_dotenv()




#Conectar base de datos
conectar=sqlite3.connect("economiaMex.db")

#Crear objeto cursor
cursor=conectar.cursor()

#Crear tablas crudas

#Primer tabla con datos diarios Tasa objetivo
cursor.execute("""
CREATE TABLE IF NOT EXISTS  tasa_objetivo(
        fecha TEXT PRIMARY KEY,
        dato REAL
    )
""")

conectar.commit()

print("Primera tabla creada con exito\n")


#Segunda tabla con datos diarios Tipo de cambio
cursor.execute("""
CREATE TABLE IF NOT EXISTS  tipo_cambio(
        fecha TEXT PRIMARY KEY,
        dato REAL
    )
""")

conectar.commit()

print("Segunda tabla creada con exito\n")





#Llenar mi primera tabla con los datos de banxico

#Definir fechas de inicio y fin para las series de tiempo
fecha_inicio="2008-01-01"
fecha_fin="2025-12-31"

#Definir los ID de la serie de tiempo que deseamos descargar 
ID_tasa_objetivo="SF61745"
API_TOKEN= os.environ.get("BANXICO_TOKEN")


df_tasa_objetivo=banco_mexico(API_TOKEN,ID_tasa_objetivo,fecha_inicio,fecha_fin)
print("DataFrame tasa objetivo creado con éxito")

engine=create_engine("sqlite:///economiaMex.db")

primera_tabla="tasa_objetivo"



#Insertar los datos

try:
 df_tasa_objetivo.to_sql(primera_tabla,con=engine,if_exists="append",index=False)
 
 print(f"\n¡Éxito! Se agregaron {len(df_tasa_objetivo)} filas a la tabla '{primera_tabla}'.")
 
    
except Exception as e:
    print(f"\nOcurrió un error al insertar los datos: {e}")




#Llenar mi segunda tabla con los datos de banxico

#Definir los ID de la serie de tiempo que deseamos descargar 
ID_tipo_cambio="SF43718"
API_TOKEN=config.API_TOKEN 

df_tipo_cambio=banco_mexico(API_TOKEN,ID_tipo_cambio,fecha_inicio,fecha_fin)
print("DataFrame tipo cambio creado con éxito")

engine=create_engine("sqlite:///economiaMex.db")

segunda_tabla="tipo_cambio"


#Insertar los datos

try:
 df_tasa_objetivo.to_sql(segunda_tabla,con=engine,if_exists="append",index=False)
 
 print(f"\n¡Éxito! Se agregaron {len(df_tipo_cambio)} filas a la tabla '{segunda_tabla}'.")
 
    
except Exception as e:
    print(f"\nOcurrió un error al insertar los datos: {e}")

