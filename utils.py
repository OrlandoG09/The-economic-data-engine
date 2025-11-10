import requests 
import pandas as pd

#Funcion para conectar con la API de Banxico

def banco_mexico(token,serie,fecha_inicio,fecha_fin):
    serie= f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{serie}/datos/{fecha_inicio}/{fecha_fin}"
    
    response=requests.get(serie, headers={"Bmx-Token": token})
    status=response.status_code 
    if status!=200:
        print("Error,codigo{}".format(status))
        return None
    raw_data=response.json()["bmx"]["series"][0]["datos"]
    dataframe=pd.DataFrame(raw_data)
    dataframe["fecha"]=pd.to_datetime(dataframe["fecha"], format="%d/%m/%Y")
    dataframe["dato"]=pd.to_numeric(dataframe["dato"])
    return dataframe

#Funcion para conectar con la API de INEGI

def inegi_pro(token, id_indicador, banco='BIE', inicio='2008-01-01', fin='2025-12-31'):
    
    url_base = f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/{id_indicador}/es/00/false/{banco}/2.0/{token}?type=json"
    
    try:
        response = requests.get(url_base)
        response.raise_for_status() 
        data = response.json()
       
        raw_data = data['Series'][0]['OBSERVATIONS']
        
        lista_datos = []
        for obs in raw_data:
            obs_val = obs['OBS_VALUE'] 
            if obs_val and obs_val.strip(): 
                dato_limpio = float(obs_val)
            else:
                dato_limpio = None 

            lista_datos.append({
                'fecha_raw': obs['TIME_PERIOD'],
                'dato': dato_limpio 
            })
        
        df = pd.DataFrame(lista_datos)
        df = df.dropna(subset=['dato'])

    
        df['fecha_raw'] = df['fecha_raw'].str.replace('/T1', '/03').str.replace('/T2', '/06').str.replace('/T3', '/09').str.replace('/T4', '/12')
        
     
        df['fecha'] = pd.to_datetime(df['fecha_raw'], format='mixed')
        # --- FIN DE LA MAGIA ---

        df = df.dropna(subset=['fecha'])
        df_final = df[ (df['fecha'] >= pd.to_datetime(inicio)) & (df['fecha'] <= pd.to_datetime(fin)) ]
        
        return df_final[['fecha', 'dato']].reset_index(drop=True)

    except Exception as e:
        print(f"¡Valió madre jalando la URL de INEGI ({id_indicador})! Error: {e}")
        return None