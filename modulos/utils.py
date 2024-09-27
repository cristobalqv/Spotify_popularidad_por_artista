import os
import json
import datetime
import spotipy 
import pandas as pd
import psycopg2
import hashlib

from pprint import pprint
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy import create_engine, text



class SpotifyAPI:
    def __init__(self, id_cliente, secret_cliente):
        self.client_id = id_cliente
        self.client_secret = secret_cliente

        self.fecha_actual = datetime.datetime.now().strftime('%Y-%m-%d')

        self.lista_por_paises = None
        self.lista_canciones_por_paises = None
        self.dataframe_final = None
        #autenticamos para generar un token y poder acceder a la API
        self.sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=self.client_id, client_secret=self.client_secret))
        

    def generar_json_playlists(self):
        lista_paises = []
        paises = ["Argentina", "Bolivia", "Brasil", "Chile", "Colombia", "Ecuador", "Paraguay", "Peru", "Uruguay", "Venezuela"]
        #buscamos playlists que contengan 'TOP 50' para los países anteriores
        for pais in paises:
            playlists_top50 = self.sp.search(q=f'TOP 50 {pais}', type='playlist', limit=10)   #ampliamos el limite de busquedas a 10 playlist para que no sea la primera coincidencia ya que api de spotify devuelve resultados relacionados (no precisos), por lo que filtramos nuevamente 
            for playlist in playlists_top50['playlists']['items']:               
                # verifica que coincida exactamente con "Top 50 - {pais}"
                if f"Top 50 - {pais}" in playlist['name']:
                    print(f'Nombre de la playlist encontrada: {playlist["name"]}')
                    lista_paises.append({pais: playlist})              
                    print(f'diccionario de {pais} creado exitosamente')
                    break    #cuando se encuentra una playlist que coincide exactamente no busca mas
        # almacenamos los resultados en un CSV
        self.almacenar_playlists_csv(lista_paises)                
        self.lista_por_paises = lista_paises
        return self.lista_por_paises
    

    def almacenar_playlists_csv(self, lista_paises):
        # transformamos los datos de playlists en un df para guardarlos
        datos = []
        for pais_playlist in lista_paises:
            for pais, playlist in pais_playlist.items():
                datos.append({
                    'fecha': self.fecha_actual,
                    'pais': pais,
                    'playlist_id': playlist['id'],
                    'nombre_playlist': playlist['name'],
                    'total_tracks': playlist['tracks']['total']})
        df = pd.DataFrame(datos)
        df.to_csv(f'top50_playlists_{self.fecha_actual}.csv', index=False)
        print(f'Datos de playlists almacenados en top50_playlists_{self.fecha_actual}.csv')
    

    def contar_artistas_y_popularidad_por_playlist(self, playlist_id):
        #obtenemos pistas de una playlist por id de playlist
        tracks = self.sp.playlist_tracks(playlist_id = playlist_id, limit = 100)
        info_canciones = []
        #iteramos sobre los tracks para contar los artistas y popularidad
        for item in tracks['items']:
            track = item['track']
            nombre_cancion = track['name']
            popularidad = track['popularity']   #valor entre 0 y 100
            artistas = [artista['name'] for artista in track['artists']]
            #agregamos la info por cada artista individualmente
            for artista in artistas:
                info_canciones.append({'nombre_cancion': nombre_cancion,
                                        'artista': artista,
                                        'popularidad': popularidad})        
        return info_canciones


    # esta funcion utiliza las funciones creadas anteriormente "generar_json_playlists" y "contar_artistas_y_popularidad_por_playlist"
    # podemos decir que esta es la principal desde donde se ejecuta todo el script
    def lista_canciones_por_pais(self):
        self.lista_canciones_por_paises = []
        for elemento in self.generar_json_playlists():
            for pais, playlist in elemento.items():
                playlist_id = playlist['id']   #se obtiene id de playlist
                canciones = self.contar_artistas_y_popularidad_por_playlist(playlist_id)
                self.lista_canciones_por_paises.append({pais: canciones})
        # Aseguramos que la lista no esté vacía o no definida
        if not self.lista_canciones_por_paises:
            print("Error: No se encontraron canciones.")
            return None
        # almacenamos canciones por pais en un CSV
        self.almacenar_canciones_csv(self.lista_canciones_por_paises)
        return self.lista_canciones_por_paises
    

    def almacenar_canciones_csv(self, lista_canciones):
        # Transformamos los datos de canciones en un DataFrame para guardarlo
        datos = []
        for pais_canciones in lista_canciones:
            for pais, canciones in pais_canciones.items():
                for cancion in canciones:
                    datos.append({
                        'fecha': self.fecha_actual,
                        'pais': pais,
                        'nombre_cancion': cancion['nombre_cancion'],
                        'artista': cancion['artista'],
                        'popularidad': cancion['popularidad']})
        df = pd.DataFrame(datos)
        df.to_csv(f'artistas_por_pais_{self.fecha_actual}.csv', index=False)
        print(f'Datos de artistas almacenados en "artistas_por_pais_{self.fecha_actual}.csv"')
        self.dataframe_final = df
        




class Transformaciones:
    def __init__(self, dataframe):
        self.df = dataframe


    def transformar(self):
        # Por el momento tenemos 2 transformaciones que corresponden al 
        # cambio de fecha de tipo object a datetime y a la creación de una clave primaria "id"
        self.df['fecha'] = pd.to_datetime(self.df['fecha'])
        self.df['id'] = self.df.apply(lambda row: hashlib.md5((str(row['fecha']) + row['pais'] + row['nombre_cancion'] + row['artista']).encode()).hexdigest(), axis=1)





class CargaAWS:
    def __init__(self, credenciales: dict, schema: str, nombre_tabla: str):
        self.credenciales = credenciales
        self.schema = schema
        self.nombre_tabla = nombre_tabla
        self.conexion = None
        self.engine = None
              

    def crear_motor_sqlalchemy(self):
        user = self.credenciales.get('redshift_user')
        password = self.credenciales.get('redshift_pass')
        host =  self.credenciales.get('redshift_host')
        port = self.credenciales.get('redshift_port')
        database = self.credenciales.get('redshift_database')
        try:
            self.engine = create_engine(f"redshift+psycopg2://{user}:{password}@{host}:{port}/{database}")
            print('motor de conexión creado exitosamente')
            try:
                # establezco nivel de aislamiento para evitar consultas adicionales
                self.conexion = self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
                #query aleatorio para verificar estabilidad de la conexión
                prueba = self.conexion.execute('SELECT 1;')
                if prueba:
                    print('Conectado a AWS Redshift con éxito')
            except Exception as e:
                print(f'Fallo al conectarse a AWS Redshift: {e}')
        except Exception as e:
            print(f'Error al crear el motor de conexión: {e}')
            

    def crear_tabla(self):
        if self.conexion is not None:
            try:
                query_crear_tabla = f"""CREATE TABLE IF NOT EXISTS {self.schema}.{self.nombre_tabla}
                            (fecha DATE, pais VARCHAR(50), nombre_cancion VARCHAR(255), artista VARCHAR(50), popularidad INTEGER, fecha_carga DATE DEFAULT CURRENT_DATE NOT NULL, hora_carga VARCHAR(8) DEFAULT TO_CHAR(CURRENT_TIMESTAMP, 'HH24:MI:SS') NOT NULL, id VARCHAR(255) NOT NULL, PRIMARY KEY (id));"""
                self.conexion.execute(text(query_crear_tabla))
                print('Tabla creada con éxito en AWS Redshift')
            except Exception as e:
                print(f'Error al crear la tabla: {e}')


    def insertar_datos_manualmente(self, dataframe):
        if self.conexion is not None:
            try:
                insert_query = f"INSERT INTO {self.schema}.{self.nombre_tabla} (fecha, pais, nombre_cancion, artista, popularidad, fecha_carga, hora_carga, id) VALUES"
                valores = []
                for index, row in dataframe.iterrows():
                    fecha = row['fecha']
                    pais = row['pais']
                    nombre_cancion = row['nombre_cancion'].replace("'", "")   #eliminamos comillas simples para que no haya incompatibilidades 
                    artista = row['artista'].replace("'", "")
                    popularidad = row['popularidad']
                    id_ = row['id']

                    valores.append(f"('{fecha}', '{pais}', '{nombre_cancion}', '{artista}', {popularidad}, CURRENT_DATE, TO_CHAR(CURRENT_TIMESTAMP, 'HH24:MI:SS'), '{id_}')")
                # unimos los valores para formar una sola consulta
                insert_query += ", ".join(valores)
                self.conexion.execute(insert_query)
                print('Datos ingresados manualmente en AWS Redshift')
            except Exception as e:
                print(f'Error al insertar datos manualmente en la nube: {e}')


    def cerrar_conexion(self):
        if self.conexion:
            try:
                self.conexion.close()
                print('Conexión cerrada')
            except Exception as e:
                print(f'Hubo una excepción al cerrar la conexión: {e}')
        else:
            print('No hay conexión abierta. Intenta abrir una conexión nueva')