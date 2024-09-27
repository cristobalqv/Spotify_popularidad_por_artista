import os

from pprint import pprint
from dotenv import load_dotenv
from modulos.utils import SpotifyAPI, Transformaciones, CargaAWS

#cargamos variables de entorno en .env y las asignamos a un diccionario
load_dotenv()

diccionario = {'id_cliente': os.getenv('client_id'),
                'secret_cliente': os.getenv('client_secret')}

credenciales = {'redshift_user': os.getenv('redshift_user'),
                'redshift_pass': os.getenv('redshift_pass'),
                'redshift_host': os.getenv('redshift_host'),
                'redshift_port': os.getenv('redshift_port'),
                'redshift_database': os.getenv('redshift_database')}

schema = 'cjquirozv_coderhouse'
nombre_tabla = 'spotify_popularidad_artistas_por_pais'



if __name__=="__main__":

    spotify = SpotifyAPI(diccionario['id_cliente'], diccionario['secret_cliente'])
    spotify.lista_canciones_por_pais()
    
    dataframe = spotify.dataframe_final

    df_a_transformar = Transformaciones(dataframe)
    df_a_transformar.transformar()

    carga_aws = CargaAWS(credenciales, schema, nombre_tabla)
    carga_aws.crear_motor_sqlalchemy()
    carga_aws.crear_tabla()
    carga_aws.insertar_datos_manualmente(df_a_transformar.df)
    carga_aws.cerrar_conexion()
