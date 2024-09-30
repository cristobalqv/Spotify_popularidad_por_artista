import os
import smtplib 
import subprocess

from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

#CREDENCIALES PARA NOTIFICACION DE CORREO
sender_email = 'cjquirozv@gmail.com'
smtp_server = 'smtp.gmail.com'
smtp_port = 587
password_gmail = os.getenv('password_gmail')

schema = 'cjquirozv_coderhouse'
nombre_tabla = 'spotify_popularidad_artistas_por_pais'



def ejecutar_etl():
    global schema, nombre_tabla
    try:
        try:                                                                        #calcula y visualiza la salida estandar y los errores
            result = subprocess.run(['python3', '/app/script/main.py'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            print(result.stdout)
        except Exception as e:
            print(Exception)
            
        with open(f'/opt/airflow/logs/informacion.txt', 'w') as f:
            f.write(f'esquema={schema}, tabla={nombre_tabla}')

        # leemos información desde archivo generado en main.py para enviar a correo
        with open('/opt/airflow/logs/informacion.txt', 'r') as f:
            informacion = f.read().strip().split(',')
            esquema = informacion[0]
            nombre_tabla = informacion[1]

        # generamos un indicador para monitorear el procedimiento etl
        with open('/opt/airflow/logs/indicador_exito.txt', 'w') as f:
            f.write('success')
        
        return {'esquema': esquema, 'tabla': nombre_tabla}
    
    except subprocess.CalledProcessError as e:
        with open('/opt/airflow/logs/indicador_exito.txt', 'w') as f:
               f.write('fail')
        raise e


#funcion que verifica si proceso fue exitoso
def verificar_proceso(info_etl):
    try:
        with open('/opt/airflow/logs/indicador_exito.txt', 'r') as f:
            status = f.read().strip()   #lee archivo y elimina espacios 
            if status == 'success':
                return f"Proceso ETL completado con éxito. Datos cargados al esquema {info_etl['esquema']} y a la tabla {info_etl['nombre_tabla']}"
            else:
                return "El proceso ETL no se completó correctamente"
             
    except Exception as e:
        print(e)


def enviar_mail(**context):
    try:
        info_etl = context['task_instance'].xcom_pull(task_ids='ejecutar_etl')
        subject = 'Notificacion proceso ETL'
        body_text = verificar_proceso(info_etl)

        #objeto de tipo MIMEMultipart. Para crear y manejar correos
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = sender_email
        msg['Subject'] = subject
        msg.attach(MIMEMultipart(body_text, 'plain'))

        #abrimos conexión con el servidor SMTP de Gmail
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, password_gmail)
            server.send_message(msg)
            print('email enviado correctamente')

    except Exception as e:
        print(f'Hubo un error al enviar el correo: {e}')




default_args = {'owner': 'cristobalqv',
                'retries': 5,
                'retry_delay': timedelta(minutes=3)}


with DAG(default_args=default_args,
         dag_id='ETL_spotify',
         description='DAG para el procedimiento ETL de Spotify y carga en AWS',
         start_date=datetime(2024,9,27),
         schedule_interval='@daily',
         catchup=False) as dag:
    

     task_ETL_DAG = PythonOperator(
        task_id='ejecutar_etl',
        python_callable=ejecutar_etl)
     

     task_envio_mail = PythonOperator(
          task_id = 'envio_correo',
          python_callable = enviar_mail,
          provide_context=True)
     

     task_ETL_DAG >> task_envio_mail