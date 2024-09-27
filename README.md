# Spotify Data Popularity Project

[![](https://github.com/cristobalqv/Spotify_popularidad_por_artista/blob/main/varios/spotify%20python.png)](https://github.com/cristobalqv/Spotify_popularidad_por_artista/blob/main/varios/spotify%20python.png)

### Descripción
Este proyecto permite la extracción, transformación y carga (ETL) de datos de popularidad de canciones en países de Latinoamérica, usando la API de Spotify y Amazon Redshift como base de datos. El objetivo principal es analizar la popularidad de los artistas y canciones de forma diaria, almacenando los resultados en AWS Redshift para su posterior análisis.

### Tecnologías utilizadas
- Python: Lenguaje de programación principal.
- Pandas: Para la manipulación y transformación de datos.
- Spotipy: Para la interacción con la API de Spotify.
- SQLAlchemy: Para la conexión y operaciones con Amazon Redshift.
- psycopg2-binary: Driver para conectarse a Redshift utilizando PostgreSQL.
- AWS Redshift: Almacenamiento y análisis de los datos.

### Estructura del proyecto
```
spotify_project/
│
├── venv/                        # Entorno virtual
│      ├── modulos/
│      │       └── utils.py       # Funciones ETL
│      ├── script/
│      │       └── main.py      # Script principal que ejecuta el proceso ETL
│      ├── varios/                 # información de apoyo
│      ├── requirements.txt       # Librerías necesarias
│      └── README.md             # Este archivo
└── .env                       # Configuración con credenciales (fuera de venv)
```

### Requisitos
- API de Spotify: Necesitas credenciales específicas para acceder a la API de Spotify. Regístrate [aquí](https://developer.spotify.com/ "aquí") para obtener un client_id y client_secret.
- AWS Redshift: Debes tener acceso a una instancia de Amazon Redshift con los permisos necesarios para crear y modificar tablas.
- Python 3.8+: Asegúrate de tener instalado Python y un entorno virtual configurado.

### Instalación
- Clona el repositorio

```
git clone https://github.com/cristobalqv/Spotify_popularidad_por_artista
cd spotify_project
```

- Crea y activa un entorno virtual

```
python -m venv venv
source venv/bin/activate     # En Linux/MacOS
# o
venv\Scripts\activate           # En Windows
```
- Instala las dependencias

```
pip install -r requirements.txt
```
- Configura tus credenciales
Necesitas crear un archivo .env en la raíz del proyecto con tus credenciales de la API de Spotify y de Redshift:

```
# .env file
client_id = tu_client_id     #SPOTIFY
client_secret = tu_client_secret     #SPOTIFY

redshift_user = tu_usuario_redshift
redshift_password = tu_contraseña_redshift
redshift_host = tu_host_redshift
redshift_port = 5439
redshift_database = tu_base_de_datos_redshift
```

### Ejecución del proyecto

Ejecuta el script principal

```
python script/main.py
```
Este script realizará los siguientes pasos:

- Extracción: Descarga datos de popularidad de canciones utilizando la API de Spotify para varios países de Latinoamérica.
- Transformación: Convierte y formatea los datos. Genera identificadores únicos (md5) y ajusta el tipo de dato de fecha.
- Carga: Inserta los datos transformados en AWS Redshift, asegurando que no haya duplicados mediante la creación de una clave primaria compuesta por varios campos.


### **Visualización de datos**
Una vez que los datos han sido cargados en Redshift, se pueden utilizar herramientas como Power BI o SQL para analizar y visualizar las tendencias de popularidad de canciones y artistas en el tiempo.

### Notas adicionales
Manejo de duplicados
Este proyecto utiliza una clave primaria compuesta por las columnas `fecha`, `pais`, `nombre_cancion`, y `artista` para evitar la duplicación de datos en Redshift. Si se intenta insertar una fila que ya existe (es decir, una con la misma combinación de estos valores), la inserción fallará.

### Problemas comunes
- Error de conexión a Redshift: Asegúrate de que tu instancia de Redshift esté correctamente configurada y que tus credenciales sean correctas.
- Límites de la API de Spotify: Spotify tiene límites en la cantidad de solicitudes que se pueden hacer por minuto. Asegúrate de estar dentro de estos límites.

### Mejoras futuras
- Automatizar el proceso ETL con Apache Airflow.
- Crear gráficos interactivos para la visualización de los datos de popularidad.
- Integrar acceso a otras fuentes de datos, como la API de YouTube.

### Contribuciones
Las contribuciones son bienvenidas. Si encuentras algún error o tienes ideas para mejorar el código, no dudes en abrir un issue o enviar un pull request. Si quisieras contactarte conmigo mejor aún!