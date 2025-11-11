# Cliente API MrBot

Este proyecto contiene una aplicación **Streamlit** que actúa como cliente para las APIs de Mr. Bot/AFIP.  Permite interactuar con el servicio **Mis Comprobantes** (consultas masivas de facturas emitidas/recibidas, descarga y consolidación de resultados) y con otros módulos de la API como **Comprobantes en Línea (RCEL)**, **Sistema de Cuentas Tributarias (SCT)**, **Cuenta Corriente de Monotributistas y Autónomos (CCMA)**, consulta de **apócrifos** y **constancias de inscripción (CUIT)**.  Además, desde la interfaz se pueden crear usuarios, restablecer la API key y consultar la cantidad de consultas disponibles asociadas a un usuario.

## Estructura del proyecto

```

.
├── cliente_api_mrbot.py     # Script principal de Streamlit que levanta la interfaz web
├── compose.yaml             # Definición de servicios para docker‑compose
├── Dockerfile               # Imagen Docker para empaquetar la aplicación
├── requirements.txt         # Lista de dependencias de Python
├── .env.example             # Plantilla de variables de entorno (no versionar .env real)
├── .dockerignore            # Exclusiones al construir la imagen
└── ...

````

> **Nota**: dependiendo de la versión del repositorio, el archivo de entrada puede llamarse `cliente_api_mrbot.py`, `api_bots_clientes.py` o `consultas_mc_final.py`.  El `Dockerfile` asume que la aplicación se ejecuta con `streamlit run cliente_api_mrbot.py`.  Si el archivo principal tiene otro nombre, ajústalo en el `CMD` del Dockerfile o renómbralo en consecuencia.

## Requisitos

La aplicación se basa en Python 3.13 y las siguientes librerías (definidas en `requirements.txt`):

- **Streamlit** para crear la interfaz web
- **pandas** y **openpyxl** para lectura/escritura de Excel
- **requests** para llamadas HTTP
- **python‑dotenv** para cargar variables de entorno desde un fichero `.env`
- Otras dependencias auxiliares que se instalan automáticamente

## Variables de entorno

Se recomienda crear un fichero `.env` en la raíz del proyecto tomando como referencia `.env.example`.  Las variables más importantes son:

- `X_API_KEY`: clave de API proporcionada por Mr. Bot.  Se utiliza en las cabeceras de cada solicitud.
- `EMAIL`: dirección de correo del usuario registrado en Mr. Bot.  Algunas operaciones requieren incluirla en la cabecera.

Ejemplo de `.env`:

```env
X_API_KEY=tu_clave_de_api
EMAIL=tu_correo@example.com
````

**No incluyas** tus credenciales sensibles en el repositorio.  El fichero `.dockerignore` ya excluye `.env` de la imagen.

## Ejecución local

1. Instala Python 3.13 (u otra versión compatible) en tu sistema.

2. Crea y activa un entorno virtual (opcional):

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. Instala las dependencias:

   ```bash
   pip install -r requirements.txt
   ```

4. Crea un archivo `.env` a partir de `.env.example` e introduce tu API key y correo.

5. Ejecuta la aplicación con Streamlit:

   ```bash
   streamlit run cliente_api_mrbot.py
   ```

6. Accede a `http://localhost:8501` en tu navegador para utilizar la interfaz.

## Ejecución con Docker

Puedes construir la imagen de la aplicación y ejecutarla en un contenedor aislado.  Asegúrate de tener Docker instalado en tu equipo.

### Construir la imagen

```bash
docker build -t cliente_api_mrbot .
```

### Ejecutar el contenedor

```bash
docker run --rm -p 8501:8501 --env-file .env cliente_api_mrbot
```

Con estas opciones se expone el puerto 8501 a tu máquina local y se cargan las variables de entorno desde `.env`.  Al abrir `http://localhost:8501` verás la aplicación en ejecución.

## Ejecución con Docker Compose

El archivo `compose.yaml` facilita el levantamiento del servicio junto con otras dependencias (por ejemplo, una red llamada `nginx_default`).  Para usarlo:

```bash
docker compose up --build
```

Esto construirá la imagen (si no existe) y levantará el contenedor `cliente_api_mrbot` en segundo plano exponiendo el puerto 8501.  Puedes personalizar la red y otros parámetros editando `compose.yaml`.

## Uso de la aplicación

Al iniciar la aplicación verás varias solapas principales:

* **Mis Comprobantes**: agrupa las herramientas relacionadas con el endpoint `/api/v1/mis_comprobantes/consulta`, permitiendo subir un Excel con múltiples CUITs y credenciales, procesar consultas de forma masiva, descargar los archivos resultantes (S3 o MinIO) en un ZIP y consolidar los CSV descargados en archivos Excel.  También permite ver los estados y mensajes devueltos por la API.
* **Usuarios**: desde aquí puedes crear un nuevo usuario (enviando la clave por correo), restablecer la API key de un usuario existente y consultar cuántas consultas disponibles tiene un correo electrónico en el servicio.
* **Otros endpoints**: incluye accesos a otros servicios de la API (Comprobantes en Línea, SCT, CCMA, Apócrifos y Consulta de CUIT).  Cada subpestaña ofrece formularios individuales y masivos según corresponda y muestra las respuestas en pantalla o como archivos descargables.

Sigue las instrucciones en cada formulario, carga los archivos solicitados y revisa las respuestas devueltas.  Para las consultas masivas, se generan resúmenes en formato Excel para facilitar su análisis.

## Contribuciones

Este proyecto es un ejemplo de integración con las APIs de Mr. Bot.  Si deseas mejorar la aplicación, reportar problemas o agregar nuevas funcionalidades, eres bienvenido a enviar pull requests o abrir issues.

## Mencion Especial

El script Original fue desarrollado por **Bautista Rubio**, luego adaptado y dockerizado para su ejecución local y/o en servidores.

Link de Linkedin de Bautista: https://www.linkedin.com/in/bautista-rubio/
