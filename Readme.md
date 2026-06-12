# Proyecto Big Data: Análisis de Transporte Público con GTFS
## Descripción General

El objetivo de este proyecto es construir un sistema completo de análisis de datos en tiempo real sobre el transporte público de la ciudad de Los Ángeles, utilizando tecnologías propias del ecosistema Big Data.

La fuente principal de datos es la API de GTFS en tiempo real proporcionada por LA Metro. A partir de ahí, se desarrolla una arquitectura distribuida para la ingestión, procesamiento, almacenamiento y visualización de datos en tiempo real.

## Arquitectura General

El sistema está compuesto por los siguientes componentes:

- Fuente de Datos: API pública GTFS de LA Metro.
- Ingesta: Python + MySQL + Kafka Connect.
- Procesamiento en Tiempo Real: Apache Spark Structured Streaming.
- Almacenamiento: Apache HDFS (formato Parquet).
- Visualización: Power BI.
- Monitorización: Prometheus + Grafana.

## Flujo de Datos

-  Obtención de Datos: Un script en Python realiza peticiones periódicas a la API GTFS, obteniendo la posición y características de los autobuses en tiempo real.
-  Persistencia Inicial: Los datos se almacenan en una base de datos MySQL para facilitar su lectura por Kafka Connect.
- Kafka Connect: Extrae automáticamente nuevos registros de MySQL y los envía al tópico `vehicle_positions` en Apache Kafka.
- Spark Structured Streaming: Un job en Spark lee el tópico `vehicle_positions`, transforma los datos y los guarda en HDFS en formato Parquet.
- HDFS: Se persisten los datos Parquet junto a los datos estáticos del estándar GTFS.
- Power BI: Se conecta a los archivos Parquet en HDFS y visualiza la información de manera clara e interactiva.

![](img/diagrama_estructura.png)

## Tecnologías Utilizadas
Python -	Extracción de datos desde la API GTFS

MySQL  -	Almacenamiento temporal

Apache Kafka -	Sistema de datos en tiempo real

Kafka Connect - Conector para mover datos desde MySQL a Kafka

Apache Spark - Procesamiento de streaming y escritura en HDFS

HDFS - Sistema de almacenamiento distribuido

Power BI - Visualización de datos

Prometheus - Monitorización de recursos y rendimiento del sistema

Docker Compose - Entorno de ejecución del sistema distribuido

## Detalles Técnicos

### Estrategia de Ingesta del Productor Python
Para evitar superar el límite de peticiones establecido por la API GTFS en tiempo real de LA Metro, se ha implementado una estrategia de ingesta por lotes escalonados. El productor en Python realiza una única petición a la API cada 10 segundos. Esta petición obtiene la información completa de todos los vehículos disponibles en ese momento.

En lugar de insertar todos los datos directamente en la base de datos, el script divide la respuesta en 10 minibatches, que son insertados en MySQL de forma progresiva, uno por segundo. De esta manera, se consigue un flujo de datos más constante hacia Kafka Connect y se evita una sobrecarga puntual tanto en MySQL como en el sistema de procesamiento.

### Cálculo de Datos Sintéticos
Para mejorar la capacidad de realizar Business Inteligence, se ha optado por generar sintéticamente unos datos de cantidad de usuarios por cada autobus:

```
def check_stop_event(bus_id, vehicle_stop):
    if bus_id not in last_stops:
        amount_people = random.randint(0, 70)
        last_stops[bus_id] = (vehicle_stop, amount_people)
        return amount_people
    elif vehicle_stop != last_stops[bus_id][0]:
        new_amount_people = min(70, max(0, last_stops[bus_id][1] + random.randint(-5, 10)))
        last_stops[bus_id] = (vehicle_stop, new_amount_people)
        return new_amount_people
    else:
        return last_stops[bus_id][1]
```

## Esquema de Datos
Los datos iniciales recibidos mediante la API se guardan en el siguiente formato:

- `internal_id`: ID interno para el correcto funcionamiento de Kafka Connect
- `id`: Identificador de autobús
- `trip_route_id`: Identificador de ruta
- `trip_trip_id`: Identificador de viaje
- `position_latitude`: Posición en latitud del vehículo
- `position_longitude`: Posición en longitud del vehículo
- `current_stop_sequence`: Orden de la siguiente parada en la ruta
- `current_status`: Estado del vehículo (parado, en movimiento)
- `amount_people`: Cantidad de personas en el vehículo
- `timestamp`: Momento en el que se envió el dato del vehículo

Por otro lado, los datos transformados, almacenados en HDFS y cargados en PowerBI siguen la siguiente estructura:

- `vehicle_id`: Identificador del autobús
- `route_id`: Identificador de ruta
- `trip_id`: Identificador de viaje
- `location`: Coordenadas GPS
- `current_status`: Estado del vehículo (parado, en movimiento)
- `timestamp`: Fecha y hora ajustada
- `stop_id`: Identificador de la siguiente parada
- `stop_name`: Nombre de la siguiente parada
- `amount_people`: Cantidad de personas en el vehículo
- `time_diff_seconds`: Retraso actual en segundos

## Requisitos del Sistema

Para el despliegue del sistema se requieren los siguientes recursos y configuraciones:

### Docker Compose
- Todos los servicios (MySQL, Kafka, Kafka Connect, Spark, HDFS, Prometheus) se despliegan con `docker compose up`.
- Los contenedores se comunican entre sí a través de una red bridge interna de Docker, sin necesidad de IPs estáticas ni configuración de hosts.
- Power BI se conecta a HDFS a través de `localhost:9870`.
- **Power BI:**
  - Es necesario contar con Power BI instalado para la visualización y análisis de los datos procesados.
  - El archivo de proyecto Power BI (`.pbix`) se encuentra disponible en el repositorio del proyecto y debe ser utilizado para generar los informes y dashboards correspondientes.

## Despliegue con Docker Compose

1. Clonar el repositorio y crear un archivo `.env` a partir de `.env.template` con la API Key:
```
API_KEY=

MYSQL_ROOT_PASSWORD=pass
MYSQL_DATABASE=vehicles

KAFKA_CLUSTER_ID=

KAFKA_CONTROLLER_DIRECTORY_ID=
```

2. Iniciar todos los servicios:
```bash
docker compose up -d
```

Esto levanta automáticamente: MySQL, Kafka (1 controller + 2 brokers), Kafka Connect (2 workers), HDFS (namenode + datanode), Spark (master + worker), el job de Spark Streaming, el productor de datos, Prometheus, y el conector JDBC de MySQL a Kafka.

El sistema empezará a consumir los datos, procesarlos y guardarlos en HDFS dentro de la ruta `/bda/data/vehicle_delays`. En ese punto podremos visualizarlos en Power BI conectándose a `localhost:9870`.

## Visualización en Power BI

El dashboard se divide en dos pestañas principales:
1. Autobuses y Paradas por Ruta
   - Contador de autobuses en operación
   - Media de retrasos por bus (en minutos)
   - Gráfico de barras con la media de retrasos por ruta
   - Mapa de Los Ángeles con ubicación en tiempo real de autobuses y paradas

![](img/captura_primer_pantalla_dashboard.png)

2. Usuarios
   - Contador de usuarios actuales
   - Media de personas por autobús
   - Gráfico de barras con número de personas por ruta
   - Gráfico de líneas con evolución de usuarios a lo largo del tiempo por ruta

![](img/captura_segunda_pantalla_dashboard.png)

## Resultados Obtenidos
- Procesamiento continuo y escalable de datos GTFS.
- Almacenamiento eficiente con formato Parquet.
- Visualización en tiempo real del estado del sistema de transporte.
- Análisis de rendimiento por ruta, retrasos y ocupación.
- Sistema monitorizado y preparado para escalar con más fuentes de datos.

## Business Inteligence
Usando los resultados de la 2º pestaña, podemos concluir que la línea `Westlake/Mcarthur Pk Sta - Dtwn La - Csu Dh Via Avalon` es la más concurrida actualmente. Si buscamos esa línea en la 1º pestaña se observa que cuenta con una media de retrasos de -0.95 minutos, lo que significa que lleva alrededor de 1 minuto de adelanto. Este es un funcionamiento correcto, y en principio no hace falta aplicar ningún cambio a la frecuencia de los autobuses.

Si lo aplicamos en el sentido contrario, podemos estudiar la línea `Lax/Metro Tc - Long Beach - Via Sepulveda Bl - Pch`, la 2º con más retrasos. Esta línea cuenta con 14 autobuses y 750 usuarios. Si dejamos que nuestro sistema recopile datos durante varias horas, podremos ver un historial de la afluencia de usuarios a lo largo de los días y las horas. Con esto podemos aumentar la frecuencia de autobuses cuando veamos que encontramos picos, o variar la ruta si es necesario para reducir el retraso.

## Referencias

[Enlace a página principal del sistema de metro/bus](https://developer.metro.net/gtfs-schedule-data/)

[Enlace a la Documentación de la API](https://swiftly-inc.stoplight.io/docs/realtime-standalone/d08fc97489edb-swiftly-api-reference)

[Enlace a GitLab con los datos estáticos del estándar GTFS de Los Ángeles](https://gitlab.com/LACMTA/gtfs_bus)
