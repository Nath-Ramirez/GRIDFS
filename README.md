
# Sistemas Distribuidos
# Link con video demostrativo
https://youtu.be/9fW-qQ6bOIg
# Estudiantes: 
John Esteban Úsuga Duarte, jeusugad@eafit.edu.co

Nathaly Ramirez Henao, nramirezh@eafitedu.co

Gia Mariana Calle Higuita, gmcalleh@eafit.edu.co

#

# Profesor: 
Edwin Nelson Montoya Munera, emontoya@eafit.brightspace.com

# Proyecto 1: GridFS

# 1. Descripción
Este proyecto implementa un sistema de archivos distribuido por bloques, inspirado en HDFS y GFS, utilizando Python, FastAPI y Docker. El sistema se compone de un NameNode, encargado de gestionar los metadatos y coordinar la ubicación de los bloques, varios DataNodes que almacenan físicamente los datos, y un cliente CLI que permite a los usuarios interactuar con el sistema, definir la cantidad de bloques en las que se partirá el archivo y recuperarlos para armar nuevamente el archivo.

Los archivos se dividen en bloques de tamaño fijo y se distribuyen entre los DataNodes siguiendo un esquema round-robin, lo que asegura un reparto equilibrado. El sistema soporta operaciones de subida (put), descarga (get), listado de directorios (ls), creación y eliminación de directorios (mkdir, rmdir), así como eliminación de archivos (rm). Además, incluye autenticación de usuarios con contraseñas encriptadas, y los DataNodes envían heartbeats periódicos al NameNode para reportar su estado.

Gracias al uso de Docker Compose, es posible desplegar el clúster de manera sencilla, escalar dinámicamente la cantidad de DataNodes y mantener un entorno de ejecución controlado y reproducible.
## 1.1. Aspectos que se cumplieron
- NameNode central que mantiene la tabla de metadatos
- Múltiples DataNodes que almacenan los bloques
- Cliente que interactúa con el DFS
- Operaciones básicas como: put, get, ls, rm, mkdir, rmdir
- Autenticación del cliente
- El tamaño del bloque puede ser configurable (establecido en 64 KB)
- Cada archivo se particiona en bloques distribuidos sin replicación
- Comunicación por REST sobre HTTP
- Ejecución nativa en Docker
## 1.2. Aspectos que no se cumplieron
- Despliegue en AWS
- Interfaz amigable para insertar los comandos como usuario
# 2. Información general de diseño de alto nivel, arquitectura, patrones, mejores prácticas utilizadas.
El sistema se diseñó bajo una arquitectura P2P híbrida con servidor, en la que el NameNode funciona como servidor central encargado de la coordinación y administración de metadatos, mientras que los DataNodes y los clientes actúan como pares que manejan directamente la transferencia y almacenamiento de bloques. De esta forma, el NameNode no almacena archivos, sino que facilita la interacción entre clientes y DataNodes.

El cliente tiene un rol fundamental dentro del diseño, ya que es el encargado de fragmentar los archivos en bloques al momento de subirlos (put), y al realizar una descarga (get), reconstruir el archivo en su forma original a partir de los bloques recuperados. Además, el cliente implementa comandos de gestión como ls, mkdir, rmdir y rm, y se autentica contra el sistema con usuario y contraseña para garantizar un acceso seguro.
Como patrones y mejores prácticas, se destacan:

- Separación clara de responsabilidades: el NameNode gestiona metadatos, los DataNodes almacenan bloques y el cliente orquesta la lógica de división y reconstrucción.
- Uso de APIs REST sobre HTTP para mantener un sistema desacoplado, modular y fácil de escalar.
- Algoritmo round-robin para distribuir bloques de manera balanceada entre los DataNodes.
- Heartbeats enviados por los DataNodes al NameNode, permitiendo monitorear disponibilidad.
- Orquestación con Docker Compose, que facilita el despliegue y escalado dinámico del número de DataNodes.
# 3. Descripción del ambiente de desarrollo y técnico: lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.
El proyecto fue desarrollado en Python 3.11, utilizando un conjunto de librerías y paquetes que soportan tanto la comunicación vía APIs REST como la gestión de seguridad y persistencia de datos. Las dependencias principales son:

- FastAPI 0.95+ → framework para construir las APIs REST del NameNode y los DataNodes.
- Uvicorn 0.22+ → servidor ASGI utilizado para desplegar los servicios web.
- Requests 2.31+ → librería empleada por el cliente CLI para comunicarse con los servicios.
- SQLite3 (módulo estándar de Python) → base de datos ligera para almacenar metadatos en el NameNode.
- bcrypt 4.0+ → librería para encriptar contraseñas de usuarios y garantizar autenticación segura.
- Docker 24+ y Docker Compose 2+ → herramientas de contenedorización y orquestación, que permiten desplegar el NameNode, los DataNodes y el cliente de forma aislada y escalable.
- El código se estructuró en tres componentes principales (namenode, datanode, client), cada uno con su propio contenedor y definido a partir de imágenes basadas en python:3.11-slim. Esto asegura un entorno reproducible y consistente en cualquier máquina de desarrollo o producción.

## ¿Cómo se compila?
Como se explicó anteriormente, la ejecución se hace a través de docker. Para esto se utilizan los siguientes comandos:

Este comando se usa para crear las imagenes e iniciar los conteiners en Docker: ```docker-compose up```

Este comando se usa para iniciar 3 datanodes en el sistema FS: ```docker-compose up -d --scale datanode=3```

Este comando se usa para registrar un usuario. Después de `register` se introduce el nombre del usuario y su contraseña: ```docker-compose run --rm client register john 123321```

Este comando se usa para hacer put. El archivo Luna.jpg se debe encontrar dentro de la carpeta testdata y debe existir el usuario y contraseña: ```docker-compose run --rm -v "$(pwd)/testdata:/data" client put /data/Luna.jpg --user john --password 123321```

Este comando se usa para hacer get del archivo Luna.jpg. La reconstrucción se guardará en la carpeta testdata como salidaLuna.jpg: ```docker-compose run --rm -v "$(pwd)/testdata:/data" client get Luna.jpg /data/salidaLuna.jpg --user john --password 123321```

Este comando sirve para ver las diferencias entre archivo original y el recuperado: ```diff testdata/Luna.jpg testdata/salidaLuna.jpg```

Este comando se usa para crear una carpeta, en este caso "docs": ```docker-compose run --rm client mkdir /user/demo/docs --user john --password 123321```

Este comando se usa para listar una carpeta, en este caso "docs": ```docker-compose run --rm client ls /user/demo/docs --user john --password 123321```

Este comando se usa para eliminar un archivo, en este caso "Luna.jpg" ```docker-compose run --rm client rm Luna.jpg --user john --password 123321```

## Detalles del desarrollo
El sistema fue implementado en Python 3.11, siguiendo una arquitectura P2P híbrida con servidor. El desarrollo se dividió en tres módulos principales: NameNode, DataNode y Cliente (CLI).

- El NameNode se encarga de la gestión de metadatos, autenticación de usuarios y coordinación de la distribución de bloques.
- Los DataNodes almacenan físicamente los bloques y envían heartbeats periódicos al NameNode para notificar su estado.
- El Cliente implementa la lógica de dividir archivos en bloques al subirlos (put), reconstruirlos al descargarlos (get) y ejecutar operaciones de gestión como ls, rm, mkdir y rmdir.
- Se trabajó bajo un esquema modular, con APIs REST expuestas mediante FastAPI y orquestación de servicios con Docker Compose, lo que permite desplegar el sistema de forma aislada, portable y escalable.
## Detalles técnicos
- Lenguaje: Python 3.11
- Framework principal: FastAPI (para NameNode y DataNode)
- Servidor ASGI: Uvicorn
- Base de datos: SQLite (para metadatos del NameNode)
- Autenticación: bcrypt (encriptación de contraseñas)
- Cliente: script en Python con librería requests
- Contenedores: Docker + Docker Compose (para crear NameNode, múltiples DataNodes y cliente)
- Imagen base: python:3.11-slim
- Arquitectura: P2P híbrida con servidor central (NameNode)
- Distribución de bloques: algoritmo round-robin
  
## Descripción y configuración parámetros del proyecto
El proyecto utiliza variables de entorno y parámetros configurables en los Dockerfiles y docker-compose.yml. Los más relevantes son:

### NameNode:
  
NN_DB=/data/metadata.db → ruta de la base de datos SQLite que guarda los metadatos.

Puerto expuesto: 8000 (configurable en docker-compose).

### DataNode:

DATA_DIR=/data/blocks → ruta donde se guardan los bloques en cada nodo.

Puerto expuesto: 8001 (puede cambiar al escalar nodos).

Heartbeat automático al NameNode cada 10s.

### Cliente (CLI):

- Parámetros de ejecución:

--user <usuario> y --password <contraseña> → autenticación.

--dest <directorio> → destino dentro del DFS para almacenar archivos.

--block_size <n> → tamaño del bloque en bytes (por defecto: 64 KB).

- Montaje de volumen: -v "$(pwd)/testdata:/data" para acceder a archivos locales desde el contenedor.

### Configuración en Docker Compose:

Escalar DataNodes: docker-compose up -d --scale datanode=N

Red interna de Docker para comunicación entre nodos.
## Detalles de la organización del código por carpetas o descripción de algún archivo.
<img width="240" height="322" alt="image" src="https://github.com/user-attachments/assets/0b7cf6c9-5511-4c1b-9e0f-8569d55cd6b3" />

### Carpeta client:
- cli.py: implementa la interfaz de línea de comandos (CLI) que permite a los usuarios interactuar con el sistema, ejecutar operaciones como put, get, ls, rm, mkdir y rmdir, además de manejar la autenticación de usuarios.
- Dockerfile: define la imagen del contenedor cliente, instalando dependencias y configurando el punto de entrada.
- requirements.txt: lista de librerías necesarias para el cliente (principalmente requests).
### Carpeta datanode:
- app.py: servicio REST que gestiona el almacenamiento de bloques. Incluye endpoints para almacenar, recuperar, listar y eliminar bloques, así como el envío de heartbeats al NameNode.
- Dockerfile: especifica la configuración del contenedor DataNode.
- requirements.txt: dependencias del servicio DataNode (FastAPI, Uvicorn, etc.).
### Carpeta namenode:
- app.py: servicio central que administra metadatos, usuarios, directorios y la asignación de bloques a los DataNodes. También maneja la autenticación y recibe heartbeats de los nodos.
- Dockerfile: configura el contenedor del NameNode.
- requirements.txt: librerías necesarias para ejecutar el servicio (FastAPI, bcrypt, SQLite, etc.).
### Carpeta testdata:
- Carpeta local utilizada para almacenar archivos de prueba que se suben y descargan a través del cliente. Se monta como volumen dentro del contenedor para facilitar la interacción entre el host y el sistema distribuido.
### Archivo docker-compose.yml:
- Archivo de orquestación que define y coordina los servicios (NameNode, DataNodes y cliente). Permite escalar el número de DataNodes y configurar redes internas para la comunicación entre los contenedores.

# 4. Descripción del ambiente de EJECUCIÓN (en producción) lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.

## Descripción y configuración de parámetros del proyecto

El proyecto está diseñado para ejecutarse en **Docker**, lo que facilita la configuración de red, almacenamiento y despliegue de servicios.  

### Componentes y Puertos
- **NameNode**
  - Puerto: `8000`
  - Función: gestiona los metadatos, usuarios y directorios.
- **DataNodes**
  - Puerto: `8001` (puede variar al escalar con Docker)
  - Función: almacenan físicamente los bloques de datos.
- **Cliente (CLI)**
  - Interactúa con NameNode y DataNodes mediante solicitudes REST.

### Base de Datos
- Se utiliza **SQLite** en el NameNode para guardar:
  - Archivos y sus metadatos.
  - Usuarios y contraseñas (encriptadas con **bcrypt**).
  - Directorios lógicos.
- Ruta de la base: configurada con `NN_DB` (por defecto `metadata.db`).

### Variables de Entorno
- `NN_DB`: ubicación de la base de datos del NameNode.
- `DATANODES`: lista inicial de DataNodes (opcional).
- `DATA_DIR`: directorio en cada DataNode donde se guardan los bloques (`/data/blocks` por defecto).
- `NAMENODE_URL`: URL del NameNode (por defecto `http://namenode:8000`).
- `BLOCK_SIZE`: tamaño de bloque en bytes (default: 64 KB).

### Parámetros del Cliente CLI
- `--user` y `--password`: credenciales del usuario.
- `--dest`: directorio destino dentro del DFS.
- `--block_size`: tamaño de bloque configurable al subir un archivo.
- Comandos disponibles: `put`, `get`, `ls`, `rm`, `mkdir`, `rmdir`, `register`.

### Otros detalles
- Los DataNodes envían **heartbeats** cada 10 segundos al NameNode para indicar que están activos.
- El sistema soporta **multiusuario** con autenticación y gestión de directorios lógicos.

## ¿Cómo se lanza el servidor?

El sistema se ejecuta con **Docker Compose** desde la raíz del proyecto:

```docker-compose up --build -d```

Esto levanta los contenedores del NameNode, los DataNodes y el Cliente.

Para escalar el número de DataNodes se puede usar, por ejemplo:

```docker-compose up -d --scale datanode=3```

# 5. Información adicional

## Referencias

https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html

https://www.analyticsvidhya.com/blog/2022/04/an-overview-of-hdfs-namenodes-and-datanodes/
