Correrlo en Docker:
Creamos las imágenes y contenedores de cliente, namenode y datanode1, desde la raiz del proyecto: docker-compose up --build -d

Para crear mas datanodes dinámicamnete: docker-compose up -d --scale datanode=3 
(se crearán datanode2 y datanode3, quedando con 3 datanodes en total. La URL de los datanodes ya no aparecerá bonita como "datanode1"... Sino que aparecerá un string raro porque así es como lo crea Docker, con hash)

Para que el contenedor client vea el archivo, lo más sencillo es montar la carpeta testdata en /data del contenedor: 
docker-compose run --rm -v "$(pwd)/testdata:/data" client put /data/Luna.jpg --user nathaly

Para ver todos los datanodes enlistados: curl http://localhost:8000/namenode/list_datanodes

Comprobar metadata desde el namenode: curl "http://localhost:8000/namenode/metadata?filename=Luna.jpg (no importa si se queda parado, es como pendejo, pero en teoría debería aparecer toda la información de la tabla, junto con la lista de todos los bloques en los que se partió el archivo)

Para ver los bloques guardados en cada datanode:
docker exec nombreRaroDeDataNode ls -l /data/blocks

Para descargar el archivo (hacer get) con el cliente: 
docker-compose run --rm -v "$(pwd)/testdata:/data" client get Luna.jpg /data/salidaLuna.jpg --user nathaly

Para comparar los archivos: diff testdata/Luna.jpg testdata/salidaLuna.jpg && echo "Coinciden" || echo "Diferentes"

Para probar el ls: docker-compose run --rm client ls /user/nathaly
