# client/cli.py
import requests
import hashlib
import os
import argparse

NAMENODE = os.environ.get("NAMENODE_URL", "http://namenode:8000")
BLOCK_SIZE = int(os.environ.get("BLOCK_SIZE", 64*1024))  #64 KB 

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def put_file(path, user="", password="", dest="" , block_size=0):
    base = os.path.basename(path)
    if dest:
        filename = f"/user/{user}/{dest.strip('/')}/{base}"
    else:
        filename = f"/user/{user}/{base}"
    if block_size > 0:
        global BLOCK_SIZE
        BLOCK_SIZE = block_size*1024  # Convertir a bytes
    # calcular cuántos bloques necesitamos
    sizes = []
    with open(path, "rb") as f:
        while True:
            chunk = f.read(BLOCK_SIZE)
            if not chunk:
                break
            sizes.append(len(chunk))
    num_blocks = len(sizes)
    if num_blocks == 0:
        print("Archivo vacío")
        return

    # pedir asignación de bloques al NameNode
    resp = requests.post(f"{NAMENODE}/namenode/allocate_blocks", json={
        "filename": filename,
        "num_blocks": num_blocks,
        "user": user,
        "password": password,
        "block_size": BLOCK_SIZE,
        "dest": dest
    })
    if resp.status_code != 200:
        print("Error al pedir asignación:", resp.text)
        return
    allocation = resp.json()["allocation"]

    # subir cada bloque al DataNode correspondiente
    with open(path, "rb") as f:
        for alloc in allocation:
            i = alloc["block_index"]
            block_id = alloc["block_id"]
            datanode_url = alloc["datanode_url"]

            chunk = f.read(BLOCK_SIZE)
            files = {"file": (f"{block_id}.bin", chunk)}

            print(f"Subiendo bloque {i} a {datanode_url}...")
            r = requests.post(f"{datanode_url}/datanode/store_block",
                              params={"block_id": block_id},
                              files=files)
            if r.status_code != 200:
                print("Error al subir bloque:", r.text)
                return

            info = r.json()
            checksum = info["checksum"]
            size = info["size"]

            # confirmar en el NameNode
            confirm = {
                "filename": filename,
                "block_index": i,
                "block_id": block_id,
                "datanode_url": datanode_url,
                "size": size,
                "checksum": checksum,
                "user": user,
                "password": password,
                "dest": dest

            }
            rc = requests.post(f"{NAMENODE}/namenode/confirm_block", json=confirm)
            if rc.status_code != 200:
                print("Error al confirmar bloque:", rc.text)
                return

            print(f"Bloque {i} confirmado ({size} bytes)")

    print("Subida completa :)")

def get_file(filename, outpath, user="", password=""):
    # Construir la ruta completa con el usuario para pedirle a NameNode
    if not filename.startswith("/user/"): #Verificamos si no ingresaron la ruta completa
        filename = f"/user/{user}/{filename}"
        
    r = requests.get(f"{NAMENODE}/namenode/metadata", params={"filename": filename, "user": user, "password": password})
    
    if r.status_code != 200:
        print("Error al obtener metadata:", r.text)
        return
    meta = r.json()

    if meta["status"] != "available":
        print(f"El archivo todavía no está disponible (status={meta['status']})")
        return

    # descargar y reconstruir archivo
    with open(outpath, "wb") as outf:
        for b in sorted(meta["blocks"], key=lambda x: x["block_index"]):
            dn = b["datanode_url"]
            block_id = b["block_id"]

            print(f"Descargando bloque {b['block_index']} desde {dn}...")
            r2 = requests.get(f"{dn}/datanode/get_block", params={"block_id": block_id}, stream=True)
            if r2.status_code != 200:
                print("Error al descargar bloque:", r2.text)
                return
            for chunk in r2.iter_content(64*1024):
                outf.write(chunk)
            print(f"Bloque {b['block_index']} descargado ({b['size']} bytes)")

    print(f"Archivo reconstruido en {outpath} :)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cliente GridDFS simplificado")
    sub = parser.add_subparsers(dest="cmd")

    # comando put
    p_put = sub.add_parser("put", help="Subir archivo")
    p_put.add_argument("path", help="Ruta local del archivo")
    p_put.add_argument("--user", default="", help="Usuario")
    p_put.add_argument("--dest", default="", help="Directorio destino en el DFS")
    p_put.add_argument("--password", default="", help="Contraseña")
    p_put.add_argument("--block_size", type=int, default=0, help="Tamaño de bloque en bytes (default: 64kb)")


    # comando get
    p_get = sub.add_parser("get", help="Descargar archivo")
    p_get.add_argument("filename", help="Nombre del archivo en el sistema")
    p_get.add_argument("outpath", help="Ruta de salida")
    p_get.add_argument("--user", default="", help="Usuario")
    p_get.add_argument("--password", default="", help="Contraseña")
    
    # comando ls
    p_ls = sub.add_parser("ls", help="Listar archivos en un directorio")
    p_ls.add_argument("path", nargs="?", default="/", help="Ruta del directorio")
    p_ls.add_argument("--user", default="", help="Usuario")
    p_ls.add_argument("--password", default="", help="Contraseña")

    # comando rm
    p_rm = sub.add_parser("rm", help="Eliminar un archivo")
    p_rm.add_argument("filename", help="Archivo a eliminar")
    p_rm.add_argument("--user", default="", help="Usuario")
    p_rm.add_argument("--password", default="", help="Contraseña")


    # comando mkdir
    p_mkdir = sub.add_parser("mkdir", help="Crear directorio lógico")
    p_mkdir.add_argument("path", help="Ruta del directorio a crear (ej: /user/demo/docs)")
    p_mkdir.add_argument("--user", default="", help="Usuario")
    p_mkdir.add_argument("--password", default="", help="Contraseña")

    # comando rmdir
    p_rmdir = sub.add_parser("rmdir", help="Eliminar directorio lógico")
    p_rmdir.add_argument("path", help="Ruta del directorio a eliminar")
    p_rmdir.add_argument("--user", default="", help="Usuario")
    p_rmdir.add_argument("--password", default="", help="Contraseña")

    # comando register
    p_register = sub.add_parser("register", help="Registrar un nuevo usuario")
    p_register.add_argument("username", help="Nombre de usuario a registrar")
    p_register.add_argument("password", help="Contraseña del usuario a registrar")

    args = parser.parse_args()

    if args.cmd == "put":
        put_file(args.path, args.user, args.password, args.dest, args.block_size)
    elif args.cmd == "get":
        get_file(args.filename, args.outpath, args.user, args.password)
    elif args.cmd == "ls":
        r = requests.get(f"{NAMENODE}/namenode/ls", params={"path": args.path, "user": args.user, "password": args.password})
        if r.status_code == 200:
            data = r.json()
            if not data.get("files"):
                print(f"(vacío) No hay archivos en {args.path}")
            for f in data.get("files", []):
                print(f"{f['filename']} - {f['size']} bytes - {f['status']}")
        else:
            print("Error:", r.text)

    elif args.cmd == "rm":
        fname = args.filename
        if not fname.startswith("/user/"):
            fname = f"/user/{args.user}/{fname}"
        r = requests.delete(f"{NAMENODE}/namenode/delete_file", params={"filename": fname, "user": args.user, "password": args.password})
        print(r.json() if r.status_code == 200 else f"Error: {r.text}")
    elif args.cmd == "mkdir":
        r = requests.post(f"{NAMENODE}/namenode/mkdir", json={"path": args.path, "user": args.user, "password": args.password})
        print(r.json() if r.status_code == 200 else f"Error: {r.text}")

    elif args.cmd == "rmdir":
        r = requests.post(f"{NAMENODE}/namenode/rmdir", json={"path": args.path, "user": args.user, "password": args.password})
        print(r.json() if r.status_code == 200 else f"Error: {r.text}")

    elif args.cmd == "register":
        r = requests.post(f"{NAMENODE}/namenode/register", json={"username": args.username, "password": args.password})
        print(r.json() if r.status_code == 200 else f"Error: {r.text}")

    else:
        parser.print_help()
