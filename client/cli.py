import argparse
import os
import requests

NAMENODE = os.environ.get("NAMENODE", "http://localhost:5000")

def put(file_path, user="demo", block_size=8*1024*1024):
    """Sube un archivo completo al NameNode"""
    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f)}
        data = {"user": user, "block_size": str(block_size)}
        r = requests.post(f"{NAMENODE}/upload", files=files, data=data)
        r.raise_for_status()
        print("PUT ok:", r.json())

def get(fname, out_path):
    """Descarga un archivo del NameNode"""
    r = requests.get(f"{NAMENODE}/download/{fname}", stream=True)
    if r.status_code != 200:
        print("Error:", r.json())
        return
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print("GET ok:", out_path)

def ls():
    """Lista archivos registrados"""
    r = requests.get(f"{NAMENODE}/files")
    r.raise_for_status()
    for f in r.json():
        print(f'{f["name"]}  owner={f["owner"]}  size={f["size"]}')

def rm(fname):
    """Elimina un archivo del sistema"""
    r = requests.delete(f"{NAMENODE}/files/{fname}")
    print("RM status:", r.status_code, r.json())

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Cliente GridDFS (versión DB centralizada)")
    sub = ap.add_subparsers(dest="cmd")

    ap_put = sub.add_parser("put")
    ap_put.add_argument("path")
    ap_put.add_argument("--user", default="demo")
    ap_put.add_argument("--block", type=int, default=8*1024*1024)

    ap_get = sub.add_parser("get")
    ap_get.add_argument("name")
    ap_get.add_argument("out")

    sub.add_parser("ls")

    ap_rm = sub.add_parser("rm")
    ap_rm.add_argument("name")

    args = ap.parse_args()

    if args.cmd == "put":
        put(args.path, args.user, args.block)
    elif args.cmd == "get":
        get(args.name, args.out)
    elif args.cmd == "ls":
        ls()
    elif args.cmd == "rm":
        rm(args.name)
    else:
        ap.print_help()
