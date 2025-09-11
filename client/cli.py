# client/client.py
import requests
import hashlib
import os
import sys
import argparse

NAMENODE = os.environ.get("NAMENODE_URL", "http://namenode:8000")
BLOCK_SIZE = int(os.environ.get("BLOCK_SIZE", 8*1024*1024))

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def put_file(path, user="demo"):
    filename = os.path.basename(path)
    # count blocks and sizes
    sizes = []
    with open(path,"rb") as f:
        while True:
            chunk = f.read(BLOCK_SIZE)
            if not chunk:
                break
            sizes.append(len(chunk))
    num_blocks = len(sizes)
    if num_blocks == 0:
        print("Empty file")
        return
    # request allocation
    r = requests.post(f"{NAMENODE}/namenode/allocate_blocks", json={"filename": filename, "num_blocks": num_blocks, "user": user, "block_size": BLOCK_SIZE})
    if r.status_code != 200:
        print("Allocation failed:", r.text); return
    allocation = r.json()["allocation"]
    # upload blocks
    with open(path,"rb") as f:
        for alloc in allocation:
            i = alloc["block_index"]
            block_id = alloc["block_id"]
            datanode_url = alloc["datanode_url"]
            chunk = f.read(BLOCK_SIZE)
            files = {"file": (f"{block_id}.bin", chunk)}
            try:
                resp = requests.post(f"{datanode_url}/datanode/store_block", params={"block_id": block_id}, files=files, timeout=10)
                if resp.status_code != 200:
                    print("Upload failed to", datanode_url, resp.text); return
                info = resp.json()
                checksum = info["checksum"]
                size = info["size"]
                # confirm to namenode
                conf = {
                    "filename": filename,
                    "block_index": i,
                    "block_id": block_id,
                    "datanode_url": datanode_url,
                    "size": size,
                    "checksum": checksum
                }
                rc = requests.post(f"{NAMENODE}/namenode/confirm_block", json=conf)
                if rc.status_code != 200:
                    print("Confirm failed:", rc.text); return
                print(f"Uploaded block {i} -> {datanode_url} ({size} bytes)")
            except Exception as e:
                print("Error uploading block:", e)
                return
    print("Upload finished")

def get_file(filename, outpath):
    r = requests.get(f"{NAMENODE}/namenode/metadata", params={"filename": filename})
    if r.status_code != 200:
        print("Metadata error:", r.text); return
    meta = r.json()
    if meta["status"] != "available":
        print("File not available yet. Status:", meta["status"])
    with open(outpath, "wb") as outf:
        for b in sorted(meta["blocks"], key=lambda x: x["block_index"]):
            dn = b["datanode_url"]
            block_id = b["block_id"]
            resp = requests.get(f"{dn}/datanode/get_block", params={"block_id": block_id}, stream=True)
            if resp.status_code != 200:
                print("Error getting block:", resp.text); return
            for chunk in resp.iter_content(64*1024):
                outf.write(chunk)
            print(f"Downloaded block {b['block_index']} from {dn}")
    print("Download finished")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    p_put = sub.add_parser("put")
    p_put.add_argument("path")
    p_put.add_argument("--user", default="demo")
    p_get = sub.add_parser("get")
    p_get.add_argument("filename")
    p_get.add_argument("outpath")
    args = parser.parse_args()
    if args.cmd == "put":
        put_file(args.path, args.user)
    elif args.cmd == "get":
        get_file(args.filename, args.outpath)
    else:
        parser.print_help()
