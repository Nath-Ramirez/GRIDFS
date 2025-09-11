# datanode/app.py
import os
import hashlib
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import aiofiles
import requests
from pydantic import BaseModel
from typing import Optional
from starlette.background import BackgroundTasks

DATA_DIR = os.environ.get("DATA_DIR", "/data/blocks")
NAMENODE = os.environ.get("NAMENODE_URL", "http://namenode:8000")
SELF_URL = os.environ.get("DATANODE_URL", "http://datanode:8001")

os.makedirs(DATA_DIR, exist_ok=True)

app = FastAPI(title="DataNode - GridDFS")

class RegInfo(BaseModel):
    datanode_url: str
    capacity: int = -1
    free: int = -1

@app.on_event("startup")
def register_to_namenode():
    try:
        info = {"datanode_url": SELF_URL, "capacity": -1, "free": -1}
        requests.post(f"{NAMENODE}/namenode/register_datanode", json=info, timeout=3)
        print("Registered to NameNode:", NAMENODE)
    except Exception as e:
        print("Could not register to NameNode:", e)

@app.post("/datanode/store_block")
async def store_block(block_id: str, file: UploadFile = File(...)):
    safe_name = block_id.replace("/", "_")
    path = os.path.join(DATA_DIR, safe_name)
    content = await file.read()
    # write atomically
    tmp = path + ".tmp"
    async with aiofiles.open(tmp, "wb") as f:
        await f.write(content)
    os.replace(tmp, path)
    checksum = hashlib.sha256(content).hexdigest()
    return {"status":"ok", "block_id": safe_name, "size": len(content), "checksum": checksum}

@app.get("/datanode/get_block")
def get_block(block_id: str):
    safe_name = block_id.replace("/", "_")
    path = os.path.join(DATA_DIR, safe_name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="block not found")
    def iterfile():
        with open(path, "rb") as f:
            while True:
                chunk = f.read(64*1024)
                if not chunk:
                    break
                yield chunk
    return StreamingResponse(iterfile(), media_type="application/octet-stream")

@app.get("/datanode/list_blocks")
def list_blocks():
    items = []
    for name in os.listdir(DATA_DIR):
        p = os.path.join(DATA_DIR, name)
        if os.path.isfile(p):
            items.append({"block_id": name, "size": os.path.getsize(p)})
    return {"blocks": items}
