# namenode/app.py
import sqlite3
import threading
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import os
from datetime import datetime

DB_PATH = os.environ.get("NN_DB", "metadata.db")
lock = threading.Lock()
app = FastAPI(title="NameNode - GridDFS (SQLite)")

# --- DB helpers --------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS datanodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        capacity INTEGER,
        free INTEGER,
        last_seen TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT UNIQUE,
        owner TEXT,
        size INTEGER,
        block_size INTEGER,
        status TEXT,
        created_at TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS blocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER,
        block_index INTEGER,
        block_id TEXT UNIQUE,
        datanode_url TEXT,
        size INTEGER,
        checksum TEXT,
        present INTEGER DEFAULT 0,
        FOREIGN KEY(file_id) REFERENCES files(id)
    )""")
    conn.commit()
    conn.close()

def db_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

init_db()

# --- Pydantic models ---------------------------------------------------
class RegisterDN(BaseModel):
    datanode_url: str
    capacity: int = None
    free: int = None

class AllocateReq(BaseModel):
    filename: str
    num_blocks: int
    user: str = "demo"
    block_size: int = None

class ConfirmBlockReq(BaseModel):
    filename: str
    block_index: int
    block_id: str
    datanode_url: str
    size: int
    checksum: str

# --- Endpoints --------------------------------------------------------
@app.post("/namenode/register_datanode")
def register_datanode(info: RegisterDN):
    with lock:
        conn = db_conn()
        c = conn.cursor()
        now = datetime.utcnow().isoformat()
        c.execute("INSERT OR REPLACE INTO datanodes(url, capacity, free, last_seen) VALUES (?, ?, ?, ?)",
                  (info.datanode_url, info.capacity or -1, info.free or -1, now))
        conn.commit()
        conn.close()
    return {"status": "ok", "datanode_url": info.datanode_url}

@app.post("/namenode/allocate_blocks")
def allocate_blocks(req: AllocateReq):
    with lock:
        conn = db_conn()
        c = conn.cursor()

        # get datanodes
        c.execute("SELECT url FROM datanodes ORDER BY id")
        dns = [row[0] for row in c.fetchall()]
        if len(dns) == 0:
            raise HTTPException(status_code=503, detail="no datanodes registered")

        # create file entry (if not exists) with status incomplete
        now = datetime.utcnow().isoformat()
        c.execute("INSERT OR IGNORE INTO files(filename, owner, size, block_size, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                  (req.filename, req.user, None, req.block_size or 0, "incomplete", now))
        conn.commit()

        # get file id
        c.execute("SELECT id FROM files WHERE filename=?", (req.filename,))
        file_id = c.fetchone()[0]

        allocation = []
        # create placeholder block rows
        for i in range(req.num_blocks):
            dn = dns[i % len(dns)]
            block_id = f"{req.filename}__{i}__{uuid.uuid4().hex}"
            c.execute("""INSERT OR IGNORE INTO blocks(file_id, block_index, block_id, datanode_url, size, checksum, present)
                         VALUES (?, ?, ?, ?, ?, ?, ?)""",
                      (file_id, i, block_id, dn, 0, "", 0))
            allocation.append({"block_index": i, "datanode_url": dn, "block_id": block_id})
        conn.commit()
        conn.close()
    return {"allocation": allocation}

@app.post("/namenode/confirm_block")
def confirm_block(info: ConfirmBlockReq):
    with lock:
        conn = db_conn()
        c = conn.cursor()
        # find file
        c.execute("SELECT id FROM files WHERE filename=?", (info.filename,))
        row = c.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="file not found")
        file_id = row[0]

        # update block
        c.execute("""UPDATE blocks SET size=?, checksum=?, present=1, datanode_url=?
                     WHERE file_id=? AND block_index=? AND block_id=?""",
                  (info.size, info.checksum, info.datanode_url, file_id, info.block_index, info.block_id))
        conn.commit()

        # check if all blocks present
        c.execute("SELECT COUNT(*) FROM blocks WHERE file_id=?",(file_id,))
        total = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM blocks WHERE file_id=? AND present=1",(file_id,))
        present = c.fetchone()[0]
        if present == total and total > 0:
            # set file available and compute total size
            c.execute("SELECT SUM(size) FROM blocks WHERE file_id=?", (file_id,))
            total_size = c.fetchone()[0] or 0
            c.execute("UPDATE files SET status='available', size=?, block_size=? WHERE id=?",
                      (total_size, None, file_id))
        conn.commit()
        conn.close()
    return {"status":"ok"}

@app.get("/namenode/metadata")
def get_metadata(filename: str):
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT id, owner, size, block_size, status, created_at FROM files WHERE filename=?", (filename,))
    r = c.fetchone()
    if not r:
        conn.close()
        raise HTTPException(status_code=404, detail="file not found")
    file_id, owner, size, block_size, status, created_at = r
    c.execute("""SELECT block_index, block_id, datanode_url, size, checksum, present
                 FROM blocks WHERE file_id=? ORDER BY block_index""", (file_id,))
    blocks = []
    for b in c.fetchall():
        blocks.append({
            "block_index": b[0],
            "block_id": b[1],
            "datanode_url": b[2],
            "size": b[3],
            "checksum": b[4],
            "present": bool(b[5])
        })
    conn.close()
    return {
        "filename": filename,
        "owner": owner,
        "size": size,
        "block_size": block_size,
        "status": status,
        "created_at": created_at,
        "blocks": blocks
    }

@app.get("/namenode/list_datanodes")
def list_datanodes():
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT url, capacity, free, last_seen FROM datanodes")
    res = [{"url":r[0],"capacity":r[1],"free":r[2],"last_seen":r[3]} for r in c.fetchall()]
    conn.close()
    return {"datanodes": res}
