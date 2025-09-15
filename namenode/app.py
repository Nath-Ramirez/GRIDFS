# namenode/app.py
import sqlite3
import threading
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import os, json
from datetime import datetime, timedelta
from passlib.context import CryptContext



DB_PATH = os.environ.get("NN_DB", "metadata.db")
lock = threading.Lock()
app = FastAPI(title="NameNode - GridDFS (SQLite single-table + datanode registry)")

# Contexto de hashing para contraseñas. Usamos el algoritmo bcrypt.
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- DB helpers --------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # tabla de archivos (metadatos)
    c.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT UNIQUE,
        owner TEXT,
        size INTEGER,
        block_size INTEGER,
        status TEXT,
        created_at TEXT,
        blocks_json TEXT
    )""")
    # tabla de datanodes (registro dinámico)
    c.execute("""
    CREATE TABLE IF NOT EXISTS datanodes (
        url TEXT PRIMARY KEY,
        capacity INTEGER,
        free INTEGER,
        last_seen TEXT
    )""")
    # tabla para usuarios (autenticación)
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password_hash TEXT
    )""")
    conn.commit()

    # seed inicial desde variable de entorno DATANODES (si viene definida)
    env = os.environ.get("DATANODES")
    if env:
        now = datetime.utcnow().isoformat()
        for url in [u.strip() for u in env.split(",") if u.strip()]:
            c.execute("INSERT OR IGNORE INTO datanodes(url, capacity, free, last_seen) VALUES (?, ?, ?, ?)",
                      (url, -1, -1, now))

    # Si no existe un usuario "demo", se crea
    c.execute("SELECT username FROM users WHERE username='demo'")
    if not c.fetchone():
        hashed_pass = pwd_context.hash("demo")
        c.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", ("demo", hashed_pass))
    conn.commit()
    conn.close()

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_user_from_db(username, conn):
    c = conn.cursor()
    c.execute("SELECT password_hash FROM users WHERE username=?", (username,))
    row = c.fetchone()
    if row:
        return {"username": username, "password_hash": row[0]}
    return None

def auth_user(username, password):
    """Función de ayuda para autenticar un usuario en cada endpoint."""
    conn = db_conn()
    user_data = get_user_from_db(username, conn)
    conn.close()
    if user_data and verify_password(password, user_data["password_hash"]):
        return True
    return False

def get_active_datanodes(conn, timeout=30):
    now = datetime.utcnow()
    c = conn.cursor()
    rows = c.execute("SELECT url, last_seen FROM datanodes").fetchall()
    active = []
    for url, last_seen in rows:
        if last_seen:
            last_seen_dt = datetime.fromisoformat(last_seen)
            if now - last_seen_dt < timedelta(seconds=timeout):
                active.append(url)
    return active

def db_conn():
    # cada llamada obtiene una conexión con check_same_thread=False para uso multi-hilo
    return sqlite3.connect(DB_PATH, check_same_thread=False)

init_db()

# --- Pydantic models ---------------------------------------------------

class BaseAuth(BaseModel):
    user: str
    password: str
class AllocateReq(BaseAuth):
    filename: str
    num_blocks: int
    block_size: int = None

class ConfirmBlockReq(BaseAuth):
    filename: str
    block_index: int
    block_id: str
    datanode_url: str
    size: int
    checksum: str

class RegInfo(BaseModel):
    datanode_url: str
    capacity: int = -1
    free: int = -1

class MkdirReq(BaseAuth):
    path: str

class RmdirReq(BaseAuth):
    path: str

class UserReq(BaseModel):
    username: str
    password: str
# --- Helper -----------------------------------------------------------
def get_registered_datanodes(conn=None):
    """Retorna lista de URLs de datanodes registrados (en orden arbitario)."""
    close_conn = False
    if conn is None:
        conn = db_conn()
        close_conn = True
    c = conn.cursor()
    rows = c.execute("SELECT url, capacity, free, last_seen FROM datanodes").fetchall()
    if close_conn:
        conn.close()
    return [{"url": r[0], "capacity": r[1], "free": r[2], "last_seen": r[3]} for r in rows]

# --- Endpoints --------------------------------------------------------
@app.post("/namenode/heartbeat")
def heartbeat(info: RegInfo):
    """
    Recibe un pulso de vida desde un DataNode y actualiza last_seen.
    """
    with lock:
        conn = db_conn()
        c = conn.cursor()
        now = datetime.utcnow().isoformat()
        c.execute("UPDATE datanodes SET last_seen=? WHERE url=?", (now, info.datanode_url))
        conn.commit()
        conn.close()
    return {"status": "ok", "msg": f"Heartbeat recibido de {info.datanode_url}"}

@app.post("/namenode/register_datanode")
def register_datanode(info: RegInfo):
    """
    Permite que un DataNode se registre o actualice su información.
    El caller (DataNode) debe enviar datanode_url, capacity y free (opcional).
    """
    with lock:
        conn = db_conn()
        c = conn.cursor()
        now = datetime.utcnow().isoformat()
        # si ya existe, actualiza; si no, inserta
        c.execute("SELECT url FROM datanodes WHERE url=?", (info.datanode_url,))
        if c.fetchone():
            c.execute("UPDATE datanodes SET capacity=?, free=?, last_seen=? WHERE url=?",
                      (info.capacity, info.free, now, info.datanode_url))
        else:
            c.execute("INSERT INTO datanodes(url, capacity, free, last_seen) VALUES (?, ?, ?, ?)",
                      (info.datanode_url, info.capacity, info.free, now))
        conn.commit()
        datanodes = [r[0] for r in c.execute("SELECT url FROM datanodes").fetchall()]
        conn.close()
    return {"status": "ok", "datanodes": datanodes}

@app.get("/namenode/list_datanodes")
def list_datanodes():
    """Devuelve información de los datanodes registrados."""
    return {"datanodes": get_registered_datanodes()}

@app.post("/namenode/allocate_blocks")
def allocate_blocks(req: AllocateReq):
    """
    Asigna bloques para un archivo; usa la lista actual de datanodes registrados.
    La asignación se hace en round-robin sobre la lista de datanodes conocida.
    """
    if not auth_user(req.user, req.password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    
    with lock:
        conn = db_conn()
        c = conn.cursor()

        # obtener datanodes activos (last_seen < 60 segundos desde ahora)
        active_datanodes = get_active_datanodes(conn, timeout=60)
        datanodes = active_datanodes
        datanode_rows = c.execute("SELECT url FROM datanodes").fetchall() 
        datanodes = [r[0] for r in datanode_rows]
        if not datanodes:
            # si no hay ninguno registrado, fallback a la var de entorno (por compatibilidad)
            env = os.environ.get("DATANODES")
            if env:
                datanodes = [u.strip() for u in env.split(",") if u.strip()]

        if not datanodes:
            conn.close()
            raise HTTPException(status_code=503, detail="no datanodes available")

        # crear entry file si no existe
        now = datetime.utcnow().isoformat()
        c.execute("INSERT OR IGNORE INTO files(filename, owner, size, block_size, status, created_at, blocks_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (req.filename, req.user, None, req.block_size or 0, "incomplete", now, "[]"))
        conn.commit()

        allocation = []
        blocks = []
        for i in range(req.num_blocks):
            dn = datanodes[i % len(datanodes)]
            block_id = f"{req.filename}__{i}__{uuid.uuid4().hex}"
            block_entry = {
                "block_index": i,
                "block_id": block_id,
                "datanode_url": dn,
                "size": 0,
                "checksum": "",
                "present": False
            }
            blocks.append(block_entry)
            allocation.append({
                "block_index": i,
                "datanode_url": dn,
                "block_id": block_id
            })

        # guardar en la BD
        c.execute("UPDATE files SET blocks_json=? WHERE filename=?", (json.dumps(blocks), req.filename))
        conn.commit()
        conn.close()

    return {"allocation": allocation}

@app.post("/namenode/confirm_block")
def confirm_block(info: ConfirmBlockReq):
    """
    El cliente (o DataNode, según diseño) confirma que un bloque fue almacenado en el DataNode.
    Actualiza la entrada del bloque en blocks_json y marca 'present'.
    """
    if not auth_user(info.user, info.password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    with lock:
        conn = db_conn()
        c = conn.cursor()

        c.execute("SELECT blocks_json FROM files WHERE filename=?", (info.filename,))
        row = c.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="file not found")

        blocks = json.loads(row[0])
        updated = False
        for b in blocks:
            if b["block_index"] == info.block_index and b["block_id"] == info.block_id:
                b["size"] = info.size
                b["checksum"] = info.checksum
                b["present"] = True
                updated = True
                break
        if not updated:
            conn.close()
            raise HTTPException(status_code=404, detail="block not found")

        all_present = all(b["present"] for b in blocks)
        total_size = sum(b["size"] for b in blocks)

        status = "available" if all_present else "incomplete"
        c.execute("UPDATE files SET blocks_json=?, size=?, status=? WHERE filename=?",
                  (json.dumps(blocks), total_size, status, info.filename))
        conn.commit()
        conn.close()
    return {"status": "ok"}

@app.get("/namenode/metadata")
def get_metadata(filename: str, user: str , password: str ):
    """
    Retorna los metadatos de un archivo, incluyendo la lista de bloques y su estado.
    """
    if not auth_user(user, password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT filename, owner, size, block_size, status, created_at, blocks_json FROM files WHERE filename=? AND owner=?", (filename, user,))
    row = c.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="file not found")

    filename, owner, size, block_size, status, created_at, blocks_json = row
    return {
        "filename": filename,
        "owner": owner,
        "size": size,
        "block_size": block_size,
        "status": status,
        "created_at": created_at,
        "blocks": json.loads(blocks_json)
    }

@app.get("/namenode/list_files")
def list_files( user: str , password: str ):
    """
    Lista todos los archivos en el sistema.
    """

    if not auth_user(user, password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT filename, size, status, created_at FROM files")
    res = [{"filename":r[0], "size":r[1], "status":r[2], "created_at":r[3]} for r in c.fetchall()]
    conn.close()
    return {"files": res}

@app.get("/namenode/ls")
def list_path(path: str = "/", user: str = "", password: str = ""):
    """
    Lista archivos cuyo nombre empieza con el prefijo `path/`.
    Ejemplo: path="/user/demo" → lista /user/demo/file1, /user/demo/file2
    """
    if not auth_user(user, password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    conn = db_conn()
    c = conn.cursor()
    like_pattern = path.rstrip("/") + "/%"
    if user:
        like_pattern = f"{path.rstrip('/')}/%"
        rows = c.execute("SELECT filename, size, status FROM files WHERE filename LIKE ? AND owner=?", (like_pattern, user)).fetchall()
    else:
        rows = c.execute(
            "SELECT filename, size, status FROM files WHERE filename LIKE ?",
            (like_pattern,)
        ).fetchall()
    conn.close()
    return {
        "files": [
            {"filename": r[0], "size": r[1], "status": r[2]} for r in rows
        ]
    }

@app.delete("/namenode/delete_file")
def delete_file(filename: str, user: str , password: str ):
    """
    Elimina un archivo del sistema:
    1. Lee los metadatos y obtiene los bloques.
    2. Envía request a cada DataNode para borrar los bloques.
    3. Elimina el registro en la tabla files.
    """
    if not auth_user(user, password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    with lock:
        conn = db_conn()
        c = conn.cursor()
        c.execute("SELECT owner, blocks_json FROM files WHERE filename=?", (filename,))
        row = c.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="file not found")

        owner, blocks_json = row
        if owner != user:
            conn.close()
            raise HTTPException(status_code=403, detail="no permission to delete this file")

        blocks = json.loads(blocks_json)

        # borrar bloques en los datanodes
        for b in blocks:
            try:
                dn = b["datanode_url"]
                block_id = b["block_id"]
                requests.delete(f"{dn}/datanode/delete_block", params={"block_id": block_id}, timeout=3)
            except Exception as e:
                print(f" Error eliminando bloque {block_id} en {dn}: {e}")

        # borrar metadatos
        c.execute("DELETE FROM files WHERE filename=?", (filename,))
        conn.commit()
        conn.close()
    return {"status": "ok", "deleted": filename}


@app.post("/namenode/mkdir")
def mkdir(req: MkdirReq):
    """
    Crea un directorio lógico.
    Realmente solo inserta un placeholder en files con status='dir'.
    """
    if not auth_user(req.user, req.password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    path = req.path
    if not path.startswith("/"):
        raise HTTPException(status_code=400, detail="path must start with /")

    with lock:
        conn = db_conn()
        c = conn.cursor()
        now = datetime.utcnow().isoformat()
        owner = req.user
        c.execute("""INSERT OR IGNORE INTO files(filename, owner, size, block_size, status, created_at, blocks_json) 
                 VALUES (?, ?, ?, ?, ?, ?, ?)""",
              (path.rstrip("/"), owner, 0, 0, "dir", now, "[]"))
        conn.commit()
        conn.close()
    return {"status": "ok", "mkdir": path}


@app.post("/namenode/rmdir")
def rmdir(req: RmdirReq):
    """
    Elimina un directorio lógico y todos los archivos bajo ese prefijo.
    """
    if not auth_user(req.user, req.password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")
    path = req.path
    prefix = path.rstrip("/") + "/%"
    with lock:
        conn = db_conn()
        c = conn.cursor()
        rows = c.execute("SELECT filename FROM files WHERE filename LIKE ?", (prefix,)).fetchall()

        for r in rows:
            fname = r[0]
            # llamada recursiva: delete_file
            try:
                delete_file(fname)
            except Exception as e:
                print(f" Error eliminando {fname}: {e}")

        # borrar el propio directorio
        c.execute("DELETE FROM files WHERE filename=?", (path.rstrip("/"),))
        conn.commit()
        conn.close()
    return {"status": "ok", "rmdir": path, "deleted_files": [r[0] for r in rows]}

@app.post("/namenode/register")
def register_user(req: UserReq):
    with lock:
        conn = db_conn()
        c = conn.cursor()
        try:
            hashed_pass = pwd_context.hash(req.password)
            c.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (req.username, hashed_pass))
            conn.commit()
            conn.close()
            return {"status": "ok", "message": f"Usuario {req.username} creado exitosamente"}
        except sqlite3.IntegrityError:
            conn.close()
            raise HTTPException(status_code=400, detail="El usuario ya existe")

@app.post("/namenode/login")
def login_user(req: UserReq):
    if auth_user(req.username, req.password):
        return {"status": "ok", "message": "Autenticación exitosa"}
    else:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")