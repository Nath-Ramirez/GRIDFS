from flask import Flask, request, jsonify, send_file
from flask_sqlalchemy import SQLAlchemy
import os, io, math, docker

app = Flask(__name__)

# --- Configuración Base de Datos (SQLite para demo) ---
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///namenode.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- Modelos SQL ---
class File(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, unique=True, nullable=False)
    owner = db.Column(db.String, nullable=False)
    size = db.Column(db.Integer, nullable=False)
    block_size = db.Column(db.Integer, nullable=False)

class Block(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    file_id = db.Column(db.Integer, db.ForeignKey('file.id'), nullable=False)
    index = db.Column(db.Integer, nullable=False)
    datanode_url = db.Column(db.String, nullable=False)
    block_id = db.Column(db.String, nullable=False)

# --- Docker client (para lanzar DataNodes) ---
docker_client = docker.from_env()

# Crear tablas
with app.app_context():
    db.create_all()

# --- Endpoints ---

@app.post("/upload")
def upload_file():
    # Recibe archivo
    file = request.files['file']
    owner = request.form.get("user", "demo")
    block_size = int(request.form.get("block_size", 8*1024*1024))  # 8MB default

    data = file.read()
    size = len(data)
    n_blocks = math.ceil(size / block_size)

    # Guardar metadatos archivo
    f = File(name=file.filename, owner=owner, size=size, block_size=block_size)
    db.session.add(f)
    db.session.commit()

    # Particionar y crear DataNodes
    for i in range(n_blocks):
        chunk = data[i*block_size:(i+1)*block_size]
        block_id = f"{file.filename}-{i}"

        # Crear contenedor Docker para este bloque
        container = docker_client.containers.run(
            "griddfs-datanode:latest",  # Imagen del DataNode
            detach=True,
            environment={
                "BLOCK_ID": block_id,
                "BLOCK_DATA": chunk.hex(),  # guardamos binario como hex string
            },
            ports={'5001/tcp': None}  # puerto dinámico
        )

        # Obtener puerto asignado dinámicamente
        container.reload()
        port = container.attrs['NetworkSettings']['Ports']['5001/tcp'][0]['HostPort']
        datanode_url = f"http://localhost:{port}"

        # Guardar metadatos bloque
        b = Block(file_id=f.id, index=i, datanode_url=datanode_url, block_id=block_id)
        db.session.add(b)

    db.session.commit()

    return jsonify({"status": "ok", "file": file.filename, "blocks": n_blocks})


@app.get("/download/<fname>")
def download_file(fname):
    # Buscar archivo
    f = File.query.filter_by(name=fname).first()
    if not f:
        return jsonify({"error": "file not found"}), 404

    # Buscar bloques
    blocks = Block.query.filter_by(file_id=f.id).order_by(Block.index).all()
    result = io.BytesIO()

    # Reconstruir el archivo desde DataNodes
    import requests
    for b in blocks:
        resp = requests.get(f"{b.datanode_url}/blocks/{b.block_id}")
        if resp.status_code != 200:
            return jsonify({"error": f"missing block {b.block_id}"}), 500
        result.write(resp.content)

    result.seek(0)
    return send_file(result, as_attachment=True, download_name=fname)


@app.get("/files")
def list_files():
    files = File.query.all()
    return jsonify([{"name": f.name, "owner": f.owner, "size": f.size} for f in files])


@app.delete("/files/<fname>")
def delete_file(fname):
    f = File.query.filter_by(name=fname).first()
    if not f:
        return jsonify({"error": "file not found"}), 404

    # Borrar bloques (y contenedores asociados)
    blocks = Block.query.filter_by(file_id=f.id).all()
    for b in blocks:
        # Buscar contenedor por block_id y detenerlo
        try:
            c = docker_client.containers.get(b.block_id)
            c.remove(force=True)
        except:
            pass
        db.session.delete(b)

    db.session.delete(f)
    db.session.commit()

    return jsonify({"status": "deleted", "file": fname})


@app.get("/health")
def health():
    return jsonify({"ok": True})
