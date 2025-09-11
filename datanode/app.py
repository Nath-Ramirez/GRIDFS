from flask import Flask, jsonify, Response
import os, binascii

app = Flask(__name__)

# --- Configuración inicial ---
BLOCK_ID = os.environ.get("BLOCK_ID")
BLOCK_DATA_HEX = os.environ.get("BLOCK_DATA")

if not BLOCK_ID or not BLOCK_DATA_HEX:
    raise SystemExit("Faltan variables de entorno BLOCK_ID o BLOCK_DATA")

# Convertimos de hex → bytes para guardarlo en memoria
BLOCK_DATA = binascii.unhexlify(BLOCK_DATA_HEX.encode())

@app.get("/blocks/<block_id>")
def get_block(block_id):
    """Devuelve el contenido del bloque si coincide el ID"""
    if block_id != BLOCK_ID:
        return jsonify({"error": "block not found"}), 404
    return Response(BLOCK_DATA, mimetype="application/octet-stream")

@app.delete("/blocks/<block_id>")
def delete_block(block_id):
    """Elimina el bloque y apaga el contenedor"""
    if block_id != BLOCK_ID:
        return jsonify({"error": "block not found"}), 404
    # Simular borrado en memoria
    global BLOCK_DATA
    BLOCK_DATA = None
    # Opcional: terminar el contenedor
    os._exit(0)  # mata el proceso → Docker lo interpreta como contenedor terminado
    return jsonify({"status": "deleted"})

@app.get("/health")
def health():
    return jsonify({"ok": True, "block_id": BLOCK_ID, "size": len(BLOCK_DATA)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
