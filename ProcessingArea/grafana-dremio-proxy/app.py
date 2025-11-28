from flask import Flask, request, jsonify
import requests
import os
import time
from threading import Lock

app = Flask(__name__)

# Configuración de Dremio desde variables de entorno
DREMIO_HOST = os.environ.get("DREMIO_HOST", "dremio2")
DREMIO_PORT = os.environ.get("DREMIO_PORT", "9047")
DREMIO_USER = os.environ.get("DREMIO_USER", "danipadi")
DREMIO_PASSWORD = os.environ.get("DREMIO_PASSWORD", "D4n13l1216+")

BASE_URL = f"http://{DREMIO_HOST}:{DREMIO_PORT}/api/v3/sql"
LOGIN_URL = f"http://{DREMIO_HOST}:{DREMIO_PORT}/apiv2/login"

# Cache del token con lock
token_cache = {"token": None, "timestamp": 0}
token_lock = Lock()
TOKEN_TTL = 60 * 60  # 1 hora

def get_token():
    """Obtiene y cachea el token de Dremio."""
    with token_lock:
        if token_cache["token"] and (time.time() - token_cache["timestamp"] < TOKEN_TTL):
            return token_cache["token"]
        resp = requests.post(LOGIN_URL, json={"userName": DREMIO_USER, "password": DREMIO_PASSWORD})
        if resp.status_code != 200:
            raise Exception(f"Login failed: {resp.status_code} - {resp.text}")
        token = resp.json()["token"]
        token_cache["token"] = token
        token_cache["timestamp"] = time.time()
        return token

def execute_query(sql):
    """Ejecuta query en Dremio con polling y timeout."""
    token = get_token()
    headers = {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}

    # Ejecutar job
    response = requests.post(BASE_URL, headers=headers, json={"sql": sql, "context": ["@personal"]})
    if response.status_code != 200:
        raise Exception(f"Dremio returned {response.status_code}: {response.text}")

    job_id = response.json()["id"]
    job_url = f"http://{DREMIO_HOST}:{DREMIO_PORT}/api/v3/job/{job_id}"

    # Polling con timeout
    start_time = time.time()
    timeout = 60  # segundos
    while True:
        job_resp = requests.get(job_url, headers=headers)
        job_data = job_resp.json()
        state = job_data.get("jobState")
        if state in ["COMPLETED", "CANCELED", "FAILED"]:
            break
        if time.time() - start_time > timeout:
            raise Exception("Job timeout")
        time.sleep(0.5)

    if state != "COMPLETED":
        raise Exception(f"Job did not complete: {state} - {job_data.get('errorMessage')}")

    # Obtener resultados
    results_url = f"{job_url}/results"
    res = requests.get(results_url, headers=headers)
    rows = res.json().get("rows", [])
    columns = [{"text": col["name"]} for col in res.json().get("columns", [])]

    if not columns and rows:
        columns = [{"text": f"col{i+1}"} for i in range(len(rows[0]))]
    # Retornar el diccionario para /query
    return {
        "columns": columns,
        "rows": [list(r.values()) for r in rows],
        "type": "table"
    }


# Endpoints
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok"})

@app.route("/query", methods=["POST"])
def query():
    payload = request.json
    result = []

    # Grafana manda {"targets":[{...}]}
    targets = payload.get("targets", payload)  # si no existe, usar payload directamente

    for q in targets:
        sql = q.get("target") if isinstance(q, dict) else q
        if not sql:
            continue
        try:
            result.append(execute_query(sql))
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return jsonify(result)


@app.route("/search", methods=["POST"])
def search():
    """Devuelve las tablas disponibles en el workspace personal (opcionalmente dinámico)."""
    try:
        token = get_token()
        headers = {"Authorization": f"_dremio{token}"}
        resp = requests.get(f"http://{DREMIO_HOST}:{DREMIO_PORT}/api/v3/catalog", headers=headers)
        resp.raise_for_status()
        tables = [item['name'] for item in resp.json().get('data', []) if item['type'] == 'TABLE']
        return jsonify(tables)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/annotations", methods=["POST"])
def annotations():
    return jsonify([])

if __name__ == "__main__":
    print("Proxy iniciado, esperando queries de Grafana...")
    app.run(host="0.0.0.0", port=5000)
