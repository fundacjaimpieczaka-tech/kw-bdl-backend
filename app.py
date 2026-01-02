from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

BDL_BASE = "https://bdl.stat.gov.pl/api/v1"

def safe_get(url, params=None, timeout=20):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def find_unit_id_by_name(name):
    data = safe_get(f"{BDL_BASE}/units", {"name": name, "page-size": 10})
    results = data.get("results", [])
    if not results:
        return None, None
    return results[0]["id"], results[0]

def find_population_variable():
    data = safe_get(f"{BDL_BASE}/variables/search", {"name": "ludność", "page-size": 10})
    results = data.get("results", [])
    if not results:
        return None, None
    return results[0]["id"], results[0]

def get_latest_value(var_id, unit_id):
    data = safe_get(
        f"{BDL_BASE}/data/by-variable/{var_id}",
        {
            "unit-id": unit_id,
            "page-size": 1,
            "sort": "-year"
        }
    )
    results = data.get("results", [])
    if not results:
        return None
    r = results[0]
    return {"year": r.get("year"), "value": r.get("val")}

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

@app.get("/bdl-basic")
def bdl_basic():
    gmina = (request.args.get("gmina") or "").strip()
    if not gmina:
        return jsonify({"error": "missing gmina"}), 400

    unit_id, unit_meta = find_unit_id_by_name(gmina)
    if not unit_id:
        return jsonify({"error": "gmina not found"}), 404

    var_id, var_meta = find_population_variable()
    if not var_id:
        return jsonify({"error": "population variable not found"}), 500

    latest = get_latest_value(var_id, unit_id)

    return jsonify({
        "input": {"gmina": gmina},
        "unit": unit_meta,
        "population": latest,
        "note": "MVP – dopasowanie heurystyczne (do weryfikacji przy niejednoznacznych nazwach)"
    })
