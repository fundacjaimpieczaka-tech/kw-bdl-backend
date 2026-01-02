import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BDL_BASE = "https://bdl.stat.gov.pl/api/v1"

# Opcjonalny klucz zwiększający limity (nagłówek X-ClientId)
BDL_CLIENT_ID = os.getenv("BDL_CLIENT_ID", "").strip()

# Domyślny var-id populacji. Jeśli okaże się nietrafiony w Twoim przypadku, ustaw w env BDL_POPULATION_VAR_ID.
DEFAULT_POP_VAR_ID = os.getenv("BDL_POPULATION_VAR_ID", "148190").strip()

# Prosty cache w pamięci (na potrzeby MVP)
CACHE: Dict[str, Tuple[float, Any]] = {}
CACHE_TTL_SECONDS = 60 * 60 * 24  # 24h


def _bdl_headers() -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if BDL_CLIENT_ID:
        h["X-ClientId"] = BDL_CLIENT_ID
    return h


def _cache_get(key: str) -> Optional[Any]:
    item = CACHE.get(key)
    if not item:
        return None
    ts, val = item
    if time.time() - ts > CACHE_TTL_SECONDS:
        try:
            del CACHE[key]
        except KeyError:
            pass
        return None
    return val


def _cache_set(key: str, val: Any) -> None:
    CACHE[key] = (time.time(), val)


def _norm(s: Optional[str]) -> str:
    return " ".join((s or "").strip().split())


def _safe_lower(s: Optional[str]) -> str:
    return _norm(s).lower()


def _req_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = requests.get(url, params=params, headers=_bdl_headers(), timeout=20)
    r.raise_for_status()
    return r.json()


def _pick_best_locality(
    items: List[Dict[str, Any]],
    miejscowosc: str,
    gmina: str,
    powiat: str,
    woj: str,
) -> Optional[Dict[str, Any]]:
    """
    Heurystyka dopasowania, bo BDL może zwrócić kilka miejscowości o tej samej nazwie.
    Staramy się dopasować po: gmina/powiat/woj (jeśli podane) + preferujemy dokładną nazwę.
    """
    if not items:
        return None

    m0 = _safe_lower(miejscowosc)
    g0 = _safe_lower(gmina)
    p0 = _safe_lower(powiat)
    w0 = _safe_lower(woj)

    def score(it: Dict[str, Any]) -> int:
        s = 0
        name = _safe_lower(it.get("name"))
        # dokładna nazwa
        if name == m0:
            s += 50
        elif m0 and m0 in name:
            s += 20

        # BDL zwraca różne pola zależnie od endpointu; próbujemy kilku
        # często spotykane: "unitName", "parentName", "administrativeUnitName", itp.
        blob = " ".join(
            [
                _safe_lower(it.get("name")),
                _safe_lower(it.get("unitName")),
                _safe_lower(it.get("parentName")),
                _safe_lower(it.get("administrativeUnitName")),
                _safe_lower(it.get("description")),
            ]
        )

        if g0 and g0 in blob:
            s += 15
        if p0 and p0 in blob:
            s += 10
        if w0 and w0 in blob:
            s += 5

        # stabilne preferencje: jeśli jest identyfikator i wygląda na pełny
        if it.get("id"):
            s += 1
        return s

    ranked = sorted(items, key=score, reverse=True)
    return ranked[0]


def _extract_latest_value(data_json: Dict[str, Any]) -> Tuple[Optional[float], Optional[int]]:
    """
    BDL zwraca strukturę z listą wyników. W praktyce spotyka się pola:
    - "results" -> [{"values": [...]}]
    - "data" / "values" / "year"
    Dlatego robimy defensywne parsowanie i wybieramy najnowszy rok z wartością.
    """
    # Najczęściej: data_json["results"][0]["values"] = [[value, attrId, year], ...]
    results = data_json.get("results") or data_json.get("result") or []
    if isinstance(results, dict):
        results = [results]

    best_year = None
    best_val = None

    for res in results:
        values = res.get("values") or res.get("data") or []
        # values może być listą trójek [val, attr, year]
        if isinstance(values, list):
            for row in values:
                if not isinstance(row, list) or len(row) < 3:
                    continue
                val = row[0]
                year = row[2]
                try:
                    v = float(val)
                    y = int(year)
                except Exception:
                    continue
                if best_year is None or y > best_year:
                    best_year = y
                    best_val = v

    return best_val, best_year


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.get("/bdl/population")
def bdl_population():
    miejscowosc = _norm(request.args.get("miejscowosc"))
    gmina = _norm(request.args.get("gmina"))
    powiat = _norm(request.args.get("powiat"))
    woj = _norm(request.args.get("woj"))
    year = _norm(request.args.get("year"))  # opcjonalnie

    if not miejscowosc:
        return jsonify(
            {
                "ok": False,
                "error": "Podaj parametr: miejscowosc (np. ?miejscowosc=Żelewo&gmina=Stare Czarnowo&powiat=gryfiński&woj=zachodniopomorskie)"
            }
        ), 400

    cache_key = f"pop::{miejscowosc}::{gmina}::{powiat}::{woj}::{year}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    # 1) Szukamy miejscowości w BDL
    # Endpoint jest udokumentowany w ReDoc: /units/localities :contentReference[oaicite:2]{index=2}
    try:
        loc_json = _req_json(f"{BDL_BASE}/units/localities", params={"name": miejscowosc, "page-size": 50})
    except Exception as e:
        return jsonify({"ok": False, "error": f"BDL units/localities error: {str(e)}"}), 502

    items = loc_json.get("results") or loc_json.get("items") or []
    if not items:
        out = {
            "ok": True,
            "found": False,
            "miejscowosc": miejscowosc,
            "message": "Nie znaleziono miejscowości w BDL (sprawdź pisownię, dodaj gminę/powiat).",
            "source": "BDL",
        }
        _cache_set(cache_key, out)
        return jsonify(out)

    best = _pick_best_locality(items, miejscowosc, gmina, powiat, woj)
    if not best:
        out = {"ok": True, "found": False, "miejscowosc": miejscowosc, "message": "Brak dopasowania.", "source": "BDL"}
        _cache_set(cache_key, out)
        return jsonify(out)

    unit_id = best.get("id")
    if not unit_id:
        out = {"ok": True, "found": False, "miejscowosc": miejscowosc, "message": "BDL zwrócił rekord bez id.", "source": "BDL"}
        _cache_set(cache_key, out)
        return jsonify(out)

    # 2) Pobieramy dane populacji (var-id)
    params = {"var-id": DEFAULT_POP_VAR_ID}
    if year:
        params["year"] = year

    # Dla miejscowości statystycznych jest endpoint: /data/localities/by-unit/... (patrz manual BDL) :contentReference[oaicite:3]{index=3}
    try:
        data_json = _req_json(f"{BDL_BASE}/data/localities/by-unit/{unit_id}", params=params)
    except Exception as e:
        return jsonify({"ok": False, "error": f"BDL data/localities/by-unit error: {str(e)}", "unit_id": unit_id}), 502

    val, val_year = _extract_latest_value(data_json)

    out = {
        "ok": True,
        "found": True,
        "miejscowosc": miejscowosc,
        "gmina_hint": gmina or None,
        "powiat_hint": powiat or None,
        "woj_hint": woj or None,
        "unit_id": unit_id,
        "picked_record": {
            "name": best.get("name"),
            "id": best.get("id"),
            "level": best.get("level"),
            "parentId": best.get("parentId"),
        },
        "population": val,
        "year": val_year,
        "var_id_used": DEFAULT_POP_VAR_ID,
        "note": "Jeśli population/year są puste, ustaw inną zmienną BDL_POPULATION_VAR_ID lub doprecyzuj miejscowość (gmina/powiat).",
        "source": "BDL (GUS)",
    }

    _cache_set(cache_key, out)
    return jsonify(out)


if __name__ == "__main__":
    # lokalnie (opcjonalnie): python app.py
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
