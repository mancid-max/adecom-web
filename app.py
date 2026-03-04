from __future__ import annotations

import csv
import hmac
import io
import json
import os
import re
import unicodedata
from datetime import date
from pathlib import Path
from difflib import SequenceMatcher
from urllib import error as url_error
from urllib import request as url_request

from flask import Flask, Response, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.exceptions import RequestEntityTooLarge

from adecom_db import (
    get_conn,
    import_corte_etapas_rows,
    import_exs_map_rows,
    import_pedidos_talla_todas_rows,
    init_db,
    import_pedidos_talla_rows,
    import_rows,
    query_assistant_rules,
    query_exs_balance_summary,
    query_pedidos_talla_sections,
    query_rows,
)
from parsers import parse_pedidos_talla_txt, parse_saldos_txt, parse_uploaded_file
from parsers import parse_corte_etapas_txt


BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DB_PATH = os.environ.get("DATABASE_URL") or os.environ.get(
    "ADECOM_DB_PATH", str(BASE_DIR / "data" / "adecom.db")
)
SEED_DIR = BASE_DIR / "seed"
SEED_SALDOS = SEED_DIR / "SALDOS-SECCI.TXT"
SEED_PEDIDOS = SEED_DIR / "PEDIDOSXTALLA.TXT"
AUTOLOAD_DIR = Path(
    os.environ.get(
        "ADECOM_AUTOLOAD_DIR",
        r"C:\Users\manuh\Desktop\APIS\Documentos a cargar ADECOM WEB",
    )
)
AUTOLOAD_SALDOS_SOURCE = os.environ.get("ADECOM_AUTOLOAD_SALDOS_SOURCE", "").strip()
AUTOLOAD_PEDIDOS_SOURCE = os.environ.get("ADECOM_AUTOLOAD_PEDIDOS_SOURCE", "").strip()
AUTOLOAD_ETAPAS_SOURCE = os.environ.get("ADECOM_AUTOLOAD_ETAPAS_SOURCE", "").strip()
AUTO_REFRESH_WEB_ON_START = os.environ.get("ADECOM_AUTO_REFRESH_WEB_ON_START", "1").strip() == "1"
ASSISTANT_ENABLED = os.environ.get("ADECOM_ASSISTANT_ENABLED", "0").strip() == "1"
NEW_SECTION_ENABLED = os.environ.get("ADECOM_ENABLE_NEW_SECTION", "0").strip() == "1"
PROYECCION_STATE_PATH = BASE_DIR / "data" / "proyeccion_personas.json"
AREA_WEIGHTS = {
    "CORTE": 600,
    "TALLER": 400,
    "TALLER EXTERNO": 200,
    "LIMPIADO": 600,
    "LAVANDERIA": 600,
    "TERMINACION": 600,
}

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
if not str(DB_PATH).startswith(("postgres://", "postgresql://")):
    Path(DB_PATH).resolve().parent.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("ADECOM_SECRET_KEY", "dev-secret-change-me")
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("ADECOM_MAX_UPLOAD_MB", "25")) * 1024 * 1024
PROYECCION_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


def _admin_key() -> str:
    return os.environ.get("ADECOM_ADMIN_KEY", "").strip()


def _access_key_web() -> str:
    return os.environ.get("ADECOM_ACCESS_KEY_WEB", "adecom-web").strip()


def _access_key_new() -> str:
    return os.environ.get("ADECOM_ACCESS_KEY_NEW", "adecom-nueva").strip()


def _access_key_web_aliases() -> list[str]:
    raw = os.environ.get("ADECOM_ACCESS_KEY_WEB_ALIASES", "adecom-web,adecom,web")
    return [x.strip() for x in raw.split(",") if x.strip()]


def _match_any_key(entered: str, keys: list[str]) -> bool:
    if not entered:
        return False
    for key in keys:
        if key and hmac.compare_digest(entered, key):
            return True
    return False


def _portal_section() -> str:
    return str(session.get("portal_section") or "").strip().lower()


def _is_authenticated() -> bool:
    return _portal_section() in {"web", "new"}


@app.before_request
def _guard_portal_routes():
    endpoint = request.endpoint or ""
    public_endpoints = {"login", "login_post", "static"}
    if endpoint in public_endpoints:
        return

    section = _portal_section()
    if not section:
        return redirect(url_for("login"))

    if section == "new":
        allowed_new = {"new_section", "logout", "static", "login"}
        if endpoint not in allowed_new:
            return redirect(url_for("new_section"))

    if section == "web" and endpoint == "new_section":
        return redirect(url_for("index"))


def _can_upload() -> bool:
    key = _admin_key()
    if not key:
        return True
    return bool(session.get("can_upload"))


@app.get("/login")
def login():
    if _is_authenticated():
        if _portal_section() == "new":
            return redirect(url_for("new_section"))
        return redirect(url_for("index"))
    return render_template("login.html")


@app.post("/login")
def login_post():
    entered_key = str(request.form.get("access_key") or "").strip()
    web_keys = [_access_key_web(), *_access_key_web_aliases()]
    key_new = _access_key_new()
    if _match_any_key(entered_key, web_keys):
        session["portal_section"] = "web"
        session.permanent = True
        return redirect(url_for("index"))
    if NEW_SECTION_ENABLED and entered_key and hmac.compare_digest(entered_key, key_new):
        session["portal_section"] = "new"
        session.permanent = True
        return redirect(url_for("new_section"))
    if not NEW_SECTION_ENABLED and entered_key and hmac.compare_digest(entered_key, key_new):
        flash("El acceso para la nueva seccion aun no esta habilitado.", "error")
        return redirect(url_for("login"))
    flash("Clave incorrecta.", "error")
    return redirect(url_for("login"))


@app.post("/logout")
def logout():
    session.pop("portal_section", None)
    session.pop("can_upload", None)
    flash("Sesion cerrada.", "success")
    return redirect(url_for("login"))


@app.get("/nueva-seccion")
def new_section():
    if not NEW_SECTION_ENABLED:
        return redirect(url_for("index"))
    return render_template("new_section.html")


def _norm_text(value: str) -> str:
    raw = unicodedata.normalize("NFKD", str(value or ""))
    no_accents = "".join(ch for ch in raw if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^a-zA-Z0-9 ]+", " ", no_accents).lower()
    return re.sub(r"\s+", " ", cleaned).strip()


def _has_keyword(text: str, options: list[str]) -> bool:
    words = _norm_text(text).split()
    normalized = _norm_text(text)
    for opt in options:
        opt_n = _norm_text(opt)
        if not opt_n:
            continue
        if opt_n in normalized:
            return True
        for w in words:
            if not w:
                continue
            if SequenceMatcher(None, w, opt_n).ratio() >= 0.82:
                return True
    return False


def _extract_rank(text: str) -> int:
    tn = _norm_text(text)
    m = re.search(r"\btop\s+(\d+)\b", tn)
    if m:
        return max(int(m.group(1)), 1)
    m2 = re.search(r"\b(\d+)(?:er|do|to|ro)?\b", tn)
    if m2 and int(m2.group(1)) <= 10:
        return max(int(m2.group(1)), 1)
    if _has_keyword(tn, ["primero", "primer"]):
        return 1
    if _has_keyword(tn, ["segundo", "segunda", "segun", "2do"]):
        return 2
    if _has_keyword(tn, ["tercero", "tercera", "3ro"]):
        return 3
    if _has_keyword(tn, ["cuarto", "cuarta", "4to"]):
        return 4
    if _has_keyword(tn, ["quinto", "quinta", "5to"]):
        return 5
    return 1


def _extract_query_code(question: str) -> str:
    digits = re.findall(r"\d+", question or "")
    if not digits:
        return ""
    # Preferimos codigos de al menos 4 digitos para familia/articulo.
    candidates = [d for d in digits if len(d) >= 4]
    return candidates[0] if candidates else digits[0]


def _extract_family_code(code: str) -> str:
    digits = "".join(ch for ch in str(code or "") if ch.isdigit())
    if not digits:
        return ""
    if len(digits) == 4:
        return digits
    candidates: list[str] = []
    if len(digits) >= 8:
        candidates.append(digits[2:6])
    if len(digits) >= 6:
        candidates.append(digits[:4])
        candidates.append(digits[-4:])
    if len(digits) >= 4:
        candidates.append(digits[:4])
    for c in candidates:
        if len(c) == 4:
            return c
    return digits[:4]


def _resolve_ex_details(code: str) -> dict | None:
    family = _extract_family_code(code)
    if not family:
        return None
    ex_summary = query_exs_balance_summary(DB_PATH, "")
    rows = ex_summary.get("rows") or []
    for item in rows:
        if str(item.get("actual") or "").strip() == family:
            ex_raw = str(item.get("ex") or "").strip()
            ex_family = _extract_family_code(ex_raw)
            return {
                "family_actual": family,
                "family_ex": ex_family,
                "ex_raw": ex_raw,
                "saldo_actual": int(item.get("saldo_actual") or 0),
                "saldo_ex": int(item.get("saldo_ex") or 0),
            }
    return None


def _build_assistant_context(question: str) -> str:
    rows, _, summary = query_rows(DB_PATH, {"q": "", "fecha": ""})
    pedidos_sections = query_pedidos_talla_sections(DB_PATH, "")
    exs_summary = query_exs_balance_summary(DB_PATH, "")
    assistant_rules = query_assistant_rules(DB_PATH, limit=60)
    ventas_rows = pedidos_sections.get("ventas", [])

    ventas_por_articulo: dict[str, int] = {}
    ventas_por_familia: dict[str, int] = {}
    ventas_por_talla: dict[int, int] = {}
    for r in ventas_rows:
        articulo = str(r.get("articulo") or "").strip()
        total = int(r.get("total") or 0)
        if not articulo:
            continue
        ventas_por_articulo[articulo] = ventas_por_articulo.get(articulo, 0) + total
        familia = articulo[2:6] if len(articulo) >= 6 else articulo
        ventas_por_familia[familia] = ventas_por_familia.get(familia, 0) + total
        for item in r.get("tallas_items") or []:
            talla = int(item.get("talla") or 0)
            cantidad = int(item.get("cantidad") or 0)
            if talla > 0:
                ventas_por_talla[talla] = ventas_por_talla.get(talla, 0) + cantidad

    top_articulos = [
        {"articulo": a, "total": t}
        for a, t in sorted(ventas_por_articulo.items(), key=lambda x: x[1], reverse=True)[:12]
    ]
    top_familias = [
        {"familia": f, "total": t}
        for f, t in sorted(ventas_por_familia.items(), key=lambda x: x[1], reverse=True)[:12]
    ]
    curva_tallas = [
        {"talla": talla, "total": total}
        for talla, total in sorted(ventas_por_talla.items(), key=lambda x: x[0])
    ]

    etapas: dict[str, int] = {}
    bodega_por_articulo: dict[str, int] = {}
    fechas_conteo: dict[str, int] = {}
    for r in rows:
        stage = str(r.get("proceso_actual") or "Sin movimiento")
        proceso = int(r.get("proceso") or 0)
        etapas[stage] = etapas.get(stage, 0) + proceso
        articulo = str(r.get("articulo") or "").strip()
        if articulo:
            bodega_por_articulo[articulo] = bodega_por_articulo.get(articulo, 0) + int(r.get("bodega") or 0)
        fecha_iso = str(r.get("fecha_iso") or "").strip()
        if fecha_iso:
            fechas_conteo[fecha_iso] = fechas_conteo.get(fecha_iso, 0) + 1

    top_etapas = [
        {"etapa": e, "total": t}
        for e, t in sorted(etapas.items(), key=lambda x: x[1], reverse=True)[:10]
    ]
    top_bodega_articulo = [
        {"articulo": a, "bodega": t}
        for a, t in sorted(bodega_por_articulo.items(), key=lambda x: x[1], reverse=True)[:10]
        if t > 0
    ]
    today_iso = date.today().isoformat()
    fechas_ordenadas = sorted(fechas_conteo.items(), key=lambda x: x[0], reverse=True)
    ultimas_fechas = [{"fecha_iso": f, "registros": c} for f, c in fechas_ordenadas[:10]]

    q_code = _extract_query_code(question or "")
    detalle_codigo = {}
    if q_code:
        code_rows, _, _ = query_rows(DB_PATH, {"q": q_code, "fecha": ""})
        code_pedidos = query_pedidos_talla_sections(DB_PATH, q_code).get("ventas", [])
        code_ex = _resolve_ex_details(q_code)
        saldos_detalle_limpio = []
        total_bodega_code = 0
        total_restante_code = 0
        total_proceso_code = 0
        for r in code_rows[:30]:
            bodega = int(r.get("bodega") or 0)
            restante = int(r.get("pendiente_en_trazabilidad") or 0)
            proceso = int(r.get("proceso") or 0)
            total_bodega_code += bodega
            total_restante_code += restante
            total_proceso_code += proceso
            saldos_detalle_limpio.append(
                {
                    "articulo": str(r.get("articulo") or ""),
                    "orden_corte": str(r.get("corte") or ""),
                    "proceso_total": proceso,
                    "bodega": bodega,
                    "restante": restante,
                    "proceso_actual": str(r.get("proceso_actual") or ""),
                    "restante_detalle": str(r.get("restante_detalle") or ""),
                }
            )
        detalle_codigo = {
            "codigo_consultado": q_code,
            "resumen_saldos": {
                "total_proceso": total_proceso_code,
                "total_bodega": total_bodega_code,
                "total_restante": total_restante_code,
            },
            "saldos": saldos_detalle_limpio,
            "ventas": code_pedidos[:20],
            "ex": code_ex or {},
        }

    context_data = {
        "reglas_interpretacion": {
            "bodega": "columna bodega (unidades actualmente en bodega)",
            "restante": "columna restante o pendiente_en_trazabilidad (NO es bodega)",
            "proceso_total": "columna proceso/total de la orden",
            "nota": "nunca confundir restante con bodega",
        },
        "reglas_negocio": [
            {
                "key": str(item.get("rule_key") or ""),
                "text": str(item.get("rule_text") or ""),
                "priority": int(item.get("priority") or 0),
            }
            for item in assistant_rules
        ],
        "fechas": {
            "campo_principal": "fecha_iso",
            "descripcion": "fecha del registro importado (no fecha de ingreso al sistema web)",
            "hoy_iso_servidor": today_iso,
            "registros_hoy": int(fechas_conteo.get(today_iso, 0)),
            "ultimas_fechas": ultimas_fechas,
        },
        "resumen_global": {
            "total_registros_saldos": len(rows),
            "ordenes_en_bodega": int(summary.get("ordenes_en_bodega", 0)),
            "cantidad_en_bodega": int(summary.get("cantidad_en_bodega", 0)),
            "pendiente_trazabilidad_bodega": int(summary.get("pendiente_en_trazabilidad_bodega", 0)),
            "total_ventas": sum(int(r.get("total") or 0) for r in ventas_rows),
        },
        "exs": {
            "vinculados": int(exs_summary.get("count", 0)),
            "saldo_actual_total": int(exs_summary.get("total_actual", 0)),
            "saldo_ex_total": int(exs_summary.get("total_ex", 0)),
            "muestras": (exs_summary.get("rows") or [])[:30],
        },
        "ventas": {
            "top_articulos": top_articulos,
            "top_familias": top_familias,
            "curva_tallas": curva_tallas,
        },
        "saldos": {
            "top_etapas": top_etapas,
            "top_bodega_articulo": top_bodega_articulo,
        },
        "detalle_codigo": detalle_codigo,
    }
    return json.dumps(context_data, ensure_ascii=False, default=str)


def _answer_precise_metrics(question: str) -> str | None:
    qn = _norm_text(question or "")
    if not qn:
        return None

    rows, _, summary = query_rows(DB_PATH, {"q": "", "fecha": ""})
    today_iso = date.today().isoformat()

    asks_orders = _has_keyword(qn, ["orden", "ordenes"])
    asks_cut_order = _has_keyword(qn, ["orden de corte", "ordenes de corte"])
    asks_bodega = _has_keyword(qn, ["bodega", "almacen"])
    asks_today = _has_keyword(qn, ["hoy", "dia de hoy", "hoy dia"])

    # "Orden de corte" se interpreta como identificador de orden, no como etapa corte_1.
    if asks_orders and asks_bodega and not asks_today:
        return (
            f"Actualmente hay {int(summary.get('ordenes_en_bodega', 0))} ordenes en bodega, "
            f"con {int(summary.get('cantidad_en_bodega', 0))} prendas en bodega y "
            f"{int(summary.get('pendiente_en_trazabilidad_bodega', 0))} restantes."
        )

    if asks_today and (asks_cut_order or asks_orders):
        rows_today = [r for r in rows if str(r.get("fecha_iso") or "") == today_iso]
        if asks_bodega:
            rows_today_bodega = [r for r in rows_today if int(r.get("bodega") or 0) > 0]
            prendas_bodega = sum(int(r.get("bodega") or 0) for r in rows_today_bodega)
            return (
                f"Hoy ({today_iso}) hay {len(rows_today_bodega)} ordenes en bodega, "
                f"con {prendas_bodega} prendas en bodega."
            )
        return f"Hoy ({today_iso}) hay {len(rows_today)} ordenes de corte registradas."

    return None


def _answer_assistant(question: str) -> str:
    q = (question or "").strip()
    if not q:
        return "Escribe una pregunta. Ejemplo: En que parte se encuentra 4210."

    ql = q.lower()
    qn = _norm_text(q)
    rows, _, summary = query_rows(DB_PATH, {"q": "", "fecha": ""})
    pedidos_sections = query_pedidos_talla_sections(DB_PATH, "")
    exs_summary = query_exs_balance_summary(DB_PATH, "")
    asks_location = _has_keyword(qn, ["donde", "parte", "encuentra", "ubicacion", "ubica"])

    if _has_keyword(
        qn,
        [
            "puedes leer txt",
            "puedes leer excel",
            "leer archivos",
            "archivos que cargue",
            "archivos cargados",
            "puedes ver mis archivos",
        ],
    ):
        return (
            "Si. Trabajo con la data que cargaste en ADECOM WEB (TXT/Excel) una vez importada a la base de datos. "
            "Puedo responder por articulo, familia, tallas, bodega, pedidos, EXS y cruces entre esas tablas."
        )

    if _has_keyword(qn, ["ayuda", "que puedes", "que sabes", "como funcionas"]):
        return (
            "Puedo responder sobre: ordenes en bodega, total muestras, tabla completa, "
            "pedidos totales, familia con mas pedidos, top articulos, EXS y ubicacion por articulo/familia "
            "(ej: 4210 o 01420100)."
        )

    code = _extract_query_code(q)
    if code and asks_location:
        code_rows, _, _ = query_rows(DB_PATH, {"q": code, "fecha": ""})
        if not code_rows:
            return f"No encontre datos para {code}."

        bodega_rows = [r for r in code_rows if int(r.get("bodega") or 0) > 0]
        prendas_bodega = sum(int(r.get("bodega") or 0) for r in code_rows)
        prendas_proceso = sum(int(r.get("proceso") or 0) for r in code_rows)
        pendientes = sum(int(r.get("pendiente_en_trazabilidad") or 0) for r in code_rows)

        if bodega_rows:
            return (
                f"{code}: se encuentra en bodega en {len(bodega_rows)} orden(es), "
                f"con {prendas_bodega} prendas en bodega. "
                f"Total en proceso: {prendas_proceso}. Pendiente en trazabilidad: {pendientes}."
            )

        top_stage: dict[str, int] = {}
        for row in code_rows:
            stage = str(row.get("proceso_actual") or "Sin movimiento")
            top_stage[stage] = top_stage.get(stage, 0) + int(row.get("proceso") or 0)
        stage_name, stage_total = max(top_stage.items(), key=lambda x: x[1])
        return (
            f"{code}: no tiene prendas en bodega actualmente. "
            f"La mayor cantidad esta en {stage_name} con {stage_total} prendas. "
            f"Total en proceso: {prendas_proceso}."
        )

    if _has_keyword(qn, ["bodega", "almacen"]):
        return (
            f"Ordenes en bodega: {summary.get('ordenes_en_bodega', 0)}. "
            f"Cantidad en bodega: {summary.get('cantidad_en_bodega', 0)}. "
            f"Pendiente en trazabilidad: {summary.get('pendiente_en_trazabilidad_bodega', 0)}."
        )

    if _has_keyword(qn, ["muestra", "muestras"]):
        muestras_rows = [
            row for row in rows if str(row.get("corte", "")).lstrip("0").startswith("96")
        ]
        muestras_total = sum(int(row.get("proceso") or 0) for row in muestras_rows)
        muestras_bodega = sum(int(row.get("bodega") or 0) for row in muestras_rows)
        return (
            f"Total muestras: {len(muestras_rows)} orden(es). "
            f"Prendas en muestras: {muestras_total}. En bodega: {muestras_bodega}."
        )

    if _has_keyword(qn, ["exs", "ex"]):
        if code:
            ex_data = _resolve_ex_details(code)
            if ex_data:
                return (
                    f"EX para {code}: familia actual {ex_data['family_actual']}, "
                    f"EX {ex_data['ex_raw']} (familia {ex_data['family_ex']}). "
                    f"Saldo actual: {ex_data['saldo_actual']}. Saldo EX: {ex_data['saldo_ex']}."
                )
            return f"No encontre mapeo EX para {code}."
        return (
            f"EXS vinculados: {exs_summary.get('count', 0)}. "
            f"Total saldo actual: {exs_summary.get('total_actual', 0)}. "
            f"Total saldo ex: {exs_summary.get('total_ex', 0)}."
        )

    if _has_keyword(qn, ["venta", "ventas", "pedido", "pedidos", "vendido", "vendida", "vender", "mas vendido", "mas pedidos"]):
        ventas_rows = pedidos_sections.get("ventas", [])
        ventas_total = sum(int(r.get("total") or 0) for r in ventas_rows)
        ventas_por_familia: dict[str, int] = {}
        ventas_por_articulo: dict[str, int] = {}
        for r in ventas_rows:
            articulo = str(r.get("articulo") or "").strip()
            total = int(r.get("total") or 0)
            if not articulo:
                continue
            familia = articulo[2:6] if len(articulo) >= 6 else articulo
            ventas_por_familia[familia] = ventas_por_familia.get(familia, 0) + total
            ventas_por_articulo[articulo] = ventas_por_articulo.get(articulo, 0) + total

        rank = _extract_rank(qn)
        familias_sorted = sorted(ventas_por_familia.items(), key=lambda x: x[1], reverse=True)
        articulos_sorted = sorted(ventas_por_articulo.items(), key=lambda x: x[1], reverse=True)
        asks_familia = _has_keyword(qn, ["familia"])
        asks_articulo = _has_keyword(qn, ["articulo", "modelo", "referencia"])
        asks_rank = rank > 1 or _has_keyword(qn, ["top", "mas vendido", "mas vendida", "mas pedidos"])

        if asks_familia:
            if not familias_sorted:
                return "No hay datos de pedidos para calcular familias."
            if rank > len(familias_sorted):
                return f"No hay suficientes familias para obtener el puesto {rank}."
            fam, total = familias_sorted[rank - 1]
            return f"Familia #{rank} en pedidos: {fam}, con {total} unidades."

        if asks_articulo:
            if not articulos_sorted:
                return "No hay datos de pedidos para calcular articulos."
            if rank > len(articulos_sorted):
                return f"No hay suficientes articulos para obtener el puesto {rank}."
            art, total = articulos_sorted[rank - 1]
            return f"Articulo #{rank} en pedidos: {art}, con {total} unidades."

        if asks_rank:
            if not articulos_sorted or not familias_sorted:
                return "No hay datos de pedidos para calcular ranking."
            if rank > len(articulos_sorted) or rank > len(familias_sorted):
                return f"No hay suficientes datos para obtener el puesto {rank}."
            art, art_total = articulos_sorted[rank - 1]
            fam, fam_total = familias_sorted[rank - 1]
            return (
                f"Puesto #{rank} en pedidos: articulo {art} ({art_total}) y familia {fam} ({fam_total}). "
                f"Si quieres uno especifico, pregunta por 'articulo' o 'familia'."
            )

        return f"Total pedidos actual: {ventas_total} unidades."

    if _has_keyword(qn, ["tabla completa", "total ordenes", "cuantas ordenes", "cuantos registros"]):
        return f"Tabla completa: {len(rows)} orden(es) registradas."

    if code and _has_keyword(qn, ["toda la informacion", "todo sobre", "detalle completo", "toda la info"]):
        code_rows, _, _ = query_rows(DB_PATH, {"q": code, "fecha": ""})
        if not code_rows:
            ex_data = _resolve_ex_details(code)
            if ex_data:
                return (
                    f"{code}: no tiene registros en saldos actuales, pero su mapeo EX es "
                    f"{ex_data['ex_raw']} (familia {ex_data['family_ex']}). "
                    f"Saldo actual: {ex_data['saldo_actual']}; saldo EX: {ex_data['saldo_ex']}."
                )
            return f"No encontre datos para {code}."
        ordenes = len(code_rows)
        prendas_bodega = sum(int(r.get("bodega") or 0) for r in code_rows)
        prendas_proceso = sum(int(r.get("proceso") or 0) for r in code_rows)
        pendientes = sum(int(r.get("pendiente_en_trazabilidad") or 0) for r in code_rows)
        by_stage: dict[str, int] = {}
        for r in code_rows:
            stage = str(r.get("proceso_actual") or "Sin movimiento")
            by_stage[stage] = by_stage.get(stage, 0) + int(r.get("proceso") or 0)
        stage_txt = ", ".join(f"{k}:{v}" for k, v in sorted(by_stage.items(), key=lambda x: x[1], reverse=True)[:4])

        ventas_related = query_pedidos_talla_sections(DB_PATH, code).get("ventas", [])
        ventas_total = sum(int(r.get("total") or 0) for r in ventas_related)
        ex_data = _resolve_ex_details(code)
        ex_txt = (
            f"EX {ex_data['ex_raw']} (familia {ex_data['family_ex']}), saldo ex {ex_data['saldo_ex']}"
            if ex_data
            else "sin mapeo EX"
        )
        return (
            f"{code}: ordenes {ordenes}; en bodega {prendas_bodega}; total proceso {prendas_proceso}; "
            f"pendiente {pendientes}; etapas {stage_txt}; pedidos relacionados {ventas_total}; {ex_txt}."
        )

    if code:
        code_rows, _, _ = query_rows(DB_PATH, {"q": code, "fecha": ""})
        if not code_rows:
            return f"No encontre datos para {code}."
        return (
            f"Encontre {len(code_rows)} registro(s) para {code}. "
            "Si quieres ubicacion exacta pregunta: 'en que parte se encuentra ...'."
        )

    return (
        "Puedo ayudarte con bodega, muestras, pedidos, EXS o por codigo de articulo/familia. "
        "Ejemplos: 'Ordenes en bodega', 'Familia con mas pedidos', 'EXS total', 'En que parte se encuentra 4210'."
    )


def _answer_with_gemini(question: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY no configurada.")

    precise_answer = _answer_precise_metrics(question)
    if precise_answer:
        return precise_answer

    env_model = os.environ.get("GEMINI_MODEL", "").strip()
    env_api_version = os.environ.get("GEMINI_API_VERSION", "").strip()
    api_versions: list[str] = []
    for v in [env_api_version, "v1", "v1beta"]:
        vv = str(v or "").strip()
        if vv and vv not in api_versions:
            api_versions.append(vv)

    discovered_models: list[str] = []
    list_errors: list[str] = []
    for api_version in api_versions:
        try:
            list_endpoint = f"https://generativelanguage.googleapis.com/{api_version}/models"
            list_req = url_request.Request(
                list_endpoint,
                headers={"x-goog-api-key": api_key},
                method="GET",
            )
            with url_request.urlopen(list_req, timeout=12) as resp:
                raw = resp.read().decode("utf-8")
            data = json.loads(raw or "{}")
            for item in data.get("models") or []:
                methods = item.get("supportedGenerationMethods") or []
                if "generateContent" not in methods:
                    continue
                name = str(item.get("name") or "").strip()
                if name.startswith("models/"):
                    name = name.split("/", 1)[1]
                if name and name not in discovered_models:
                    discovered_models.append(name)
            if discovered_models:
                break
        except Exception as exc:
            list_errors.append(f"{api_version}:{exc}")
            continue

    fallback_models = [
        "gemini-3-flash-preview",
        "gemini-2.5-flash",
        "gemini-2.0-flash",
        "gemini-2.0-flash-lite",
        "gemini-1.5-flash",
    ]
    model_candidates: list[str] = []
    for m in [env_model, *discovered_models, *fallback_models]:
        m = str(m or "").strip()
        if m and m not in model_candidates:
            model_candidates.append(m)

    context = _build_assistant_context(question)
    instruction = (
        "Responde en espanol con tono cercano y natural, como un asistente humano. "
        "Prioriza frases cortas y claras; evita sonar mecanico. "
        "No uses listas largas salvo que te pidan detalle. "
        "Usa SOLO el contexto entregado (proviene de todos los archivos cargados e importados en ADECOM WEB). "
        "Aplica siempre 'reglas_negocio' y 'reglas_interpretacion' antes de responder. "
        "Interpretacion obligatoria: BODEGA es solo columna bodega; RESTANTE (pendiente_en_trazabilidad) es distinto y no debe reportarse como bodega. "
        "Cuando pregunten por fecha o por 'hoy', usa el bloque fechas (campo fecha_iso). "
        "No digas que no hay fecha si existe fechas.ultimas_fechas o fechas.registros_hoy. "
        "Si preguntan por un codigo/familia, prioriza detalle_codigo.resumen_saldos para los totales. "
        "Si te preguntan si puedes leer archivos, aclara que si puedes analizarlos una vez cargados al sistema. "
        "Si falta dato, dilo explicitamente sin inventar."
    )
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": f"{instruction}\n{context}\nPregunta: {question}"}],
            }
        ]
    }

    # 1) Intento principal con SDK oficial de Gemini.
    sdk_errors: list[str] = []
    try:
        from google import genai  # type: ignore

        client = genai.Client(api_key=api_key)
        for model in model_candidates:
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=f"{instruction}\n{context}\nPregunta: {question}",
                )
                text = str(getattr(response, "text", "") or "").strip()
                if text:
                    return text
                sdk_errors.append(f"{model}:sin_texto")
            except Exception as exc:
                sdk_errors.append(f"{model}:{exc}")
                continue
    except Exception as exc:
        sdk_errors.append(f"sdk_import_or_client:{exc}")

    # 2) Fallback REST si el SDK falla.
    last_error = None
    tried: list[str] = []
    for api_version in api_versions:
        for model in model_candidates:
            tried.append(f"{api_version}:{model}")
            endpoint = (
                f"https://generativelanguage.googleapis.com/{api_version}/models/{model}:generateContent"
            )
            req = url_request.Request(
                endpoint,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": api_key,
                },
                method="POST",
            )
            try:
                with url_request.urlopen(req, timeout=18) as resp:
                    raw = resp.read().decode("utf-8")
                data = json.loads(raw or "{}")
                candidates = data.get("candidates") or []
                if not candidates:
                    last_error = RuntimeError(f"Gemini ({model}) no retorno candidatos.")
                    continue
                parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
                text = " ".join(str(p.get("text") or "").strip() for p in parts).strip()
                if not text:
                    last_error = RuntimeError(f"Gemini ({model}) no retorno texto.")
                    continue
                return text
            except url_error.HTTPError as exc:
                # 404: modelo no disponible. 403/401: key/permisos.
                last_error = RuntimeError(f"Gemini ({model}) HTTP {exc.code}")
                continue
            except Exception as exc:
                last_error = exc
                continue

    raise RuntimeError(
        "Gemini no disponible. "
        f"SDK: {'; '.join(sdk_errors[:8]) if sdk_errors else 'sin_intentos_sdk'}. "
        f"Modelos intentados: {', '.join(tried)}. "
        f"Listado modelos: {'; '.join(list_errors) if list_errors else 'ok'}. "
        f"Detalle final: {last_error}"
    )


def _answer_assistant_router(question: str) -> dict:
    provider = os.environ.get("ADECOM_ASSISTANT_PROVIDER", "local").strip().lower()
    if provider in {"gemini", "google"}:
        return {
            "answer": _answer_with_gemini(question),
            "provider": "gemini",
            "fallback": False,
            "detail": "",
        }
    try:
        return {
            "answer": _answer_assistant(question),
            "provider": "local",
            "fallback": False,
            "detail": "",
        }
    except Exception as exc:
        app.logger.exception("Fallo en asistente local", exc_info=exc)
        return {
            "answer": "No fue posible responder en este momento. Intenta nuevamente.",
            "provider": "local",
            "fallback": True,
            "detail": str(exc),
        }


def _table_count(table_name: str) -> int:
    conn = get_conn(DB_PATH)
    try:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM {table_name}").fetchone()
        return int(row["n"] if row else 0)
    except Exception as exc:
        app.logger.warning("No se pudo contar tabla %s: %s", table_name, exc)
        return 0
    finally:
        conn.close()


def _norm_file_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())


def _find_autoload_file(folder: Path, token: str, *, exclude_token: str = "") -> Path | None:
    if not folder.exists() or not folder.is_dir():
        return None
    token_n = _norm_file_key(token)
    exclude_n = _norm_file_key(exclude_token) if exclude_token else ""
    candidates: list[Path] = []
    for p in folder.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".txt", ".csv"}:
            continue
        key = _norm_file_key(p.name)
        if token_n not in key:
            continue
        if exclude_n and exclude_n in key:
            continue
        candidates.append(p)
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return candidates[0]


def _read_source_bytes(source: str) -> bytes:
    src = str(source or "").strip()
    if not src:
        raise ValueError("Fuente vacia.")
    if src.startswith(("http://", "https://")):
        req = url_request.Request(
            src,
            headers={
                "User-Agent": "ADECOM-WEB/1.0",
                "Accept": "text/plain,application/octet-stream,*/*",
            },
        )
        with url_request.urlopen(req, timeout=35) as resp:
            return resp.read()
    path = Path(src)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"No existe archivo fuente: {src}")
    return path.read_bytes()


def _refresh_web_data() -> dict:
    saldos_rows = parse_saldos_txt(_read_source_bytes(AUTOLOAD_SALDOS_SOURCE))
    pedidos_rows = parse_pedidos_talla_txt(_read_source_bytes(AUTOLOAD_PEDIDOS_SOURCE))
    etapas_rows = parse_corte_etapas_txt(_read_source_bytes(AUTOLOAD_ETAPAS_SOURCE))
    if not saldos_rows or not pedidos_rows or not etapas_rows:
        raise ValueError(
            f"Lectura vacia: saldos={len(saldos_rows)}, pedidos={len(pedidos_rows)}, etapas={len(etapas_rows)}"
        )
    stats_saldos = import_rows(DB_PATH, saldos_rows, replace_all=True)
    stats_pedidos = import_pedidos_talla_rows(DB_PATH, pedidos_rows)
    stats_etapas = import_corte_etapas_rows(DB_PATH, etapas_rows)
    return {
        "saldos": stats_saldos,
        "pedidos": stats_pedidos,
        "etapas": stats_etapas,
    }


def _auto_refresh_web_on_startup() -> None:
    sources = [AUTOLOAD_SALDOS_SOURCE, AUTOLOAD_PEDIDOS_SOURCE, AUTOLOAD_ETAPAS_SOURCE]
    if not AUTO_REFRESH_WEB_ON_START:
        app.logger.info("Auto refresh web al iniciar deshabilitado (ADECOM_AUTO_REFRESH_WEB_ON_START=0).")
        return
    if not all(str(src or "").strip() for src in sources):
        app.logger.warning(
            "Auto refresh web omitido: faltan variables ADECOM_AUTOLOAD_SALDOS_SOURCE/ADECOM_AUTOLOAD_PEDIDOS_SOURCE/ADECOM_AUTOLOAD_ETAPAS_SOURCE."
        )
        return
    try:
        stats = _refresh_web_data()
        app.logger.info(
            "Auto refresh web OK al iniciar. SALDOS I%s/A%s | PEDIDOS I%s/A%s | ETAPAS I%s/A%s",
            stats["saldos"].get("inserted", 0),
            stats["saldos"].get("updated", 0),
            stats["pedidos"].get("inserted", 0),
            stats["pedidos"].get("updated", 0),
            stats["etapas"].get("inserted", 0),
            stats["etapas"].get("updated", 0),
        )
    except Exception as exc:
        app.logger.exception("Auto refresh web fallo al iniciar: %s", exc, exc_info=exc)


def _to_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    text = text.replace("%", "").replace(" ", "")
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def _canonical_area(area: object) -> str:
    probe = _norm_text(str(area or ""))
    if "corte" in probe:
        return "CORTE"
    if "taller externo" in probe or "t externo" in probe or "ext" in probe:
        return "TALLER EXTERNO"
    if "taller" in probe:
        return "TALLER"
    if "limpi" in probe:
        return "LIMPIADO"
    if "lavander" in probe:
        return "LAVANDERIA"
    if "termina" in probe:
        return "TERMINACION"
    if "bodega" in probe:
        return "BODEGA"
    return str(area or "").strip().upper() or "SIN AREA"


def _status_from_ratio(ratio: float) -> str:
    if ratio >= 1:
        return "green"
    if ratio >= 0.85:
        return "yellow"
    return "red"


def _month_from_text(value: object) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "", ""
    probe = _norm_text(raw)
    months = {
        "enero": 1,
        "ene": 1,
        "febrero": 2,
        "feb": 2,
        "marzo": 3,
        "mar": 3,
        "abril": 4,
        "abr": 4,
        "mayo": 5,
        "may": 5,
        "junio": 6,
        "jun": 6,
        "julio": 7,
        "jul": 7,
        "agosto": 8,
        "ago": 8,
        "septiembre": 9,
        "setiembre": 9,
        "sep": 9,
        "octubre": 10,
        "oct": 10,
        "noviembre": 11,
        "nov": 11,
        "diciembre": 12,
        "dic": 12,
    }
    # YYYY-MM
    m = re.search(r"(\d{4})[-/](\d{1,2})", raw)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        if 1 <= mo <= 12:
            key = f"{y:04d}-{mo:02d}"
            return key, f"{mo:02d}/{y}"
    # MM-YYYY or MM/YY
    m2 = re.search(r"(\d{1,2})[-/](\d{2,4})", raw)
    if m2:
        mo = int(m2.group(1))
        y = int(m2.group(2))
        if y < 100:
            y += 2000
        if 1 <= mo <= 12:
            key = f"{y:04d}-{mo:02d}"
            return key, f"{mo:02d}/{y}"
    # Marzo 2026
    y3 = re.search(r"(20\d{2})", probe)
    for token, mo in months.items():
        if token in probe:
            y = int(y3.group(1)) if y3 else date.today().year
            key = f"{y:04d}-{mo:02d}"
            return key, f"{mo:02d}/{y}"
    return "", ""


def _parse_day(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        d = int(value)
        return d if 1 <= d <= 31 else 0
    text = str(value).strip()
    if text.isdigit():
        d = int(text)
        return d if 1 <= d <= 31 else 0
    m = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})", text)
    if m:
        d = int(m.group(1))
        if 1 <= d <= 31:
            return d
    m2 = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", text)
    if m2:
        d = int(m2.group(3))
        if 1 <= d <= 31:
            return d
    return 0


def _parse_proyeccion_rows_from_bytes(content: bytes, filename: str) -> list[dict[str, object]]:
    def parse_tabular_rows(values: list[tuple]) -> list[dict[str, object]]:
        if not values:
            return []
        header = [str(c or "").strip() for c in values[0]]
        rows_dict = [dict(zip(header, row)) for row in values[1:]]
        out: list[dict[str, object]] = []
        for row in rows_dict:
            keys = {_norm_text(k): k for k in row.keys() if str(k or "").strip()}
            area_key = next((keys[k] for k in keys if k in {"area", "seccion", "proceso"}), None)
            actual_key = next(
                (
                    keys[k]
                    for k in keys
                    if k in {"actual", "producido", "avance", "unidades", "cantidad", "real", "total"}
                ),
                None,
            )
            fecha_key = next((keys[k] for k in keys if k in {"fecha", "date"}), None)
            mes_key = next((keys[k] for k in keys if k in {"mes", "month", "periodo", "periodo mes"}), None)
            dia_key = next((keys[k] for k in keys if k in {"dia", "day"}), None)
            meta_key = next((keys[k] for k in keys if k in {"meta", "meta dia", "meta_diaria", "objetivo"}), None)
            if not area_key or not actual_key:
                continue
            month_key = ""
            month_label = ""
            day = 0
            if fecha_key:
                mk, ml = _month_from_text(row.get(fecha_key))
                month_key, month_label = mk, ml
                day = _parse_day(row.get(fecha_key))
            if not month_key and mes_key:
                mk, ml = _month_from_text(row.get(mes_key))
                month_key, month_label = mk, ml
            if day == 0 and dia_key:
                day = _parse_day(row.get(dia_key))
            if not month_key:
                continue
            out.append(
                {
                    "month_key": month_key,
                    "month_label": month_label or month_key,
                    "area": str(row.get(area_key) or "").strip(),
                    "day": day,
                    "actual": int(round(_to_float(row.get(actual_key)))),
                    "meta_day": int(round(_to_float(row.get(meta_key)))) if meta_key else 0,
                }
            )
        return out

    def parse_matrix_rows(sheet_name: str, values: list[tuple]) -> list[dict[str, object]]:
        if len(values) < 6:
            return []
        mk, ml = _month_from_text(sheet_name)
        if not mk:
            return []
        head1 = list(values[0]) if len(values) > 0 else []
        head2 = list(values[1]) if len(values) > 1 else []
        head3 = list(values[2]) if len(values) > 2 else []
        head4 = list(values[3]) if len(values) > 3 else []

        days_month = 0
        line0 = [str(c or "").strip() for c in head1]
        for i, cell in enumerate(line0[:-1]):
            n = _norm_text(cell)
            if "dias habiles mes" in n:
                days_month = int(round(_to_float(line0[i + 1])))
                break
        if days_month <= 0:
            for row in values[4:]:
                day = _parse_day(row[0] if len(row) > 0 else "")
                if not day:
                    continue
                marker = _norm_text(str(row[2] if len(row) > 2 else ""))
                if marker in {"f", "feriado"}:
                    continue
                days_month += 1

        max_cols = max(len(head2), len(head3), len(head4))
        current_area = ""
        out: list[dict[str, object]] = []
        for col in range(3, max_cols):
            area_raw = str(head2[col] if col < len(head2) else "").strip()
            if area_raw:
                current_area = area_raw
            area_name = current_area.strip()
            tipo_raw = str(head3[col] if col < len(head3) else "").strip()
            meta_day = int(round(_to_float(head4[col] if col < len(head4) else 0)))
            if not area_name and not tipo_raw:
                continue
            area_label = area_name or tipo_raw
            tipo_norm = _norm_text(tipo_raw)
            if tipo_norm and tipo_norm not in {"dia"}:
                area_label = f"{area_label} / {tipo_raw}"

            has_any = False
            for row in values[4:]:
                day = _parse_day(row[0] if len(row) > 0 else "")
                if not day:
                    continue
                raw_val = row[col] if col < len(row) else 0
                val = int(round(_to_float(raw_val)))
                if val == 0 and not str(raw_val or "").strip():
                    continue
                has_any = True
                out.append(
                    {
                        "month_key": mk,
                        "month_label": ml or mk,
                        "area": area_label,
                        "day": day,
                        "actual": val,
                        "meta_day": max(meta_day, 0),
                        "days_month": max(days_month, 0),
                    }
                )
            if not has_any and meta_day > 0:
                out.append(
                    {
                        "month_key": mk,
                        "month_label": ml or mk,
                        "area": area_label,
                        "day": 0,
                        "actual": 0,
                        "meta_day": max(meta_day, 0),
                        "days_month": max(days_month, 0),
                    }
                )
        return out

    ext = Path(filename or "").suffix.lower()
    if ext in {".csv", ".txt"}:
        text = content.decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(io.StringIO(text), delimiter=";" if ";" in text.splitlines()[0] else ",")
        raw_rows = list(reader)
    elif ext == ".xlsx":
        from openpyxl import load_workbook

        wb = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
        parsed: list[dict[str, object]] = []
        for ws in wb.worksheets:
            values = list(ws.iter_rows(values_only=True, min_row=1, max_row=220))
            if not values:
                continue
            sheet_rows = parse_matrix_rows(ws.title, values)
            if not sheet_rows:
                sheet_rows = parse_tabular_rows(values)
            parsed.extend(sheet_rows)
        return parsed
    elif ext == ".xls":
        try:
            import xlrd  # type: ignore
        except Exception as exc:
            raise ValueError("Para leer .xls falta dependencia xlrd. Guarda el archivo como .xlsx.") from exc
        wb = xlrd.open_workbook(file_contents=content)
        parsed: list[dict[str, object]] = []
        for sheet in wb.sheets():
            values: list[tuple] = []
            for r in range(min(sheet.nrows, 220)):
                row = tuple(sheet.cell_value(r, c) for c in range(sheet.ncols))
                values.append(row)
            if not values:
                continue
            sheet_rows = parse_matrix_rows(sheet.name, values)
            if not sheet_rows:
                sheet_rows = parse_tabular_rows(values)
            parsed.extend(sheet_rows)
        return parsed
    else:
        raise ValueError("Formato no soportado para proyeccion. Usa CSV, XLS o XLSX.")
    # CSV/TXT tabular
    parsed: list[dict[str, object]] = []
    for row in raw_rows:
        keys = {_norm_text(k): k for k in row.keys() if str(k or "").strip()}
        area_key = next((keys[k] for k in keys if k in {"area", "seccion", "proceso"}), None)
        actual_key = next((keys[k] for k in keys if k in {"actual", "real", "total", "cantidad", "producido"}), None)
        fecha_key = next((keys[k] for k in keys if k in {"fecha", "date"}), None)
        mes_key = next((keys[k] for k in keys if k in {"mes", "month", "periodo"}), None)
        dia_key = next((keys[k] for k in keys if k in {"dia", "day"}), None)
        meta_key = next((keys[k] for k in keys if k in {"meta", "meta dia", "meta_diaria"}), None)
        if not area_key or not actual_key:
            continue
        month_key = ""
        month_label = ""
        day = 0
        if fecha_key:
            mk, ml = _month_from_text(row.get(fecha_key))
            month_key, month_label = mk, ml
            day = _parse_day(row.get(fecha_key))
        if not month_key and mes_key:
            mk, ml = _month_from_text(row.get(mes_key))
            month_key, month_label = mk, ml
        if day == 0 and dia_key:
            day = _parse_day(row.get(dia_key))
        if not month_key:
            continue
        parsed.append(
            {
                "month_key": month_key,
                "month_label": month_label or month_key,
                "area": str(row.get(area_key) or "").strip(),
                "day": day,
                "actual": int(round(_to_float(row.get(actual_key)))),
                "meta_day": int(round(_to_float(row.get(meta_key)))) if meta_key else 0,
            }
        )
    return parsed


def _build_proyeccion_view(monthly_goal: int, rows: list[dict[str, object]]) -> dict[str, object]:
    goal = max(int(monthly_goal or 0), 0)
    total_weight = sum(v for v in AREA_WEIGHTS.values() if v > 0) or 1

    by_month_area_daily: dict[str, dict[str, dict[int, int]]] = {}
    by_month_area_meta_day: dict[str, dict[str, int]] = {}
    by_month_days_count: dict[str, int] = {}
    by_month_label: dict[str, str] = {}
    for row in rows:
        month_key = str(row.get("month_key") or "").strip()
        if not month_key:
            continue
        by_month_label[month_key] = str(row.get("month_label") or month_key)
        area = str(row.get("area") or "").strip().upper()
        area = _canonical_area(area) if area in AREA_WEIGHTS else area
        day = int(row.get("day") or 0)
        day = day if 1 <= day <= 31 else 0
        by_month_area_daily.setdefault(month_key, {}).setdefault(area, {})
        by_month_area_daily[month_key][area][day] = by_month_area_daily[month_key][area].get(day, 0) + int(row.get("actual") or 0)
        meta_day = int(row.get("meta_day") or 0)
        if meta_day > 0:
            by_month_area_meta_day.setdefault(month_key, {})
            by_month_area_meta_day[month_key][area] = max(by_month_area_meta_day[month_key].get(area, 0), meta_day)
        dm = int(row.get("days_month") or 0)
        if dm > 0:
            by_month_days_count[month_key] = max(by_month_days_count.get(month_key, 0), dm)

    months: list[dict[str, object]] = []
    for month_key in sorted(by_month_area_daily.keys()):
        area_rows: list[dict[str, object]] = []
        month_total = 0
        month_areas = sorted(by_month_area_daily[month_key].keys())
        if not month_areas:
            continue
        days_month = by_month_days_count.get(month_key, 0)
        if days_month <= 0:
            day_set = set()
            for a in month_areas:
                for d in by_month_area_daily[month_key].get(a, {}):
                    if d > 0:
                        day_set.add(d)
            days_month = len(day_set)

        use_sheet_meta = bool(by_month_area_meta_day.get(month_key))
        for area in month_areas:
            area_weight = AREA_WEIGHTS.get(area, 0)
            fallback_target = int(round(goal * (area_weight / total_weight))) if area_weight > 0 else 0
            meta_day = by_month_area_meta_day.get(month_key, {}).get(area, 0)
            area_target = (meta_day * days_month) if use_sheet_meta and meta_day > 0 else fallback_target
            if area_target <= 0 and not use_sheet_meta:
                # para areas extras (ej. ventas) sin peso fijo, repartir una fraccion minima
                area_target = int(round(goal / max(len(month_areas), 1)))
            area_actual = sum(int(v) for v in area_daily_map.values())
            month_total += area_actual
            ratio = (area_actual / area_target) if area_target > 0 else 0.0
            daily = [
                {"day": int(d), "actual": int(v)}
                for d, v in sorted(area_daily_map.items(), key=lambda x: x[0])
                if int(d) > 0
            ]
            area_rows.append(
                {
                    "area": area,
                    "target": area_target,
                    "actual": area_actual,
                    "ratio_pct": round(ratio * 100, 1),
                    "status": _status_from_ratio(ratio),
                    "daily_rows": daily,
                }
            )
        month_ratio = (month_total / goal) if goal > 0 else 0.0
        months.append(
            {
                "key": month_key,
                "label": by_month_label.get(month_key, month_key),
                "areas": area_rows,
                "total_actual": month_total,
                "total_ratio_pct": round(month_ratio * 100, 1),
                "status": _status_from_ratio(month_ratio),
            }
        )

    default_month_key = months[-1]["key"] if months else ""
    return {
        "monthly_goal": goal,
        "months": months,
        "default_month_key": default_month_key,
    }


def _load_proyeccion_state() -> dict[str, object]:
    if not PROYECCION_STATE_PATH.exists():
        auto_rows = _autoload_proyeccion_rows()
        return {"monthly_goal": 48000, "rows": auto_rows, "view": _build_proyeccion_view(48000, auto_rows)}
    try:
        payload = json.loads(PROYECCION_STATE_PATH.read_text(encoding="utf-8"))
        goal = int(payload.get("monthly_goal") or payload.get("weekly_goal") or 48000)
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            rows = []
        if not rows:
            rows = _autoload_proyeccion_rows()
        view = _build_proyeccion_view(goal, rows)
        return {"monthly_goal": goal, "rows": rows, "view": view}
    except Exception:
        auto_rows = _autoload_proyeccion_rows()
        return {"monthly_goal": 48000, "rows": auto_rows, "view": _build_proyeccion_view(48000, auto_rows)}


def _save_proyeccion_state(monthly_goal: int, rows: list[dict[str, object]]) -> None:
    payload = {"monthly_goal": int(monthly_goal), "rows": rows}
    PROYECCION_STATE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _autoload_proyeccion_rows() -> list[dict[str, object]]:
    candidates = []
    if AUTOLOAD_DIR.exists():
        for pattern in ("*MOVTOS*SECCIONES*.xlsx", "*MOVTOS*SECCIONES*.xls", "*MOVTOS*.xlsx", "*MOVTOS*.xls"):
            candidates.extend(sorted(AUTOLOAD_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True))
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            rows = _parse_proyeccion_rows_from_bytes(path.read_bytes(), path.name)
            if rows:
                return rows
        except Exception:
            continue
    return []


def ensure_seed_data() -> None:
    init_db(DB_PATH)
    if _table_count("saldos_seccion") == 0 and SEED_SALDOS.exists():
        saldos_rows = parse_saldos_txt(SEED_SALDOS.read_bytes())
        if saldos_rows:
            import_rows(DB_PATH, saldos_rows)
    if _table_count("pedidos_talla") == 0 and SEED_PEDIDOS.exists():
        pedidos_rows = parse_pedidos_talla_txt(SEED_PEDIDOS.read_bytes())
        if pedidos_rows:
            import_pedidos_talla_rows(DB_PATH, pedidos_rows)


# En despliegues, no sembrar datos automaticamente a menos que se solicite.
# Esto evita "volver" a los datos del seed cuando el hosting reinicia con disco efimero.
if os.environ.get("ADECOM_ENABLE_SEED", "0").strip() == "1":
    ensure_seed_data()
else:
    init_db(DB_PATH)

_auto_refresh_web_on_startup()


@app.template_filter("miles")
def miles(value):
    try:
        number = int(value or 0)
    except (TypeError, ValueError):
        return value
    return f"{number:,}".replace(",", ".")


@app.get("/")
def index():
    assistant_provider = os.environ.get("ADECOM_ASSISTANT_PROVIDER", "local").strip().lower()
    if assistant_provider in {"google", "gemini"}:
        assistant_provider = "gemini"
    else:
        assistant_provider = "local"
    if not ASSISTANT_ENABLED:
        assistant_provider = "off"
    filters = {
        "q": request.args.get("q", "").strip(),
        "fecha": request.args.get("fecha", "").strip(),
    }
    rows, totals, summary = query_rows(DB_PATH, filters)
    pedidos_sections = query_pedidos_talla_sections(DB_PATH, filters["q"])
    exs_summary = query_exs_balance_summary(DB_PATH, filters["q"])
    pedidos_count = sum(len(section_rows) for section_rows in pedidos_sections.values())
    search_error = ""
    if filters["q"] and not rows and pedidos_count == 0:
        search_error = "No se encontraron resultados. Escriba el articulo completo o familia. Ej: 01420100 o 4201."
    ventas_rows = pedidos_sections.get("ventas", [])
    ventas_total = sum(int(r.get("total") or 0) for r in ventas_rows)
    ventas_por_articulo: dict[str, int] = {}
    ventas_por_familia: dict[str, dict] = {}
    ventas_por_talla: dict[int, int] = {}
    for r in ventas_rows:
        articulo = str(r.get("articulo") or "").strip()
        total = int(r.get("total") or 0)
        if not articulo:
            continue
        ventas_por_articulo[articulo] = ventas_por_articulo.get(articulo, 0) + total
        for item in r.get("tallas_items") or []:
            talla = int(item.get("talla") or 0)
            cantidad = int(item.get("cantidad") or 0)
            if talla > 0:
                ventas_por_talla[talla] = ventas_por_talla.get(talla, 0) + cantidad
        familia = articulo[2:6] if len(articulo) >= 6 else articulo
        if familia not in ventas_por_familia:
            ventas_por_familia[familia] = {
                "familia": familia,
                "total": 0,
                "articulos": {},
                "sufijos": {},
            }
        ventas_por_familia[familia]["total"] += total
        if articulo not in ventas_por_familia[familia]["articulos"]:
            ventas_por_familia[familia]["articulos"][articulo] = {
                "articulo": articulo,
                "total": 0,
            }
        ventas_por_familia[familia]["articulos"][articulo]["total"] += total
        sufijo = articulo[-2:] if len(articulo) >= 2 else articulo
        if sufijo not in ventas_por_familia[familia]["sufijos"]:
            ventas_por_familia[familia]["sufijos"][sufijo] = {
                "sufijo": sufijo,
                "total": 0,
            }
        ventas_por_familia[familia]["sufijos"][sufijo]["total"] += total

    ventas_grouped = sorted(
        [
            {
                "familia": g["familia"],
                "total": g["total"],
                "articulos": sorted(
                    g["articulos"].values(),
                    key=lambda v: v["total"],
                    reverse=True,
                ),
                "sufijos": sorted(
                    g["sufijos"].values(),
                    key=lambda s: s["total"],
                    reverse=True,
                ),
            }
            for g in ventas_por_familia.values()
        ],
        key=lambda g: g["total"],
        reverse=True,
    )
    ventas_top_familia = ventas_grouped[0] if ventas_grouped else None
    ventas_tallas = sorted(
        [{"talla": talla, "total": total} for talla, total in ventas_por_talla.items()],
        key=lambda x: x["talla"],
    )
    ventas_top_talla = max(ventas_tallas, key=lambda x: x["total"]) if ventas_tallas else None
    ventas_top_articulo = None
    if ventas_por_articulo:
        top_articulo, top_total = max(ventas_por_articulo.items(), key=lambda x: x[1])
        ventas_top_articulo = {"articulo": top_articulo, "total": int(top_total)}
    ventas_top_articulos = sorted(
        [
            {"articulo": articulo, "total": int(total)}
            for articulo, total in ventas_por_articulo.items()
        ],
        key=lambda x: x["total"],
        reverse=True,
    )
    bodega_rows = [row for row in rows if int(row.get("bodega") or 0) > 0]
    bodega_total = sum(int(row.get("proceso") or 0) for row in bodega_rows)
    bodega_en_bodega = sum(int(row.get("bodega") or 0) for row in bodega_rows)
    bodega_restante = sum(int(row.get("pendiente_en_trazabilidad") or 0) for row in bodega_rows)
    bodega_con_etapas = sum(1 for row in bodega_rows if str(row.get("etapas_fechas_detalle") or "-") != "-")
    muestras_rows = [
        row
        for row in rows
        if str(row.get("corte", "")).lstrip("0").startswith("96")
    ]
    muestras_total = sum(int(row.get("proceso") or 0) for row in muestras_rows)
    muestras_bodega = sum(int(row.get("bodega") or 0) for row in muestras_rows)
    muestras_restante = max(muestras_total - muestras_bodega, 0)
    muestras_con_etapas = sum(1 for row in muestras_rows if str(row.get("etapas_fechas_detalle") or "-") != "-")
    upload_debug = session.get("upload_debug", "")
    proyeccion_state = _load_proyeccion_state()
    return render_template(
        "index.html",
        rows=rows,
        totals=totals,
        summary=summary,
        pedidos_sections=pedidos_sections,
        ventas_total=ventas_total,
        ventas_grouped=ventas_grouped,
        ventas_top_familia=ventas_top_familia,
        ventas_tallas=ventas_tallas,
        ventas_top_talla=ventas_top_talla,
        ventas_top_articulo=ventas_top_articulo,
        ventas_top_articulos=ventas_top_articulos,
        exs_summary=exs_summary,
        search_error=search_error,
        filters=filters,
        bodega_rows=bodega_rows,
        bodega_total=bodega_total,
        bodega_en_bodega=bodega_en_bodega,
        bodega_restante=bodega_restante,
        bodega_con_etapas=bodega_con_etapas,
        muestras_rows=muestras_rows,
        muestras_total=muestras_total,
        muestras_bodega=muestras_bodega,
        muestras_restante=muestras_restante,
        muestras_con_etapas=muestras_con_etapas,
        upload_debug=upload_debug,
        proyeccion_state=proyeccion_state,
        can_upload=_can_upload(),
        admin_key_enabled=bool(_admin_key()),
        assistant_enabled=ASSISTANT_ENABLED,
        assistant_provider=assistant_provider,
    )


@app.post("/upload-proyeccion")
def upload_proyeccion():
    if not _can_upload():
        flash("Acceso denegado para actualizar proyeccion.", "error")
        return redirect(url_for("index"))
    try:
        monthly_goal = int(
            str(request.form.get("monthly_goal") or request.form.get("weekly_goal") or "48000").strip() or "48000"
        )
        if monthly_goal <= 0:
            raise ValueError("La meta mensual debe ser mayor que 0.")
        file = request.files.get("proyeccion_file")
        if not file or not file.filename:
            raise ValueError("Debes seleccionar una hoja CSV o XLSX.")
        rows = _parse_proyeccion_rows_from_bytes(file.read(), file.filename or "")
        if not rows:
            raise ValueError("No se detectaron filas validas. Usa columnas: area, real y fecha/mes.")
        _save_proyeccion_state(monthly_goal, rows)
        flash(
            f"Proyeccion cargada. Meta mensual: {monthly_goal}. Filas validas: {len(rows)}.",
            "success",
        )
    except Exception as exc:
        flash(f"No se pudo cargar la proyeccion: {exc}", "error")
    return redirect(url_for("index"))


@app.post("/clear-proyeccion")
def clear_proyeccion():
    if not _can_upload():
        flash("Acceso denegado para limpiar proyeccion.", "error")
        return redirect(url_for("index"))
    try:
        if PROYECCION_STATE_PATH.exists():
            PROYECCION_STATE_PATH.unlink()
        flash("Proyeccion reiniciada.", "success")
    except Exception as exc:
        flash(f"No se pudo limpiar la proyeccion: {exc}", "error")
    return redirect(url_for("index"))


@app.post("/upload")
def upload():
    if not _can_upload():
        flash("Acceso denegado para cargar archivos.", "error")
        return redirect(url_for("index"))

    try:
        file = request.files.get("file")
        if not file or not file.filename:
            flash("No se pudo cargar la data. Intentelo nuevamente.", "error")
            return redirect(url_for("index"))

        parsed = parse_uploaded_file(file)
        kind = parsed["kind"]
        rows = parsed["rows"]
        if not rows:
            flash("No se encontraron filas validas en el archivo. Verifique formato e intentelo nuevamente.", "error")
            return redirect(url_for("index"))
        if kind == "pedidos_talla":
            stats = import_pedidos_talla_rows(DB_PATH, rows)
        elif kind == "pedidos_talla_todas":
            stats = import_pedidos_talla_todas_rows(DB_PATH, rows)
        elif kind == "corte_etapas":
            stats = import_corte_etapas_rows(DB_PATH, rows)
        elif kind == "exs_map":
            stats = import_exs_map_rows(DB_PATH, rows)
        else:
            stats = import_rows(DB_PATH, rows, replace_all=True)
    except RequestEntityTooLarge:
        session["upload_debug"] = "RequestEntityTooLarge: archivo supera limite ADECOM_MAX_UPLOAD_MB."
        flash("El archivo supera el tamano permitido. Intentelo con un archivo mas liviano.", "error")
        return redirect(url_for("index"))
    except Exception as exc:
        app.logger.exception("Fallo en carga de archivo", exc_info=exc)
        session["upload_debug"] = f"{exc.__class__.__name__}: {exc}"
        flash("No se pudo cargar la data. Intentelo nuevamente.", "error")
        return redirect(url_for("index"))

    session.pop("upload_debug", None)
    flash(
        f"Data cargada con exito. Tipo: {kind}. Leidos: {stats.get('read', 0)} | Insertados: {stats.get('inserted', 0)} | Actualizados: {stats.get('updated', 0)}",
        "success",
    )
    return redirect(url_for("index"))


@app.get("/upload")
def upload_get_redirect():
    return redirect(url_for("index"))


@app.post("/upload/refresh-web")
def upload_refresh_web():
    if not _can_upload():
        flash("Acceso denegado para cargar archivos.", "error")
        return redirect(url_for("index"))

    sources = {
        "SALDOS-SECCI": AUTOLOAD_SALDOS_SOURCE,
        "PEDIDOSXTALLA": AUTOLOAD_PEDIDOS_SOURCE,
        "Grande-Adecom": AUTOLOAD_ETAPAS_SOURCE,
    }
    missing_cfg = [label for label, src in sources.items() if not str(src or "").strip()]
    if missing_cfg:
        session["upload_debug"] = (
            "Faltan variables de entorno para actualizacion web: "
            "ADECOM_AUTOLOAD_SALDOS_SOURCE, ADECOM_AUTOLOAD_PEDIDOS_SOURCE, "
            "ADECOM_AUTOLOAD_ETAPAS_SOURCE."
        )
        flash(
            f"Configuracion incompleta para actualizacion web. Falta: {', '.join(missing_cfg)}.",
            "error",
        )
        return redirect(url_for("index"))

    try:
        stats = _refresh_web_data()
        stats_saldos = stats["saldos"]
        stats_pedidos = stats["pedidos"]
        stats_etapas = stats["etapas"]

        session.pop("upload_debug", None)
        flash(
            "Actualizacion web completa. "
            f"SALDOS-SECCI: I {stats_saldos.get('inserted', 0)} / A {stats_saldos.get('updated', 0)}. "
            f"PEDIDOSXTALLA: I {stats_pedidos.get('inserted', 0)} / A {stats_pedidos.get('updated', 0)}. "
            f"Grande-Adecom: I {stats_etapas.get('inserted', 0)} / A {stats_etapas.get('updated', 0)}.",
            "success",
        )
    except url_error.URLError as exc:
        app.logger.exception("Error de red en actualizacion web", exc_info=exc)
        session["upload_debug"] = f"URLError: {exc}"
        flash("No se pudo descargar una o mas fuentes web.", "error")
    except Exception as exc:
        app.logger.exception("Fallo en actualizacion web", exc_info=exc)
        session["upload_debug"] = f"{exc.__class__.__name__}: {exc}"
        flash("No se pudo actualizar la data web. Intentelo nuevamente.", "error")
    return redirect(url_for("index"))


@app.post("/upload/refresh-local")
def upload_refresh_local():
    return upload_refresh_web()


@app.post("/admin/login")
def admin_login():
    key = _admin_key()
    if not key:
        flash("La clave de administrador no esta configurada.", "error")
        return redirect(url_for("index"))
    entered = str(request.form.get("admin_key") or "").strip()
    if entered and entered == key:
        session["can_upload"] = True
        flash("Modo carga activado.", "success")
    else:
        flash("Clave incorrecta.", "error")
    return redirect(url_for("index"))


@app.post("/admin/logout")
def admin_logout():
    session.pop("can_upload", None)
    flash("Modo carga desactivado.", "success")
    return redirect(url_for("index"))


@app.post("/assistant/query")
def assistant_query():
    if not ASSISTANT_ENABLED:
        return jsonify(
            {
                "answer": "Asistente virtual deshabilitado.",
                "provider": "off",
                "fallback": False,
                "detail": "ADECOM_ASSISTANT_ENABLED=0",
            }
        ), 410
    provider = os.environ.get("ADECOM_ASSISTANT_PROVIDER", "local").strip().lower()
    try:
        payload = request.get_json(silent=True) or {}
        question = str(payload.get("question") or "").strip()
        result = _answer_assistant_router(question)
        return jsonify(result)
    except Exception as exc:
        app.logger.exception("Error en /assistant/query", exc_info=exc)
        if provider in {"gemini", "google"}:
            return jsonify(
                {
                    "answer": f"Gemini no disponible. Detalle: {exc}",
                    "provider": "gemini",
                    "fallback": False,
                    "detail": str(exc),
                }
            ), 503
        return jsonify(
            {
                "answer": "No fue posible responder en este momento. Intenta nuevamente.",
                "provider": "local",
                "fallback": True,
                "detail": str(exc),
            }
        ), 200


@app.get("/export.csv")
def export_csv():
    filters = {
        "q": request.args.get("q", "").strip(),
        "fecha": request.args.get("fecha", "").strip(),
    }
    rows, _, _ = query_rows(DB_PATH, filters)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "articulo",
            "corte",
            "fecha",
            "programa",
            "proceso",
            "bodega",
            "saldo",
            "corte_1",
            "taller",
            "t_externo",
            "limpiado",
            "lavanderia",
            "terminacion",
            "muestra",
            "segunda",
            "taller_nombre",
            "proceso_actual",
            "faltante",
            "restante_fuera_bodega",
            "restante",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row["articulo"],
                row["corte"],
                row["fecha_display"],
                row["programa"],
                row["proceso"],
                row["bodega"],
                row["saldo"],
                row["corte_1"],
                row["taller"],
                row["t_externo"],
                row["limpiado"],
                row["lavanderia"],
                row["terminacion"],
                row["muestra"],
                row["segunda"],
                row["taller_nombre"],
                row["proceso_actual"],
                row["faltante"],
                row["restante_fuera_bodega"],
                row["restante_detalle"],
            ]
        )

    return Response(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=saldos_seccion.csv"},
    )


if __name__ == "__main__":
    init_db(DB_PATH)
    app.run(debug=True)

