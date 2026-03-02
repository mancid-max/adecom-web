from __future__ import annotations

import csv
import io
import json
import os
import re
import unicodedata
from pathlib import Path
from difflib import SequenceMatcher
from urllib import error as url_error
from urllib import request as url_request

from flask import Flask, Response, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.exceptions import RequestEntityTooLarge

from adecom_db import (
    get_conn,
    import_exs_map_rows,
    import_pedidos_talla_todas_rows,
    init_db,
    import_pedidos_talla_rows,
    import_rows,
    query_exs_balance_summary,
    query_pedidos_talla_sections,
    query_rows,
)
from parsers import parse_pedidos_talla_txt, parse_saldos_txt, parse_uploaded_file


BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
DB_PATH = os.environ.get("DATABASE_URL") or os.environ.get(
    "ADECOM_DB_PATH", str(BASE_DIR / "data" / "adecom.db")
)
SEED_DIR = BASE_DIR / "seed"
SEED_SALDOS = SEED_DIR / "SALDOS-SECCI.TXT"
SEED_PEDIDOS = SEED_DIR / "PEDIDOSXTALLA.TXT"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
if not str(DB_PATH).startswith(("postgres://", "postgresql://")):
    Path(DB_PATH).resolve().parent.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("ADECOM_SECRET_KEY", "dev-secret-change-me")
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("ADECOM_MAX_UPLOAD_MB", "25")) * 1024 * 1024


def _admin_key() -> str:
    return os.environ.get("ADECOM_ADMIN_KEY", "").strip()


def _can_upload() -> bool:
    key = _admin_key()
    if not key:
        return True
    return bool(session.get("can_upload"))


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
    for r in rows:
        stage = str(r.get("proceso_actual") or "Sin movimiento")
        proceso = int(r.get("proceso") or 0)
        etapas[stage] = etapas.get(stage, 0) + proceso
        articulo = str(r.get("articulo") or "").strip()
        if articulo:
            bodega_por_articulo[articulo] = bodega_por_articulo.get(articulo, 0) + int(r.get("bodega") or 0)

    top_etapas = [
        {"etapa": e, "total": t}
        for e, t in sorted(etapas.items(), key=lambda x: x[1], reverse=True)[:10]
    ]
    top_bodega_articulo = [
        {"articulo": a, "bodega": t}
        for a, t in sorted(bodega_por_articulo.items(), key=lambda x: x[1], reverse=True)[:10]
        if t > 0
    ]

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
            "Puedo responder por articulo, familia, tallas, bodega, ventas, EXS y cruces entre esas tablas."
        )

    if _has_keyword(qn, ["ayuda", "que puedes", "que sabes", "como funcionas"]):
        return (
            "Puedo responder sobre: ordenes en bodega, total muestras, tabla completa, "
            "ventas totales, familia mas vendida, top articulos, EXS y ubicacion por articulo/familia "
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

    if _has_keyword(qn, ["venta", "ventas", "vendido", "vendida", "vender", "mas vendido"]):
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
        asks_rank = rank > 1 or _has_keyword(qn, ["top", "mas vendido", "mas vendida"])

        if asks_familia:
            if not familias_sorted:
                return "No hay datos de ventas para calcular familias."
            if rank > len(familias_sorted):
                return f"No hay suficientes familias para obtener el puesto {rank}."
            fam, total = familias_sorted[rank - 1]
            return f"Familia #{rank} en ventas: {fam}, con {total} unidades."

        if asks_articulo:
            if not articulos_sorted:
                return "No hay datos de ventas para calcular articulos."
            if rank > len(articulos_sorted):
                return f"No hay suficientes articulos para obtener el puesto {rank}."
            art, total = articulos_sorted[rank - 1]
            return f"Articulo #{rank} en ventas: {art}, con {total} unidades."

        if asks_rank:
            if not articulos_sorted or not familias_sorted:
                return "No hay datos de ventas para calcular ranking."
            if rank > len(articulos_sorted) or rank > len(familias_sorted):
                return f"No hay suficientes datos para obtener el puesto {rank}."
            art, art_total = articulos_sorted[rank - 1]
            fam, fam_total = familias_sorted[rank - 1]
            return (
                f"Puesto #{rank} en ventas: articulo {art} ({art_total}) y familia {fam} ({fam_total}). "
                f"Si quieres uno especifico, pregunta por 'articulo' o 'familia'."
            )

        return f"Total ventas actual: {ventas_total} unidades."

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
            f"pendiente {pendientes}; etapas {stage_txt}; ventas relacionadas {ventas_total}; {ex_txt}."
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
        "Puedo ayudarte con bodega, muestras, ventas, EXS o por codigo de articulo/familia. "
        "Ejemplos: 'Ordenes en bodega', 'Familia mas vendida', 'EXS total', 'En que parte se encuentra 4210'."
    )


def _answer_with_gemini(question: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY no configurada.")

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
        "Responde en espanol natural, claro y breve (tono humano, no robotico). "
        "Usa SOLO el contexto entregado (proviene de todos los archivos cargados e importados en ADECOM WEB). "
        "Interpretacion obligatoria: BODEGA es solo columna bodega; RESTANTE (pendiente_en_trazabilidad) es distinto y no debe reportarse como bodega. "
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
    finally:
        conn.close()


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
    muestras_rows = [
        row
        for row in rows
        if str(row.get("corte", "")).lstrip("0").startswith("96")
    ]
    muestras_total = sum(int(row.get("proceso") or 0) for row in muestras_rows)
    muestras_bodega = sum(int(row.get("bodega") or 0) for row in muestras_rows)
    muestras_restante = max(muestras_total - muestras_bodega, 0)
    upload_debug = session.get("upload_debug", "")
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
        muestras_rows=muestras_rows,
        muestras_total=muestras_total,
        muestras_bodega=muestras_bodega,
        muestras_restante=muestras_restante,
        upload_debug=upload_debug,
        can_upload=_can_upload(),
        admin_key_enabled=bool(_admin_key()),
        assistant_provider=assistant_provider,
    )


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
    flash("Data cargada con exito.", "success")
    return redirect(url_for("index"))


@app.get("/upload")
def upload_get_redirect():
    return redirect(url_for("index"))


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

