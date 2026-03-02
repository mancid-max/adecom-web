from __future__ import annotations

import csv
import io
import os
import re
from pathlib import Path

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


def _extract_query_code(question: str) -> str:
    digits = re.findall(r"\d+", question or "")
    if not digits:
        return ""
    # Preferimos codigos de al menos 4 digitos para familia/articulo.
    candidates = [d for d in digits if len(d) >= 4]
    return candidates[0] if candidates else digits[0]


def _answer_assistant(question: str) -> str:
    q = (question or "").strip()
    if not q:
        return "Escribe una pregunta. Ejemplo: En que parte se encuentra 4210."

    code = _extract_query_code(q)
    if not code:
        return "No detecte articulo o familia en tu pregunta. Incluye un codigo como 4210 o 01420100."

    rows, _, _ = query_rows(DB_PATH, {"q": code, "fecha": ""})
    if not rows:
        return f"No encontre datos para {code}."

    bodega_rows = [r for r in rows if int(r.get("bodega") or 0) > 0]
    prendas_bodega = sum(int(r.get("bodega") or 0) for r in rows)
    prendas_proceso = sum(int(r.get("proceso") or 0) for r in rows)
    pendientes = sum(int(r.get("pendiente_en_trazabilidad") or 0) for r in rows)

    if bodega_rows:
        return (
            f"{code}: se encuentra en bodega en {len(bodega_rows)} orden(es), "
            f"con {prendas_bodega} prendas en bodega. "
            f"Total en proceso: {prendas_proceso}. Pendiente en trazabilidad: {pendientes}."
        )

    top_stage: dict[str, int] = {}
    for row in rows:
        stage = str(row.get("proceso_actual") or "Sin movimiento")
        top_stage[stage] = top_stage.get(stage, 0) + int(row.get("proceso") or 0)
    stage_name, stage_total = max(top_stage.items(), key=lambda x: x[1])
    return (
        f"{code}: no tiene prendas en bodega actualmente. "
        f"La mayor cantidad esta en {stage_name} con {stage_total} prendas. "
        f"Total en proceso: {prendas_proceso}."
    )


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
    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question") or "").strip()
    answer = _answer_assistant(question)
    return jsonify({"answer": answer})


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

