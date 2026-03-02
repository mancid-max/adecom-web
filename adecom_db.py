from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable


NUMERIC_FIELDS = [
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
]


def _is_postgres(db_path: str | Path) -> bool:
    value = str(db_path)
    return value.startswith("postgres://") or value.startswith("postgresql://")


def _to_driver_sql(db_path: str | Path, sql: str) -> str:
    if not _is_postgres(db_path):
        return sql
    return sql.replace("?", "%s")


def _execute(conn: Any, sql: str, params: Iterable | None = None):
    driver_sql = sql if isinstance(conn, sqlite3.Connection) else sql.replace("?", "%s")
    if params is None:
        return conn.execute(driver_sql)
    return conn.execute(driver_sql, tuple(params))


def get_conn(db_path: str | Path):
    if _is_postgres(db_path):
        from psycopg import connect
        from psycopg.rows import dict_row

        return connect(str(db_path), row_factory=dict_row)

    local_path = Path(db_path)
    conn = sqlite3.connect(local_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path) -> None:
    if not _is_postgres(db_path):
        local_path = Path(db_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_conn(db_path)
    with conn:
        if _is_postgres(db_path):
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS saldos_seccion (
                    id BIGSERIAL PRIMARY KEY,
                    articulo TEXT NOT NULL,
                    corte TEXT NOT NULL UNIQUE,
                    fecha_iso TEXT,
                    programa INTEGER NOT NULL DEFAULT 0,
                    proceso INTEGER NOT NULL DEFAULT 0,
                    bodega INTEGER NOT NULL DEFAULT 0,
                    saldo INTEGER NOT NULL DEFAULT 0,
                    corte_1 INTEGER NOT NULL DEFAULT 0,
                    taller INTEGER NOT NULL DEFAULT 0,
                    t_externo INTEGER NOT NULL DEFAULT 0,
                    limpiado INTEGER NOT NULL DEFAULT 0,
                    lavanderia INTEGER NOT NULL DEFAULT 0,
                    terminacion INTEGER NOT NULL DEFAULT 0,
                    muestra INTEGER NOT NULL DEFAULT 0,
                    segunda INTEGER NOT NULL DEFAULT 0,
                    taller_nombre TEXT NOT NULL DEFAULT '',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS pedidos_talla (
                    id BIGSERIAL PRIMARY KEY,
                    articulo TEXT NOT NULL,
                    descripcion TEXT NOT NULL DEFAULT '',
                    tipo TEXT NOT NULL,
                    tallas_json TEXT NOT NULL DEFAULT '[]',
                    total INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(articulo, tipo)
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS pedidos_talla_todas (
                    id BIGSERIAL PRIMARY KEY,
                    articulo TEXT NOT NULL,
                    descripcion TEXT NOT NULL DEFAULT '',
                    tipo TEXT NOT NULL,
                    familia TEXT NOT NULL DEFAULT '',
                    tallas_json TEXT NOT NULL DEFAULT '[]',
                    total INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(articulo, tipo)
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS exs_map (
                    id BIGSERIAL PRIMARY KEY,
                    actual TEXT NOT NULL UNIQUE,
                    ex TEXT NOT NULL DEFAULT '',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
        else:
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS saldos_seccion (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    articulo TEXT NOT NULL,
                    corte TEXT NOT NULL UNIQUE,
                    fecha_iso TEXT,
                    programa INTEGER NOT NULL DEFAULT 0,
                    proceso INTEGER NOT NULL DEFAULT 0,
                    bodega INTEGER NOT NULL DEFAULT 0,
                    saldo INTEGER NOT NULL DEFAULT 0,
                    corte_1 INTEGER NOT NULL DEFAULT 0,
                    taller INTEGER NOT NULL DEFAULT 0,
                    t_externo INTEGER NOT NULL DEFAULT 0,
                    limpiado INTEGER NOT NULL DEFAULT 0,
                    lavanderia INTEGER NOT NULL DEFAULT 0,
                    terminacion INTEGER NOT NULL DEFAULT 0,
                    muestra INTEGER NOT NULL DEFAULT 0,
                    segunda INTEGER NOT NULL DEFAULT 0,
                    taller_nombre TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS pedidos_talla (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    articulo TEXT NOT NULL,
                    descripcion TEXT NOT NULL DEFAULT '',
                    tipo TEXT NOT NULL,
                    tallas_json TEXT NOT NULL DEFAULT '[]',
                    total INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(articulo, tipo)
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS pedidos_talla_todas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    articulo TEXT NOT NULL,
                    descripcion TEXT NOT NULL DEFAULT '',
                    tipo TEXT NOT NULL,
                    familia TEXT NOT NULL DEFAULT '',
                    tallas_json TEXT NOT NULL DEFAULT '[]',
                    total INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(articulo, tipo)
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS exs_map (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actual TEXT NOT NULL UNIQUE,
                    ex TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
    conn.close()


def import_rows(db_path: Path, rows: Iterable[dict], replace_all: bool = False) -> dict:
    init_db(db_path)
    conn = get_conn(db_path)
    inserted = 0
    updated = 0
    read = 0

    with conn:
        if replace_all:
            _execute(conn, "DELETE FROM saldos_seccion")
        for row in rows:
            read += 1
            existing = _execute(conn, 
                "SELECT id FROM saldos_seccion WHERE corte = ?",
                (row["corte"],),
            ).fetchone()

            values = (
                row["articulo"],
                row["corte"],
                row["fecha_iso"],
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
            )

            if existing:
                _execute(conn, 
                    """
                    UPDATE saldos_seccion
                    SET articulo = ?,
                        fecha_iso = ?,
                        programa = ?,
                        proceso = ?,
                        bodega = ?,
                        saldo = ?,
                        corte_1 = ?,
                        taller = ?,
                        t_externo = ?,
                        limpiado = ?,
                        lavanderia = ?,
                        terminacion = ?,
                        muestra = ?,
                        segunda = ?,
                        taller_nombre = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE corte = ?
                    """,
                    (
                        row["articulo"],
                        row["fecha_iso"],
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
                        row["corte"],
                    ),
                )
                updated += 1
            else:
                _execute(conn, 
                    """
                    INSERT INTO saldos_seccion (
                        articulo, corte, fecha_iso, programa, proceso, bodega, saldo,
                        corte_1, taller, t_externo, limpiado, lavanderia, terminacion,
                        muestra, segunda, taller_nombre
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
                inserted += 1

    conn.close()
    return {"read": read, "inserted": inserted, "updated": updated}


def import_pedidos_talla_rows(db_path: Path, rows: Iterable[dict]) -> dict:
    init_db(db_path)
    conn = get_conn(db_path)
    inserted = 0
    updated = 0
    read = 0

    with conn:
        for row in rows:
            read += 1
            existing = _execute(conn, 
                "SELECT id FROM pedidos_talla WHERE articulo = ? AND tipo = ?",
                (row["articulo"], row["tipo"]),
            ).fetchone()

            tallas_json = json.dumps(row.get("tallas", []), ensure_ascii=True)
            if existing:
                _execute(conn, 
                    """
                    UPDATE pedidos_talla
                    SET descripcion = ?,
                        tallas_json = ?,
                        total = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE articulo = ? AND tipo = ?
                    """,
                    (
                        row.get("descripcion", ""),
                        tallas_json,
                        int(row.get("total") or 0),
                        row["articulo"],
                        row["tipo"],
                    ),
                )
                updated += 1
            else:
                _execute(conn, 
                    """
                    INSERT INTO pedidos_talla (
                        articulo, descripcion, tipo, tallas_json, total
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        row["articulo"],
                        row.get("descripcion", ""),
                        row["tipo"],
                        tallas_json,
                        int(row.get("total") or 0),
                    ),
                )
                inserted += 1

    conn.close()
    return {"read": read, "inserted": inserted, "updated": updated}


def import_pedidos_talla_todas_rows(db_path: Path, rows: Iterable[dict]) -> dict:
    init_db(db_path)
    conn = get_conn(db_path)
    inserted = 0
    updated = 0
    read = 0

    with conn:
        for row in rows:
            read += 1
            existing = _execute(conn, 
                "SELECT id FROM pedidos_talla_todas WHERE articulo = ? AND tipo = ?",
                (row["articulo"], row["tipo"]),
            ).fetchone()

            tallas_json = json.dumps(row.get("tallas", []), ensure_ascii=True)
            familia_digits = "".join(ch for ch in str(row.get("articulo") or "") if ch.isdigit())
            familia = familia_digits[2:6] if len(familia_digits) >= 6 else familia_digits
            if existing:
                _execute(conn, 
                    """
                    UPDATE pedidos_talla_todas
                    SET descripcion = ?,
                        familia = ?,
                        tallas_json = ?,
                        total = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE articulo = ? AND tipo = ?
                    """,
                    (
                        row.get("descripcion", ""),
                        familia,
                        tallas_json,
                        int(row.get("total") or 0),
                        row["articulo"],
                        row["tipo"],
                    ),
                )
                updated += 1
            else:
                _execute(conn, 
                    """
                    INSERT INTO pedidos_talla_todas (
                        articulo, descripcion, tipo, familia, tallas_json, total
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["articulo"],
                        row.get("descripcion", ""),
                        row["tipo"],
                        familia,
                        tallas_json,
                        int(row.get("total") or 0),
                    ),
                )
                inserted += 1

    conn.close()
    return {"read": read, "inserted": inserted, "updated": updated}


def import_exs_map_rows(db_path: Path, rows: Iterable[dict]) -> dict:
    init_db(db_path)
    conn = get_conn(db_path)
    inserted = 0
    updated = 0
    read = 0

    with conn:
        for row in rows:
            read += 1
            actual = str(row.get("actual") or "").strip()
            ex = str(row.get("ex") or "").strip()
            if not actual:
                continue
            existing = _execute(conn, 
                "SELECT id FROM exs_map WHERE actual = ?",
                (actual,),
            ).fetchone()
            if existing:
                _execute(conn, 
                    """
                    UPDATE exs_map
                    SET ex = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE actual = ?
                    """,
                    (ex, actual),
                )
                updated += 1
            else:
                _execute(conn, 
                    """
                    INSERT INTO exs_map (actual, ex)
                    VALUES (?, ?)
                    """,
                    (actual, ex),
                )
                inserted += 1

    conn.close()
    return {"read": read, "inserted": inserted, "updated": updated}


def query_pedidos_talla_sections(db_path: Path, q: str = "") -> dict[str, list[dict]]:
    init_db(db_path)
    conn = get_conn(db_path)
    params: list = []
    where = ""
    if q:
        like = f"%{q}%"
        q_digits = "".join(ch for ch in q if ch.isdigit())
        clauses = ["articulo LIKE ?", "descripcion LIKE ?", "tipo LIKE ?"]
        params = [like, like, like]
        if q_digits:
            like_digits = f"%{q_digits}%"
            clauses.append("articulo LIKE ?")
            params.append(like_digits)
            clauses.append("ltrim(articulo, '0') LIKE ?")
            params.append(like_digits)
            if len(q_digits) == 4:
                clauses.append("substr(articulo, 3, 4) = ?")
                params.append(q_digits)
        where = "WHERE " + " OR ".join(clauses)

    rows = _execute(conn, 
        f"""
        SELECT articulo, descripcion, tipo, tallas_json, total
        FROM pedidos_talla
        {where}
        ORDER BY tipo ASC, articulo ASC
        """,
        params,
    ).fetchall()
    conn.close()

    sections = {
        "ventas": [],
        "despacho": [],
        "saldo": [],
        "stock": [],
        "corte": [],
        "sugerido": [],
    }
    for r in rows:
        row = dict(r)
        tipo = (row.get("tipo") or "").strip().lower()
        if tipo not in sections:
            sections[tipo] = []
        tallas = []
        try:
            tallas = json.loads(row.get("tallas_json") or "[]")
        except json.JSONDecodeError:
            tallas = []
        row["tallas_items"] = [
            {"talla": 36 + (i * 2), "cantidad": int(v)}
            for i, v in enumerate(tallas)
            if (36 + (i * 2)) <= 46
        ]
        # Mapeo solicitado: T1->36, T2->38, T3->40, ...
        tallas_detalle = " | ".join(
            f"Talla {36 + (i * 2)}: {int(v)}"
            for i, v in enumerate(tallas)
            if (36 + (i * 2)) <= 46
        )
        row["tallas_detalle"] = tallas_detalle or "-"
        sections[tipo].append(row)

    return sections


def _build_where(filters: dict) -> tuple[str, list]:
    clauses = []
    params: list = []

    if filters.get("q"):
        q_raw = str(filters["q"]).strip()
        q = f"%{q_raw}%"
        q_digits = "".join(ch for ch in q_raw if ch.isdigit())
        q_clauses = ["articulo LIKE ?", "corte LIKE ?", "taller_nombre LIKE ?"]
        q_params = [q, q, q]
        if q_digits:
            like_digits = f"%{q_digits}%"
            q_clauses.append("articulo LIKE ?")
            q_params.append(like_digits)
            q_clauses.append("ltrim(articulo, '0') LIKE ?")
            q_params.append(like_digits)
            if len(q_digits) == 4:
                q_clauses.append("substr(articulo, 3, 4) = ?")
                q_params.append(q_digits)
        clauses.append("(" + " OR ".join(q_clauses) + ")")
        params.extend(q_params)

    if filters.get("fecha"):
        clauses.append("fecha_iso = ?")
        params.append(filters["fecha"])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, params


def query_rows(db_path: Path, filters: dict) -> tuple[list[dict], dict, dict]:
    init_db(db_path)
    conn = get_conn(db_path)
    where, params = _build_where(filters)

    sql = f"""
        SELECT *
        FROM saldos_seccion
        {where}
        ORDER BY fecha_iso DESC, corte ASC
    """
    db_rows = _execute(conn, sql, params).fetchall()

    rows: list[dict] = []
    totals = {key: 0 for key in NUMERIC_FIELDS}
    summary = {
        "ordenes_en_bodega": 0,
        "cantidad_en_bodega": 0,
        "restante_fuera_bodega": 0,
        "pendiente_en_trazabilidad_bodega": 0,
        "total_proceso": 0,
        "total_en_bodega_del_proceso": 0,
        "total_en_proceso": 0,
    }
    by_proceso: dict[int, dict] = {}
    for r in db_rows:
        row = dict(r)
        row["fecha_display"] = _format_date(row.get("fecha_iso"))
        row["proceso_actual"] = _proceso_actual(row)
        row["faltante"] = _faltante(row)
        row["restante_fuera_bodega"] = max(int(row.get("saldo") or 0) - int(row.get("bodega") or 0), 0)
        row["pendiente_en_trazabilidad"] = _pendiente_en_trazabilidad(row)
        row["tiene_pendiente_trazabilidad"] = row["pendiente_en_trazabilidad"] > 0
        row["ubicacion_restante"] = _ubicacion_restante(row)
        row["restante_detalle"] = _restante_detalle(row)

        if int(row.get("bodega") or 0) > 0:
            summary["ordenes_en_bodega"] += 1
            summary["pendiente_en_trazabilidad_bodega"] += row["pendiente_en_trazabilidad"]
        summary["cantidad_en_bodega"] += int(row.get("bodega") or 0)
        summary["restante_fuera_bodega"] += row["restante_fuera_bodega"]
        summary["total_proceso"] += int(row.get("proceso") or 0)
        summary["total_en_bodega_del_proceso"] += int(row.get("bodega") or 0)
        proceso_key = int(row.get("proceso") or 0)
        if proceso_key not in by_proceso:
            by_proceso[proceso_key] = {
                "proceso": proceso_key,
                "total": 0,
                "en_bodega": 0,
            }
        by_proceso[proceso_key]["total"] += int(row.get("proceso") or 0)
        by_proceso[proceso_key]["en_bodega"] += int(row.get("bodega") or 0)

        for field in NUMERIC_FIELDS:
            totals[field] += int(row.get(field) or 0)
        rows.append(row)

    summary["total_en_proceso"] = max(
        summary["total_proceso"] - summary["total_en_bodega_del_proceso"],
        0,
    )
    summary["por_proceso"] = sorted(
        [
            {
                "proceso": item["proceso"],
                "total": item["total"],
                "en_bodega": item["en_bodega"],
                "siguen": max(item["total"] - item["en_bodega"], 0),
            }
            for item in by_proceso.values()
        ],
        key=lambda x: x["proceso"],
    )

    conn.close()
    return rows, totals, summary


def query_exs_balance_summary(db_path: Path, q: str = "") -> dict:
    init_db(db_path)
    conn = get_conn(db_path)
    saldo_rows = _execute(conn, 
        """
        SELECT familia, SUM(total) AS total
        FROM pedidos_talla_todas
        WHERE lower(trim(tipo)) LIKE 'saldo%'
        GROUP BY familia
        """
    ).fetchall()
    saldo_by_familia = {
        str(r["familia"]).strip(): int(r["total"] or 0) for r in saldo_rows
    }

    map_rows = _execute(conn, 
        """
        SELECT actual, ex
        FROM exs_map
        ORDER BY actual ASC
        """
    ).fetchall()
    conn.close()

    q_digits = "".join(ch for ch in str(q or "") if ch.isdigit())

    def _extract_family(code: str) -> str:
        digits = "".join(ch for ch in str(code or "") if ch.isdigit())
        if not digits:
            return ""
        # Regla de negocio: familia son los primeros 4 digitos.
        # Ej: 416900 / 416901 / 4169-01 => 4169
        return digits[:4] if len(digits) >= 4 else digits

    def resolve_saldo(code: str) -> int:
        family = _extract_family(code)
        if not family:
            return 0
        return int(saldo_by_familia.get(family, 0))

    rows: list[dict] = []
    total_actual = 0
    total_ex = 0
    for r in map_rows:
        actual = str(r["actual"] or "").strip()
        ex = str(r["ex"] or "").strip()
        if q_digits and q_digits not in actual and q_digits not in ex:
            continue
        saldo_actual = resolve_saldo(actual)
        saldo_ex = resolve_saldo(ex)
        rows.append(
            {
                "actual": actual,
                "ex": ex,
                "saldo_actual": saldo_actual,
                "saldo_ex": saldo_ex,
            }
        )
        total_actual += saldo_actual
        total_ex += saldo_ex

    return {
        "rows": rows,
        "count": len(rows),
        "total_actual": total_actual,
        "total_ex": total_ex,
    }


def _format_date(fecha_iso: str | None) -> str:
    if not fecha_iso:
        return ""
    parts = fecha_iso.split("-")
    if len(parts) != 3:
        return fecha_iso
    return f"{parts[2]}/{parts[1]}/{parts[0]}"


def _proceso_actual(row: dict) -> str:
    stages = [
        ("bodega", "BODEGA"),
        ("corte_1", "CORTE"),
        ("taller", "TALLER"),
        ("t_externo", "T.EXTERNO"),
        ("limpiado", "LIMPIADO"),
        ("lavanderia", "LAVANDERIA"),
        ("terminacion", "TERMINACION"),
        ("muestra", "MUESTRA"),
        ("segunda", "SEGUNDA"),
    ]
    for key, label in stages:
        if int(row.get(key) or 0) > 0:
            return label
    return "Sin movimiento"


def _faltante(row: dict) -> int:
    saldo = int(row.get("saldo") or 0)
    tracked = sum(
        int(row.get(key) or 0)
        for key in [
            "corte_1",
            "taller",
            "t_externo",
            "limpiado",
            "lavanderia",
            "terminacion",
            "muestra",
            "segunda",
        ]
    )
    return max(saldo - tracked, 0)


def _ubicacion_restante(row: dict) -> str:
    labels = [
        ("corte_1", "CORTE"),
        ("taller", "TALLER"),
        ("t_externo", "T.EXTERNO"),
        ("limpiado", "LIMPIADO"),
        ("lavanderia", "LAVANDERIA"),
        ("terminacion", "TERMINACION"),
        ("muestra", "MUESTRA"),
        ("segunda", "SEGUNDA"),
    ]
    active = [label for key, label in labels if int(row.get(key) or 0) > 0]
    return ", ".join(active) if active else "Sin restante fuera de bodega"


def _trazabilidad_detalle(row: dict) -> str:
    labels = [
        ("bodega", "BODEGA"),
        ("corte_1", "CORTE"),
        ("taller", "TALLER"),
        ("t_externo", "T.EXTERNO"),
        ("limpiado", "LIMPIADO"),
        ("lavanderia", "LAVANDERIA"),
        ("terminacion", "TERMINACION"),
        ("muestra", "MUESTRA"),
        ("segunda", "SEGUNDA"),
    ]
    parts = []
    for key, label in labels:
        value = int(row.get(key) or 0)
        if value > 0:
            parts.append(f"{label}:{value}")
    return " | ".join(parts) if parts else "Sin movimiento"


def _pendiente_en_trazabilidad(row: dict) -> int:
    return sum(
        int(row.get(key) or 0)
        for key in [
            "corte_1",
            "taller",
            "t_externo",
            "limpiado",
            "lavanderia",
            "terminacion",
            "muestra",
            "segunda",
        ]
    )


def _restante_detalle(row: dict) -> str:
    labels = [
        ("corte_1", "Corte"),
        ("taller", "Taller"),
        ("t_externo", "T. Externo"),
        ("limpiado", "Limpiado"),
        ("lavanderia", "Lavanderia"),
        ("terminacion", "Terminacion"),
        ("muestra", "Muestra"),
        ("segunda", "Segunda"),
    ]
    parts = []
    for key, label in labels:
        value = int(row.get(key) or 0)
        if value > 0:
            parts.append(f"{label}: {value}")
    return " | ".join(parts) if parts else "Sin restante"
