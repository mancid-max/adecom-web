from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
import sys


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from adecom_db import (
    import_corte_etapas_rows,
    import_exs_map_rows,
    import_pedidos_talla_rows,
    import_pedidos_talla_todas_rows,
    import_rows,
    init_db,
    upsert_assistant_rule,
)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _load_sqlite_rows(sqlite_path: Path, table_name: str) -> list[dict]:
    with sqlite3.connect(sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        if not _table_exists(conn, table_name):
            return []
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        return [dict(r) for r in rows]


def _json_list(value: object) -> list:
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def migrate(sqlite_path: Path, postgres_url: str) -> dict:
    if not sqlite_path.exists():
        raise FileNotFoundError(f"No existe SQLite: {sqlite_path}")
    if not postgres_url.startswith(("postgres://", "postgresql://")):
        raise ValueError("DATABASE_URL debe iniciar con postgres:// o postgresql://")

    init_db(postgres_url)

    saldos_rows = _load_sqlite_rows(sqlite_path, "saldos_seccion")
    pedidos_rows_raw = _load_sqlite_rows(sqlite_path, "pedidos_talla")
    pedidos_todas_rows_raw = _load_sqlite_rows(sqlite_path, "pedidos_talla_todas")
    exs_rows = _load_sqlite_rows(sqlite_path, "exs_map")
    etapas_rows = _load_sqlite_rows(sqlite_path, "corte_etapas")
    assistant_rows = _load_sqlite_rows(sqlite_path, "assistant_rules")

    pedidos_rows = [
        {
            "articulo": str(r.get("articulo") or "").strip(),
            "descripcion": str(r.get("descripcion") or "").strip(),
            "tipo": str(r.get("tipo") or "").strip().lower(),
            "tallas": _json_list(r.get("tallas_json")),
            "total": int(r.get("total") or 0),
        }
        for r in pedidos_rows_raw
        if str(r.get("articulo") or "").strip() and str(r.get("tipo") or "").strip()
    ]
    pedidos_todas_rows = [
        {
            "articulo": str(r.get("articulo") or "").strip(),
            "descripcion": str(r.get("descripcion") or "").strip(),
            "tipo": str(r.get("tipo") or "").strip().lower(),
            "tallas": _json_list(r.get("tallas_json")),
            "total": int(r.get("total") or 0),
        }
        for r in pedidos_todas_rows_raw
        if str(r.get("articulo") or "").strip() and str(r.get("tipo") or "").strip()
    ]
    exs_rows_clean = [
        {
            "actual": str(r.get("actual") or "").strip(),
            "ex": str(r.get("ex") or "").strip(),
        }
        for r in exs_rows
        if str(r.get("actual") or "").strip()
    ]
    etapas_rows_clean = [
        {
            "corte": str(r.get("corte") or "").strip(),
            "articulo": str(r.get("articulo") or "").strip(),
            "fecha_orden_iso": r.get("fecha_orden_iso"),
            "programado": int(r.get("programado") or 0),
            "cortado": int(r.get("cortado") or 0),
            "entrega": int(r.get("entrega") or 0),
            "saldo": int(r.get("saldo") or 0),
            "corte_inicio_iso": r.get("corte_inicio_iso"),
            "corte_fin_iso": r.get("corte_fin_iso"),
            "taller_inicio_iso": r.get("taller_inicio_iso"),
            "taller_fin_iso": r.get("taller_fin_iso"),
            "t_externo_inicio_iso": r.get("t_externo_inicio_iso"),
            "t_externo_fin_iso": r.get("t_externo_fin_iso"),
            "limpiado_inicio_iso": r.get("limpiado_inicio_iso"),
            "limpiado_fin_iso": r.get("limpiado_fin_iso"),
            "lavanderia_inicio_iso": r.get("lavanderia_inicio_iso"),
            "lavanderia_fin_iso": r.get("lavanderia_fin_iso"),
            "terminacion_inicio_iso": r.get("terminacion_inicio_iso"),
            "terminacion_fin_iso": r.get("terminacion_fin_iso"),
            "muestra_inicio_iso": r.get("muestra_inicio_iso"),
            "muestra_fin_iso": r.get("muestra_fin_iso"),
        }
        for r in etapas_rows
        if str(r.get("corte") or "").strip()
    ]

    saldos_stats = import_rows(postgres_url, saldos_rows, replace_all=True)
    pedidos_stats = import_pedidos_talla_rows(postgres_url, pedidos_rows)
    pedidos_todas_stats = import_pedidos_talla_todas_rows(postgres_url, pedidos_todas_rows)
    exs_stats = import_exs_map_rows(postgres_url, exs_rows_clean)
    etapas_stats = import_corte_etapas_rows(postgres_url, etapas_rows_clean)

    assistant_upserts = 0
    for r in assistant_rows:
        key = str(r.get("rule_key") or "").strip()
        text = str(r.get("rule_text") or "").strip()
        if not key or not text:
            continue
        upsert_assistant_rule(
            postgres_url,
            rule_key=key,
            rule_text=text,
            priority=int(r.get("priority") or 100),
            enabled=bool(int(r.get("enabled") or 0)),
        )
        assistant_upserts += 1

    return {
        "saldos": saldos_stats,
        "pedidos_talla": pedidos_stats,
        "pedidos_talla_todas": pedidos_todas_stats,
        "exs_map": exs_stats,
        "corte_etapas": etapas_stats,
        "assistant_rules_upserted": assistant_upserts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migra data ADECOM desde SQLite local a PostgreSQL (DATABASE_URL)."
    )
    parser.add_argument(
        "--sqlite",
        default="data/adecom.db",
        help="Ruta al archivo SQLite origen (default: data/adecom.db)",
    )
    parser.add_argument(
        "--database-url",
        required=True,
        help="Connection string de PostgreSQL destino.",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite).resolve()
    stats = migrate(sqlite_path, args.database_url.strip())

    print("Migracion completada:")
    print(json.dumps(stats, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
