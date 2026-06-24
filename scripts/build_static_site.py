from __future__ import annotations

import re
import shutil
import sys
import os
from pathlib import Path

from flask import session


ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs"
STATIC_DIR = ROOT / "static"
SEED_DIR = ROOT / "seed"
BI_DIR = Path(os.environ.get("ADECOM_BI_DIR", r"Z:\BI"))
BUILD_DB = ROOT / "data" / "static-build.db"
if BUILD_DB.exists():
    BUILD_DB.unlink()
os.environ["ADECOM_DB_PATH"] = str(BUILD_DB)
os.environ["ADECOM_AUTO_REFRESH_WEB_ON_START"] = "0"
os.environ["ADECOM_AUTO_REFRESH_WEB_BACKGROUND"] = "0"
os.environ["ADECOM_STATIC_EXPORT"] = "1"


ONEDRIVE_INVENTARIO = Path(
    os.environ.get(
        "ADECOM_INVENTARIO_XLSX_FALLBACK",
        r"C:\Users\Lenovo\OneDrive - Mohicano Jeans\INVENTARIO 01-04 COMPLETO.xlsx",
    )
)
SEED_INVENTARIO = SEED_DIR / "INVENTARIO 01-04 COMPLETO.xlsx"


def _sync_bi_to_seed() -> None:
    if not BI_DIR.exists():
        print(f"Z:\\BI no disponible, usando seed existente.")
    else:
        copies = [
            ("VENTAS-TOD-2026.CSV", "VENTAS-TOD-2026.CSV"),
            ("TRAZABILIDAD.CSV", "TRAZABILIDAD_OP.TXT"),
        ]
        for bi_name, seed_name in copies:
            src = BI_DIR / bi_name
            dst = SEED_DIR / seed_name
            if src.exists() and src.stat().st_size > 0:
                shutil.copy2(src, dst)
                print(f"  Sync: {bi_name} -> seed/{seed_name}")

    # Sincronizar inventario desde OneDrive si es más nuevo
    if ONEDRIVE_INVENTARIO.exists() and ONEDRIVE_INVENTARIO.stat().st_size > 0:
        if (
            not SEED_INVENTARIO.exists()
            or ONEDRIVE_INVENTARIO.stat().st_mtime > SEED_INVENTARIO.stat().st_mtime
        ):
            shutil.copy2(ONEDRIVE_INVENTARIO, SEED_INVENTARIO)
            print(f"  Sync: INVENTARIO 01-04 COMPLETO.xlsx -> seed/")

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app as app_module


STATIC_BRIDGE_SCRIPT = """
<script>
(() => {
  document.querySelectorAll('form[action="#"]').forEach((form) => {
    form.addEventListener("submit", (event) => event.preventDefault());
  });

  const form = document.getElementById("filtersForm");
  const input = document.getElementById("articuloExactInput");
  const resetLink = document.querySelector(".search-reset-btn");
  const openFullTableBtn = document.getElementById("openFullTableModal");

  if (resetLink) {
    resetLink.setAttribute("href", "#");
    resetLink.addEventListener("click", (event) => {
      event.preventDefault();
      if (input) input.value = "";
    });
  }

  if (!form || !input) return;

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const query = (input.value || "").trim();
    if (!query) return;

    if (openFullTableBtn && typeof openFullTableBtn.click === "function") {
      openFullTableBtn.click();
    }

    const rows = Array.from(document.querySelectorAll('[data-articulo][data-clickable="1"]'));
    const target = rows.find((row) => (row.dataset.articulo || "").trim() === query);
    if (!target) {
      window.alert("Articulo no encontrado en esta version web.");
      return;
    }

    setTimeout(() => {
      target.scrollIntoView({ behavior: "smooth", block: "center" });
      if (typeof target.click === "function") target.click();
    }, 120);
  });
})();
</script>
"""

def _render_main_html() -> str:
    app_module.ASSISTANT_ENABLED = False
    app_module._refresh_seed_data()
    with app_module.app.test_request_context("/"):
        session["portal_section"] = "main"
        session["can_upload"] = False
        rendered = app_module.index()
        if hasattr(rendered, "get_data"):
            return rendered.get_data(as_text=True)
        return str(rendered)


def _postprocess_main_html(html: str) -> str:
    html = html.replace('href="/static/styles.css"', 'href="styles.css"')
    html = html.replace('<body class="theme-gentelella">', '<body class="theme-gentelella static-export">')
    html = html.replace('action="/logout"', 'action="#"')
    html = html.replace('action="/"', 'action="#"')
    html = html.replace('href="/" class="btn-ghost search-reset-btn"', 'href="#" class="btn-ghost search-reset-btn"')
    html = re.sub(
        r'<form method="post" action="/logout" class="upload-quick-form">\s*<button type="submit" class="top-logout-btn">Salir</button>\s*</form>',
        "",
        html,
        count=1,
        flags=re.S,
    )
    html = html.replace("</body>", f"{STATIC_BRIDGE_SCRIPT}\n</body>")
    return html
def _write_docs(main_html: str) -> None:
    DOCS_DIR.mkdir(exist_ok=True)
    shutil.copy2(STATIC_DIR / "styles.css", DOCS_DIR / "styles.css")
    if (STATIC_DIR / "articulos").exists():
        shutil.rmtree(DOCS_DIR / "articulos", ignore_errors=True)
        shutil.copytree(STATIC_DIR / "articulos", DOCS_DIR / "articulos")
    (DOCS_DIR / "index.html").write_text(main_html, encoding="utf-8")
    (DOCS_DIR / "404.html").write_text(main_html, encoding="utf-8")
    (DOCS_DIR / ".nojekyll").write_text("", encoding="utf-8")


def main() -> None:
    print("Sincronizando desde Z:\\BI...")
    _sync_bi_to_seed()
    main_html = _postprocess_main_html(_render_main_html())
    _write_docs(main_html)
    print(f"Static site generated in {DOCS_DIR}")


if __name__ == "__main__":
    main()
