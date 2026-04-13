from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

from flask import session


ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs"
STATIC_DIR = ROOT / "static"
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
    app_module.ensure_seed_data()
    with app_module.app.test_request_context("/"):
        session["portal_section"] = "main"
        session["can_upload"] = False
        rendered = app_module.index()
        if hasattr(rendered, "get_data"):
            return rendered.get_data(as_text=True)
        return str(rendered)


def _render_other_html() -> str:
    app_module.ASSISTANT_ENABLED = False
    app_module.ensure_seed_data()
    with app_module.app.test_request_context("/otra-landing"):
        session["portal_section"] = "other"
        session["can_upload"] = False
        rendered = app_module.other_section()
        if hasattr(rendered, "get_data"):
            return rendered.get_data(as_text=True)
        return str(rendered)


def _postprocess_main_html(html: str) -> str:
    html = html.replace('href="/static/styles.css"', 'href="styles.css"')
    html = html.replace('<body class="theme-gentelella">', '<body class="theme-gentelella static-export">')
    html = html.replace('action="/logout"', 'action="#"')
    html = html.replace('action="/"', 'action="#"')
    html = html.replace('href="/" class="btn-ghost search-reset-btn"', 'href="#" class="btn-ghost search-reset-btn"')
    html = html.replace('href="/otra-landing"', 'href="otra-landing/"')
    html = re.sub(
        r'<form method="post" action="/logout" class="upload-quick-form">\s*<button type="submit" class="top-logout-btn">Salir</button>\s*</form>',
        "",
        html,
        count=1,
        flags=re.S,
    )
    html = html.replace("</body>", f"{STATIC_BRIDGE_SCRIPT}\n</body>")
    return html


def _postprocess_other_html(html: str) -> str:
    html = html.replace('action="/logout"', 'action="#"')
    html = html.replace('action="/otra-landing"', 'action="#"')
    html = html.replace('action="/otra-landing/import-excel"', 'action="#"')
    html = html.replace('action="/otra-landing/add"', 'action="#"')
    html = re.sub(r'action="/otra-landing/delete/\d+"', 'action="#"', html)
    html = html.replace("</body>", "\n</body>")
    return html


def _write_docs(main_html: str, other_html: str) -> None:
    DOCS_DIR.mkdir(exist_ok=True)
    (DOCS_DIR / "otra-landing").mkdir(exist_ok=True)
    shutil.copy2(STATIC_DIR / "styles.css", DOCS_DIR / "styles.css")
    (DOCS_DIR / "index.html").write_text(main_html, encoding="utf-8")
    (DOCS_DIR / "404.html").write_text(main_html, encoding="utf-8")
    (DOCS_DIR / "otra-landing" / "index.html").write_text(other_html, encoding="utf-8")
    (DOCS_DIR / "otra-landing.html").write_text(other_html, encoding="utf-8")
    (DOCS_DIR / ".nojekyll").write_text("", encoding="utf-8")


def main() -> None:
    main_html = _postprocess_main_html(_render_main_html())
    other_html = _postprocess_other_html(_render_other_html())
    _write_docs(main_html, other_html)
    print(f"Static site generated in {DOCS_DIR}")


if __name__ == "__main__":
    main()
