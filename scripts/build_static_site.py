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
  const ACCESS_KEY = "adecom-web";
  const STORAGE_KEY = "adecom_static_access_ok";
  const authed = window.localStorage.getItem(STORAGE_KEY) === "1";
  if (!authed) {
    document.documentElement.classList.add("static-auth-locked");
  }

  const unlock = () => {
    window.localStorage.setItem(STORAGE_KEY, "1");
    document.documentElement.classList.remove("static-auth-locked");
  };

  document.querySelectorAll('form[action="#"]').forEach((form) => {
    form.addEventListener("submit", (event) => event.preventDefault());
  });

  const form = document.getElementById("filtersForm");
  const input = document.getElementById("articuloExactInput");
  const resetLink = document.querySelector(".search-reset-btn");
  const openFullTableBtn = document.getElementById("openFullTableModal");
  const staticLoginForm = document.getElementById("staticLoginForm");
  const staticLoginInput = document.getElementById("staticLoginPassword");
  const staticLoginError = document.getElementById("staticLoginError");
  const staticLogoutBtn = document.getElementById("staticLogoutBtn");

  if (staticLoginForm && staticLoginInput) {
    staticLoginForm.addEventListener("submit", (event) => {
      event.preventDefault();
      if (String(staticLoginInput.value || "").trim() === ACCESS_KEY) {
        unlock();
        staticLoginInput.value = "";
        if (staticLoginError) staticLoginError.textContent = "";
        return;
      }
      if (staticLoginError) staticLoginError.textContent = "Clave incorrecta.";
      staticLoginInput.select();
    });
  }

  if (staticLogoutBtn) {
    staticLogoutBtn.addEventListener("click", () => {
      window.localStorage.removeItem(STORAGE_KEY);
      document.documentElement.classList.add("static-auth-locked");
      if (staticLoginInput) staticLoginInput.focus();
    });
  }

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

STATIC_AUTH_HTML = """
<div class="static-auth-gate" id="staticAuthGate">
  <form class="static-auth-card" id="staticLoginForm">
    <span class="static-auth-mark">A</span>
    <h1>ADECOM WEB</h1>
    <p>Ingrese su clave para continuar.</p>
    <input id="staticLoginPassword" type="password" autocomplete="current-password" placeholder="Clave">
    <button type="submit">Entrar</button>
    <small id="staticLoginError"></small>
  </form>
</div>
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


def _postprocess_main_html(html: str) -> str:
    html = html.replace('href="/static/styles.css"', 'href="styles.css"')
    html = html.replace('<body class="theme-gentelella">', '<body class="theme-gentelella static-export">')
    html = html.replace('<body class="theme-gentelella static-export">', f'<body class="theme-gentelella static-export">{STATIC_AUTH_HTML}')
    html = html.replace('action="/logout"', 'action="#"')
    html = html.replace('action="/"', 'action="#"')
    html = html.replace('href="/" class="btn-ghost search-reset-btn"', 'href="#" class="btn-ghost search-reset-btn"')
    html = html.replace(
        '<div class="top-nav-actions">',
        '<div class="top-nav-actions"><button type="button" class="top-logout-btn static-logout-btn" id="staticLogoutBtn">Cerrar sesion</button>',
        1,
    )
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
    (DOCS_DIR / "index.html").write_text(main_html, encoding="utf-8")
    (DOCS_DIR / "404.html").write_text(main_html, encoding="utf-8")
    (DOCS_DIR / ".nojekyll").write_text("", encoding="utf-8")


def main() -> None:
    main_html = _postprocess_main_html(_render_main_html())
    _write_docs(main_html)
    print(f"Static site generated in {DOCS_DIR}")


if __name__ == "__main__":
    main()
