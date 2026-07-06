# Operacion: Subida De Datos (Stock + Metas + Saldos 42/43)

Este archivo deja el flujo estable para actualizar la web y publicar en GitHub Pages.

## 1) Archivos fuente que debemos usar

- Stock:
  - `C:\Users\manuh\OneDrive - Mohicano Jeans\INVENTARIO 01-04 COMPLETO.xlsx`
- Metas:
  - `C:\Users\manuh\Downloads\1_PROGRAMAS DE PRODUCCION MHC .xlsx`
- Tabla completa por temporada:
  - `seed/SALDOS-SECCI 42.TXT`
  - `seed/SALDOS-SECCI 43.TXT`

## 2) Reglas importantes

- El Excel de metas (`1_PROGRAMAS...xlsx`) pesa mas de 100MB y GitHub no lo acepta.
- Por eso, para metas se publica `seed/PROGRAMAS_MHC_SNAPSHOT.json` (no el xlsx).
- La tabla completa usa `SALDOS-SECCI 42/43` y se filtra con botones `Todas / 42 / 43`.

## 3) Flujo rapido de actualizacion

Desde la raiz del proyecto:

```powershell
# 1) Copiar stock actualizado a seed
Copy-Item -LiteralPath "C:\Users\manuh\OneDrive - Mohicano Jeans\INVENTARIO 01-04 COMPLETO.xlsx" `
  -Destination ".\seed\INVENTARIO 01-04 COMPLETO.xlsx" -Force

# 2) Copiar metas actualizadas a seed (solo para lectura local)
Copy-Item -LiteralPath "C:\Users\manuh\Downloads\1_PROGRAMAS DE PRODUCCION MHC .xlsx" `
  -Destination ".\seed\1_PROGRAMAS DE PRODUCCION MHC .xlsx" -Force

# 3) Regenerar snapshot de metas desde ese Excel
@'
from app import _load_programas_mhc_snapshot
s = _load_programas_mhc_snapshot() or {}
print("sheet:", s.get("sheet_name"))
'@ | python -

# 4) Generar docs para Pages con ese Excel de metas
$env:ADECOM_PROGRAMAS_MHC_XLSX='seed\1_PROGRAMAS DE PRODUCCION MHC .xlsx'
python .\scripts\build_static_site.py
Remove-Item Env:ADECOM_PROGRAMAS_MHC_XLSX -ErrorAction SilentlyContinue
```

## 4) Validacion recomendada antes de push

```powershell
# Verifica que metas quedo en el snapshot
@'
import json
from pathlib import Path
s=json.loads(Path("seed/PROGRAMAS_MHC_SNAPSHOT.json").read_text(encoding="utf-8"))
for w in s.get("weeks", []):
    if list(w.get("fechas") or []) == [13,14,15,16,17,18,19]:
        print({r.get("key"): r.get("total") for r in (w.get("rows") or [])})
        break
'@ | python -

# Verifica que hay cambios listos
git status --short
```

## 5) Publicacion a GitHub Pages

```powershell
git add seed/INVENTARIO\ 01-04\ COMPLETO.xlsx `
        seed/PROGRAMAS_MHC_SNAPSHOT.json `
        seed/SALDOS-SECCI\ 42.TXT `
        seed/SALDOS-SECCI\ 43.TXT `
        docs/index.html docs/404.html docs/styles.css `
        app.py templates/index.html static/styles.css

git commit -m "Actualizar stock, metas y tabla completa por temporada"
git push origin main
```

## 6) Resultado esperado

- GitHub Actions despliega Pages en 1-3 minutos.
- URL:
  - `https://mancid-max.github.io/adecom-web/`
