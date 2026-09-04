"""
Importa la foto principal de cada artículo desde el proyecto PAGINA WEB
(data-catalogo-<temp>.json: family "4413-00" -> main_image) al dashboard.

Salida: docs/img/art/01441300.jpg (+ copia en dashboard-test/img/art/), 600px máx, JPEG q80.
También genera <modelo>.jpg (ej 4413.jpg) con el color 00 (o el primero) como respaldo.

Uso: python scripts/importar_fotos_catalogo.py
"""
import os, json, re
from PIL import Image, ImageOps

ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB    = r"C:\Users\Lenovo\Desktop\Backup\Data Manu\Backup\PAGINA WEB"
OUTS   = [os.path.join(ROOT, 'docs', 'img', 'art'), os.path.join(ROOT, 'dashboard-test', 'img', 'art')]
TEMPS  = ['40', '41', '42', '43', '44']
MAX_W  = 600

def save(im, code):
    for o in OUTS:
        im.save(os.path.join(o, f"{code}.jpg"), 'JPEG', quality=80, optimize=True)

def main():
    for o in OUTS: os.makedirs(o, exist_ok=True)
    ok, missing, bad = 0, [], []
    modelo_done = {}
    for t in TEMPS:
        jf = os.path.join(WEB, f"data-catalogo-{t}.json")
        if not os.path.exists(jf):
            print(f"  sin data-catalogo-{t}.json"); continue
        for it in json.load(open(jf, encoding='utf-8')):
            fam = str(it.get('family') or '').strip()
            m = re.match(r'^(\d{4})-?(\d{2})$', fam)
            if not m:
                bad.append(fam); continue
            modelo, color = m.group(1), m.group(2)
            code = f"01{modelo}{color}"
            src = it.get('main_image') or (it.get('gallery') or [None])[0]
            path = os.path.join(WEB, src.replace('/', os.sep)) if src else None
            if not path or not os.path.exists(path):
                missing.append((fam, src)); continue
            try:
                im = ImageOps.exif_transpose(Image.open(path)).convert('RGB')
            except Exception as e:
                bad.append(f"{fam}: {e}"); continue
            if im.width > MAX_W:
                im = im.resize((MAX_W, round(im.height * MAX_W / im.width)), Image.LANCZOS)
            save(im, code); ok += 1
            # respaldo por modelo: color 00 manda; si no, el primero que aparezca
            if modelo not in modelo_done or color == '00':
                save(im, modelo); modelo_done[modelo] = color
    print(f"{ok} fotos por artículo + {len(modelo_done)} por modelo -> docs/img/art/")
    if missing: print(f"{len(missing)} sin archivo de imagen:", missing[:10])
    if bad:     print(f"{len(bad)} family no reconocido:", bad[:10])

if __name__ == '__main__':
    main()
