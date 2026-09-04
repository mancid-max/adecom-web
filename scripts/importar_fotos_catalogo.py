"""
Importa las fotos de cada artículo desde el proyecto PAGINA WEB
(data-catalogo-<temp>.json: family "4413-00" -> main_image + gallery) al dashboard.

Salida en docs/img/art/ (+ copia en dashboard-test/img/art/), 600px máx, JPEG q78:
  01441300.jpg, 01441300_2.jpg, ... (hasta MAX_FOTOS por artículo)
  4413.jpg  -> respaldo por modelo (color 00 o el primero)
  galeria.json -> {"01441300": 5, "4413": "01441300"}  (n fotos / modelo -> artículo)

Uso: python scripts/importar_fotos_catalogo.py
"""
import os, json, re
from PIL import Image, ImageOps

ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB       = r"C:\Users\Lenovo\Desktop\Backup\Data Manu\Backup\PAGINA WEB"
OUTS      = [os.path.join(ROOT, 'docs', 'img', 'art'), os.path.join(ROOT, 'dashboard-test', 'img', 'art')]
TEMPS     = ['40', '41', '42', '43', '44']
MAX_W     = 600
MAX_FOTOS = 5

def load_img(rel):
    path = os.path.join(WEB, rel.replace('/', os.sep))
    if not os.path.exists(path): return None
    try:
        im = ImageOps.exif_transpose(Image.open(path)).convert('RGB')
    except Exception:
        return None
    if im.width > MAX_W:
        im = im.resize((MAX_W, round(im.height * MAX_W / im.width)), Image.LANCZOS)
    return im

def save(im, name):
    for o in OUTS:
        im.save(os.path.join(o, f"{name}.jpg"), 'JPEG', quality=78, optimize=True)

def main():
    for o in OUTS: os.makedirs(o, exist_ok=True)
    galeria, modelo_done, missing, bad, fotos = {}, {}, [], [], 0
    for t in TEMPS:
        jf = os.path.join(WEB, f"data-catalogo-{t}.json")
        if not os.path.exists(jf):
            print(f"  sin data-catalogo-{t}.json"); continue
        for it in json.load(open(jf, encoding='utf-8')):
            fam = str(it.get('family') or '').strip()
            m = re.match(r'^(\d{4})(?:-?(\d{2}))?$', fam)
            if not m:
                bad.append(fam); continue
            modelo, color = m.group(1), m.group(2) or '00'
            code = f"01{modelo}{color}"
            main_img = it.get('main_image')
            srcs = [s for s in ([main_img] + list(it.get('gallery') or [])) if s]
            srcs = list(dict.fromkeys(srcs))[:MAX_FOTOS]   # únicos, main primero
            n = 0
            for s in srcs:
                im = load_img(s)
                if im is None: continue
                n += 1
                save(im, code if n == 1 else f"{code}_{n}")
                if n == 1 and (modelo not in modelo_done or color == '00'):
                    save(im, modelo); modelo_done[modelo] = code
            if n == 0:
                missing.append(fam); continue
            galeria[code] = n; fotos += n
    for modelo, code in modelo_done.items():
        galeria[modelo] = code
    for o in OUTS:
        with open(os.path.join(o, 'galeria.json'), 'w', encoding='utf-8') as f:
            json.dump(galeria, f, ensure_ascii=False)
    arts = sum(1 for k, v in galeria.items() if isinstance(v, int))
    print(f"{arts} artículos, {fotos} fotos, {len(modelo_done)} modelos -> docs/img/art/")
    if missing: print(f"{len(missing)} sin imagen:", missing[:10])
    if bad:     print(f"{len(bad)} family no reconocido:", bad[:10])

if __name__ == '__main__':
    main()
