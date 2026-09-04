"""
Prepara fotos de artículos para el dashboard.

Entrada : seed/fotos/<codigo>.(jpg|jpeg|png|webp)   codigo = 8 dígitos (modelo+color, ej 01441300)
                                                     o 4 dígitos (modelo, ej 4413 → sirve a todos sus colores)
Salida  : docs/img/art/<codigo>.jpg  y  dashboard-test/img/art/<codigo>.jpg  (600px máx, JPEG q80)

Uso: python scripts/preparar_fotos.py
El nombre puede traer texto extra: "4413 azul.jpg" o "IMG_01441300.jpeg" → se toma el primer grupo de 8 o 4 dígitos.
"""
import os, re, sys
from PIL import Image, ImageOps

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC  = os.path.join(ROOT, 'seed', 'fotos')
OUTS = [os.path.join(ROOT, 'docs', 'img', 'art'), os.path.join(ROOT, 'dashboard-test', 'img', 'art')]
MAX_W = 600
EXT = {'.jpg', '.jpeg', '.png', '.webp'}

def codigo(nombre):
    stem = os.path.splitext(nombre)[0]
    m = re.search(r'(?<!\d)(\d{8})(?!\d)', stem) or re.search(r'(?<!\d)(\d{4})(?!\d)', stem)
    return m.group(1) if m else None

def main():
    if not os.path.isdir(SRC):
        os.makedirs(SRC); print(f"Creada {SRC}. Deja ahí las fotos y vuelve a correr."); return
    for o in OUTS: os.makedirs(o, exist_ok=True)
    ok, skip = 0, []
    for f in sorted(os.listdir(SRC)):
        if os.path.splitext(f)[1].lower() not in EXT: continue
        code = codigo(f)
        if not code: skip.append(f); continue
        im = Image.open(os.path.join(SRC, f))
        im = ImageOps.exif_transpose(im).convert('RGB')
        if im.width > MAX_W:
            im = im.resize((MAX_W, round(im.height * MAX_W / im.width)), Image.LANCZOS)
        for o in OUTS:
            im.save(os.path.join(o, f"{code}.jpg"), 'JPEG', quality=80, optimize=True)
        ok += 1; print(f"  {f} -> {code}.jpg ({im.width}x{im.height})")
    print(f"{ok} fotos listas en docs/img/art/")
    if skip: print("Sin código en el nombre (omitidas):", skip)

if __name__ == '__main__':
    main()
