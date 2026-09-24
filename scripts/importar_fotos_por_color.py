"""
Importa las fotos de las carpetas por COLOR del proyecto PAGINA WEB.

El script importar_fotos_catalogo.py lee data-catalogo-<temp>.json, que no incluye las carpetas
sueltas del tipo  42/4227-01/  ,  43/4322-60/  ,  44/4431-00/  . Esas carpetas sí tienen la foto
del color exacto, y sin ellas el dashboard muestra la foto del color 00 para todos los colores
del modelo, como si fueran el mismo jean.

Solo agrega artículos que todavía no tienen foto propia: nunca pisa lo ya importado.
Mismo formato que el otro script (600 px, JPEG q78) y actualiza galeria.json sin borrar nada.

Uso: python scripts/importar_fotos_por_color.py
"""
import os, re, json
from PIL import Image, ImageOps

ROOT  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB   = r"C:\Users\Lenovo\Desktop\Backup\Data Manu\Backup\PAGINA WEB"
OUTS  = [os.path.join(ROOT, 'docs', 'img', 'art'), os.path.join(ROOT, 'dashboard-test', 'img', 'art')]
TEMPS = ['40', '41', '42', '43', '44']
MAX_W, MAX_FOTOS, CALIDAD = 600, 5, 78
EXT = ('.jpg', '.jpeg', '.png', '.webp')


def abrir(path):
    try:
        im = ImageOps.exif_transpose(Image.open(path)).convert('RGB')
    except Exception:
        return None
    if im.width > MAX_W:
        im = im.resize((MAX_W, round(im.height * MAX_W / im.width)), Image.LANCZOS)
    return im


def guardar(im, nombre):
    for o in OUTS:
        im.save(os.path.join(o, f"{nombre}.jpg"), 'JPEG', quality=CALIDAD, optimize=True)


def main():
    for o in OUTS:
        os.makedirs(o, exist_ok=True)

    # Lo que ya está importado, para no pisarlo
    ya = {f.split('.')[0].split('_')[0] for f in os.listdir(OUTS[0]) if f.lower().endswith('.jpg')}

    galeria = {}
    gpath = os.path.join(OUTS[0], 'galeria.json')
    if os.path.exists(gpath):
        try:
            galeria = json.load(open(gpath, encoding='utf-8'))
        except Exception:
            galeria = {}

    agregados, fotos_total, sin_leer = [], 0, []
    for temp in TEMPS:
        base = os.path.join(WEB, temp)
        if not os.path.isdir(base):
            continue
        for carpeta in sorted(os.listdir(base)):
            m = re.fullmatch(r'(\d{4})-(\d{2})', carpeta)
            if not m:
                continue
            modelo, color = m.group(1), m.group(2)
            art = '01' + modelo + color
            if art in ya:
                continue                      # ya tiene foto propia
            ruta = os.path.join(base, carpeta)
            archivos = sorted(f for f in os.listdir(ruta)
                              if f.lower().endswith(EXT) and not f.startswith('._'))
            if not archivos:
                continue

            guardadas = 0
            for f in archivos[:MAX_FOTOS]:
                im = abrir(os.path.join(ruta, f))
                if im is None:
                    sin_leer.append(f"{temp}/{carpeta}/{f}")
                    continue
                guardadas += 1
                guardar(im, art if guardadas == 1 else f"{art}_{guardadas}")
            if not guardadas:
                continue

            galeria[art] = guardadas
            # Respaldo por modelo: solo si ese modelo aún no tiene ninguno
            if modelo not in galeria and not os.path.exists(os.path.join(OUTS[0], f"{modelo}.jpg")):
                im = abrir(os.path.join(ruta, archivos[0]))
                if im is not None:
                    guardar(im, modelo)
                    galeria[modelo] = art
            agregados.append((art, guardadas))
            fotos_total += guardadas
            ya.add(art)

    for o in OUTS:
        with open(os.path.join(o, 'galeria.json'), 'w', encoding='utf-8') as f:
            json.dump(galeria, f, ensure_ascii=False, separators=(',', ':'))

    print(f"Artículos nuevos con foto propia de su color: {len(agregados)}  ({fotos_total} fotos)")
    for art, n in agregados:
        print(f"  {art}  {n} foto{'s' if n != 1 else ''}")
    if sin_leer:
        print(f"  {len(sin_leer)} archivo(s) no se pudieron leer: {', '.join(sin_leer[:4])}")


if __name__ == '__main__':
    main()
