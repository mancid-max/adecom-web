"""
Actualiza los 4 archivos JSON con datos reales desde Z:\BI
Ejecutar: python actualizar_datos.py
"""
import csv, json, os
from datetime import datetime, date

TODAY = date.today()
BI = r"Z:\BI"
OUT      = os.path.dirname(os.path.abspath(__file__))
DOCS_OUT = os.path.join(os.path.dirname(OUT), 'docs')

# Copia local de la última versión buena de cada archivo de Z:\BI. Si el ERP está exportando
# (borra y reescribe) o el export falló, se usa la copia en vez de abortar todo el build.
BI_CACHE = os.path.join(os.path.dirname(OUT), 'data', 'bi_cache')
os.makedirs(BI_CACHE, exist_ok=True)
import shutil
def bi_file(name):
    src, cache = os.path.join(BI, name), os.path.join(BI_CACHE, name)
    if os.path.exists(src) and os.path.getsize(src) > 0:
        try:
            if (not os.path.exists(cache) or os.path.getmtime(src) > os.path.getmtime(cache)
                    or os.path.getsize(src) != os.path.getsize(cache)):
                shutil.copy2(src, cache)
        except Exception as ex:
            print(f"  aviso: no se pudo copiar {name} a bi_cache: {ex}")
        return src
    if os.path.exists(cache):
        print(f"  AVISO: {name} no esta en Z:\\BI - usando copia local del "
              f"{datetime.fromtimestamp(os.path.getmtime(cache)).strftime('%d/%m/%Y %H:%M')}")
        return cache
    return src

TEMP_MIN, TEMP_MAX = 27, 99  # Todas las temporadas disponibles
def temp_valida(t):
    try: return TEMP_MIN <= int(t) <= TEMP_MAX
    except: return False

def parse_date(s):
    for fmt in ['%d/%m/%Y','%d-%m-%Y','%Y-%m-%d']:
        try: return datetime.strptime(str(s).strip(), fmt).date()
        except: pass
    return None

def to_iso(s): d = parse_date(s); return d.strftime('%Y-%m-%d') if d else ''
def fmt_date(s): d = parse_date(s); return d.strftime('%d/%m/%Y') if d else ''
def clean_int(s):
    try: return int(str(s).strip().replace(' ',''))
    except: return 0
def has_date(s):
    s = str(s).strip()
    return 1 if (s and '    ' not in s and parse_date(s)) else 0
def dias_desde(s):
    d = parse_date(s)
    return (TODAY - d).days if d else 0
def stage_dias(ini, fin):
    d1, d2 = parse_date(ini), parse_date(fin)
    if d1 and d2: return (d2 - d1).days or 1
    return 1 if d1 else 0

# ── 1. TRAZABILIDAD ────────────────────────────────────────────
print("Leyendo TRAZABILIDAD2.CSV...")
with open(bi_file("TRAZABILIDAD2.CSV"), encoding="latin-1") as f:
    traza_rows = list(csv.DictReader(f, delimiter=';'))

full_table = []
traza_oc   = []
for r in traza_rows:
    art  = r['Articulo'].strip()
    temp = art[2:4] if len(art) >= 4 else ''
    if not temp_valida(temp):
        continue
    tipo_r = r['Tipo'].strip().upper()
    tipo = 'Muestras' if 'MUESTRA' in tipo_r else ('Set' if 'SET' in tipo_r else 'Producción')

    full_table.append({
        "articulo": art, "corte": r['O.Corte'].strip(),
        "fecha": fmt_date(r['Fecha']), "fecha_iso": to_iso(r['Fecha']),
        "temporada": temp, "tipo": tipo, "m": r['Muestra'].strip(),
        "programa": clean_int(r['Programado']), "proceso": clean_int(r['Cortado']),
        "bodega": clean_int(r['Entrega']), "saldo": clean_int(r['Saldo']),
        "corte_u": has_date(r['Corte']), "taller": has_date(r['Taller']),
        "texterno": has_date(r['Taller Ext']), "limpiado": has_date(r['Limpiado']),
        "lavanderia": has_date(r['Lavander']), "terminacion": has_date(r['Terminacion']),
        "muestra": 1 if r['Muestras'].strip() else 0, "segunda": 0
    })

    stage_cols = [("Corte","Corte"),("Taller","Taller"),("Taller Ext","Taller Ext"),
                  ("Limpiado","Limpiado"),("Lavander","Lavander"),("Terminacion","Terminacion")]
    stages = []
    prev_fin = ''
    for name, col in stage_cols:
        ini_raw = str(r.get(col,'')).strip()
        ini_raw = '' if '    ' in ini_raw else ini_raw
        fin_raw = ''
        if name == "Corte":      fin_raw = ini_raw
        elif name == "Lavander": fin_raw = str(r.get('Terminacion','')).strip()
        fin_raw = '' if '    ' in fin_raw else fin_raw
        stages.append({"name": name,
                        "ini": fmt_date(ini_raw) if ini_raw else "",
                        "fin": fmt_date(fin_raw) if fin_raw else "",
                        "dias": stage_dias(ini_raw, fin_raw)})

    traza_oc.append({
        "oc": r['O.Corte'].strip(), "tipo": r['Tipo'].strip(),
        "m": r['Muestra'].strip(), "fecha": fmt_date(r['Fecha']),
        "articulo": art, "prog": clean_int(r['Programado']),
        "cort": clean_int(r['Cortado']), "ent": clean_int(r['Entrega']),
        "saldo": clean_int(r['Saldo']), "stages": stages,
        "totDias": clean_int(r['Tot.dias']), "inc": r.get('Incidencias','').strip()
    })

# ── 2. PEDIDOS ─────────────────────────────────────────────────
print("Leyendo PEDIDOS.CSV...")
with open(bi_file("PEDIDOS.CSV"), encoding="latin-1") as f:
    ped_rows = list(csv.DictReader(f, delimiter=';'))

pedidos_dict = {}

for r in ped_rows:
    pid  = r['PEDIDO'].strip()
    art  = r.get('ARTICULO','').strip()
    temp = art[2:4] if len(art) >= 4 else r.get('TEMPORADA','').strip()[:2]
    if not temp_valida(temp):
        continue

    if pid not in pedidos_dict:
        pedidos_dict[pid] = {
            "pedido": pid, "fecha": fmt_date(r['FECHA']), "fecha_iso": to_iso(r['FECHA']),
            "rut": r['RUT'].strip(), "nombre": r['CLIENTE'].strip(),
            "ciudad": r['CIUDAD'].strip(), "vendedor": r.get('VENDEDOR','').strip(),
            "unidades": 0, "despacho": 0, "saldo": 0,
            "valor": 0, "valor_desp": 0, "valor_sal": 0,
            "dias": dias_desde(r['FECHA']), "temps": [], "u_temp": {}, "u_desp": {}, "u_sal": {},
            "_arts": {}
        }
    p = pedidos_dict[pid]
    if temp and temp not in p['temps']:
        p['temps'].append(temp)
    sol  = clean_int(r.get('SOLICITADO', 0))
    desp = clean_int(r.get('DESPACHADO', 0))
    sal  = clean_int(r.get('saldo', 0))
    try:
        # DCTO columna contiene códigos internos, no porcentajes → ignorar
        precio = float(str(r.get('PRECIO','0')).strip() or 0)
        p['valor']      += int(sol  * precio)
        p['valor_desp'] += int(desp * precio)
        p['valor_sal']  += int(sal  * precio)
    except: pass
    p['unidades'] += sol
    p['despacho'] += desp
    p['saldo']    += sal
    # Unidades desglosadas por temporada para que el filtro por temp sea exacto
    p['u_temp'][temp] = p['u_temp'].get(temp, 0) + sol
    p['u_desp'][temp] = p['u_desp'].get(temp, 0) + desp
    p['u_sal'][temp]  = p['u_sal'].get(temp, 0)  + sal
    # Detalle por artículo (art8 = sin talla)
    art8 = art[:8]
    if art8 and len(art8) == 8:
        if art8 not in p['_arts']:
            p['_arts'][art8] = {'sol': 0, 'desp': 0, 'sal': 0}
        p['_arts'][art8]['sol']  += sol
        p['_arts'][art8]['desp'] += desp
        p['_arts'][art8]['sal']  += sal

# Convertir _arts en lineas y limpiar clave interna
for p in pedidos_dict.values():
    arts = p.pop('_arts', {})
    p['lineas'] = sorted(
        [{'art': k, 'temp': k[2:4], 'modelo': k[2:6], 'color': k[6:8], **v}
         for k, v in arts.items() if v['sol'] > 0],
        key=lambda x: -(x['sal'])
    )

pedidos = list(pedidos_dict.values())

# Mapeo art8 → tipo de bota (desde SubCateg de PEDIDOS.CSV)
def norm_bota(sc):
    s = sc.strip().upper()
    if 'PITILLO' in s: return 'Pitillo'
    if 'FLARE'   in s: return 'Flare'
    if 'BOOTCUT' in s: return 'Bootcut'
    if 'WIDE LEG' in s: return 'Wide Leg'
    if 'OXFORD'  in s: return 'Oxford'
    if 'PALAZZO' in s: return 'Palazzo'
    if 'RECTO'   in s: return 'Recto'
    if 'BALLOON' in s: return 'Balloon'
    if 'BERMUDA' in s: return 'Bermuda'
    if 'CALZA'   in s: return 'Calza'
    return s.title() if s else ''

mod_bota = {}
for r in ped_rows:
    art8 = r.get('ARTICULO','').strip()[:8]
    sc   = r.get('SubCateg','').strip()
    if art8 and sc:
        mod_bota[art8] = norm_bota(sc)

# ── 2b. ARTÍCULOS POR TALLA (ARCHIVO_TALLAS.CSV) ───────────────────────────
# Fuente correcta para unidades pedidas por artículo/modelo.
# PEDIDOS.CSV suma SOLICITADO que incluye líneas no confirmadas → cifra mayor.
# ARCHIVO_TALLAS.CSV "Ventas" refleja las unidades reales de la temporada.
print("Leyendo ARCHIVO_TALLAS.CSV...")
art_dict = {}  # {temp: {base: {mod: qty}}}
tallas_fallback = False
try:
    with open(bi_file("ARCHIVO_TALLAS.CSV"), encoding="latin-1") as f:
        for line in f:
            cells = [c.strip() for c in line.strip().split(';')]
            if len(cells) < 6 or not cells[0] or not cells[2]:
                continue
            if cells[3].lower() != 'ventas':
                continue
            art  = cells[0]
            temp = art[2:4] if len(art) >= 4 else ''
            if not temp_valida(temp):
                continue
            base = art[2:6] if len(art) >= 6 else ''
            mod  = art[6:8] if len(art) >= 8 else ''
            if not base:
                continue
            # El último campo no vacío (desde posición 4) es el total de la fila
            non_empty = [c for c in cells[4:] if c]
            if not non_empty:
                continue
            qty = clean_int(non_empty[-1])
            if temp not in art_dict:
                art_dict[temp] = {}
            if base not in art_dict[temp]:
                art_dict[temp][base] = {}
            art_dict[temp][base][mod] = art_dict[temp][base].get(mod, 0) + qty
except FileNotFoundError:
    print("  ARCHIVO_TALLAS.CSV no encontrado, usando PEDIDOS.CSV para artículos")
    tallas_fallback = True
    for r in ped_rows:
        art  = r.get('ARTICULO','').strip()
        temp = art[2:4] if len(art) >= 4 else r.get('TEMPORADA','').strip()[:2]
        if not temp_valida(temp):
            continue
        base = art[2:6] if len(art) >= 6 else ''
        mod  = art[6:8] if len(art) >= 8 else ''
        sol  = clean_int(r.get('SOLICITADO', 0))
        if base:
            if temp not in art_dict:
                art_dict[temp] = {}
            if base not in art_dict[temp]:
                art_dict[temp][base] = {}
            art_dict[temp][base][mod] = art_dict[temp][base].get(mod, 0) + sol

# Construir pedidos_art: lista de {temp, base, total, modelos:[{mod,qty}]}
pedidos_art = []
for temp, bases in art_dict.items():
    for base, mods in bases.items():
        total = sum(mods.values())
        pedidos_art.append({
            "temp": temp, "base": base, "total": total,
            "modelos": [{"mod": m, "qty": q} for m, q in sorted(mods.items())]
        })

# ── 3. VENTAS ──────────────────────────────────────────────────
# Total C/descto = neto con descuentos aplicados (pre-IVA)
# Bruto = neto × 1.19 para tipos afectos (Factura, Boleta)
EXENTOS = {'02', '34', '56'}
def calc_bruto(neto, tipo):
    return neto if tipo in EXENTOS else int(round(neto * 1.19))

print("Leyendo VENTAS-TOD-2026.CSV...")
with open(bi_file("VENTAS-TOD-2026.CSV"), encoding="latin-1") as f:
    reader = csv.DictReader(f, delimiter=';')
    venta_rows = list(reader)

docs_dict = {}
for r in venta_rows:
    tipo  = str(r.get('Tipo') or '').strip()
    num   = str(r.get('Numero') or '').strip()
    bod   = str(r.get('Bod') or '').strip().zfill(2)
    # Todas las bodegas: 04 San Gerardo (mayorista) + 00 Central / 12 Outlet (retail boletas)
    key = (tipo, num)
    if key not in docs_dict:
        fecha = str(r.get('fecha') or '').strip()
        docs_dict[key] = {
            "tipo": tipo, "dcto": num, "bod": bod,
            "fecha": fmt_date(fecha), "fecha_iso": to_iso(fecha),
            "rut": str(r.get('Rut') or '').strip(),
            "razon": str(r.get('cliente') or '').strip(),
            "vendedor": str(r.get('Vendedor') or '').strip(),
            "prendas": 0, "neto": 0, "bruto": 0,
            "fpago": str(r.get('Fpago') or '').strip()
        }
    d = docs_dict[key]
    d['prendas'] += clean_int(r.get('Cant', 0))
    # Usar Total C/descto si existe, sino Total como fallback
    desc_raw = str(r.get('Total C/descto') or '').strip()
    tot_raw  = str(r.get('Total') or '0').strip()
    neto_val = clean_int(desc_raw) if desc_raw else clean_int(tot_raw)
    d['neto'] += neto_val

for d in docs_dict.values():
    d['bruto'] = calc_bruto(d['neto'], d['tipo'])
    d['total'] = d['neto']  # compatibilidad con vistas existentes

docs_venta = list(docs_dict.values())

# ── 4. SALDOS POR LOCAL (SALDOSXLOCAL.CSV desde Z:\BI) ────────
SUCURSALES_PRENDAS = {'01','02','04','05','10','12','33'}

print("Leyendo SALDOSXLOCAL.CSV...")
# OJO con la columna 'Cajas': NO es por bodega. Es un TOTAL por artículo+talla que el ERP repite
# idéntico en la fila de cada bodega (verificado 2026-09-10: art 0144140038 sale con 29 cajas en la
# bodega 04 y otras 29 en la 10, que son las mismas). Sumarla por bodega la duplicaba y dejaba
# artículos con más cajas que stock. Se toma UNA vez por talla y se asigna a la bodega que concentra
# el stock de esa talla, que es donde están físicamente.
raw = {}        # {(art8, talla): {'cajas': n, 'sucs': {suc: qty}}}
saldo_map = {}  # {art8: {'sucs': {suc: qty}, 'tallas': {talla: qty}, ...}}
saldo_file = bi_file('SALDOSXLOCAL.CSV')
try:
    with open(saldo_file, encoding='latin-1') as f:
        reader = csv.DictReader(f, delimiter=';')
        for r in reader:
            code = str(r.get('Articulo', '')).strip()
            suc  = str(r.get('Bodega', '')).strip().zfill(2)
            try:
                qty = float(str(r.get('SaldoFisico', '0')).strip() or 0)
            except:
                continue
            try:
                cajas = float(str(r.get('Cajas', '0')).strip() or 0)
            except:
                cajas = 0
            if qty <= 0 and cajas <= 0:
                continue
            if not code.startswith('01') or len(code) < 10:
                continue
            try:
                t_num = int(code[2:4])
            except:
                continue
            # T40–T44 siempre; colecciones anteriores solo si tienen cajas (para limpiar ese dato)
            if not (40 <= t_num <= 44) and cajas <= 0:
                continue
            art8  = code[:8]
            talla = code[8:10].lstrip('0') or code[8:10]
            d = raw.setdefault((art8, talla), {'cajas': 0, 'sucs': {}})
            d['cajas'] = max(d['cajas'], cajas)          # mismo total repetido: tomarlo una vez
            d['sucs'][suc] = d['sucs'].get(suc, 0) + qty

    for (art8, talla), d in raw.items():
        if art8 not in saldo_map:
            saldo_map[art8] = {'sucs': {}, 'tallas': {}, 'cajas': {}, 'cajas_talla': {},
                               't_suc': {}, 'ct_suc': {}}
        e = saldo_map[art8]
        for suc, qty in d['sucs'].items():
            e['sucs'][suc] = e['sucs'].get(suc, 0) + qty
            if talla and suc in SUCURSALES_PRENDAS:
                e['tallas'][talla] = e['tallas'].get(talla, 0) + qty
                ts = e['t_suc'].setdefault(suc, {})
                ts[talla] = ts.get(talla, 0) + qty
        # Las cajas de esta talla van completas a la bodega con más stock de la talla
        cajas = d['cajas']
        if cajas > 0:
            prio = [(s, q) for s, q in d['sucs'].items() if s in SUCURSALES_PRENDAS] or list(d['sucs'].items())
            principal = max(prio, key=lambda kv: (kv[1], kv[0]))[0] if prio else None
            if principal:
                e['cajas'][principal] = e['cajas'].get(principal, 0) + cajas
                if talla and principal in SUCURSALES_PRENDAS:
                    e['cajas_talla'][talla] = e['cajas_talla'].get(talla, 0) + cajas
                    cs = e['ct_suc'].setdefault(principal, {})
                    cs[talla] = cs.get(talla, 0) + cajas
except FileNotFoundError:
    print("  SALDOSXLOCAL.CSV no encontrado en Z:\\BI")

saldos_bodega = []
for art8, data in sorted(saldo_map.items()):
    sucs   = data['sucs']
    tallas = data['tallas']
    t      = art8[2:4]
    modelo = art8[2:6]
    color  = art8[6:8]
    total_prendas = sum(v for k, v in sucs.items() if k in SUCURSALES_PRENDAS)
    total_all = sum(sucs.values())
    if total_all <= 0:
        continue
    def _tsort(k):
        try: return int(k)
        except: return 999
    cajas_d = data.get('cajas', {})
    cajas_talla = data.get('cajas_talla', {})
    total_cajas = int(sum(v for k, v in cajas_d.items() if k in SUCURSALES_PRENDAS))
    tallas_sorted = {k: int(v) for k, v in sorted(tallas.items(), key=lambda x: _tsort(x[0]))}
    saldo_talla = {k: max(0, int(v) - int(cajas_talla.get(k, 0))) for k, v in tallas_sorted.items()}
    # Tallas por sucursal (solo las que tienen stock), para el filtro por local
    tallas_suc, saldo_talla_suc = {}, {}
    for s, tt in data.get('t_suc', {}).items():
        ts = {k: int(v) for k, v in sorted(tt.items(), key=lambda x: _tsort(x[0])) if int(v) > 0}
        if not ts:
            continue
        cs = data.get('ct_suc', {}).get(s, {})
        tallas_suc[s] = ts
        saldo_talla_suc[s] = {k: max(0, v - int(cs.get(k, 0))) for k, v in ts.items()}
    saldos_bodega.append({
        "art": art8, "temp": t, "modelo": modelo, "color": color,
        "suc": {k: int(v) for k, v in sucs.items()},
        "cajas": {k: int(v) for k, v in cajas_d.items()},
        "cajas_total": total_cajas,
        "saldo": max(0, int(total_prendas) - total_cajas),
        "tallas": tallas_sorted,
        "saldo_talla": saldo_talla,
        "tallas_suc": tallas_suc,
        "saldo_talla_suc": saldo_talla_suc,
        "prendas": int(total_prendas), "total": int(total_all),
        "bota": mod_bota.get(art8, '')
    })
saldos_bodega.sort(key=lambda x: -x['prendas'])

# ── 5. PVC EX MAPPING (COLE44_ORIGEN.xlsx + TRAZABILIDAD T40-T43) ─
import re as _re

# Saldo EX = Cortado - Entrega en T40-T43
corte_hist = {}; entrega_hist = {}
for r in traza_rows:
    art_r = r['Articulo'].strip()
    t_str = art_r[2:4] if len(art_r) >= 4 else ''
    try:
        t_num = int(t_str)
    except:
        continue
    if not (40 <= t_num <= 43):
        continue
    if 'PRODUCCION' not in r['Tipo'].strip().upper():
        continue
    key = art_r[2:6]
    corte_hist[key]   = corte_hist.get(key, 0)   + clean_int(r['Cortado'])
    entrega_hist[key] = entrega_hist.get(key, 0) + clean_int(r['Entrega'])

seed_dir = os.path.join(os.path.dirname(OUT), 'seed')
origen_xlsx = os.path.join(seed_dir, 'COLE44_ORIGEN.xlsx')
pvc_ex = {}
try:
    from openpyxl import load_workbook
    wb = load_workbook(origen_xlsx, read_only=True, data_only=True)
    ws = wb.active
    for row in ws.iter_rows(min_row=6, values_only=True):
        art_raw    = row[2] if len(row) > 2 else None
        origen_raw = row[3] if len(row) > 3 else None
        if not art_raw:
            continue
        parts  = str(art_raw).strip().split('-')
        modelo = parts[0].strip()
        if len(modelo) != 4:
            continue
        origen_str = str(origen_raw).strip() if origen_raw else ''
        m = _re.match(r'(?i)^EX\s*(\d{4})', origen_str)
        if not m:
            continue
        ex_base  = m.group(1)
        ex_saldo = max(0, corte_hist.get(ex_base, 0) - entrega_hist.get(ex_base, 0))
        if modelo not in pvc_ex:
            pvc_ex[modelo] = {"ex_base": ex_base, "ex_saldo": ex_saldo}
except Exception as e:
    print(f"  COLE44_ORIGEN.xlsx: {e}")

# ── 6. CAJAS EN BODEGA (CAJAS.TXT) ─────────────────────────────
# Cajas físicas armadas y asignadas a un pedido. Se cruza con PEDIDOS.CSV
# (despachado/precio/vendedor) y VENTAS (facturas posteriores del RUT).
# estado: bodega (sin despachar) | despachada (sin factura) | facturada
print("Leyendo CAJAS.TXT...")
def _rut_norm(s): return str(s or '').strip().replace('.', '').replace('-', '').upper()
cajas_file = bi_file('CAJAS.TXT')
cajas = []
cajas_meta = {"archivo_fecha": "", "archivo_fecha_iso": ""}
try:
    _mt = datetime.fromtimestamp(os.path.getmtime(cajas_file))
    cajas_meta = {"archivo_fecha": _mt.strftime('%d/%m/%Y %H:%M'), "archivo_fecha_iso": _mt.strftime('%Y-%m-%dT%H:%M')}
    ped_line = {}   # (pedido, art10) -> (sol, desp, precio)
    ped_info = {}   # pedido -> ciudad/vendedor
    for r in ped_rows:
        pid = r['PEDIDO'].strip()
        k = (pid, r.get('ARTICULO', '').strip() + r.get('TALLA', '').strip().zfill(2))
        try: _pr = float(str(r.get('PRECIO', '0')).strip() or 0)
        except: _pr = 0.0
        ped_line[k] = (clean_int(r.get('SOLICITADO', 0)), clean_int(r.get('DESPACHADO', 0)), _pr)
        if pid not in ped_info:
            ped_info[pid] = {"ciudad": r['CIUDAD'].strip(), "vendedor": r.get('VENDEDOR', '').strip()}
    fact_rut = {}   # rut -> [(fecha, numero, art8)]
    for r in venta_rows:
        if str(r.get('Tipo') or '').strip() != 'F/Elec':
            continue
        _d = parse_date(r.get('fecha') or '')
        if not _d:
            continue
        fact_rut.setdefault(_rut_norm(r.get('Rut')), []).append((_d, str(r.get('Numero') or '').strip(), str(r.get('Articulo') or '').strip()[:8]))
    cj = {}
    with open(cajas_file, encoding='latin-1') as f:
        for r in csv.DictReader(f, delimiter=';'):
            r = {(k or '').strip(): (v or '').strip() for k, v in r.items()}
            cid = r.get('Caja', '')
            if not cid:
                continue
            art10 = r.get('Articulo', ''); pid = r.get('Pedido', '')
            cant = clean_int(r.get('Cant', 0))
            if cid not in cj:
                cj[cid] = {
                    "caja": cid, "fecha": fmt_date(r.get('Fecha', '')), "fecha_iso": to_iso(r.get('Fecha', '')),
                    "dias": dias_desde(r.get('Fecha', '')), "pedido": pid, "rut": r.get('RUT', ''), "cliente": r.get('Cliente', ''),
                    "ciudad": ped_info.get(pid, {}).get('ciudad', ''), "vendedor": ped_info.get(pid, {}).get('vendedor', ''),
                    "pedido_existe": pid in ped_info,
                    "prendas": 0, "valor": 0, "sol": 0, "desp": 0, "temps": [], "lineas": []
                }
            c = cj[cid]
            sol, desp, precio = ped_line.get((pid, art10), (0, 0, 0.0))
            c['prendas'] += cant; c['valor'] += int(cant * precio); c['sol'] += sol; c['desp'] += desp
            t = art10[2:4]
            if t and t not in c['temps']:
                c['temps'].append(t)
            c['lineas'].append({"art": art10[:8], "talla": art10[8:10].lstrip('0') or art10[8:10], "cant": cant, "desp": desp})
    for c in cj.values():
        fd = parse_date(c['fecha'])
        arts = {l['art'] for l in c['lineas']}
        facts = sorted({(d.strftime('%Y-%m-%d'), nm) for d, nm, a in fact_rut.get(_rut_norm(c['rut']), []) if fd and d >= fd and a in arts})
        c['facturas'] = [nm for _, nm in facts]
        c['factura_fecha'] = facts[0][0] if facts else ''
        c['estado'] = 'bodega' if c['desp'] == 0 else ('facturada' if facts else 'despachada')
        c['lineas'].sort(key=lambda l: (l['art'], l['talla']))
    cajas = sorted(cj.values(), key=lambda c: (c['estado'] != 'bodega', -c['dias']))
    print(f"  CAJAS.TXT del {cajas_meta['archivo_fecha']}: {len(cajas)} cajas, {sum(1 for c in cajas if c['estado']=='bodega')} en bodega")
except FileNotFoundError:
    print("  CAJAS.TXT no encontrado en Z:\\BI")
cajas_out = {"meta": cajas_meta, "cajas": cajas}

# ── 7. FICHA CLIENTE (CLIENTE.Txt) + cajas/bloqueo por pedido ──
# CLIENTE.Txt: Credito (cupo), Ctacte (deuda), Cheques, Disponible = Credito - Ctacte - Cheques.
# Se cuelga de cada pedido como 'cli'; 'cajas' = cajas armadas del pedido; 'bloqueo' = alguna línea con BLOQUEO=S.
print("Leyendo CLIENTE.Txt...")
clientes_meta = {"archivo_fecha": ""}
clientes = {}
try:
    cli_file = bi_file('CLIENTE.Txt')
    clientes_meta["archivo_fecha"] = datetime.fromtimestamp(os.path.getmtime(cli_file)).strftime('%d/%m/%Y')
    # Layout: 23 columnas. El ERP exporta la 'Ñ' como ';' → algunas filas traen 24 columnas y los índices
    # fijos se corren. Por eso: RUT se busca por patrón en las primeras columnas, Fpago por patrón 'NN - ',
    # y Tipo..Disponible se leen desde la cola (las 4 últimas columnas Contacto/Fono/Mail/Observacion son fijas).
    _RUT_RE   = _re.compile(r'^\d{1,2}\.\d{3}\.\d{3}-[\dKk]$')
    _FPAGO_RE = _re.compile(r'^\d{2} - ')
    fichas = {}          # rut -> [ficha, ...]  (un RUT puede tener varias fichas: sucursales / razones sociales)
    descartadas = 0
    with open(cli_file, encoding='latin-1', errors='replace') as f:
        rd = csv.reader(f, delimiter=';')
        next(rd, None)
        for x in rd:
            if len(x) < 23:
                descartadas += 1; continue
            x = [c.strip() for c in x]
            rut_raw = next((c for c in x[:10] if _RUT_RE.match(c)), None)
            if not rut_raw:
                descartadas += 1; continue
            fpago = next((c for c in x[7:12] if _FPAGO_RE.match(c)), '')
            ficha = {
                "razon": x[0], "credito": clean_int(x[-8]), "deuda": clean_int(x[-7]), "cheques": clean_int(x[-6]),
                "disponible": clean_int(x[-5]), "fpago": fpago, "tipo": x[-9],
            }
            fichas.setdefault(_rut_norm(rut_raw).lstrip('0'), []).append(ficha)
    # Regla de merge por RUT: ficha principal = mayor crédito (empate → mayor deuda); se informa cuántas fichas hay
    for k, lst in fichas.items():
        lst.sort(key=lambda c: (c['credito'], c['deuda']), reverse=True)
        principal = dict(lst[0])
        principal['fichas'] = len(lst)
        principal['deuda_otras'] = sum(c['deuda'] for c in lst[1:])   # deuda en otras fichas del mismo RUT
        clientes[k] = principal
    print(f"  CLIENTE.Txt del {clientes_meta['archivo_fecha']}: {len(clientes)} clientes ({sum(len(v) for v in fichas.values())} fichas, {descartadas} filas descartadas)")
except FileNotFoundError:
    print("  CLIENTE.Txt no encontrado")

bloqueados = set()
for r in ped_rows:
    if r.get('BLOQUEO', '').strip().upper() == 'S':
        bloqueados.add(r['PEDIDO'].strip())

cajas_por_pedido = {}
for c in cajas:
    cajas_por_pedido.setdefault(c['pedido'], []).append(
        {"caja": c['caja'], "fecha": c['fecha'], "dias": c['dias'], "prendas": c['prendas'], "estado": c['estado']})

# PUBLICAR_CLI: la ficha de crédito/deuda va en pedidos.json. Desde 2026-09-08 los JSON se sirven
# desde Supabase Storage (bucket privado 'bi', solo usuarios logueados), por eso puede ir encendida.
# Si algún día los JSON vuelven a un sitio público, apagar esto.
PUBLICAR_CLI = True
for p in pedidos:
    p['cli']     = clientes.get(_rut_norm(p['rut']).lstrip('0')) if PUBLICAR_CLI else None
    p['bloqueo'] = p['pedido'] in bloqueados
    p['cajas']   = cajas_por_pedido.get(p['pedido'], [])

# ── Guardar ────────────────────────────────────────────────────
from datetime import datetime
NOW = datetime.now()
meta = {
    "updated_iso": NOW.strftime('%Y-%m-%dT%H:%M'),
    "updated_str": NOW.strftime('%d/%m/%Y %H:%M'),
    "updated_date": NOW.strftime('%d/%m'),
    "updated_time": NOW.strftime('%H:%M'),
    "clientes_fecha": clientes_meta.get("archivo_fecha", ""),
    "cajas_fecha": cajas_meta.get("archivo_fecha", ""),
}

DATASETS = [("full_table", full_table), ("traza_oc", traza_oc),
            ("pedidos", pedidos), ("docs_venta", docs_venta),
            ("pedidos_art", pedidos_art),
            ("saldos_bodega", saldos_bodega), ("pvc_ex", pvc_ex),
            ("cajas", cajas_out),
            ("meta", meta)]

for name, data in DATASETS:
    for dest in [OUT, DOCS_OUT]:
        os.makedirs(dest, exist_ok=True)
        path = os.path.join(dest, f"{name}.json")
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    n = len(data) if isinstance(data, list) else 1
    print(f"  {name}.json -> {n} registros")

print("Datos actualizados. Recarga el navegador.")
