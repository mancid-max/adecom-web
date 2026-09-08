"""
Sube los JSON del dashboard al bucket privado 'bi' de Supabase Storage.
Los usuarios logueados los leen desde el dashboard; sin login no hay acceso.

Uso: python scripts/subir_json_supabase.py          (lo llama auto_build.bat después de actualizar_datos.py)
Requiere supabase_config.json en la raíz (url, service_role_key) — no versionado.
"""
import os, json, sys, urllib.request, urllib.error

ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC    = os.path.join(ROOT, 'dashboard-test')
BUCKET = 'bi'
FILES  = ['full_table.json', 'traza_oc.json', 'pedidos.json', 'docs_venta.json',
          'pedidos_art.json', 'saldos_bodega.json', 'pvc_ex.json', 'cajas.json', 'meta.json']

def main():
    cfg = json.load(open(os.path.join(ROOT, 'supabase_config.json'), encoding='utf-8'))
    url, key = cfg['url'].rstrip('/'), cfg['service_role_key']
    ok, fail = 0, []
    for name in FILES:
        path = os.path.join(SRC, name)
        if not os.path.exists(path):
            fail.append((name, 'no existe')); continue
        data = open(path, 'rb').read()
        req = urllib.request.Request(
            f"{url}/storage/v1/object/{BUCKET}/{name}", data=data, method='POST',
            headers={'apikey': key, 'Authorization': f'Bearer {key}',
                     'Content-Type': 'application/json', 'x-upsert': 'true',
                     'Cache-Control': 'no-cache'})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                r.read(); ok += 1
                print(f"  {name}: {len(data)/1024:.0f} KB OK")
        except urllib.error.HTTPError as ex:
            fail.append((name, f"HTTP {ex.code} {ex.read()[:120].decode('utf-8', 'replace')}"))
        except Exception as ex:
            fail.append((name, str(ex)))
    print(f"Supabase Storage: {ok}/{len(FILES)} archivos subidos")
    if fail:
        for n, m in fail: print(f"  ERROR {n}: {m}")
        sys.exit(1)

if __name__ == '__main__':
    main()
