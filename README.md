# ADECOM WEB - Saldos por Seccion

Aplicacion web simple para:

- Subir archivos `SALDOS-SECCI.TXT` (y opcionalmente `.xlsx`)
- Actualizar datos por `orden de corte` (`CORTE`)
- Ver la informacion en una tabla web con filtros
- Exportar resultados a CSV

## Requisitos

- Python 3.10+ (probado con Python 3.14)

## Instalacion

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Ejecutar

```powershell
python app.py
```

Luego abrir `http://127.0.0.1:5000`

## Trabajar con PostgreSQL (Railway)

La app ya soporta Postgres por `DATABASE_URL`.

1. Crea una base PostgreSQL (Railway, Supabase o Neon) y copia su `DATABASE_URL`.
2. Define variables:

```text
DATABASE_URL=postgresql://...
ADECOM_ENABLE_SEED=0
```

3. Si ya tienes data local en `data/adecom.db`, migrala:

```powershell
python .\scripts\migrate_sqlite_to_postgres.py --database-url "postgresql://..."
```

4. Inicia la app normalmente:

```powershell
python app.py
```

Con eso, toda lectura/escritura queda en PostgreSQL.

## Deploy recomendado (gratis y con almacenamiento): PythonAnywhere

Si necesitas algo gratis que no se suspenda tan rapido como Render y que guarde tu SQLite, esta opcion suele funcionar mejor para este proyecto.

Limites actuales del plan gratis (referencia 2026):

- 1 web app
- 512 MiB de disco
- expiracion si no usas la cuenta por 1 mes

Pasos:

1. Sube este proyecto a GitHub.
2. Crea una cuenta gratis en PythonAnywhere.
3. Abre una consola Bash en PythonAnywhere y clona tu repo:

```bash
git clone <TU_REPO_GITHUB>
cd "ADECOM WEB"
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

4. En la pestana `Web`, crea una nueva web app (manual config, Flask/Python 3.12).
5. En el archivo WSGI de PythonAnywhere, deja esto:

```python
import sys
path = "/home/TU_USUARIO/ADECOM WEB"
if path not in sys.path:
    sys.path.append(path)

from app import app as application
```

6. En `Working directory`, usa:

```text
/home/TU_USUARIO/ADECOM WEB
```

7. Haz `Reload` en la web app.

Con esto, `data/adecom.db` queda en disco persistente dentro de tu cuenta.

## Para que siempre quede la ultima carga

Define estas variables en produccion:

- `ADECOM_ENABLE_SEED=0` (evita volver a datos semilla al reiniciar)
- `ADECOM_DB_PATH=/ruta/persistente/adecom.db` (BD en disco persistente)

Ejemplo en PythonAnywhere:

```text
ADECOM_ENABLE_SEED=0
ADECOM_DB_PATH=/home/TU_USUARIO/ADECOM WEB/data/adecom.db
```

Con esto, la ultima carga se mantiene tras reinicios.

## Ocultar carga para solo lectura (opcional)

Si quieres que solo administradores puedan subir archivos:

- `ADECOM_ADMIN_KEY=tu_clave_segura`

Con esa variable activa, la web queda en modo lectura para todos y solo quien ingrese la clave en `Entrar modo carga` vera los botones de importacion.

## Landing separada para otro usuario

Puedes habilitar una landing independiente con una clave propia:

```text
ADECOM_ENABLE_OTHER_SECTION=1
ADECOM_ACCESS_KEY_OTHER=tu_clave_segura
```

- Login: usa solo `clave` para este acceso.
- Ruta de destino: `/otra-landing`.
- El acceso principal ADECOM WEB sigue funcionando solo con clave.

## Asistente con Gemini (opcional)

Por defecto el asistente usa logica local de la app. Si quieres usar Gemini:

- `ADECOM_ASSISTANT_PROVIDER=gemini`
- `GEMINI_API_KEY=tu_api_key`
- `GEMINI_MODEL=gemini-2.5-flash` (opcional)
- `GEMINI_API_VERSION=v1` (opcional, recomendado)

Si Gemini falla o no responde, el sistema vuelve automaticamente al asistente local.

## Opcion alternativa: Koyeb (gratis)

Koyeb tambien es una buena alternativa. En free tier suele escalar a cero despues de inactividad (mas tolerante que Render), y despierta al recibir trafico.

Pasos rapidos:

1. `Create Web Service` en Koyeb.
2. Conecta tu repo de GitHub.
3. Builder: `Buildpack`.
4. Run command:

```bash
gunicorn app:app
```

5. Deploy.

## Auto deploy gratis: Koyeb + Neon (Postgres persistente)

Si quieres deploy automatico en cada `git push` y datos persistentes:

1. Crea una BD gratis en Neon y copia `DATABASE_URL`.
2. En Koyeb crea un `Web Service` desde este repo (rama `main`).
3. En variables de entorno del servicio configura:

```text
DATABASE_URL=postgresql://...
ADECOM_ENABLE_SEED=0
```

4. Comando de inicio:

```bash
gunicorn app:app
```

Desde este punto, cada push a `main` dispara redeploy y la data queda persistente en Postgres.

## Render (actual)

Tu proyecto ya incluye archivos para Render (`Procfile` y `render.yaml`), pero Render Free suspende el servicio tras 15 minutos sin trafico y el filesystem es efimero.

## Recomendado ahora: Railway + Supabase

Para evitar lentitud/costos de Render y mantener datos persistentes:

1. Crea proyecto en Supabase y copia `DATABASE_URL` (pooler).
2. En Railway crea un servicio desde este repo (`main`).
3. En Railway define variables:

```text
DATABASE_URL=postgresql://...
ADECOM_ENABLE_SEED=0
ADECOM_ADMIN_KEY=tu_clave
ADECOM_ASSISTANT_PROVIDER=gemini
GEMINI_API_KEY=...
GEMINI_MODEL=gemini-2.5-flash
GEMINI_API_VERSION=v1
ADECOM_AUTOLOAD_SALDOS_SOURCE=https://raw.githubusercontent.com/mancid-max/adecom-web/main/seed/SALDOS-SECCI.TXT
ADECOM_AUTOLOAD_PEDIDOS_SOURCE=https://raw.githubusercontent.com/mancid-max/adecom-web/main/seed/PEDIDOSXTALLA.TXT
ADECOM_AUTOLOAD_ETAPAS_SOURCE=https://raw.githubusercontent.com/mancid-max/adecom-web/main/seed/Grande-Adecom.TXT
```

4. Start command:

```text
gunicorn app:app
```

5. Deploy.

Luego de desplegar, usa el boton `Actualizar data web` para poblar/actualizar la BD de Supabase.

## Regla de actualizacion (upsert)

Se usa `CORTE` como clave de negocio principal:

- Si `CORTE` existe, se actualiza el registro
- Si `CORTE` no existe, se inserta

Nota: si en el futuro aparecen casos donde `CORTE` no sea unico globalmente,
se puede cambiar a clave compuesta (`ARTICULO + CORTE`) con un ajuste pequeno.

## Auto-sync carpeta local a Git (Windows)

Si quieres que todo lo que cambies en:

`C:\Users\manuh\Desktop\APIS\Documentos a cargar ADECOM WEB`

se copie solo a `seed/` y haga push automatico a GitHub:

1. Ejecutar una sincronizacion manual:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\sync_data_to_git.ps1
```

2. Instalar tarea programada (se inicia al entrar a Windows):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_sync_task.ps1
```

Archivos que sincroniza:

- `SALDOS-SECCI*` -> `seed/SALDOS-SECCI.TXT`
- `PEDIDOSXTALLA*` (excluye `TODAS`) -> `seed/PEDIDOSXTALLA.TXT`
- `Grande-Adecom*` -> `seed/Grande-Adecom.TXT`

Cuando detecta cambios, ejecuta:

- `git add`
- `git commit`
- `git push origin main`
