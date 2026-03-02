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

## Render (actual)

Tu proyecto ya incluye archivos para Render (`Procfile` y `render.yaml`), pero Render Free suspende el servicio tras 15 minutos sin trafico y el filesystem es efimero.

## Regla de actualizacion (upsert)

Se usa `CORTE` como clave de negocio principal:

- Si `CORTE` existe, se actualiza el registro
- Si `CORTE` no existe, se inserta

Nota: si en el futuro aparecen casos donde `CORTE` no sea unico globalmente,
se puede cambiar a clave compuesta (`ARTICULO + CORTE`) con un ajuste pequeno.
