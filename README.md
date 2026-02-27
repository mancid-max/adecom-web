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

## Deploy Gratis (Render)

Archivos ya incluidos para deploy:

- `Procfile`
- `render.yaml`

Pasos:

1. Sube este proyecto a GitHub.
2. Entra a Render -> `New` -> `Blueprint`.
3. Conecta tu repo y selecciona este proyecto.
4. Render detecta `render.yaml` y publica automaticamente.

Nota: en plan gratis, la app puede dormir por inactividad y el almacenamiento local (SQLite) no es persistente a largo plazo. Para demo funciona bien; para produccion conviene migrar la BD a un servicio externo.

## Regla de actualizacion (upsert)

Se usa `CORTE` como clave de negocio principal:

- Si `CORTE` existe, se actualiza el registro
- Si `CORTE` no existe, se inserta

Nota: si en el futuro aparecen casos donde `CORTE` no sea unico globalmente,
se puede cambiar a clave compuesta (`ARTICULO + CORTE`) con un ajuste pequeño.
