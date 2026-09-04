@echo off
setlocal

set PROJECT_DIR=c:\Users\Lenovo\Desktop\Backup\Data Manu\APIS\ADECOM WEB
set PYTHON="%PROJECT_DIR%\.venv\Scripts\python.exe"
set GIT="C:\Program Files\Git\cmd\git.exe"
set LOG="%PROJECT_DIR%\logs\auto_build.log"

cd /d "%PROJECT_DIR%"

echo [%date% %time%] Iniciando build automatico... >> %LOG%

:: Generar JSONs desde Z:\BI → dashboard-test/ y docs/
%PYTHON% "dashboard-test\actualizar_datos.py" >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%date% %time%] ERROR: actualizar_datos.py fallo con codigo %ERRORLEVEL% >> %LOG%
    exit /b 1
)

:: Copiar index.html del dashboard a docs/
copy /y "dashboard-test\index.html" "docs\index.html" >> %LOG% 2>&1

:: Agregar archivos modificados
%GIT% add docs\index.html docs\full_table.json docs\traza_oc.json docs\pedidos.json docs\docs_venta.json docs\pedidos_art.json docs\saldos_bodega.json docs\pvc_ex.json docs\cajas.json docs\meta.json docs\img >> %LOG% 2>&1

:: Si no hay cambios, salir sin error
%GIT% diff --cached --quiet
if %ERRORLEVEL% equ 0 (
    echo [%date% %time%] Sin cambios detectados, nada que publicar. >> %LOG%
    exit /b 0
)

:: Commit y push
%GIT% commit -m "Auto-actualizar web %date%  %time:~0,5%" >> %LOG% 2>&1
%GIT% push origin main >> %LOG% 2>&1

if %ERRORLEVEL% equ 0 (
    echo [%date% %time%] Publicacion exitosa. >> %LOG%
) else (
    echo [%date% %time%] ERROR: Push fallo con codigo %ERRORLEVEL% >> %LOG%
    exit /b 1
)

endlocal
