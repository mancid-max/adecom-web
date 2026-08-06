@echo off
setlocal

set PROJECT_DIR=c:\Users\Lenovo\Desktop\Backup\Data Manu\APIS\ADECOM WEB
set PYTHON="%PROJECT_DIR%\.venv\Scripts\python.exe"
set GIT="C:\Program Files\Git\cmd\git.exe"
set LOG="%PROJECT_DIR%\logs\auto_build.log"
set BI_DIR=Z:\BI
set SEED_DIR=%PROJECT_DIR%\seed
set ONEDRIVE_INV=C:\Users\Lenovo\OneDrive - Mohicano Jeans\INVENTARIO 01-04 COMPLETO.xlsx

cd /d "%PROJECT_DIR%"

echo [%date% %time%] Iniciando sincronizacion de datos... >> %LOG%

:: Sincronizar archivos de Z:\BI a seed/
if exist "%BI_DIR%\VENTAS-TOD-2026.CSV" (
    xcopy /Y /Q "%BI_DIR%\VENTAS-TOD-2026.CSV" "%SEED_DIR%\VENTAS-TOD-2026.CSV*" >> %LOG% 2>&1
    echo [%date% %time%] Sync: VENTAS-TOD-2026.CSV >> %LOG%
)
if exist "%BI_DIR%\TRAZABILIDAD2.CSV" (
    xcopy /Y /Q "%BI_DIR%\TRAZABILIDAD2.CSV" "%SEED_DIR%\TRAZABILIDAD_OP.TXT*" >> %LOG% 2>&1
    echo [%date% %time%] Sync: TRAZABILIDAD2.CSV >> %LOG%
)
if exist "%BI_DIR%\PEDIDOS.CSV" (
    xcopy /Y /Q "%BI_DIR%\PEDIDOS.CSV" "%SEED_DIR%\PEDIDOS.Txt*" >> %LOG% 2>&1
    echo [%date% %time%] Sync: PEDIDOS.CSV >> %LOG%
)
if exist "%ONEDRIVE_INV%" (
    xcopy /Y /Q "%ONEDRIVE_INV%" "%SEED_DIR%\INVENTARIO 01-04 COMPLETO.xlsx*" >> %LOG% 2>&1
    echo [%date% %time%] Sync: INVENTARIO >> %LOG%
)

:: Agregar seed files modificados
%GIT% add seed/VENTAS-TOD-2026.CSV seed/TRAZABILIDAD_OP.TXT seed/PEDIDOS.Txt "seed/INVENTARIO 01-04 COMPLETO.xlsx" >> %LOG% 2>&1

:: Si no hay cambios, salir sin error
%GIT% diff --cached --quiet
if %ERRORLEVEL% equ 0 (
    echo [%date% %time%] Sin cambios detectados, nada que publicar. >> %LOG%
    exit /b 0
)

:: Commit y push - Render auto-despliega al recibir el push
%GIT% commit -m "Auto-actualizar web %date%  %time:~0,5%" >> %LOG% 2>&1
%GIT% push origin main >> %LOG% 2>&1

if %ERRORLEVEL% equ 0 (
    echo [%date% %time%] Datos publicados. Render desplegara en ~1 minuto. >> %LOG%
) else (
    echo [%date% %time%] ERROR: Push fallo con codigo %ERRORLEVEL% >> %LOG%
    exit /b 1
)

endlocal
