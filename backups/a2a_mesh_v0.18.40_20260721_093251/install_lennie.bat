@echo off
chcp 65001 >nul 2>&1
title A2A Mesh - Lennie telepítés
echo ============================================================
echo    A2A Mesh telepítés - Lennie (Windows)
echo ============================================================
echo.

:: Check Python
where python >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [HIBA] Python nincs telepítve!
    echo.
    echo Kérlek töltsd le és telepítsd:
    echo https://www.python.org/ftp/python/3.12.4/python-3.12.4-amd64.exe
    echo.
    echo Telepítésnél pipáljd be: "Add Python to PATH"
    echo.
    pause
    exit /b 1
)

python --version
echo.

:: Check Git
where git >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [HIBA] Git nincs telepítve!
    echo.
    echo Kérlek töltsd le és telepítsd:
    echo https://git-scm.com/download/win
    echo.
    pause
    exit /b 1
)

git --version
echo.

:: Create directory
if not exist "%USERPROFILE%\a2a_mesh" (
    echo [1/6] Klónozás a Gitea-ról...
    cd %USERPROFILE%
    git clone http://192.168.1.100:3001/nova/a2a-mesh.git a2a_mesh
) else (
    echo [1/6] A2A Mesh könyvtár már létezik, frissítés...
    cd %USERPROFILE%\a2a_mesh
    git fetch origin --tags --force
)

cd %USERPROFILE%\a2a_mesh
echo.

:: Checkout latest version
echo [2/6] Verzió váltás...
git checkout v0.18.28
echo.

:: Create virtual environment
if not exist ".venv" (
    echo [3/6] Virtual environment létrehozása...
    python -m venv .venv
) else (
    echo [3/6] Virtual environment már létezik.
)
echo.

:: Activate venv and install dependencies
echo [4/6] Csomagok telepítése...
call .venv\Scripts\activate.bat
pip install -r requirements.txt
echo.

:: Copy config
echo [5/6] Konfiguráció beállítása...
if not exist "mesh_config.yaml" (
    copy mesh_config_lennie.yaml mesh_config.yaml
    echo Lennie konfiguráció másolva.
) else (
    echo mesh_config.yaml már létezik.
)
echo.

:: Windows Firewall rules
echo [6/6] Windows Firewall szabályok...
netsh advfirewall firewall delete rule name="A2A Mesh P2P" >nul 2>&1
netsh advfirewall firewall delete rule name="A2A Mesh Health" >nul 2>&1
netsh advfirewall firewall add rule name="A2A Mesh P2P" dir=in action=allow protocol=TCP localport=8645
netsh advfirewall firewall add rule name="A2A Mesh Health" dir=in action=allow protocol=TCP localport=8650
echo Firewall szabályok hozzáadva.
echo.

echo ============================================================
echo    Telepítés kész!
echo ============================================================
echo.
echo Indításhoz futtasd:
echo   cd %USERPROFILE%\a2a_mesh
echo   .venv\Scripts\activate
echo   python cli.py start --name lennie --config mesh_config.yaml
echo.
echo Vagy használd a start_lennie.bat fájlt!
echo.
pause