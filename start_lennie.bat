@echo off
chcp 65001 >nul 2>&1
title A2A Mesh - Lennie indítás
echo A2A Mesh indítása (Lennie)...
cd %USERPROFILE%\a2a_mesh
call .venv\Scripts\activate.bat
python cli.py start --name lennie --config mesh_config.yaml
pause