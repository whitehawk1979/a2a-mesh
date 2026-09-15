# A2A Mesh — Windows Telepítési Útmutató

## Gyors telepítés (Windows)

### 1. Python telepítése
```powershell
# winget (Windows Package Manager) — Windows 10/11
winget install Python.Python.3.12

# Vagy manuálisan: https://www.python.org/downloads/
# ⚠️ Pipelnáld be az "Add Python to PATH" opciót!
```

### 2. Git clone
```powershell
cd C:\
git clone http://192.168.1.100:3001/nova/a2a-mesh.git
cd a2a-mesh
```

### 3. Auto-Bootstrap
```powershell
# Automatikus konfiguráció — generálja a config-ot, TLS cert-et, service-t
python bootstrap_cli.py --name lennie --pg-host 192.168.1.30

# Vagy csak detektálás (dry run)
python bootstrap_cli.py --name lennie --detect-only
```

### 4. Indítás
```powershell
# Venv létrehozása (auto a bootstrap során, de manuálisan is):
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Node indítása:
python cli.py start --name lennie --config mesh_config_lennie.yaml
```

### 5. Auto-start (Task Scheduler)
```powershell
# A bootstrap automatikusan telepíti, de manuálisan is:
schtasks /Create /TN "A2A-Mesh-lennie" /TR "wscript.exe C:\a2a_mesh\start_lennie.vbs" /SC ONLOGON /RL HIGHEST /F

# Eltávolítás:
schtasks /Delete /TN "A2A-Mesh-lennie" /F
```

### 6. Ellenőrzés
```powershell
# Health check
curl http://localhost:8650/health

# Dashboard
start http://localhost:8650/dashboard
```

## Windows-specifikus beállítások

### Firewall
```powershell
# P2P port (8645) megnyitása
netsh advfirewall firewall add rule name="A2A Mesh P2P" dir=in action=allow protocol=TCP localport=8645

# Health port (8650) megnyitása
netsh advfirewall firewall add rule name="A2A Mesh Health" dir=in action=allow protocol=TCP localport=8650
```

### Tailscale (opcionális)
```powershell
winget install Tailscale.Tailscale
tailscale up
```

### Service wrapper (PowerShell)
```powershell
# Háttérben futó service wrapper — auto-restart
powershell -ExecutionPolicy Bypass -File C:\a2a_mesh\service_lennie.ps1
```

## Hibaelhárítás

### "Python not found"
- Telepítsd a Python 3.10+-t: `winget install Python.Python.3.12`
- Ellenőrizd: `python --version`

### "pip install fails"
- Próbáld: `python -m pip install --upgrade pip`
- Vagy: `python -m pip install --user <package>`

### "Port already in use"
- A bootstrap automatikusan keres szabad portot
- Manuálisan: `netstat -ano | findstr :8645`

### "Cannot connect to PG"
- Ellenőrizd a PG elérhetőségét: `telnet 192.168.1.30 5432`
- VPN/Tailscale szükséges lehet

### Node nem csatlakozik a mesh-hez
- Firewall: nyisd meg a P2P portot
- Ellenőrizd a config-ot: `type mesh_config_lennie.yaml`
- Nézd a log-ot: `type C:\a2a_mesh\logs\lennie.log`