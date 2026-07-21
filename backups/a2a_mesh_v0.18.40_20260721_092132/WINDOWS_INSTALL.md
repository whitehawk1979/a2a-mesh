# A2A Mesh — Windows Telepítési Útmutató (Lennie)

## Előfeltételek
- Python 3.10+ telepítve (https://www.python.org/downloads/)
- Git telepítve (https://git-scm.com/download/win)
- pip és venv elérhető

## 1. Repo klónozása
```cmd
cd %USERPROFILE%
git clone http://192.168.1.100:3001/nova/a2a-mesh.git a2a_mesh
cd a2a_mesh
git checkout v0.18.23
```

## 2. Virtual environment létrehozása
```cmd
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Config fájl létrehozása
Másold a `mesh_config.yaml` fájlt a repo gyökérbe (a Nova gépen):
```cmd
copy mesh_config.yaml mesh_config.yaml.bak
```

A configban állítsd be:
- `node_name: lennie`
- `p2p_port: 8645`
- `health_port: 8650`

## 4. Indítás
```cmd
.venv\Scripts\activate
python cli.py start --name lennie --config mesh_config.yaml
```

## 5. Tűzfal beállítások
Windows Defender Firewall → Inbound Rules:
- TCP 8645 (P2P)
- TCP 8650 (Health/Dashboard)

## Megjegyzések
- A `psycopg2-binary` Windows-on működik
- A `zeroconf` Windows-on is támogatott (mDNS discovery)
- A BLE transport Windows-on nem elérhető (hiba esetén auto-disable)