---
name: git-workflow
description: Git műveletek: commit, push, branch management, Gitea integration. A2A mesh repo automation.
tags: [git, gitea, version-control, commit, push]
---

# Git Workflow

A2A mesh repo kezelése és Gitea integráció.

## Gitea hozzáférés
- URL: http://192.168.1.100:3001
- User: zsolt, pw: admin1234
- SSH: gitea-ssh, port 2222

## Standard commit flow
1. `git add -A`
2. `git commit -m "feat/fix/chore: description"`
3. `git push origin main`

## Auto-deploy
- Gitea webhook id=4 → POST /api/webhook/deploy
- auto_deploy.py: git pull + SCP + restart + health check (35s boot)
- Push után automatikusan deployol minden node-ra

## Szabályok
- MINDIG commit+push fejlesztés után (Zsolt kérése)
- Soha ne rewrite history
- Soha ne commitolj secrets (.env, credentials)
- Branch: main → gitea/main
