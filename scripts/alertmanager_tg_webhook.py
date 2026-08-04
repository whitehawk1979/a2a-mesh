#!/usr/bin/env python3
"""A2A Mesh — Prometheus Alertmanager → Telegram webhook receiver.

Receives Prometheus alert notifications via webhook and forwards them
to Telegram using the hermes send CLI.

Runs as a small aiohttp server on port 9091 (Runa).
Prometheus Alertmanager sends alerts to http://localhost:9091/alerts

Setup:
  1. Run this script on the monitoring host (Runa)
  2. Configure Alertmanager to send webhooks to this endpoint
  3. Alerts are forwarded to Telegram via hermes send CLI

Usage:
  python3 alertmanager_tg_webhook.py [--port 9091] [--chat-id 7796035659]
"""
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("alertmanager_tg")

# ─── Config ───
PORT = int(os.environ.get("ALERT_WEBHOOK_PORT", "9091"))
CHAT_ID = os.environ.get("ALERT_TELEGRAM_CHAT_ID", "7796035659")
HERMES_SEND = os.environ.get("HERMES_SEND_PATH", "/usr/local/bin/hermes")
WEBHOOK_TOKEN = os.environ.get("ALERT_WEBHOOK_TOKEN", "a2a-mesh-alert-2026")

# Alert emoji mapping by severity
SEVERITY_EMOJI = {
    "critical": "🔴",
    "warning": "🟠",
    "info": "🔵",
}


def send_telegram(message: str) -> bool:
    """Send message via hermes send CLI."""
    try:
        result = subprocess.run(
            [HERMES_SEND, "send", "-t", f"telegram:{CHAT_ID}", message],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            log.info("Telegram message sent")
            return True
        else:
            log.error(f"hermes send failed: {result.stderr.strip()}")
            return False
    except FileNotFoundError:
        log.warning("hermes CLI not found, trying curl fallback")
        # Fallback: direct Telegram Bot API
        bot_token = os.environ.get("A2A_TELEGRAM_BOT_TOKEN", "")
        if not bot_token:
            log.error("No hermes CLI and no A2A_TELEGRAM_BOT_TOKEN — cannot send alert")
            return False
        import urllib.request
        import urllib.parse
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }).encode()
        try:
            req = urllib.request.Request(url, data=data)
            urllib.request.urlopen(req, timeout=10)
            return True
        except Exception as e:
            log.error(f"Telegram API fallback failed: {e}")
            return False
    except Exception as e:
        log.error(f"send_telegram error: {e}")
        return False


def format_alert(alert: dict) -> str:
    """Format a single Prometheus alert for Telegram."""
    status = alert.get("status", "firing").upper()
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})

    alertname = labels.get("alertname", "Unknown")
    severity = labels.get("severity", "info")
    node = labels.get("node", "?")
    emoji = SEVERITY_EMOJI.get(severity, "⚪")

    summary = annotations.get("summary", "No summary")
    description = annotations.get("description", "")

    # Status indicator
    if status == "FIRING":
        status_icon = "🔥"
    elif status == "RESOLVED":
        status_icon = "✅"
    else:
        status_icon = "📋"

    msg = f"{emoji} {status_icon} <b>A2A Mesh Alert</b>\n"
    msg += f"<b>{alertname}</b> — {status}\n"
    msg += f"Node: {node}\n"
    msg += f"Severity: {severity}\n"
    if summary:
        msg += f"Summary: {summary}\n"
    if description:
        msg += f"Description: {description}\n"
    msg += f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    return msg


async def handle_alerts(request):
    """Handle incoming Prometheus Alertmanager webhook."""
    from aiohttp import web
    # Auth check — Bearer token
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {WEBHOOK_TOKEN}":
        log.warning(f"Unauthorized webhook request from {request.remote}")
        return web.json_response({"error": "Unauthorized"}, status=401)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    alerts = data.get("alerts", [])
    if not alerts:
        return web.json_response({"status": "no alerts"}, status=200)

    log.info(f"Received {len(alerts)} alert(s) from Alertmanager")

    sent = 0
    for alert in alerts:
        # Only send FIRING alerts — skip resolved to reduce noise
        if alert.get("status") == "resolved":
            continue
        msg = format_alert(alert)
        if send_telegram(msg):
            sent += 1
        else:
            log.warning(f"Failed to send alert: {alert.get('labels', {}).get('alertname', '?')}")

    return web.json_response({"status": "ok", "sent": sent, "total": len(alerts)})


async def handle_health(request):
    """Health endpoint."""
    from aiohttp import web
    return web.json_response({"status": "running", "service": "alertmanager-telegram-webhook"})


def main():
    """Start the webhook server."""
    try:
        from aiohttp import web
    except ImportError:
        log.error("aiohttp not installed. Install with: pip install aiohttp")
        sys.exit(1)

    app = web.Application()
    app.router.add_post("/alerts", handle_alerts)
    app.router.add_get("/health", handle_health)

    log.info(f"Starting Alertmanager→Telegram webhook on port {PORT}")
    log.info(f"Telegram target: telegram:{CHAT_ID}")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)


if __name__ == "__main__":
    main()