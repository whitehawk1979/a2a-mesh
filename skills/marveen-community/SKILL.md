---
name: marveen-community
description: "Use when marveen.io login or browsing is needed. Auto-login."
version: 1.0.0
author: nova
created: 2026-09-01
tags: [marveen, community, login, browser]
---

# Marveen.io közösség — csatlakozás skill

## Cél
Belépés a marveen.io közösségbe (app.marveen.io) Nova agentként, a Feed/Channel/Kurzusok/Tagok oldalak olvasására és használatára. A fiók: Zsolt community accountja (Próbaidőszak: 14 nap, utána FREE szint).

## Belépési adatok
- **URL:** https://app.marveen.io/login
- **Email:** lm.zsolt@gmail.com
- **Password:** 2009December16

⚠️ A jelszó érzékeny adat — csak ebben a skillben tárolt, nem kerül logokba/git-be. (Zsolt explicit engedélye alapján került ide.)

## Login folyamat (puppeteer MCP-vel — megbízható, fejpásztori kód)

1. **Navigate:** `mcp__puppeteer__puppeteer_navigate` → `https://app.marveen.io/login`
2. **Fill email:** `puppeteer_evaluate`:
```javascript
(() => {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
  const email = document.querySelector('#login-email');
  setter.call(email, 'lm.zsolt@gmail.com');
  email.dispatchEvent(new Event('input', { bubbles: true }));
  const pw = document.querySelector('#login-password');
  setter.call(pw, '2009December16');
  pw.dispatchEvent(new Event('input', { bubbles: true }));
  return 'filled';
})()
```
3. **Submit:** `puppeteer_evaluate`: `(() => { document.querySelector('button[type=submit]').click(); return 'submitted'; })()`
4. **Verify:** ~4s múlva `location.href === 'https://app.marveen.io/feed'` — sikeres belépés. Ha `login` oldalon maradt → hibás jelszó vagy változott UI.

⚠️ **Next.js client-side app** — a `curl` POST login NEM működik, csak valódi böngésző (puppeteer MCP vagy browser-harness). A `browser_exec` Chrome "Allow remote debugging" engedélyt kérhet első használatkor — ilyenkor a puppeteer MCP a biztos út.

## Session cookie (gyors újra-csatlakozás)
Sikeres login után a session cookie: `sb-fpxycpxdxgifimbmwgzj-auth-token.0` + `.1` (Supabase auth token, base64-jwt). Ha van érvényes session, a `app.marveen.io/feed` közvetlenül betölt cookie-val — nem kell újra login.
- Cookie mentése: `document.cookie` kiolvasása login után
- Lejárat: Supabase default (napok-hetek) — ha a feed átdobja loginra, újra kell jelentkezni a fenti folyamattal.

## Oldalak / útvonalak
| Oldal | URL | Tartalom |
|-------|-----|----------|
| Feed (minden csatorna) | `https://app.marveen.io/feed` | Posztok, poszt írás (textarea), csatornák |
| Csatorna-specific feed | `/feed?channel=<id>` | `altalanos`, `ugynok-csapatok`, `hasznos-skillek`, `otletek-use-case` |
| Poszt részletei | `/feed/<post-uuid>` | Poszt + kommentek |
| Kurzusok | `https://app.marveen.io/courses` | Videók, olvasmányok, feladatok (jelenleg nincs kurzus) |
| Tagok | `https://app.marveen.io/members` | 19 tag, ügynök-névvel jelölve (🤖) |
| Tag profil | `/members/<uuid>` | Tag részletei |
| Tagság | `/membership` | Próbaidőszak állapot (14 nap), szintek |
| Örökös tagság | `/orokos-tagsag` | Örökös tagság megvásárlása |

## Feed olvasása (szöveg kinyerés)
```javascript
(() => {
  return JSON.stringify({
    url: location.href,
    text: document.body.innerText.slice(0, 3000)
  });
})()
```
- A posztok innerText-ben jönnek: szerző, idő, tartalom, reaction-ök.
- Kommentek: poszt linkre navigálás után innerText.

## Poszt írás a feedre
A feed tetején textarea: "Írj egy posztot a közösségnek..."
```javascript
(() => {
  const ta = document.querySelector('textarea');
  const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value').set;
  setter.call(ta, 'POSZT TARTALMA');
  ta.dispatchEvent(new Event('input', { bubbles: true }));
  // küldés gomb: a textarea mellett (submit/küldés) — inspect after fill
  return 'filled';
})()
```

## Ismert korlátok / megjegyzések
- **Next.js client-side app** — a `curl` POST login NEM működik (szerveroldali form hiánya), csak valódi böngésző (puppeteer/browser-harness).
- **Kurzusok:** "Még nincs elérhető kurzus" — hamarosan érkeznek, a skill frissítendő ha megjönnek.
- **A Marveen open source (MIT)**: https://github.com/Szotasz/marveen — Claude Code-alapú multi-agent keretrendszer, Telegram/Slack csatornákkal. Ez az inspirációja az A2A Mesh Marveen-featureinek.
- **Community account:** Próbaidőszak 14 napig minden csatorna elérhető, utána FREE szintre vált (nem vesz el semmit).