#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=====================================================================
  MERIDIAN — Backend Trade Republic  (à HÉBERGER par vous)
=====================================================================

Ce service permet à VOS utilisateurs de connecter leur compte Trade
Republic depuis votre app, SANS rien installer. Le navigateur envoie
le numéro + PIN, le serveur déclenche le code 2FA de Trade Republic,
l'utilisateur saisit le code, et le serveur récupère le portefeuille.

Trade Republic n'ayant pas d'API officielle, ce service s'appuie sur
la librairie open-source `pytr` (non affiliée à Trade Republic).

---------------------------------------------------------------------
INSTALLATION
    pip install pytr flask flask-cors

LANCEMENT (local, pour tester)
    python meridian_tr_server.py
    -> écoute sur http://localhost:8765

ENDPOINTS
    POST /tr/login/start    {phone, pin, locale}     -> {session, countdown}
    POST /tr/login/verify   {session, code}          -> {ok}
    GET  /tr/portfolio?session=...                    -> {positions, cash}
    POST /tr/logout         {session}                 -> {ok}

DÉPLOIEMENT (production) — voir notes en bas de fichier.
---------------------------------------------------------------------
SÉCURITÉ / CONFORMITÉ — à lire avant d'ouvrir à de vrais utilisateurs :
  * Les identifiants (PIN) ne sont JAMAIS écrits sur disque : ils ne
    vivent qu'en mémoire, le temps de la session, dans l'objet pytr.
  * Servez TOUJOURS derrière HTTPS en production (reverse proxy).
  * Ajoutez une politique de confidentialité + le consentement RGPD,
    et vérifiez les CGU de Trade Republic avant un usage commercial.
  * Limitez le débit (rate-limit) et l'origine CORS à votre domaine.
=====================================================================
"""

import time, uuid, inspect, asyncio, threading

try:
    from flask import Flask, request, jsonify
    from flask_cors import CORS
except ImportError:
    raise SystemExit("Manque Flask. Lancez :  pip install flask flask-cors")

try:
    # pytr-org/pytr expose la classe ici ; certains forks utilisent py_tr
    try:
        from pytr.api import TradeRepublicApi
    except ImportError:
        from py_tr import TradeRepublicApi
except ImportError:
    raise SystemExit("Manque pytr. Lancez :  pip install pytr")

app = Flask(__name__)
CORS(app)  # En production : CORS(app, origins=["https://votre-domaine.com"])

# session_token -> {"tr": TradeRepublicApi, "exp": ts}
SESSIONS = {}
SESSION_TTL = 60 * 30  # 30 min
_lock = threading.Lock()

TYPE_TO_CLS = {"stock": "Actions", "fund": "ETF", "etf": "ETF",
               "crypto": "Crypto", "bond": "Obligations", "derivative": "Actions"}


def _gc():
    now = time.time()
    with _lock:
        for k in [k for k, v in SESSIONS.items() if v["exp"] < now]:
            SESSIONS.pop(k, None)


def _maybe(x):
    """Exécute le résultat s'il s'agit d'une coroutine (login sync OU async)."""
    if inspect.iscoroutine(x):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(x)
        finally:
            loop.close()
    return x


def _first_method(tr, names):
    for n in names:
        m = getattr(tr, n, None)
        if m is not None:
            return m
    raise AttributeError("Méthode pytr introuvable parmi : %s" % names)


def _topic(tr, names, *args):
    """Récupère la 1re réponse d'un topic websocket, de façon bloquante."""
    m = _first_method(tr, names)
    coro = m(*args)
    rb = getattr(tr, "run_blocking", None)
    if rb is not None:
        return rb(coro, timeout=8.0)
    # Fallback (versions sans run_blocking) : boucle recv manuelle
    async def _run():
        sid = await coro
        for _ in range(60):
            r_sid, _sub, resp = await tr.recv()
            if r_sid == sid:
                return resp
        return None
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_run())
    finally:
        loop.close()


def fetch_portfolio(tr):
    pf = _topic(tr, ["compact_portfolio", "portfolio"])
    cash_resp = _topic(tr, ["cash"])
    positions = pf.get("positions", []) if isinstance(pf, dict) else (pf or [])
    cash = 0.0
    if isinstance(cash_resp, list):
        for c in cash_resp:
            try: cash += float(c.get("amount", 0) or 0)
            except Exception: pass

    out = []
    for p in positions:
        isin = p.get("instrumentId") or p.get("isin") or p.get("instrument")
        if not isin:
            continue
        qty = float(p.get("netSize") or p.get("netQuantity") or p.get("quantity") or 0)
        try: buy = float(p.get("averageBuyIn") or p.get("avgPrice") or 0)
        except Exception: buy = 0.0
        price, name, typ = buy, isin, "stock"
        try:
            try:    tick = _topic(tr, ["ticker"], isin, "LSX")
            except TypeError: tick = _topic(tr, ["ticker"], isin)
            if isinstance(tick, dict):
                last = tick.get("last") or {}
                price = float(last.get("price") or (tick.get("bid") or {}).get("price") or price)
        except Exception:
            pass
        try:
            ins = _topic(tr, ["instrument_details", "instrument"], isin)
            if isinstance(ins, dict):
                name = ins.get("shortName") or ins.get("name") or name
                typ = ins.get("typeId") or ins.get("type") or typ
        except Exception:
            pass
        cls = TYPE_TO_CLS.get(str(typ).lower(), "Actions")
        if qty > 0 and price > 0:
            out.append({"n": name, "t": isin, "cls": cls, "q": round(qty, 6),
                        "p": round(price, 4), "b": round(buy or price, 4), "ccy": "EUR"})
    return {"positions": out, "cash": round(cash, 2), "source": "Trade Republic"}


@app.post("/tr/login/start")
def login_start():
    _gc()
    d = request.get_json(force=True) or {}
    phone, pin = (d.get("phone") or "").strip(), (d.get("pin") or "").strip()
    if not phone or not pin:
        return jsonify(error="Numéro et PIN requis"), 400
    try:
        tr = TradeRepublicApi(phone_no=phone, pin=pin, locale=d.get("locale", "fr"))
        countdown = _maybe(_first_method(tr, ["inititate_weblogin", "initiate_weblogin"])())
    except Exception as e:
        return jsonify(error="Connexion refusée : %s" % e), 401
    token = uuid.uuid4().hex
    with _lock:
        SESSIONS[token] = {"tr": tr, "exp": time.time() + SESSION_TTL}
    return jsonify(session=token, countdown=countdown)


@app.post("/tr/login/verify")
def login_verify():
    d = request.get_json(force=True) or {}
    s = SESSIONS.get(d.get("session", ""))
    if not s:
        return jsonify(error="Session expirée, recommencez"), 440
    code = (d.get("code") or "").strip()
    try:
        _maybe(_first_method(s["tr"], ["complete_weblogin"])(code))
    except Exception as e:
        return jsonify(error="Code refusé : %s" % e), 401
    s["exp"] = time.time() + SESSION_TTL
    return jsonify(ok=True)


@app.get("/tr/portfolio")
def portfolio():
    s = SESSIONS.get(request.args.get("session", ""))
    if not s:
        return jsonify(error="Session expirée, reconnectez-vous"), 440
    try:
        data = fetch_portfolio(s["tr"])
    except Exception as e:
        return jsonify(error="Lecture du portefeuille impossible : %s" % e), 502
    s["exp"] = time.time() + SESSION_TTL
    return jsonify(**data)


@app.post("/tr/logout")
def logout():
    d = request.get_json(force=True) or {}
    SESSIONS.pop(d.get("session", ""), None)
    return jsonify(ok=True)


@app.get("/")
def health():
    return jsonify(service="meridian-tr", status="ok", sessions=len(SESSIONS))


if __name__ == "__main__":
    print("Meridian — backend Trade Republic sur http://localhost:8765")
    app.run(host="0.0.0.0", port=8765, threaded=True)

# =====================================================================
# DÉPLOIEMENT EN PRODUCTION (résumé)
# ---------------------------------------------------------------------
#  1) Mettez ce fichier + `pip install pytr flask flask-cors gunicorn`
#     sur un petit serveur (Render, Railway, Fly.io, ou un VPS).
#  2) Lancez derrière HTTPS, ex. :  gunicorn -w 2 -b 0.0.0.0:8765 meridian_tr_server:app
#  3) Restreignez CORS à votre domaine : CORS(app, origins=["https://votre-app.com"]).
#  4) Dans Meridian (onglet « Serveur »), indiquez l'URL HTTPS de ce service.
#  5) Pour de vrais utilisateurs : ajoutez consentement RGPD + politique de
#     confidentialité, et vérifiez les CGU de Trade Republic.
# =====================================================================
