#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MERIDIAN — Backend Trade Republic (v4)
- Login : géré nativement par pytr (comme la version qui fonctionnait).
- Données : UNE boucle asyncio dédiée PAR SESSION, réutilisée pour tous les
  appels websocket de cette session -> plus d'erreur "different loop".

  pip install pytr flask flask-cors
  gunicorn -w 1 --threads 4 --timeout 120 -b 0.0.0.0:$PORT meridian_tr_server:app
"""

import time, uuid, inspect, asyncio, threading

try:
    from flask import Flask, request, jsonify
    from flask_cors import CORS
except ImportError:
    raise SystemExit("Manque Flask. Lancez : pip install flask flask-cors")

try:
    try:
        from pytr.api import TradeRepublicApi
    except ImportError:
        from py_tr import TradeRepublicApi
except ImportError:
    raise SystemExit("Manque pytr. Lancez : pip install pytr")

app = Flask(__name__)
CORS(app)

SESSIONS = {}
SESSION_TTL = 60 * 30
_glock = threading.Lock()

TYPE_TO_CLS = {"stock": "Actions", "fund": "ETF", "etf": "ETF", "crypto": "Crypto",
               "bond": "Obligations", "derivative": "Actions", "warrant": "Actions"}


def _gc():
    now = time.time()
    with _glock:
        for k in [k for k, v in SESSIONS.items() if v["exp"] < now]:
            SESSIONS.pop(k, None)


def _method(tr, names):
    for n in names:
        m = getattr(tr, n, None)
        if m is not None:
            return m
    raise AttributeError("pytr: aucune methode parmi %s" % names)


def _run_on(s, coro):
    """Exécute une coroutine sur la boucle DÉDIÉE de la session (sérialisé)."""
    loop = s.get("loop")
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        s["loop"] = loop
    with s["lock"]:
        return loop.run_until_complete(coro)


def _maybe(s, x):
    return _run_on(s, x) if inspect.iscoroutine(x) else x


async def _topic_coro(tr, m, args):
    sid = await m(*args)
    resp = None
    try:
        for _ in range(120):
            r_sid, _sub, r = await tr.recv()
            if r_sid == sid:
                resp = r
                break
    finally:
        try:
            u = getattr(tr, "unsubscribe", None)
            if u is not None:
                await u(sid)
        except Exception:
            pass
    return resp


def _topic(s, names, *args):
    m = _method(s["tr"], names)
    return _run_on(s, _topic_coro(s["tr"], m, args))


def _num(*vals):
    for v in vals:
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            try:
                return float(str(v).replace(",", "."))
            except Exception:
                pass
    return 0.0


def _positions_from(pf):
    if isinstance(pf, list):
        return pf
    if isinstance(pf, dict):
        for k in ("positions", "holdings"):
            if isinstance(pf.get(k), list):
                return pf[k]
        for k in ("compactPortfolio", "portfolio"):
            v = pf.get(k)
            if isinstance(v, dict) and isinstance(v.get("positions"), list):
                return v["positions"]
    return []


def fetch_portfolio(s):
    dbg = {}
    positions = []
    for name in ("compact_portfolio", "portfolio"):
        try:
            pf = _topic(s, [name])
        except Exception as e:
            dbg[name + "_err"] = str(e)
            continue
        dbg[name + "_keys"] = list(pf.keys()) if isinstance(pf, dict) else type(pf).__name__
        positions = _positions_from(pf)
        if positions:
            dbg["used"] = name
            break

    cash = 0.0
    try:
        c = _topic(s, ["cash"])
        if isinstance(c, list):
            for x in c:
                cash += _num(x.get("amount"))
        elif isinstance(c, dict):
            cash = _num(c.get("amount"))
    except Exception as e:
        dbg["cash_err"] = str(e)

    out = []
    for p in positions:
        if not isinstance(p, dict):
            continue
        isin = p.get("instrumentId") or p.get("isin") or p.get("instrument")
        qty = _num(p.get("netSize"), p.get("netQuantity"), p.get("quantity"), p.get("size"))
        buy = _num(p.get("averageBuyIn"), p.get("avgPrice"), p.get("averagePrice"), p.get("buyIn"))
        price, name, typ = buy, (isin or "Position"), "stock"
        if isin:
            try:
                try: tick = _topic(s, ["ticker"], isin, "LSX")
                except TypeError: tick = _topic(s, ["ticker"], isin)
                if isinstance(tick, dict):
                    last = tick.get("last") or {}
                    price = _num(last.get("price"), (tick.get("bid") or {}).get("price")) or price
            except Exception:
                pass
            try:
                ins = _topic(s, ["instrument_details", "instrument"], isin)
                if isinstance(ins, dict):
                    name = ins.get("shortName") or ins.get("name") or isin
                    typ = ins.get("typeId") or ins.get("type") or "stock"
            except Exception:
                pass
        cls = TYPE_TO_CLS.get(str(typ).lower(), "Actions")
        if qty > 0 and (price > 0 or buy > 0):
            out.append({"n": name, "t": isin or name, "cls": cls, "q": round(qty, 6),
                        "p": round(price or buy, 4), "b": round(buy or price, 4), "ccy": "EUR"})

    res = {"positions": out, "cash": round(cash, 2), "source": "Trade Republic"}
    if not out:
        dbg["raw_count"] = len(positions)
        dbg["sample"] = positions[:2]
        res["debug"] = dbg
    return res


@app.post("/tr/login/start")
def login_start():
    _gc()
    d = request.get_json(force=True) or {}
    phone, pin = (d.get("phone") or "").strip(), (d.get("pin") or "").strip()
    if not phone or not pin:
        return jsonify(error="Numero et PIN requis"), 400
    s = {"lock": threading.Lock(), "loop": asyncio.new_event_loop(), "exp": time.time() + SESSION_TTL}
    try:
        s["tr"] = TradeRepublicApi(phone_no=phone, pin=pin, locale=d.get("locale", "fr"))
        _maybe(s, _method(s["tr"], ["inititate_weblogin", "initiate_weblogin"])())
    except Exception as e:
        return jsonify(error="Connexion refusee : %s" % (e or type(e).__name__)), 401
    token = uuid.uuid4().hex
    with _glock:
        SESSIONS[token] = s
    return jsonify(session=token)


@app.post("/tr/login/verify")
def login_verify():
    d = request.get_json(force=True) or {}
    s = SESSIONS.get(d.get("session", ""))
    if not s:
        return jsonify(error="Session expiree"), 440
    try:
        _maybe(s, _method(s["tr"], ["complete_weblogin"])((d.get("code") or "").strip()))
    except Exception as e:
        return jsonify(error="Code refuse : %s" % (e or type(e).__name__)), 401
    s["exp"] = time.time() + SESSION_TTL
    return jsonify(ok=True)


@app.get("/tr/portfolio")
def portfolio():
    s = SESSIONS.get(request.args.get("session", ""))
    if not s:
        return jsonify(error="Session expiree"), 440
    try:
        data = fetch_portfolio(s)
    except Exception as e:
        return jsonify(error="Lecture impossible : %s" % e), 502
    s["exp"] = time.time() + SESSION_TTL
    return jsonify(**data)


@app.get("/tr/raw")
def raw():
    s = SESSIONS.get(request.args.get("session", ""))
    if not s:
        return jsonify(error="Session expiree"), 440
    out = {}
    for n in ("compact_portfolio", "portfolio", "cash"):
        try:
            out[n] = _topic(s, [n])
        except Exception as e:
            out[n] = "ERR: %s" % e
    return jsonify(**out)


@app.post("/tr/logout")
def logout():
    d = request.get_json(force=True) or {}
    SESSIONS.pop(d.get("session", ""), None)
    return jsonify(ok=True)


@app.get("/")
def health():
    return jsonify(service="meridian-tr", status="ok", version=4, sessions=len(SESSIONS))


if __name__ == "__main__":
    print("Meridian — backend Trade Republic (v4) sur http://localhost:8765")
    app.run(host="0.0.0.0", port=8765, threaded=True)
