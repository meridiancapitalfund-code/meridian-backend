#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MERIDIAN — Backend Trade Republic (v2, lecture de portefeuille renforcée)
À héberger par vous.
  pip install pytr flask flask-cors
  python meridian_tr_server.py        (local)
  gunicorn -w 1 --threads 4 --timeout 120 -b 0.0.0.0:$PORT meridian_tr_server:app
"""

import time, uuid, inspect, asyncio, threading

try:
    from flask import Flask, request, jsonify
    from flask_cors import CORS
except ImportError:
    raise SystemExit("Manque Flask. Lancez :  pip install flask flask-cors")

try:
    try:
        from pytr.api import TradeRepublicApi
    except ImportError:
        from py_tr import TradeRepublicApi
except ImportError:
    raise SystemExit("Manque pytr. Lancez :  pip install pytr")

app = Flask(__name__)
CORS(app)

SESSIONS = {}
SESSION_TTL = 60 * 30
_lock = threading.Lock()

TYPE_TO_CLS = {"stock": "Actions", "fund": "ETF", "etf": "ETF", "crypto": "Crypto",
               "bond": "Obligations", "derivative": "Actions", "warrant": "Actions"}


def _gc():
    now = time.time()
    with _lock:
        for k in [k for k, v in SESSIONS.items() if v["exp"] < now]:
            SESSIONS.pop(k, None)


def _maybe(x):
    if inspect.iscoroutine(x):
        loop = asyncio.new_event_loop()
        try: return loop.run_until_complete(x)
        finally: loop.close()
    return x


def _method(tr, names):
    for n in names:
        m = getattr(tr, n, None)
        if m is not None:
            return m
    raise AttributeError("pytr: aucune methode parmi %s" % names)


def _topic(tr, names, *args):
    m = _method(tr, names)
    coro = m(*args)
    rb = getattr(tr, "run_blocking", None)
    if rb is not None:
        return rb(coro, timeout=10.0)
    async def _run():
        sid = await coro
        for _ in range(80):
            r_sid, _sub, resp = await tr.recv()
            if r_sid == sid:
                return resp
        return None
    loop = asyncio.new_event_loop()
    try: return loop.run_until_complete(_run())
    finally: loop.close()


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


def fetch_portfolio(tr):
    dbg = {}
    positions = []
    for name in ("compact_portfolio", "portfolio"):
        try:
            pf = _topic(tr, [name])
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
        c = _topic(tr, ["cash"])
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
                try: tick = _topic(tr, ["ticker"], isin, "LSX")
                except TypeError: tick = _topic(tr, ["ticker"], isin)
                if isinstance(tick, dict):
                    last = tick.get("last") or {}
                    price = _num(last.get("price"), (tick.get("bid") or {}).get("price")) or price
            except Exception:
                pass
            try:
                ins = _topic(tr, ["instrument_details", "instrument"], isin)
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
    try:
        tr = TradeRepublicApi(phone_no=phone, pin=pin, locale=d.get("locale", "fr"))
        _maybe(_method(tr, ["inititate_weblogin", "initiate_weblogin"])())
    except Exception as e:
        return jsonify(error="Connexion refusee : %s" % e), 401
    token = uuid.uuid4().hex
    with _lock:
        SESSIONS[token] = {"tr": tr, "exp": time.time() + SESSION_TTL}
    return jsonify(session=token)


@app.post("/tr/login/verify")
def login_verify():
    d = request.get_json(force=True) or {}
    s = SESSIONS.get(d.get("session", ""))
    if not s:
        return jsonify(error="Session expiree"), 440
    try:
        _maybe(_method(s["tr"], ["complete_weblogin"])((d.get("code") or "").strip()))
    except Exception as e:
        return jsonify(error="Code refuse : %s" % e), 401
    s["exp"] = time.time() + SESSION_TTL
    return jsonify(ok=True)


@app.get("/tr/portfolio")
def portfolio():
    s = SESSIONS.get(request.args.get("session", ""))
    if not s:
        return jsonify(error="Session expiree"), 440
    try:
        data = fetch_portfolio(s["tr"])
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
            out[n] = _topic(s["tr"], [n])
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
    return jsonify(service="meridian-tr", status="ok", version=2, sessions=len(SESSIONS))


if __name__ == "__main__":
    print("Meridian — backend Trade Republic (v2) sur http://localhost:8765")
    app.run(host="0.0.0.0", port=8765, threaded=True)
