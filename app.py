import os
import time
import json
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

CG_BASE = "https://api.coingecko.com/api/v3"
BINANCE_BASE = "https://api.binance.com"

# -----------------------------
# CryptoWaves Default Liste (Symbole) – schlank als Ticker-Liste
# Du kannst die Liste in der UI bearbeiten (Textfeld).
# -----------------------------
CW_DEFAULT_TICKERS = """
BTC
ETH
BNB
XRP
USDC
SOL
TRX
DOGE
ADA
BCH
LINK
XLM
USDE
ZEC
SUI
AVAX
LTC
SHIB
HBAR
WLFI
TON
USD1
DOT
UNI
TAO
AAVE
PEPE
ICP
NEAR
ETC
PAXG
ONDO
ASTER
ENA
SKY
POL
WLD
APT
ATOM
ARB
ALGO
RENDER
FIL
TRUMP
QNT
PUMP
DASH
VET
BONK
SEI
CAKE
PENGU
JUP
XTZ
OP
NEXO
U
STX
ZRO
CRV
FET
VIRTUAL
CHZ
IMX
FDUSD
TUSD
INJ
LDO
MORPHO
ETHFI
FLOKI
SYRUP
TIA
STRK
2Z
GRT
SAND
SUN
DCR
TWT
CFX
GNO
JASMY
JST
IOTA
ENS
AXS
WIF
PYTH
KAIA
PENDLE
MANA
ZK
GALA
THETA
BAT
RAY
NEO
DEXE
COMP
AR
XPL
GLM
RUNE
XEC
WAL
S
""".strip()

# -----------------------------
# CoinGecko: Hard rate limit + retry (429)
# -----------------------------
_CG_LAST_CALL = 0.0

def _cg_rate_limit(min_interval_sec: float):
    global _CG_LAST_CALL
    now = time.time()
    wait = (_CG_LAST_CALL + min_interval_sec) - now
    if wait > 0:
        time.sleep(wait)
    _CG_LAST_CALL = time.time()

def cg_get(path, params=None, max_retries=8, min_interval_sec=1.2):
    if params is None:
        params = {}
    key = os.getenv("COINGECKO_DEMO_API_KEY", "").strip()
    if not key:
        raise RuntimeError("COINGECKO_DEMO_API_KEY ist nicht gesetzt (Streamlit Secrets).")

    params["x_cg_demo_api_key"] = key

    backoff = 2.0
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            _cg_rate_limit(min_interval_sec)
            r = requests.get(CG_BASE + path, params=params, timeout=30)

            if r.status_code == 429:
                time.sleep(backoff * attempt)
                continue

            r.raise_for_status()
            return r.json()

        except requests.RequestException as e:
            last_exc = e
            if attempt == max_retries:
                raise
            time.sleep(backoff * attempt)

    raise last_exc if last_exc else RuntimeError("CoinGecko Fehler (unbekannt).")

@st.cache_data(ttl=3600)
def get_top_markets(vs="usd", top_n=150):
    out = []
    per_page = 250
    page = 1
    while len(out) < top_n:
        batch = cg_get("/coins/markets", {
            "vs_currency": vs,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false"
        })
        if not batch:
            break
        out.extend(batch)
        page += 1
    return out[:top_n]

def load_cw_id_map():
    try:
        with open("cw_id_map.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

@st.cache_data(ttl=86400)
def resolve_coingecko_id_via_search(symbol: str, name_hint: str = ""):
    """
    Variante 1: Fehlende IDs automatisch über CoinGecko /search auflösen.
    Priorität:
      1) Coin mit exakt passendem symbol
      2) Coin dessen Name den name_hint enthält
      3) Fallback: erster Coin Treffer
    """
    q = (name_hint or symbol).strip()
    if not q:
        return None

    data = cg_get("/search", {"query": q}, max_retries=8, min_interval_sec=1.2)
    coins = data.get("coins", []) if isinstance(data, dict) else []
    if not coins:
        return None

    sym_u = symbol.upper().strip()

    # 1) exakter symbol match
    for c in coins:
        if str(c.get("symbol", "")).upper() == sym_u:
            return c.get("id")

    # 2) name hint enthalten
    nh = (name_hint or "").lower().strip()
    if nh:
        for c in coins:
            if nh in str(c.get("name", "")).lower():
                return c.get("id")

    # 3) fallback
    return coins[0].get("id")

@st.cache_data(ttl=6*3600)
def cg_ohlc_utc_daily_cached(coin_id, vs="usd", days_fetch=30):
    """
    UTC-Modus: days_fetch wird automatisch auf 30 gesetzt (wie gewünscht).
    Cached, damit du nicht bei jedem Run alles neu ziehst.
    """
    raw = cg_get(f"/coins/{coin_id}/ohlc", {"vs_currency": vs, "days": days_fetch}, max_retries=10, min_interval_sec=1.2)

    # UTC daily aggregation + drop today's incomplete UTC day
    day = {}
    for ts, o, h, l, c in raw:
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        key = dt.date().isoformat()
        if key not in day:
            day[key] = {"high": h, "low": l, "close": c, "last_ts": ts}
        else:
            day[key]["high"] = max(day[key]["high"], h)
            day[key]["low"] = min(day[key]["low"], l)
            if ts > day[key]["last_ts"]:
                day[key]["close"] = c
                day[key]["last_ts"] = ts

    today_utc = datetime.now(timezone.utc).date().isoformat()
    keys = sorted(k for k in day.keys() if k != today_utc)

    rows = []
    for k in keys:
        rows.append({
            "date_utc": k,
            "high": float(day[k]["high"]),
            "low": float(day[k]["low"]),
            "close": float(day[k]["close"]),
            "range": float(day[k]["high"] - day[k]["low"]),
        })
    return rows

# -----------------------------
# Binance (Exchange Close)
# -----------------------------
@st.cache_data(ttl=3600)
def binance_symbols_set():
    r = requests.get(BINANCE_BASE + "/api/v3/exchangeInfo", timeout=30)
    r.raise_for_status()
    info = r.json()
    return {s.get("symbol") for s in info.get("symbols", []) if s.get("status") == "TRADING"}

def binance_klines(symbol, interval, limit=200):
    r = requests.get(BINANCE_BASE + "/api/v3/klines", params={
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }, timeout=30)
    r.raise_for_status()
    data = r.json()
    rows = []
    for k in data:
        rows.append({
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "close_time": int(k[6]),
        })
    return rows

# -----------------------------
# NR logic (LuxAlgo)
# NR7: range == lowest(range,7)
# NR4: range == lowest(range,4) AND NOT NR7
# -----------------------------
def is_nrn(rows, n):
    if not rows or len(rows) < n:
        return False
    lastn = rows[-n:]
    ranges = [r["range"] for r in lastn]
    return ranges[-1] == min(ranges)

# -----------------------------
# UI
# -----------------------------
def main():
    st.set_page_config(page_title="NR4/NR7 Scanner", layout="wide")
    st.title("NR4 / NR7 Scanner")

    # Schlanke Top-Leiste
    universe = st.selectbox("Coins", ["CryptoWaves (Default)", "CoinGecko Top 150"], index=0)
    tf = st.selectbox("Timeframe", ["1D", "4H", "1W"], index=0)

    # Close Modus: ideal default = Exchange Close (weniger 429, schneller)
    # UTC ist nur relevant für 1D
    if tf == "1D":
        mode = st.selectbox("Close", ["Exchange Close (empfohlen)", "UTC (langsam, days_fetch=30)"], index=0)
    else:
        mode = "Exchange Close (empfohlen)"

    # Patterns: nur NR4/NR7. NR7 default an.
    c1, c2 = st.columns(2)
    want_nr7 = c1.checkbox("NR7", value=True)
    want_nr4 = c2.checkbox("NR4", value=False)

    # Coins Eingabe (klein: ca. 3–4 Zeilen sichtbar)
    # Nur zeigen, wenn CryptoWaves gewählt ist (sonst unnötig)
    tickers_text = None
    if universe.startswith("CryptoWaves"):
        tickers_text = st.text_area(
            "Ticker (1 pro Zeile)",
            value=CW_DEFAULT_TICKERS,
            height=110  # klein -> ca. 3–4 Zeilen sichtbar auf mobile
        )

    # Top 150 ist fix, UI bleibt schlank
    run = st.button("Scan")

    if not run:
        return

    if not (want_nr7 or want_nr4):
        st.warning("Bitte NR7 und/oder NR4 auswählen.")
        return

    interval = {"1D": "1d", "4H": "4h", "1W": "1w"}[tf]

    # Datenquelle
    use_utc = (tf == "1D" and str(mode).startswith("UTC"))
    vs = "usd"  # schlank: fix auf USD

    # Liste bauen
    markets = []
    cw_map = load_cw_id_map()

    if universe == "CoinGecko Top 150":
        markets = get_top_markets(vs=vs, top_n=150)
    else:
        # Aus Textfeld parse
        raw = tickers_text or ""
        symbols = []
        for line in raw.splitlines():
            s = line.strip().upper()
            if s and s not in symbols:
                symbols.append(s)

        # Märkte im gleichen Format wie CoinGecko markets
        for sym in symbols:
            cid = cw_map.get(sym)
            markets.append({"id": cid, "symbol": sym.lower(), "name": sym})

    # Binance Symbols (nur wenn Exchange Close)
    symset = None
    if not use_utc:
        symset = binance_symbols_set()

    results = []
    errors = 0
    skipped = 0
    last_errors = []
    progress = st.progress(0)

    with st.spinner("Scanne..."):
        for i, coin in enumerate(markets, 1):
            try:
                sym = (coin.get("symbol") or "").upper()
                name = coin.get("name") or sym
                coin_id = coin.get("id")

                # Wenn CryptoWaves: coin_id ggf. auto-resolve
                if universe.startswith("CryptoWaves"):
                    # symbol aus coin['symbol'] ist lower, wir brauchen uppercase:
                    sym_u = sym
                    if not coin_id:
                        coin_id = cw_map.get(sym_u)
                    if not coin_id:
                        coin_id = resolve_coingecko_id_via_search(sym_u, name_hint=name)
                    if not coin_id:
                        skipped += 1
                        progress.progress(i / len(markets))
                        continue

                # Kerzen holen
                if use_utc:
                    # days_fetch = 30 (fix, wie gewünscht)
                    rows = cg_ohlc_utc_daily_cached(coin_id, vs=vs, days_fetch=30)
                    if not rows or len(rows) < 12:
                        skipped += 1
                        progress.progress(i / len(markets))
                        continue
                    closed = rows
                    source = "CoinGecko UTC"
                    last_closed = closed[-1]["date_utc"]
                    last_range = closed[-1]["range"]

                else:
                    # Exchange Close via Binance (USDT)
                    pair = f"{sym}USDT"
                    if symset is not None and pair not in symset:
                        skipped += 1
                        progress.progress(i / len(markets))
                        continue

                    kl = binance_klines(pair, interval=interval, limit=200)
                    if len(kl) < 15:
                        skipped += 1
                        progress.progress(i / len(markets))
                        continue

                    kl = kl[:-1]  # letzte kann live sein
                    closed = []
                    for k in kl:
                        dt = datetime.fromtimestamp(k["close_time"] / 1000, tz=timezone.utc)
                        closed.append({
                            "date_utc": dt.isoformat(),
                            "high": k["high"],
                            "low": k["low"],
                            "close": k["close"],
                            "range": k["high"] - k["low"]
                        })

                    if len(closed) < 12:
                        skipped += 1
                        progress.progress(i / len(markets))
                        continue

                    source = f"Binance {interval}"
                    last_closed = closed[-1]["date_utc"]
                    last_range = closed[-1]["range"]

                # NR Logik wie LuxAlgo (NR4 unterdrückt wenn NR7)
                nr7 = want_nr7 and is_nrn(closed, 7)
                nr4_raw = want_nr4 and is_nrn(closed, 4)
                nr4 = nr4_raw and (not nr7)

                if nr7 or nr4:
                    results.append({
                        "symbol": sym,
                        "name": name,
                        "NR7": nr7,
                        "NR4": nr4,
                        "coingecko_id": coin_id,
                        "source": source,
                        "last_closed": last_closed,
                        "range_last": last_range,
                    })

            except Exception as e:
                errors += 1
                # Kein Key-Leak
                key = os.getenv("COINGECKO_DEMO_API_KEY", "")
                msg = str(e).replace(key, "***")
                if len(last_errors) < 8:
                    last_errors.append(f"{coin.get('symbol','').upper()} -> {type(e).__name__}: {msg[:140]}")

            progress.progress(i / len(markets))

    # Ausgabe (schlank)
    df = pd.DataFrame(results)
    if df.empty:
        st.warning(f"Keine Treffer. Skipped: {skipped} | Errors: {errors}")
        if last_errors:
            with st.expander("Fehlerdetails"):
                for x in last_errors:
                    st.write(x)
        return

    # Reihenfolge
    df = df[["symbol", "name", "NR7", "NR4", "coingecko_id", "source", "last_closed", "range_last"]]
    df = df.sort_values(["NR7", "NR4", "symbol"], ascending=[False, False, True]).reset_index(drop=True)

    st.write(f"Treffer: {len(df)} | Skipped: {skipped} | Errors: {errors}")

    st.dataframe(df, use_container_width=True)

    # Optional CSV (praktisch fürs Handy)
    st.download_button(
        "CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"nr_scan_{tf}.csv",
        mime="text/csv"
    )

    if last_errors:
        with st.expander("Fehlerdetails"):
            for x in last_errors:
                st.write(x)

if __name__ == "__main__":
    main()
