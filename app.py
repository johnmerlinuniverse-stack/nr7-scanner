import os, time, requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

CG_BASE = "https://api.coingecko.com/api/v3"
BINANCE_BASE = "https://api.binance.com"

DEFAULT_TOPN = 150

def cg_get(path, params=None):
    if params is None: params = {}
    key = os.getenv("COINGECKO_DEMO_API_KEY", "").strip()
    if not key:
        raise RuntimeError("COINGECKO_DEMO_API_KEY ist nicht gesetzt (Streamlit Secrets / Env).")
    params["x_cg_demo_api_key"] = key
    r = requests.get(CG_BASE + path, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

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
        if not batch: break
        out.extend(batch)
        page += 1
        time.sleep(0.8)
    return out[:top_n]

@st.cache_data(ttl=3600)
def binance_exchange_info():
    r = requests.get(BINANCE_BASE + "/api/v3/exchangeInfo", timeout=30)
    r.raise_for_status()
    return r.json()

def binance_symbol_exists(exchange_info, symbol):
    # symbol like "BTCUSDT"
    for s in exchange_info.get("symbols", []):
        if s.get("symbol") == symbol and s.get("status") == "TRADING":
            return True
    return False

def binance_klines(symbol, interval, limit=200):
    r = requests.get(BINANCE_BASE + "/api/v3/klines", params={
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }, timeout=30)
    r.raise_for_status()
    data = r.json()
    # [ openTime, open, high, low, close, volume, closeTime, ... ]
    rows = []
    for k in data:
        rows.append({
            "open_time": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": int(k[6]),
        })
    return rows

def cg_ohlc_utc_daily(coin_id, vs="usd", days_fetch=90):
    # Returns aggregated UTC-daily rows: date_utc, high, low, close, range
    raw = cg_get(f"/coins/{coin_id}/ohlc", {"vs_currency": vs, "days": days_fetch})
    day = {}
    for ts, o, h, l, c in raw:
        dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
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

def last_closed_rows(rows, n):
    if not rows or len(rows) < n:
        return None
    return rows[-n:]

def is_nrn(rows, n):
    lastn = last_closed_rows(rows, n)
    if not lastn: return False
    ranges = [r["range"] for r in lastn]
    return ranges[-1] == min(ranges)  # tie zählt als NR

def main():
    st.set_page_config(page_title="NR Scanner (Top Coins)", layout="wide")
    st.title("NR4 / NR7 / NR10 Scanner – Top Coins nach Market Cap")

    colA, colB, colC, colD = st.columns(4)
    vs = colA.selectbox("Quote (MarketCap)", ["usd", "eur"], index=0)
    top_n = colB.number_input("Top N (Market Cap)", 10, 300, DEFAULT_TOPN, 10)
    tf = colC.selectbox("Timeframe", ["1D", "4H", "1W"], index=0)
    mode = colD.selectbox("Close-Modus", ["UTC (letzte abgeschlossene Kerze)", "Exchange Close"], index=0)

    st.caption("Hinweis: 4H/1W werden über Binance-Kerzen berechnet (Exchange Close). 1D kann UTC (CoinGecko) oder Exchange (Binance) sein.")

    col1, col2, col3 = st.columns(3)
    want_nr4 = col1.checkbox("NR4", value=True)
    want_nr7 = col2.checkbox("NR7", value=True)
    want_nr10 = col3.checkbox("NR10", value=True)

    run = st.button("Scan starten")

    if not run:
        return

    if not (want_nr4 or want_nr7 or want_nr10):
        st.warning("Bitte mindestens NR4/NR7/NR10 auswählen.")
        return

    with st.spinner("Hole Top Coins + scanne..."):
        markets = get_top_markets(vs=vs, top_n=int(top_n))

        exchange_info = None
        if tf != "1D" or mode == "Exchange Close":
            exchange_info = binance_exchange_info()

        interval = {"1D": "1d", "4H": "4h", "1W": "1w"}[tf]

        results = []
        progress = st.progress(0)

        for i, coin in enumerate(markets, 1):
            coin_id = coin["id"]
            sym = (coin.get("symbol") or "").upper()
            name = coin.get("name") or ""
            mcap = coin.get("market_cap")
            price = coin.get("current_price")
            vol24 = coin.get("total_volume")

            try:
                # Datenquelle wählen
                if tf == "1D" and mode.startswith("UTC"):
                    rows = cg_ohlc_utc_daily(coin_id, vs=vs, days_fetch=90)
                    # rows already closed UTC-day except "today"
                    closed = rows
                    last_day = closed[-1]["date_utc"] if closed else None
                    last_range = closed[-1]["range"] if closed else None
                    source = "CoinGecko UTC"
                else:
                    # Binance Exchange Close
                    pair = f"{sym}USDT"
                    if not binance_symbol_exists(exchange_info, pair):
                        progress.progress(i/len(markets))
                        continue
                    kl = binance_klines(pair, interval=interval, limit=200)
                    # Letzte Kerze ist evtl. live -> wir nehmen die letzte *abgeschlossene*
                    if len(kl) < 12:
                        progress.progress(i/len(markets))
                        continue
                    closed_kl = kl[:-1]
                    closed = []
                    for k in closed_kl:
                        dt = datetime.fromtimestamp(k["close_time"]/1000, tz=timezone.utc)
                        closed.append({
                            "date_utc": dt.isoformat(),
                            "high": k["high"],
                            "low": k["low"],
                            "close": k["close"],
                            "range": k["high"] - k["low"]
                        })
                    last_day = closed[-1]["date_utc"]
                    last_range = closed[-1]["range"]
                    source = f"Binance {interval}"

                nr4 = want_nr4 and is_nrn(closed, 4)
                nr7 = want_nr7 and is_nrn(closed, 7)
                nr10 = want_nr10 and is_nrn(closed, 10)

                if nr4 or nr7 or nr10:
                    results.append({
                        "symbol": sym,
                        "name": name,
                        "market_cap": mcap,
                        "price": price,
                        "volume_24h": vol24,
                        "timeframe": tf,
                        "mode": mode,
                        "source": source,
                        "last_closed": last_day,
                        "range_last": last_range,
                        "NR4": nr4,
                        "NR7": nr7,
                        "NR10": nr10,
                        "coingecko_id": coin_id
                    })

            except Exception:
                pass

            progress.progress(i / len(markets))
            time.sleep(0.15)

        df = pd.DataFrame(results)
        if df.empty:
            st.warning("Keine Treffer gefunden (oder API/Limit/Mapping-Probleme).")
            return

        df = df.sort_values("market_cap", ascending=False).reset_index(drop=True)

        st.subheader(f"Treffer: {len(df)}")
        st.dataframe(df, use_container_width=True)

        st.download_button(
            "CSV herunterladen",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"nr_scan_{tf}.csv",
            mime="text/csv"
        )

main()
