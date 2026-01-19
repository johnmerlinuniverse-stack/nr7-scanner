import os
import time
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

CG_BASE = "https://api.coingecko.com/api/v3"
BINANCE_BASE = "https://api.binance.com"
DEFAULT_TOPN = 150

# -----------------------------
# Helpers: CoinGecko
# -----------------------------
def cg_get(path, params=None):
    if params is None:
        params = {}
    key = os.getenv("COINGECKO_DEMO_API_KEY", "").strip()
    if not key:
        raise RuntimeError("COINGECKO_DEMO_API_KEY ist nicht gesetzt (Streamlit Secrets / Environment).")
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
        if not batch:
            break
        out.extend(batch)
        page += 1
        time.sleep(1.2)
    return out[:top_n]

def cg_ohlc_utc_daily(coin_id, vs="usd", days_fetch=90):
    raw = cg_get(f"/coins/{coin_id}/ohlc", {"vs_currency": vs, "days": days_fetch})

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
# Helpers: Binance
# -----------------------------
@st.cache_data(ttl=3600)
def binance_symbols_set():
    r = requests.get(BINANCE_BASE + "/api/v3/exchangeInfo", timeout=30)
    r.raise_for_status()
    info = r.json()
    syms = set()
    for s in info.get("symbols", []):
        if s.get("status") == "TRADING":
            syms.add(s.get("symbol"))
    return syms

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
            "open_time": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": int(k[6]),
        })
    return rows

# -----------------------------
# NR logic (wie LuxAlgo)
# -----------------------------
def last_closed_rows(rows, n):
    if not rows or len(rows) < n:
        return None
    return rows[-n:]

def is_nrn(rows, n):
    lastn = last_closed_rows(rows, n)
    if not lastn:
        return False
    ranges = [r["range"] for r in lastn]
    return ranges[-1] == min(ranges)

# -----------------------------
# App
# -----------------------------
def main():
    st.set_page_config(page_title="NR Scanner (Top Coins)", layout="wide")
    st.title("NR4 / NR7 / NR10 Scanner – Top Coins nach Market Cap")

    colA, colB, colC, colD = st.columns(4)
    vs = colA.selectbox("Quote (Market Cap)", ["usd", "eur"], index=0)
    top_n = colB.number_input("Top N (Market Cap)", min_value=10, max_value=300, value=DEFAULT_TOPN, step=10)
    tf = colC.selectbox("Timeframe", ["1D", "4H", "1W"], index=0)
    mode = colD.selectbox("Close-Modus", ["UTC (letzte abgeschlossene Kerze)", "Exchange Close"], index=0)

    st.caption("Hinweis: 4H/1W laufen über Binance-Kerzen. 1D kann UTC (CoinGecko) oder Exchange (Binance) sein.")

    col1, col2, col3 = st.columns(3)
    want_nr4 = col1.checkbox("NR4", value=True)
    want_nr7 = col2.checkbox("NR7", value=True)
    want_nr10 = col3.checkbox("NR10", value=True)

    min_vol = st.number_input(
        "Min. 24h Volumen (Quote)",
        min_value=0.0,
        value=0.0,
        step=1000000.0,
        help="Optional: z.B. 10000000 für > 10M"
    )

    speed = st.slider("Scan Geschwindigkeit (Pause pro Coin)", min_value=0.05, max_value=1.5, value=0.25, step=0.05)

    run = st.button("Scan starten")

    if not run:
        return

    if not (want_nr4 or want_nr7 or want_nr10):
        st.warning("Bitte mindestens NR4/NR7/NR10 auswählen.")
        return

    interval = {"1D": "1d", "4H": "4h", "1W": "1w"}[tf]

    with st.spinner("Hole Top Coins + scanne..."):
        markets = get_top_markets(vs=vs, top_n=int(top_n))
        st.write("✅ Geladene Coins (CoinGecko):", len(markets))

        # Binance symbols nur laden, wenn nötig
        symset = None
        if tf != "1D" or mode == "Exchange Close":
            symset = binance_symbols_set()

        results = []
        progress = st.progress(0)

        scanned = 0
        skipped_low_vol = 0
        skipped_no_data = 0
        skipped_no_binance_pair = 0
        errors = 0

        last_errors = []
        status_box = st.empty()

        for i, coin in enumerate(markets, 1):
            coin_id = coin["id"]
            sym = (coin.get("symbol") or "").upper()
            name = coin.get("name") or ""
            mcap = coin.get("market_cap")
            price = coin.get("current_price")
            vol24 = float(coin.get("total_volume") or 0.0)

            if min_vol and vol24 < float(min_vol):
                skipped_low_vol += 1
                progress.progress(i / len(markets))
                continue

            try:
                # Datenquelle wählen
                if tf == "1D" and mode.startswith("UTC"):
                    rows = cg_ohlc_utc_daily(coin_id, vs=vs, days_fetch=90)
                    closed = rows
                    if not closed or len(closed) < 10:
                        skipped_no_data += 1
                        progress.progress(i / len(markets))
                        continue
                    last_day = closed[-1]["date_utc"]
                    last_range = closed[-1]["range"]
                    source = "CoinGecko UTC"

                else:
                    pair = f"{sym}USDT"

                    if (symset is not None) and (pair not in symset):
                        skipped_no_binance_pair += 1
                        progress.progress(i / len(markets))
                        continue

                    kl = binance_klines(pair, interval=interval, limit=200)
                    if len(kl) < 12:
                        skipped_no_data += 1
                        progress.progress(i / len(markets))
                        continue

                    closed_kl = kl[:-1]
                    closed = []
                    for k in closed_kl:
                        dt = datetime.fromtimestamp(k["close_time"] / 1000, tz=timezone.utc)
                        closed.append({
                            "date_utc": dt.isoformat(),
                            "high": k["high"],
                            "low": k["low"],
                            "close": k["close"],
                            "range": k["high"] - k["low"]
                        })

                    if len(closed) < 10:
                        skipped_no_data += 1
                        progress.progress(i / len(markets))
                        continue

                    last_day = closed[-1]["date_utc"]
                    last_range = closed[-1]["range"]
                    source = f"Binance {interval}"

                scanned += 1

                # --- NR Logik wie LuxAlgo ---
                nr7 = want_nr7 and is_nrn(closed, 7)
                nr4_raw = want_nr4 and is_nrn(closed, 4)
                nr4 = nr4_raw and (not nr7)  # LuxAlgo Regel
                nr10 = want_nr10 and is_nrn(closed, 10)

                if nr4 or nr7 or nr10:
                    results.append({
                        # ✅ DEINE Wunsch-Reihenfolge am Anfang:
                        "symbol": sym,
                        "name": name,
                        "NR4": nr4,
                        "NR7": nr7,
                        "NR10": nr10,
                        "coingecko_id": coin_id,

                        # danach Rest:
                        "market_cap": mcap,
                        "price": price,
                        "volume_24h": vol24,
                        "timeframe": tf,
                        "mode": mode,
                        "source": source,
                        "last_closed": last_day,
                        "range_last": last_range,
                    })

            except Exception as e:
                errors += 1
                if len(last_errors) < 15:
                    last_errors.append(f"{sym} ({coin_id}) -> {type(e).__name__}: {str(e)[:160]}")

            progress.progress(i / len(markets))
            status_box.info(
                f"Fortschritt: {i}/{len(markets)} | gescannt: {scanned} | "
                f"skip Vol: {skipped_low_vol} | skip no data: {skipped_no_data} | "
                f"skip no Binance pair: {skipped_no_binance_pair} | errors: {errors}"
            )
            time.sleep(float(speed))

        if last_errors:
            st.warning("⚠️ Fehlerdetails (max 15):")
            for err in last_errors:
                st.write(err)

        df = pd.DataFrame(results)
        if df.empty:
            st.warning("Keine Treffer gefunden (oder API/Limit/Mapping-Probleme).")
            return

        # ✅ Erzwinge die Spaltenreihenfolge
        first_cols = ["symbol", "name", "NR4", "NR7", "NR10", "coingecko_id"]
        other_cols = [c for c in df.columns if c not in first_cols]
        df = df[first_cols + other_cols]

        # optional: sort nach MarketCap
        if "market_cap" in df.columns:
            df = df.sort_values("market_cap", ascending=False)

        df = df.reset_index(drop=True)

        st.subheader(f"✅ Treffer: {len(df)}")
        st.dataframe(df, use_container_width=True)

        st.download_button(
            "CSV herunterladen",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"nr_scan_{tf}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
