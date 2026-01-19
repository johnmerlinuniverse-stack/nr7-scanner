import os
import re
import time
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

CG_BASE = "https://api.coingecko.com/api/v3"
BINANCE_BASE = "https://api.binance.com"
DEFAULT_TOPN = 150

# -----------------------------
# CoinGecko: Rate limiter + retries
# -----------------------------
_CG_LAST_CALL = 0.0

def _cg_rate_limit(min_interval_sec: float):
    global _CG_LAST_CALL
    now = time.time()
    wait = (_CG_LAST_CALL + min_interval_sec) - now
    if wait > 0:
        time.sleep(wait)
    _CG_LAST_CALL = time.time()

def cg_get(path, params=None, max_retries=8, min_interval_sec=1.0):
    if params is None:
        params = {}
    key = os.getenv("COINGECKO_DEMO_API_KEY", "").strip()
    if not key:
        raise RuntimeError("COINGECKO_DEMO_API_KEY ist nicht gesetzt (Streamlit Secrets / Environment).")
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

@st.cache_data(ttl=6*3600)
def cg_ohlc_utc_daily_cached(coin_id, vs="usd", days_fetch=30):
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
# Binance helpers
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
# NR logic (LuxAlgo: NR4 suppressed if NR7)
# -----------------------------
def is_nrn(rows, n):
    if not rows or len(rows) < n:
        return False
    lastn = rows[-n:]
    ranges = [r["range"] for r in lastn]
    return ranges[-1] == min(ranges)

# -----------------------------
# CryptoWaves parsing
# -----------------------------
STABLE_HINTS = {"USDT","USDC","DAI","TUSD","FDUSD","USDE","USDD","USDP","BUSD","EURC","USD1","RLUSD","PYUSD","GUSD","FRAX","LUSD","USTC","U","USDS"}

def extract_tickers_from_text(text: str):
    """
    Robust extraction from messy paste:
    - picks tokens like BTC, ETH, 2Z, ENA, etc.
    - removes common UI noise words
    """
    if not text:
        return []

    t = text.replace("\u200b", " ").replace("\xa0", " ").replace("￼", " ")
    raw = re.findall(r"\b[A-Z0-9]{2,10}\b", t)

    noise = {
        "NOW","NEUTRAL","BUY","WAIT","CTB","CTS",
        "RSI","R4H","R1D","SHOW","CHART","MARKET","PORTFOLIO",
        "ANALYTICS","LIST","ALERTS","SORT","CAP","STAR",
        "USD","EUR","DROP","DOWN","BY"
    }

    tickers = []
    for w in raw:
        if w in noise:
            continue
        if w.isdigit():
            continue
        tickers.append(w)

    seen = set()
    out = []
    for x in tickers:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

# -----------------------------
# App
# -----------------------------
def main():
    st.set_page_config(page_title="NR Scanner (Top Coins / CryptoWaves)", layout="wide")
    st.title("NR4 / NR7 / NR10 Scanner – Universe Auswahl")

    colA, colB, colC, colD = st.columns(4)
    vs = colA.selectbox("Quote (Market Cap)", ["usd", "eur"], index=0)
    top_n = colB.number_input("Top N (Market Cap)", 10, 300, DEFAULT_TOPN, 10)
    tf = colC.selectbox("Timeframe", ["1D", "4H", "1W"], index=0)
    mode = colD.selectbox("Close-Modus", ["UTC (letzte abgeschlossene Kerze)", "Exchange Close"], index=1)

    interval = {"1D": "1d", "4H": "4h", "1W": "1w"}[tf]

    universe_mode = st.radio(
        "Welche Coins sollen gescannt werden?",
        ["CoinGecko Top N", "CryptoWaves Liste", "Schnittmenge (Top N ∩ CryptoWaves)"],
        index=0,
        horizontal=True
    )

    st.markdown("### CryptoWaves Coinliste (optional)")
    cw_text = st.text_area(
        "Füge hier deine CryptoWaves Ausgabe ein (oder nur die Ticker, je Zeile).",
        height=220,
        placeholder="BTC\nETH\nENA\n..."
    )

    cw_tickers = extract_tickers_from_text(cw_text)
    cw_set = set(cw_tickers)

    if cw_text.strip():
        st.write(f"Erkannte CryptoWaves Ticker: **{len(cw_tickers)}**")
        st.caption(", ".join(cw_tickers[:60]) + (" ..." if len(cw_tickers) > 60 else ""))

    col1, col2, col3 = st.columns(3)
    want_nr4 = col1.checkbox("NR4", value=True)
    want_nr7 = col2.checkbox("NR7", value=True)
    want_nr10 = col3.checkbox("NR10", value=True)

    days_fetch = st.slider("UTC OHLC Fetch-Tage (nur für UTC-Modus)", 14, 60, 30, 1)

    colx, coly = st.columns(2)
    min_vol = colx.number_input("Min. 24h Volumen (Quote)", min_value=0.0, value=0.0, step=1000000.0)
    drop_stables = coly.checkbox("Stablecoins rausfiltern", value=False)

    run = st.button("Scan starten")

    if not run:
        return
    if not (want_nr4 or want_nr7 or want_nr10):
        st.warning("Bitte mindestens NR4/NR7/NR10 auswählen.")
        return
    if universe_mode != "CoinGecko Top N" and not cw_tickers:
        st.warning("Du hast 'CryptoWaves' gewählt, aber es wurden keine Ticker erkannt. Bitte Liste einfügen.")
        return

    with st.spinner("Hole Top Coins + baue Universe + scanne..."):
        markets = get_top_markets(vs=vs, top_n=int(top_n))
        st.write("✅ Geladene Coins (CoinGecko):", len(markets))

        top_by_symbol = {}
        for c in markets:
            sym = (c.get("symbol") or "").upper()
            if sym and sym not in top_by_symbol:
                top_by_symbol[sym] = c

        # Build selected universe
        if universe_mode == "CoinGecko Top N":
            selected = markets
        elif universe_mode == "CryptoWaves Liste":
            selected = [top_by_symbol[s] for s in cw_tickers if s in top_by_symbol]
        else:
            selected = [c for c in markets if (c.get("symbol") or "").upper() in cw_set]

        # Show missing CW tickers (not in top_n)
        if universe_mode != "CoinGecko Top N":
            missing = [s for s in cw_tickers if s not in top_by_symbol]
            if missing:
                st.info(f"CryptoWaves Ticker nicht in Top {int(top_n)} enthalten (werden ignoriert): {len(missing)}")
                st.caption(", ".join(missing[:80]) + (" ..." if len(missing) > 80 else ""))

        if drop_stables:
            before = len(selected)
            selected = [c for c in selected if (c.get("symbol") or "").upper() not in STABLE_HINTS]
            st.write(f"Stablecoins gefiltert: {before} → {len(selected)}")

        st.write("✅ Coins im Universe (werden gescannt):", len(selected))
        if len(selected) == 0:
            st.warning("Universe ist leer. Tipp: Top N erhöhen oder CryptoWaves Liste prüfen.")
            return

        symset = None
        if tf != "1D" or mode == "Exchange Close":
            symset = binance_symbols_set()

        progress = st.progress(0)
        status_box = st.empty()

        results = []
        scanned = 0
        skipped_low_vol = 0
        skipped_no_data = 0
        skipped_no_binance_pair = 0
        errors = 0
        last_errors = []

        for i, coin in enumerate(selected, 1):
            coin_id = coin["id"]
            sym = (coin.get("symbol") or "").upper()
            name = coin.get("name") or ""
            vol24 = float(coin.get("total_volume") or 0.0)

            if min_vol and vol24 < float(min_vol):
                skipped_low_vol += 1
                progress.progress(i / len(selected))
                continue

            try:
                if tf == "1D" and mode.startswith("UTC"):
                    rows = cg_ohlc_utc_daily_cached(coin_id, vs=vs, days_fetch=int(days_fetch))
                    if not rows or len(rows) < 12:
                        skipped_no_data += 1
                        progress.progress(i / len(selected))
                        continue
                    closed = rows
                    last_closed = closed[-1]["date_utc"]
                    last_range = closed[-1]["range"]
                    source = "CoinGecko UTC"
                else:
                    pair = f"{sym}USDT"
                    if symset is not None and pair not in symset:
                        skipped_no_binance_pair += 1
                        progress.progress(i / len(selected))
                        continue

                    kl = binance_klines(pair, interval=interval, limit=200)
                    if len(kl) < 15:
                        skipped_no_data += 1
                        progress.progress(i / len(selected))
                        continue

                    kl = kl[:-1]  # drop live candle
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
                        skipped_no_data += 1
                        progress.progress(i / len(selected))
                        continue

                    last_closed = closed[-1]["date_utc"]
                    last_range = closed[-1]["range"]
                    source = f"Binance {interval}"

                scanned += 1

                # LuxAlgo rule: NR4 suppressed if NR7
                nr7 = want_nr7 and is_nrn(closed, 7)
                nr4_raw = want_nr4 and is_nrn(closed, 4)
                nr4 = nr4_raw and (not nr7)
                nr10 = want_nr10 and is_nrn(closed, 10)

                if nr4 or nr7 or nr10:
                    results.append({
                        "symbol": sym,
                        "name": name,
                        "NR4": nr4,
                        "NR7": nr7,
                        "NR10": nr10,
                        "coingecko_id": coin_id,

                        "market_cap": coin.get("market_cap"),
                        "price": coin.get("current_price"),
                        "volume_24h": vol24,
                        "timeframe": tf,
                        "mode": mode,
                        "source": source,
                        "last_closed": last_closed,
                        "range_last": last_range,
                        "universe": universe_mode
                    })

            except Exception as e:
                errors += 1
                key = os.getenv("COINGECKO_DEMO_API_KEY", "")
                msg = str(e).replace(key, "***")
                if len(last_errors) < 15:
                    last_errors.append(f"{sym} ({coin_id}) -> {type(e).__name__}: {msg[:170]}")

            progress.progress(i / len(selected))
            status_box.info(
                f"Fortschritt: {i}/{len(selected)} | gescannt: {scanned} | "
                f"skip Vol: {skipped_low_vol} | skip no data: {skipped_no_data} | "
                f"skip no Binance pair: {skipped_no_binance_pair} | errors: {errors}"
            )

        if last_errors:
            st.warning("⚠️ Fehlerdetails (max 15):")
            for err in last_errors:
                st.write(err)

        df = pd.DataFrame(results)
        if df.empty:
            st.warning("Keine Treffer gefunden. Tipp: 1D + Exchange Close ist am stabilsten.")
            return

        first_cols = ["symbol", "name", "NR4", "NR7", "NR10", "coingecko_id"]
        other_cols = [c for c in df.columns if c not in first_cols]
        df = df[first_cols + other_cols]

        if "market_cap" in df.columns:
            df = df.sort_values("market_cap", ascending=False).reset_index(drop=True)

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
