import os
import time
import json
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

CG_BASE = "https://api.coingecko.com/api/v3"

BINANCE_EXCHANGEINFO_ENDPOINTS = [
    "https://api.binance.com/api/v3/exchangeInfo",
    "https://data-api.binance.vision/api/v3/exchangeInfo",
]
BINANCE_KLINES_ENDPOINTS = [
    "https://api.binance.com/api/v3/klines",
    "https://data-api.binance.vision/api/v3/klines",
]

# Quote-Priority: reduziert massiv "skipped" bei Exchange Close
QUOTE_PRIORITY = ["USDT", "USDC", "FDUSD", "BUSD", "TUSD", "BTC", "ETH"]

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
# Fancy theme helpers
# -----------------------------
def apply_theme(dark: bool):
    if dark:
        bg = "#0b1020"
        card = "#121a33"
        text = "#e8ecff"
        muted = "#a9b1d6"
        accent = "#7aa2f7"
        border = "rgba(255,255,255,0.08)"
        shadow = "rgba(0,0,0,0.28)"
    else:
        bg = "#f6f7fb"
        card = "#ffffff"
        text = "#121826"
        muted = "#5b6475"
        accent = "#2563eb"
        border = "rgba(0,0,0,0.08)"
        shadow = "rgba(0,0,0,0.10)"

    st.markdown(f"""
    <style>
      .stApp {{
        background: {bg};
        color: {text};
      }}
      html, body, [class*="css"] {{
        color: {text} !important;
      }}
      section.main > div {{
        max-width: 980px;
        padding-top: 0.8rem;
      }}
      .card {{
        background: {card};
        border: 1px solid {border};
        border-radius: 16px;
        padding: 14px 16px;
        box-shadow: 0 12px 28px {shadow};
      }}
      .card h3 {{
        margin: 0 0 6px 0;
        font-size: 16px;
        color: {text};
      }}
      .muted {{
        color: {muted};
        font-size: 13px;
        line-height: 1.35;
      }}
      .badge {{
        display: inline-block;
        padding: 3px 10px;
        border-radius: 999px;
        font-size: 12px;
        border: 1px solid {border};
        background: rgba(127,127,127,0.08);
        margin-right: 6px;
        margin-bottom: 6px;
      }}
      .badge-accent {{
        border-color: rgba(122,162,247,0.35);
        background: rgba(122,162,247,0.12);
        color: {accent};
      }}
      .stButton>button {{
        border-radius: 12px !important;
        padding: 0.55rem 0.95rem !important;
        border: 1px solid {border} !important;
      }}
      .stTextArea textarea, .stTextInput input, .stSelectbox div[data-baseweb="select"] > div {{
        border-radius: 12px !important;
        border: 1px solid {border} !important;
      }}
      div[data-testid="stDataFrame"] {{
        border: 1px solid {border};
        border-radius: 16px;
        overflow: hidden;
      }}
      @media (max-width: 640px) {{
        section.main > div {{
          padding-left: 0.8rem;
          padding-right: 0.8rem;
        }}
      }}
    </style>
    """, unsafe_allow_html=True)

def fancy_header():
    st.markdown("""
    <div class="card">
      <h3>NR4 / NR7 Scanner</h3>
      <div class="muted">
        <span class="badge badge-accent">NR7 default</span>
        <span class="badge">LuxAlgo-Logik</span>
        <span class="badge">Pair-Fallback (USDT/USDC/FDUSD/...)</span>
        <span class="badge">Exchange Close + UTC Fallback (1D)</span>
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.write("")

def fancy_summary(hit_count: int, skipped_count: int, errors_count: int):
    st.markdown(f"""
    <div class="card">
      <h3>Scan Summary</h3>
      <div class="muted">
        <span class="badge badge-accent">Treffer: {hit_count}</span>
        <span class="badge">Skipped: {skipped_count}</span>
        <span class="badge">Errors: {errors_count}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.write("")

# -----------------------------
# CoinGecko rate-limit + retry
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

def is_stablecoin_marketrow(row: dict) -> bool:
    sym = (row.get("symbol") or "").lower()
    name = (row.get("name") or "").lower()
    price = row.get("current_price")

    stable_keywords = ["usd", "usdt", "usdc", "dai", "tusd", "usde", "fdusd", "usdp", "gusd", "eur", "euro", "gbp"]
    if any(k in sym for k in stable_keywords) or any(k in name for k in stable_keywords):
        if isinstance(price, (int, float)) and 0.97 <= float(price) <= 1.03:
            return True

    if isinstance(price, (int, float)) and 0.985 <= float(price) <= 1.015 and "btc" not in sym and "eth" not in sym:
        return True

    return False

@st.cache_data(ttl=6*3600)
def cg_ohlc_utc_daily_cached(coin_id, vs="usd", days_fetch=30):
    raw = cg_get(f"/coins/{coin_id}/ohlc", {"vs_currency": vs, "days": days_fetch}, max_retries=10, min_interval_sec=1.2)

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

def load_cw_id_map():
    try:
        with open("cw_id_map.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

# -----------------------------
# Binance robust
# -----------------------------
def binance_symbols_set():
    for url in BINANCE_EXCHANGEINFO_ENDPOINTS:
        try:
            r = requests.get(url, timeout=25)
            r.raise_for_status()
            info = r.json()
            return {s.get("symbol") for s in info.get("symbols", []) if s.get("status") == "TRADING"}
        except Exception:
            continue
    return set()

def binance_klines(symbol, interval, limit=200):
    last_err = None
    for url in BINANCE_KLINES_ENDPOINTS:
        try:
            r = requests.get(url, params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=25)
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
        except Exception as e:
            last_err = e
            continue
    raise last_err if last_err else RuntimeError("Binance klines Fehler")

def find_best_binance_pair(sym: str, symset: set):
    for q in QUOTE_PRIORITY:
        pair = f"{sym}{q}"
        if pair in symset:
            return pair
    return None

# -----------------------------
# NR logic (LuxAlgo)
# -----------------------------
def is_nrn(rows, n):
    if not rows or len(rows) < n:
        return False
    lastn = rows[-n:]
    ranges = [r["range"] for r in lastn]
    return ranges[-1] == min(ranges)

# -----------------------------
# App
# -----------------------------
def main():
    st.set_page_config(page_title="NR4/NR7 Scanner", layout="wide")
    st.title("NR4 / NR7 Scanner")

    # Mode switch: Simple vs Fancy
    view_mode = st.radio("Ansicht", ["Fancy", "Simple"], horizontal=True, index=0)

    if view_mode == "Fancy":
        dark_mode = st.toggle("🌙 Dark Mode", value=True)
        apply_theme(dark_mode)
        fancy_header()

    # Controls (kept lean)
    universe = st.selectbox("Coins", ["CryptoWaves (Default)", "CoinGecko Top N"], index=0)

    top_n = 150
    stable_toggle = False
    if universe == "CoinGecko Top N":
        top_n = st.number_input("Top N", min_value=10, max_value=500, value=150, step=10)
        stable_toggle = st.checkbox("Stablecoins scannen", value=False)

    tf = st.selectbox("Timeframe", ["1D", "4H", "1W"], index=0)

    if tf == "1D":
        close_mode = st.selectbox("Close", ["Exchange Close (empfohlen)", "UTC (langsam, days_fetch=30)"], index=0)
    else:
        close_mode = "Exchange Close (empfohlen)"

    with st.expander("ℹ️ Unterschied: Exchange Close vs UTC"):
        st.markdown("""
**Exchange Close (empfohlen)**  
- Kerzen kommen direkt von einer Börse (z. B. Binance).  
- Tages-Close = Close der Börsen-Tageskerze.  
- ✅ Vorteil: Sehr praxisnah fürs Trading, meist schneller.  
- ❌ Nachteil: Nicht jeder Coin hat ein passendes Pair (z.B. kein USDT/USDC/FDUSD Pair).

**UTC (letzte abgeschlossene Tageskerze)**  
- Ein Tag läuft immer von **00:00 bis 23:59 UTC** (einheitlich).  
- ✅ Vorteil: Vergleichbar und konsistent.  
- ❌ Nachteil: Langsamer (mehr API-Requests), kann minimal von Exchange-Kerzen abweichen.
        """)

    c1, c2 = st.columns(2)
    want_nr7 = c1.checkbox("NR7", value=True)
    want_nr4 = c2.checkbox("NR4", value=False)

    tickers_text = None
    if universe == "CryptoWaves (Default)":
        tickers_text = st.text_area("Ticker (1 pro Zeile)", value=CW_DEFAULT_TICKERS, height=110)

    run = st.button("Scan")

    if not run:
        return
    if not (want_nr7 or want_nr4):
        st.warning("Bitte NR7 und/oder NR4 auswählen.")
        return

    interval = {"1D": "1d", "4H": "4h", "1W": "1w"}[tf]
    use_utc = (tf == "1D" and str(close_mode).startswith("UTC"))

    # Build scan list
    scan_list = []  # {"symbol","name","coingecko_id"}
    cw_map = load_cw_id_map()

    if universe == "CoinGecko Top N":
        markets = get_top_markets(vs="usd", top_n=int(top_n))
        if not stable_toggle:
            markets = [m for m in markets if not is_stablecoin_marketrow(m)]
        for m in markets:
            scan_list.append({
                "symbol": (m.get("symbol") or "").upper(),
                "name": m.get("name") or "",
                "coingecko_id": m.get("id") or ""
            })
    else:
        symbols = []
        for line in (tickers_text or "").splitlines():
            s = line.strip().upper()
            if s and s not in symbols:
                symbols.append(s)
        for sym in symbols:
            scan_list.append({
                "symbol": sym,
                "name": sym,
                "coingecko_id": cw_map.get(sym, "")
            })

    # Binance symbols set
    symset = set()
    if not use_utc:
        symset = binance_symbols_set()
        if not symset:
            if tf == "1D":
                use_utc = True
                st.warning("Binance ist nicht erreichbar. Fallback auf UTC (CoinGecko) aktiviert. (Langsamer)")
            else:
                st.error("Binance ist nicht erreichbar. Für 4H/1W ist ohne Binance kein zuverlässiger Exchange-Close-Feed möglich.")
                return

    results = []
    skipped = []
    errors = []
    progress = st.progress(0)

    with st.spinner("Scanne..."):
        for i, item in enumerate(scan_list, 1):
            sym = item["symbol"]
            name = item.get("name", sym)
            coin_id = item.get("coingecko_id", "")

            try:
                # Exchange Close: try Binance pairs; if not found and 1D -> UTC fallback
                if not use_utc:
                    pair = find_best_binance_pair(sym, symset)
                    closed = None
                    source = None
                    last_closed = None
                    last_range = None

                    if pair:
                        kl = binance_klines(pair, interval=interval, limit=200)
                        if len(kl) >= 15:
                            kl = kl[:-1]
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
                            if len(closed) >= 12:
                                source = f"Binance {interval} ({pair})"
                                last_closed = closed[-1]["date_utc"]
                                last_range = closed[-1]["range"]
                            else:
                                closed = None

                    # Fallback for 1D if no binance data
                    if closed is None and tf == "1D":
                        if coin_id:
                            rows = cg_ohlc_utc_daily_cached(coin_id, vs="usd", days_fetch=30)
                            if rows and len(rows) >= 12:
                                closed = rows
                                source = "CoinGecko UTC (fallback)"
                                last_closed = closed[-1]["date_utc"]
                                last_range = closed[-1]["range"]
                            else:
                                skipped.append(f"{sym} (no data Binance+UTC)")
                                progress.progress(i / len(scan_list))
                                continue
                        else:
                            skipped.append(f"{sym} (no Binance pair + no coingecko_id)")
                            progress.progress(i / len(scan_list))
                            continue

                    if closed is None and tf != "1D":
                        skipped.append(f"{sym} (no Binance pair)")
                        progress.progress(i / len(scan_list))
                        continue

                # UTC Mode
                else:
                    if not coin_id:
                        skipped.append(f"{sym} (no coingecko_id)")
                        progress.progress(i / len(scan_list))
                        continue

                    rows = cg_ohlc_utc_daily_cached(coin_id, vs="usd", days_fetch=30)
                    if not rows or len(rows) < 12:
                        skipped.append(f"{sym} (no utc data)")
                        progress.progress(i / len(scan_list))
                        continue

                    closed = rows
                    source = "CoinGecko UTC"
                    last_closed = closed[-1]["date_utc"]
                    last_range = closed[-1]["range"]

                # NR logic (LuxAlgo): NR4 suppressed if NR7
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
                        "range_last": last_range
                    })

            except Exception as e:
                key = os.getenv("COINGECKO_DEMO_API_KEY", "")
                msg = str(e).replace(key, "***")
                errors.append(f"{sym}: {type(e).__name__} - {msg[:140]}")

            progress.progress(i / len(scan_list))

    df = pd.DataFrame(results)
    if df.empty:
        if view_mode == "Fancy":
            fancy_summary(0, len(skipped), len(errors))
        st.warning(f"Keine Treffer. Skipped: {len(skipped)} | Errors: {len(errors)}")
    else:
        df = df[["symbol", "name", "NR7", "NR4", "coingecko_id", "source", "last_closed", "range_last"]]
        df = df.sort_values(["NR7", "NR4", "symbol"], ascending=[False, False, True]).reset_index(drop=True)

        if view_mode == "Fancy":
            fancy_summary(len(df), len(skipped), len(errors))
        else:
            st.write(f"Treffer: {len(df)} | Skipped: {len(skipped)} | Errors: {len(errors)}")

        st.dataframe(df, use_container_width=True)

        st.download_button(
            "CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"nr_scan_{tf}.csv",
            mime="text/csv"
        )

    if skipped or errors:
        with st.expander("Report (nicht gescannt / Fehler)"):
            if skipped:
                st.write("**Nicht gescannt (skipped):**")
                for s in skipped[:250]:
                    st.write(s)
                if len(skipped) > 250:
                    st.caption(f"... und {len(skipped)-250} weitere")
            if errors:
                st.write("**Fehler:**")
                for e in errors[:250]:
                    st.write(e)
                if len(errors) > 250:
                    st.caption(f"... und {len(errors)-250} weitere")

if __name__ == "__main__":
    main()
