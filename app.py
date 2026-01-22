import os
import time
import json
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

# -----------------------------
# OPTIONAL: ccxt
# -----------------------------
try:
    import ccxt
except Exception:
    ccxt = None

CG_BASE = "https://api.coingecko.com/api/v3"

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
# Theme CSS (Light/Dark Toggle)
# -----------------------------
def inject_theme_css(mode: str):
    # mode: "dark" or "light"
    if mode == "dark":
        bg = "#0B0F17"
        card = "#111827"
        card2 = "#0F172A"
        text = "#E5E7EB"
        muted = "#9CA3AF"
        border = "rgba(255,255,255,0.08)"
        accent = "#60A5FA"
        tablebg = "#0F172A"
        inputbg = "#0B1220"
    else:
        bg = "#F6F7FB"
        card = "#FFFFFF"
        card2 = "#FFFFFF"
        text = "#111827"
        muted = "#6B7280"
        border = "rgba(17,24,39,0.10)"
        accent = "#2563EB"
        tablebg = "#FFFFFF"
        inputbg = "#FFFFFF"

    st.markdown(f"""
<style>
:root {{
  --bg: {bg};
  --card: {card};
  --card2: {card2};
  --text: {text};
  --muted: {muted};
  --border: {border};
  --accent: {accent};
  --tablebg: {tablebg};
  --inputbg: {inputbg};
}}
html, body, [data-testid="stAppViewContainer"] {{
  background: var(--bg) !important;
  color: var(--text) !important;
}}
/* Header spacing */
.block-container {{
  padding-top: 3.1rem;
  padding-bottom: 1.2rem;
  max-width: 1100px;
}}
/* Cards */
.nr-card {{
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 14px 14px;
  box-shadow: 0 10px 30px rgba(0,0,0,0.10);
}}
.nr-card-title {{
  font-size: 18px;
  font-weight: 700;
  letter-spacing: -0.2px;
}}
.nr-card-sub {{
  color: var(--muted);
  font-size: 13px;
  margin-top: 2px;
}}
.badge {{
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border-radius: 999px;
  padding: 6px 10px;
  border: 1px solid var(--border);
  background: var(--card2);
  font-size: 12px;
  color: var(--text);
}}
.badge-accent {{
  border-color: rgba(96,165,250,0.35);
}}
.hr {{
  height: 1px;
  background: var(--border);
  margin: 12px 0;
}}
/* Inputs */
div[data-testid="stTextArea"] textarea,
div[data-testid="stNumberInput"] input,
div[data-testid="stSelectbox"] div,
div[data-testid="stRadio"] div,
div[data-testid="stCheckbox"] div {{
  color: var(--text) !important;
}}
/* Try to tint inputs */
textarea, input {{
  background: var(--inputbg) !important;
  border-radius: 12px !important;
}}
/* Buttons */
.stButton > button {{
  border-radius: 12px !important;
  border: 1px solid var(--border) !important;
  background: var(--card) !important;
  color: var(--text) !important;
  font-weight: 700 !important;
}}
.stButton > button:hover {{
  border-color: rgba(96,165,250,0.35) !important;
}}
/* Dataframe wrapper */
div[data-testid="stDataFrame"] {{
  border-radius: 16px;
  overflow: hidden;
  border: 1px solid var(--border);
  background: var(--tablebg) !important;
}}
/* Reduce layout gaps */
div[data-testid="stHorizontalBlock"] {{
  gap: 0.7rem;
}}
/* Make expander look cleaner */
details {{
  border-radius: 14px;
  border: 1px solid var(--border);
  background: var(--card);
  padding: 6px 10px;
}}
summary {{
  color: var(--text);
  font-weight: 700;
}}
</style>
""", unsafe_allow_html=True)

# -----------------------------
# CoinGecko helpers (only used when needed)
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
        rows.append({"time": k, "high": float(day[k]["high"]), "low": float(day[k]["low"]), "close": float(day[k]["close"]), "range": float(day[k]["high"] - day[k]["low"])})
    return rows

def load_cw_id_map():
    try:
        with open("cw_id_map.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

# -----------------------------
# ccxt Futures Provider Layer
# -----------------------------
PROVIDER_CHAIN = ["bitget", "bingx", "bybit", "mexc", "blofin", "okx"]

def ccxt_available():
    return ccxt is not None

def _make_exchange(exchange_id: str):
    klass = getattr(ccxt, exchange_id)
    ex = klass({"enableRateLimit": True, "timeout": 20000})
    opt = ex.options if hasattr(ex, "options") and isinstance(ex.options, dict) else {}
    if exchange_id in ["bybit", "okx", "bitget", "bingx", "mexc", "blofin"]:
        opt = {**opt, "defaultType": "swap"}
    ex.options = opt
    return ex

@st.cache_resource(ttl=3600)
def get_exchange_client(exchange_id: str):
    if not ccxt_available():
        raise RuntimeError("ccxt ist nicht installiert. Bitte 'ccxt' in requirements.txt hinzufügen.")
    if not hasattr(ccxt, exchange_id):
        raise RuntimeError(f"ccxt unterstützt Exchange '{exchange_id}' nicht in dieser Version.")
    return _make_exchange(exchange_id)

@st.cache_data(ttl=3600)
def load_markets_cached(exchange_id: str):
    ex = get_exchange_client(exchange_id)
    return ex.load_markets()

def _is_usdt_linear_perp_market(m: dict) -> bool:
    quote = (m.get("quote") or "")
    active = m.get("active", True)
    is_swap = bool(m.get("swap", False))
    is_future = bool(m.get("future", False))
    is_contract = bool(m.get("contract", False))
    contract_type = m.get("type") or ""
    settle = (m.get("settle") or "")
    linear = bool(m.get("linear", False)) or (settle == "USDT")
    if not active:
        return False
    if quote != "USDT":
        return False
    if not (is_swap or is_future or is_contract or contract_type in ["swap", "future"]):
        return False
    if linear:
        return True
    return True

def find_ccxt_futures_symbol(exchange_id: str, base_sym: str):
    markets = load_markets_cached(exchange_id)
    candidates = []
    for sym, m in markets.items():
        if (m.get("base") or "").upper() != base_sym.upper():
            continue
        if not _is_usdt_linear_perp_market(m):
            continue
        candidates.append((sym, m))
    if not candidates:
        return None

    def score(sym, m):
        s = 0
        if ":USDT" in sym:
            s += 3
        if "SWAP" in sym.upper():
            s += 1
        if m.get("swap"):
            s += 2
        if m.get("linear"):
            s += 1
        return s

    candidates.sort(key=lambda x: score(x[0], x[1]), reverse=True)
    return candidates[0][0]

def fetch_ohlcv_ccxt(exchange_id: str, ccxt_symbol: str, timeframe: str, limit: int = 200):
    ex = get_exchange_client(exchange_id)
    ohlcv = ex.fetch_ohlcv(ccxt_symbol, timeframe=timeframe, limit=limit)
    if not ohlcv or len(ohlcv) < 15:
        return None
    ohlcv = ohlcv[:-1]
    if len(ohlcv) < 12:
        return None
    rows = []
    for ts, o, h, l, c, v in ohlcv:
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
        rows.append({"time": dt, "high": float(h), "low": float(l), "close": float(c), "range": float(h - l)})
    return rows

# -----------------------------
# NR + Breakout logic (LuxAlgo-style) + NR10
# -----------------------------
def compute_nr_flags(closed):
    n = len(closed)
    rngs = [c["range"] for c in closed]
    nr10 = [False] * n
    nr7 = [False] * n
    nr4 = [False] * n
    for i in range(n):
        w10 = rngs[max(0, i - 9): i + 1]
        w7 = rngs[max(0, i - 6): i + 1]
        w4 = rngs[max(0, i - 3): i + 1]
        lst10 = min(w10) if len(w10) >= 10 else None
        lst7 = min(w7) if len(w7) >= 7 else None
        lst4 = min(w4) if len(w4) >= 4 else None
        is10 = (lst10 is not None and rngs[i] == lst10)
        is7 = (lst7 is not None and rngs[i] == lst7 and (not is10))
        is4 = (lst4 is not None and rngs[i] == lst4 and (not is7) and (not is10))
        nr10[i] = is10
        nr7[i] = is7
        nr4[i] = is4
    return nr4, nr7, nr10

def simulate_breakouts_since_last_nr(closed):
    if len(closed) < 12:
        return "", "", "-", "-", 0, 0, None, None
    nr4_flags, nr7_flags, nr10_flags = compute_nr_flags(closed)
    setup_idx = -1
    setup_type = ""
    for i in range(len(closed) - 1, -1, -1):
        if nr10_flags[i] or nr7_flags[i] or nr4_flags[i]:
            setup_idx = i
            setup_type = "NR10" if nr10_flags[i] else ("NR7" if nr7_flags[i] else "NR4")
            break
    if setup_idx == -1:
        return "", "", "-", "-", 0, 0, None, None
    rh = closed[setup_idx]["high"]
    rl = closed[setup_idx]["low"]
    mid = (rh + rl) / 2.0
    up_check = True
    down_check = True
    up_count = 0
    down_count = 0
    breakout_state = "-"
    breakout_tag = "-"
    for j in range(setup_idx + 1, len(closed)):
        prev_close = closed[j - 1]["close"]
        cur_close = closed[j]["close"]
        if cur_close > mid and down_check is False:
            down_check = True
        if (prev_close >= rl) and (cur_close < rl) and down_check:
            down_count += 1
            down_check = False
            breakout_state = "DOWN"
            breakout_tag = f"DOWN#{down_count}"
        if cur_close < mid and up_check is False:
            up_check = True
        if (prev_close <= rh) and (cur_close > rh) and up_check:
            up_count += 1
            up_check = False
            breakout_state = "UP"
            breakout_tag = f"UP#{up_count}"
    setup_time = closed[setup_idx]["time"]
    return setup_time, setup_type, breakout_state, breakout_tag, up_count, down_count, rh, rl

# -----------------------------
# Display helpers
# -----------------------------
def mk_pattern_badge(nr10: bool, nr7: bool, nr4: bool) -> str:
    parts = []
    if nr10: parts.append("🟣 NR10")
    if nr7:  parts.append("🟡 NR7")
    if nr4:  parts.append("🟢 NR4")
    return " ".join(parts) if parts else "—"

def mk_breakout_badge(state: str, tag: str) -> str:
    if state == "UP":
        return f"🟢 ▲ {tag}"
    if state == "DOWN":
        return f"🔴 ▼ {tag}"
    return "—"

def mk_range_badge(in_range: bool) -> str:
    return "✅ InRange" if in_range else "—"

def short_ex(exchange_id: str) -> str:
    if exchange_id == "coingecko":
        return "UTC"
    return exchange_id.upper()

# -----------------------------
# App
# -----------------------------
def main():
    st.set_page_config(page_title="Merlin's NR Scanner", layout="wide")

    # default theme
    if "theme_mode" not in st.session_state:
        st.session_state["theme_mode"] = "dark"

    # Header Card + Theme toggle
    header_left, header_right = st.columns([3, 1], vertical_alignment="center")
    with header_left:
        st.markdown('<div class="nr-card"><div class="nr-card-title">Merlins NR Scanner</div><div class="nr-card-sub">Futures-Kerzen via ccxt · Auto-Fallback: Bitget → BingX → Bybit → MEXC → BloFin → OKX</div></div>', unsafe_allow_html=True)

    with header_right:
        theme_label = "🌙 Dark" if st.session_state["theme_mode"] == "dark" else "☀️ Light"
        if st.button(f"Theme: {theme_label}", use_container_width=True):
            st.session_state["theme_mode"] = "light" if st.session_state["theme_mode"] == "dark" else "dark"
            st.rerun()

    inject_theme_css(st.session_state["theme_mode"])

    # Controls Card
    st.markdown('<div class="nr-card">', unsafe_allow_html=True)
    cA, cB = st.columns([1, 1])
    with cA:
        universe = st.selectbox("Coins", ["CryptoWaves (Default)", "CoinGecko Top N"], index=0)
        tf = st.selectbox("Timeframe", ["1D", "4H", "1W"], index=0)
    with cB:
        provider_mode = st.selectbox(
            "Futures Quelle",
            ["Auto (Bitget→BingX→Bybit→MEXC→BloFin→OKX)"] + [f"Nur {p.upper()}" for p in PROVIDER_CHAIN],
            index=0
        )
        allow_utc_fallback = st.checkbox("UTC-Fallback (CoinGecko)", value=False)

    with st.expander("ℹ️ Unterschied: Exchange Close vs UTC"):
        st.markdown("""
**Exchange Close (Futures / Börsen-Close)**  
- Wir nutzen die **Futures/SWAP-Kerzen** direkt von deiner Börse (Bitget/BingX/Bybit/MEXC/BloFin/OKX).  
- Tages-Close = Close der **Börsen-Kerze** (Tagesgrenze kann je nach Börse minimal abweichen).  
- ✅ Vorteil: passt zu deinem Futures-Trading & Exchange-Feed.

**UTC (letzte abgeschlossene Tageskerze)**  
- Ein Tag läuft **00:00–23:59 UTC**.  
- Wir nehmen die letzte **vollständig abgeschlossene** UTC-Tageskerze.  
- ✅ Vorteil: einheitlich/vergleichbar  
- ❌ Nachteil: langsamer + kann leicht vom Exchange-Close abweichen.
        """)

    p1, p2, p3 = st.columns(3)
    want_nr7 = p1.checkbox("NR7", value=True)
    want_nr4 = p2.checkbox("NR4", value=False)
    want_nr10 = p3.checkbox("NR10", value=False)

    show_inrange_only = st.checkbox("Nur Coins anzeigen, die aktuell im NR-Range sind", value=False)

    view_mode = st.radio("Ansicht", ["Kompakt (Mobile)", "Detail (Desktop)"], index=0, horizontal=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # Universe configs
    top_n = 150
    stable_toggle = False
    tickers_text = None
    cw_map = load_cw_id_map()

    if universe == "CoinGecko Top N":
        st.markdown('<div class="nr-card">', unsafe_allow_html=True)
        top_n = st.number_input("Top N", min_value=10, max_value=500, value=150, step=10)
        stable_toggle = st.checkbox("Stablecoins scannen", value=False)
        if not os.getenv("COINGECKO_DEMO_API_KEY", "").strip():
            st.info("Für CoinGecko Top-N brauchst du COINGECKO_DEMO_API_KEY in Streamlit Secrets.")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="nr-card">', unsafe_allow_html=True)
        tickers_text = st.text_area("Ticker (1 pro Zeile)", value=CW_DEFAULT_TICKERS, height=110)
        st.markdown('</div>', unsafe_allow_html=True)

    # ccxt check
    if ccxt is None:
        st.error("ccxt ist nicht installiert. Bitte 'ccxt' in requirements.txt hinzufügen und neu deployen.")
        return

    # Scan Button
    st.markdown('<div class="nr-card">', unsafe_allow_html=True)
    run = st.button("🚀 Scan starten", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    if not run:
        return
    if not (want_nr7 or want_nr4 or want_nr10):
        st.warning("Bitte mindestens NR7/NR4/NR10 auswählen.")
        return

    tf_map = {"1D": "1d", "4H": "4h", "1W": "1w"}
    ccxt_tf = tf_map[tf]

    if provider_mode.startswith("Nur "):
        selected = provider_mode.replace("Nur ", "").strip().lower()
        provider_chain = [selected]
    else:
        provider_chain = PROVIDER_CHAIN[:]

    # Build scan list
    scan_list = []
    if universe == "CoinGecko Top N":
        if not os.getenv("COINGECKO_DEMO_API_KEY", "").strip():
            st.error("CoinGecko Top-N geht nur, wenn COINGECKO_DEMO_API_KEY gesetzt ist.")
            return
        markets = get_top_markets(vs="usd", top_n=int(top_n))
        if not stable_toggle:
            markets = [m for m in markets if not is_stablecoin_marketrow(m)]
        for m in markets:
            scan_list.append({"symbol": (m.get("symbol") or "").upper(), "name": m.get("name") or "", "coingecko_id": m.get("id") or ""})
    else:
        symbols = []
        for line in (tickers_text or "").splitlines():
            s = line.strip().upper()
            if s and s not in symbols:
                symbols.append(s)
        for sym in symbols:
            scan_list.append({"symbol": sym, "name": sym, "coingecko_id": cw_map.get(sym, "")})

    results, skipped, errors = [], [], []
    progress = st.progress(0)

    with st.spinner("Scanne..."):
        for i, item in enumerate(scan_list, 1):
            base = item["symbol"]
            name = item.get("name", base)
            coin_id = item.get("coingecko_id", "")

            closed = None
            exchange_used = ""
            pair_used = ""
            data_source = ""
            last_reason = None

            # Futures chain
            for ex_id in provider_chain:
                try:
                    if not hasattr(ccxt, ex_id):
                        last_reason = f"{ex_id}: not supported"
                        continue

                    # load markets (cached)
                    _ = load_markets_cached(ex_id)

                    sym = find_ccxt_futures_symbol(ex_id, base)
                    if not sym:
                        last_reason = f"{ex_id}: symbol not listed"
                        continue

                    rows = fetch_ohlcv_ccxt(ex_id, sym, timeframe=ccxt_tf, limit=200)
                    if not rows:
                        last_reason = f"{ex_id}: no data"
                        continue

                    closed = rows
                    exchange_used = ex_id
                    pair_used = sym
                    data_source = "Futures"
                    break

                except Exception as e:
                    msg = str(e)
                    if "451" in msg:
                        last_reason = f"{ex_id}: blocked (451)"
                    elif "429" in msg:
                        last_reason = f"{ex_id}: rate limit (429)"
                    elif "timed out" in msg.lower():
                        last_reason = f"{ex_id}: timeout"
                    else:
                        last_reason = f"{ex_id}: error"
                    continue

            # Optional UTC fallback (only 1D)
            if (closed is None) and allow_utc_fallback and tf == "1D":
                try:
                    if not os.getenv("COINGECKO_DEMO_API_KEY", "").strip():
                        skipped.append(f"{base}: UTC fallback aktiv, aber CoinGecko Key fehlt")
                    elif not coin_id:
                        skipped.append(f"{base}: UTC fallback möglich, aber coingecko_id fehlt (cw_id_map.json)")
                    else:
                        rows = cg_ohlc_utc_daily_cached(coin_id, vs="usd", days_fetch=30)
                        if rows and len(rows) >= 12:
                            closed = rows
                            exchange_used = "coingecko"
                            pair_used = coin_id
                            data_source = "UTC"
                        else:
                            skipped.append(f"{base}: UTC no data")
                except Exception as e:
                    errors.append(f"{base}: UTC error - {type(e).__name__} - {str(e)[:160]}")

            if closed is None:
                skipped.append(f"{base}: {last_reason or 'no data'}")
                progress.progress(i / len(scan_list))
                continue

            try:
                nr4_flags, nr7_flags, nr10_flags = compute_nr_flags(closed)
                last_nr10 = bool(nr10_flags[-1])
                last_nr7 = bool(nr7_flags[-1])
                last_nr4 = bool(nr4_flags[-1])

                nr10 = want_nr10 and last_nr10
                nr7 = want_nr7 and last_nr7
                nr4 = want_nr4 and last_nr4

                setup_time, setup_type, breakout_state, breakout_tag, up_count, down_count, rh, rl = simulate_breakouts_since_last_nr(closed)
                last_close = float(closed[-1]["close"])

                in_nr_range = False
                if rh is not None and rl is not None:
                    lo = min(rl, rh)
                    hi = max(rl, rh)
                    in_nr_range = (lo <= last_close <= hi)

                if show_inrange_only and (not in_nr_range):
                    progress.progress(i / len(scan_list))
                    continue

                show_row = (nr10 or nr7 or nr4) or (show_inrange_only and in_nr_range)
                if show_row:
                    results.append({
                        "symbol": base,
                        "name": name,
                        "NR10": nr10,
                        "NR7": nr7,
                        "NR4": nr4,
                        "in_nr_range_now": in_nr_range,
                        "breakout_state": breakout_state,
                        "breakout_tag": breakout_tag,
                        "nr_setup_type": setup_type,
                        "nr_setup_time": setup_time,
                        "exchange_used": exchange_used,
                        "pair_used": pair_used,
                        "data_source": data_source,
                        "last_close": last_close,
                        "range_low": rl,
                        "range_high": rh,
                        "coingecko_id": coin_id,
                    })
            except Exception as e:
                errors.append(f"{base}: calc error - {type(e).__name__} - {str(e)[:160]}")

            progress.progress(i / len(scan_list))

    df = pd.DataFrame(results)
    if df.empty:
        st.warning(f"Keine Treffer. Skipped: {len(skipped)} | Errors: {len(errors)}")
        if skipped or errors:
            with st.expander("Report"):
                for s in skipped[:200]:
                    st.write(s)
                for e in errors[:200]:
                    st.write(e)
        return

    # Sort
    df = df.sort_values(["in_nr_range_now","NR10","NR7","NR4","symbol"], ascending=[False, False, False, False, True]).reset_index(drop=True)

    # Display dataframe prep
    df_disp = df.copy()
    df_disp["Pattern"] = df_disp.apply(lambda r: mk_pattern_badge(bool(r["NR10"]), bool(r["NR7"]), bool(r["NR4"])), axis=1)
    df_disp["Range"] = df_disp["in_nr_range_now"].apply(lambda x: mk_range_badge(bool(x)))
    df_disp["Breakout"] = df_disp.apply(lambda r: mk_breakout_badge(str(r["breakout_state"]), str(r["breakout_tag"])), axis=1)
    df_disp["Ex"] = df_disp["exchange_used"].apply(short_ex)

    st.markdown('<div class="nr-card">', unsafe_allow_html=True)
    st.markdown(f"""
<div class="badge badge-accent">✅ Treffer: <b>{len(df_disp)}</b></div>
<div style="height:8px"></div>
<div class="badge">⏭️ Skipped: <b>{len(skipped)}</b></div>
<div class="badge">⚠️ Errors: <b>{len(errors)}</b></div>
""", unsafe_allow_html=True)
    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    if view_mode == "Kompakt (Mobile)":
        compact = df_disp[["symbol","Pattern","Range","Breakout","Ex"]].copy()
        compact.rename(columns={"symbol":"Coin"}, inplace=True)
        st.dataframe(compact, use_container_width=True, hide_index=True)

        with st.expander("Details zu Treffern"):
            for _, r in df_disp.iterrows():
                with st.expander(f"{r['symbol']} — {r['name']}"):
                    st.write(f"**Provider:** {r['exchange_used']}  |  **Pair:** {r['pair_used']}  |  **Quelle:** {r['data_source']}")
                    st.write(f"**Pattern:** {r['Pattern']}")
                    st.write(f"**Range:** {r['Range']}")
                    st.write(f"**Breakout:** {r['Breakout']}")
                    st.write(f"**NR Setup:** {r['nr_setup_type']} @ {r['nr_setup_time']}")
                    st.write(f"**Last Close:** {r['last_close']}")
                    st.write(f"**Range Low/High:** {r['range_low']} / {r['range_high']}")
                    if r.get("coingecko_id"):
                        st.caption(f"coingecko_id: {r['coingecko_id']}")
    else:
        detail = df_disp[["symbol","name","Pattern","Range","Breakout","nr_setup_type","nr_setup_time","Ex","data_source"]].copy()
        detail.rename(columns={"symbol":"Coin","name":"Name","nr_setup_type":"Setup","nr_setup_time":"Setup Time","data_source":"Source"}, inplace=True)
        st.dataframe(detail, use_container_width=True, hide_index=True)

        with st.expander("Technische Details (Pair/Range/Close)"):
            tech = df_disp[["symbol","exchange_used","pair_used","data_source","last_close","range_low","range_high","coingecko_id"]].copy()
            tech.rename(columns={"symbol":"Coin"}, inplace=True)
            st.dataframe(tech, use_container_width=True, hide_index=True)

    st.download_button("CSV Export (voll)", df.to_csv(index=False).encode("utf-8"), file_name=f"nr_scan_{tf}.csv", mime="text/csv", use_container_width=True)

    st.markdown('</div>', unsafe_allow_html=True)

    if skipped or errors:
        with st.expander("Report (nicht gescannt / Fehler)"):
            if skipped:
                st.write("**Skipped (kurz):**")
                for s in skipped[:300]:
                    st.write(s)
                if len(skipped) > 300:
                    st.caption(f"... und {len(skipped)-300} weitere")
            if errors:
                st.write("**Errors (kurz):**")
                for e in errors[:200]:
                    st.write(e)
                if len(errors) > 200:
                    st.caption(f"... und {len(errors)-200} weitere")

if __name__ == "__main__":
    main()
