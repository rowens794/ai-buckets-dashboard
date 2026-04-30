from pathlib import Path
import json
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

GROUPS = [
    {"Category":"Beneficiary","Layer":"Physical infrastructure","Tickers":"VST, CEG, NRG, TLN, GEV, ETN, PWR, EME, FIX, VRT, EQIX, DLR"},
    {"Category":"Beneficiary","Layer":"Semiconductors & hardware","Tickers":"NVDA, AMD, AVGO, MRVL, ASML, AMAT, KLAC, LRCX, MU, TSM, ARM, SMCI"},
    {"Category":"Beneficiary","Layer":"Cloud & compute infrastructure","Tickers":"ORCL, NET, CRWV, NBIS, CLS, DELL, HPE, PSTG, WDC, STX"},
    {"Category":"Beneficiary","Layer":"Networking & connectivity","Tickers":"CSCO, ANET, COHR, LITE, VIAV, CIEN, NOK, ERIC, JNPR, ALAB, FN"},
    {"Category":"Beneficiary","Layer":"Public AI platforms / foundation-adjacent","Tickers":"AI, SOUN, BBAI, RDDT, TEM"},
    {"Category":"Beneficiary","Layer":"Data & MLOps","Tickers":"SNOW, MDB, DDOG, CFLT, ESTC, GTLB, DT, IOT"},
    {"Category":"Beneficiary","Layer":"AI-enabled vertical apps","Tickers":"PLTR, PATH, VEEV, DOCS, DUOL, APPF, VERX, PAYC"},
    {"Category":"Beneficiary","Layer":"Robotics & physical AI","Tickers":"ABBNY, ROK, ISRG, MBLY, SYM, TER, CGNX, OUST, LAZR, SERV"},
    {"Category":"Disrupted","Layer":"Legacy SaaS / seat-based software","Tickers":"CRM, ADBE, WDAY, HUBS, ZM, DOCU, ASAN, MNDY, TEAM, BOX"},
    {"Category":"Disrupted","Layer":"IT services & outsourcing","Tickers":"ACN, CTSH, WIT, INFY, EPAM, GLOB, DXC, EXLS, WNS, TTEC"},
    {"Category":"Disrupted","Layer":"Professional services / staffing","Tickers":"MAN, KFRC, RHI, KFY, BBSI, NSP, HURN, CRAI, HQI, TBI"},
    {"Category":"Disrupted","Layer":"Digital advertising & search-adjacent","Tickers":"TTD, IAS, DV, ROKU, PINS, SNAP, YELP, GCI"},
    {"Category":"Disrupted","Layer":"Content & media","Tickers":"NYT, WBD, PSKY, NWS, IAC, ZD, GTN, SSP, SCHL, TGNA"},
    {"Category":"Disrupted","Layer":"Online education & credentialing","Tickers":"CHGG, UDMY, LRN, COUR, STRA, LINC, ATGE, LAUR, PRDO, AFYA"},
    {"Category":"Disrupted","Layer":"Legal, compliance & information services","Tickers":"TRI, FICO, BILL, DFIN, EFX, WK, RELX, MCO, SPGI"},
]

for i, g in enumerate(GROUPS, 1):
    g["Group_ID"] = f"V2G{i:02d}"
    g["Ticker_List"] = [x.strip().upper() for x in g["Tickers"].split(",")]
    g["Label"] = f"{g['Group_ID']} | {g['Category']} | {g['Layer']}"

unique = sorted({t for g in GROUPS for t in g["Ticker_List"]})
raw = yf.download(unique, period="2y", interval="1d", auto_adjust=False, actions=True, progress=False, group_by="column", threads=True)
if raw.empty:
    raise SystemExit("No Yahoo Finance data returned")
adj = raw["Adj Close"].copy() if "Adj Close" in raw.columns.get_level_values(0) else raw["Close"].copy()
adj.index = pd.to_datetime(adj.index).tz_localize(None)
adj = adj.sort_index().reindex(columns=unique).dropna(how="all")
start = adj.index.min()
valid = [t for t in unique if t in adj and not adj[t].dropna().empty]
full = [t for t in valid if pd.notna(adj.loc[start, t])]
missing = [t for t in unique if t not in valid]
partial = [t for t in valid if t not in full]

def group_index(group, full_period_only=True):
    tickers = [t for t in group["Ticker_List"] if t in (full if full_period_only else valid)]
    if not tickers:
        return pd.Series(index=adj.index, dtype="float64")
    prices = adj[tickers].copy()
    indexed = prices.divide(prices.apply(lambda s: s.dropna().iloc[0] if not s.dropna().empty else pd.NA), axis=1) * 100
    return indexed.mean(axis=1, skipna=True)

out = pd.DataFrame({"Date": adj.index.strftime("%Y-%m-%d")})
meta = []
for g in GROUPS:
    out[g["Label"]] = group_index(g, full_period_only=True).values
    included = [t for t in g["Ticker_List"] if t in full]
    meta.append({
        "group_id": g["Group_ID"], "category": g["Category"], "layer": g["Layer"],
        "label": g["Label"], "requested_tickers": g["Ticker_List"],
        "included_full_period": included,
        "partial_history": [t for t in g["Ticker_List"] if t in partial],
        "missing": [t for t in g["Ticker_List"] if t in missing],
    })

out.to_csv(DATA / "bucket_indexes.csv", index=False)
(DATA / "metadata.json").write_text(json.dumps({
    "source": "Yahoo Finance via yfinance",
    "price_field": "Adj Close (dividend/split adjusted)",
    "start_date": str(adj.index.min().date()),
    "end_date": str(adj.index.max().date()),
    "valid_tickers": len(valid),
    "full_period_tickers": len(full),
    "partial_history_tickers": partial,
    "missing_tickers": missing,
    "groups": meta,
}, indent=2))
print(f"Wrote {DATA / 'bucket_indexes.csv'}")
