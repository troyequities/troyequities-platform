import xml.etree.ElementTree as ET
import re
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import yfinance as yf
import warnings

# Suppress pandas/yfinance future warnings for a clean terminal output
warnings.simplefilter(action='ignore', category=FutureWarning)

# =====================================================================
# 1. ENGINE CONFIGURATION (FINNHUB API)
# =====================================================================
# Paste your Finnhub API Key (from finnhub.io/dashboard) below:
FINNHUB_API_KEY = "d9nkj11r01qvumgam520d9nkj11r01qvumgam52g"

BENCHMARK_TICKER = "SPY"

# =====================================================================
# 2. CORPORATE INSIDER DATA LAYER (FINNHUB)
# =====================================================================
def fetch_finnhub_insider(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    """
    Pulls live Corporate Insider trades using the Finnhub Insider API.
    """
    print(f"[+] Querying Finnhub Corporate Insider API for ${ticker.upper()}...")
    
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    
    url = f"https://finnhub.io/api/v1/stock/insider-transactions?symbol={ticker.upper()}&from={from_date}&to={to_date}&token={FINNHUB_API_KEY}"
    
    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            print(f"  [-] Finnhub Corporate API returned HTTP {res.status_code}")
            return pd.DataFrame()
            
        data = res.json().get("data", [])
        records = []
        
        for row in data:
            date_str = str(row.get("filingDate", row.get("transactionDate", "")))[:10]
            if not date_str:
                continue
                
            entity = str(row.get("name", "Corporate Insider")).strip()
            
            change = float(row.get("change", 0))
            if change > 0:
                pos = "BUY"
            elif change < 0:
                pos = "SELL"
            else:
                continue
            
            shares = abs(change)
            price = float(row.get("transactionPrice", 0))
            
            # Filter out zero-price basis option executions
            if price <= 0:
                continue
                
            records.append({
                "Date": date_str,
                "Entity": entity[:26],
                "Source": "SEC Corporate (Finnhub)",
                "Position": pos,
                "Volume": f"{shares:,.0f} shs",
                "Est_Value": round(shares * price, 2)
            })
            
        if records:
            print(f"  [+] Successfully extracted {len(records)} Finnhub corporate transactions.")
        else:
            print(f"  [-] No corporate transactions returned for ${ticker.upper()}.")
            
        return pd.DataFrame(records)
        
    except Exception as e:
        print(f"  [!] Finnhub Corporate API Error: {str(e)[:50]}")
        return pd.DataFrame()

# =====================================================================
# 3. CONGRESSIONAL DATA LAYER (FINNHUB WITH OPEN FALLBACK)
# =====================================================================
def fetch_congressional_data(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    """
    Attempts to pull Congressional trades via Finnhub. If Finnhub returns 403 
    (indicating standard tier restrictions), falls back seamlessly to open datasets.
    """
    print(f"[+] Querying Finnhub Congressional API for ${ticker.upper()}...")
    
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    url = f"https://finnhub.io/api/v1/stock/congressional-trading?symbol={ticker.upper()}&from={from_date}&to={to_date}&token={FINNHUB_API_KEY}"
    
    try:
        res = requests.get(url, timeout=10)
        
        # If Finnhub key has access to Congressional Data
        if res.status_code == 200:
            data = res.json().get("data", [])
            records = []
            for row in data:
                date_str = str(row.get("disclosureDate", row.get("transactionDate", "")))[:10]
                if not date_str:
                    continue
                entity = str(row.get("representative", row.get("name", "Lawmaker"))).strip()
                tx_type = str(row.get("type", "")).upper()
                pos = "BUY" if "BUY" in tx_type else ("SELL" if "SELL" in tx_type else "UNKNOWN")
                if pos == "UNKNOWN":
                    continue
                
                val_str = str(row.get("amount", "15000")).replace("$", "").replace(",", "")
                est_val = 15000.0
                if "-" in val_str:
                    parts = [float(p.strip()) for p in val_str.split("-") if p.strip().replace('.','',1).isdigit()]
                    if len(parts) == 2:
                        est_val = sum(parts) / 2.0
                elif val_str.replace('.','',1).isdigit():
                    est_val = float(val_str)
                    
                records.append({
                    "Date": date_str,
                    "Entity": f"CONGRESS: {entity}"[:26],
                    "Source": "Congress (Finnhub)",
                    "Position": pos,
                    "Volume": f"${est_val:,.0f}",
                    "Est_Value": est_val
                })
            if records:
                print(f"  [+] Extracted {len(records)} Congressional trades via Finnhub.")
                return pd.DataFrame(records)

        elif res.status_code == 403:
            print("  [-] Finnhub Congressional API requires tier upgrade (HTTP 403). Redirecting to Open Data Lakes...")
            
    except Exception as e:
        print(f"  [!] Finnhub Congressional API Error: {str(e)[:50]}")

    # Fallback Open Data Lakes
    cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    open_url = "https://raw.githubusercontent.com/kadoa-org/congress-trading-monitor/main/public/data/trades.json"
    
    try:
        res = requests.get(open_url, timeout=10)
        if res.status_code == 200 and isinstance(res.json(), list):
            data = res.json()
            records = []
            for row in data:
                row_ticker = str(row.get("ticker", row.get("symbol", ""))).upper().strip()
                if row_ticker != ticker.upper():
                    continue
                date_str = str(row.get("disclosure_date", row.get("transaction_date", "")))[:10]
                if not date_str or date_str < cutoff_date:
                    continue
                entity = str(row.get("representative", row.get("senator", "Lawmaker"))).strip()
                chamber = str(row.get("chamber", "Congress")).capitalize()
                tx_type = str(row.get("type", "")).upper()
                pos = "BUY" if "BUY" in tx_type else ("SELL" if "SELL" in tx_type else "UNKNOWN")
                if pos == "UNKNOWN":
                    continue
                
                records.append({
                    "Date": date_str,
                    "Entity": f"CONGRESS: {entity} ({chamber})"[:26],
                    "Source": f"Congress ({chamber})",
                    "Position": pos,
                    "Volume": "$15,000",
                    "Est_Value": 15000.0
                })
            if records:
                print(f"  [+] Extracted {len(records)} Congressional trades via Open Data Lakes.")
                return pd.DataFrame(records)
    except Exception:
        pass

    print(f"  [-] No political trades located for ${ticker.upper()}.")
    return pd.DataFrame()

# =====================================================================
# 4. UNIFIED DATA PIPELINE
# =====================================================================
def get_unified_flow_data(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "YOUR_FINNHUB_API_KEY_HERE":
        print("\n[-] FATAL: Please replace YOUR_FINNHUB_API_KEY_HERE on Line 17 with your actual Finnhub key.")
        return pd.DataFrame()
        
    sec_df = fetch_finnhub_insider(ticker, lookback_days)
    pol_df = fetch_congressional_data(ticker, lookback_days)
    
    master_frames = [df for df in [sec_df, pol_df] if not df.empty]
    if not master_frames:
        return pd.DataFrame()
        
    unified_df = pd.concat(master_frames, ignore_index=True)
    unified_df["Date"] = pd.to_datetime(unified_df["Date"])
    return unified_df.sort_values(by="Date", ascending=False).reset_index(drop=True)

# =====================================================================
# 5. CORE ALGORITHMIC SCORING ENGINE
# =====================================================================
def apply_alpha_scoring_math(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    today = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))

    df["Days_Old"] = (today - df["Date"]).dt.days
    decay_constant = np.log(2) / 45
    df["Decay_Weight"] = np.exp(-decay_constant * df["Days_Old"])

    df["Est_Value"] = pd.to_numeric(df["Est_Value"], errors='coerce').fillna(0)
    df["Size_Conviction"] = np.log10(df["Est_Value"].clip(lower=1000)) - 2 

    def map_lynch_conviction(row):
        pos = row["Position"]
        source = row["Source"]
        
        if pos == "BUY":
            base = 3.0
        elif pos == "SHORT":
            base = -2.5
        else:
            base = -1.0 
            
        if "Congress" in source:
            base *= 1.5
            
        return base

    df["Direction"] = df.apply(map_lynch_conviction, axis=1)
    df["Raw_Score"] = df["Direction"] * df["Size_Conviction"] * df["Decay_Weight"]

    df = df.sort_values("Date").reset_index(drop=True)
    cluster_multipliers = []
    
    for i, row in df.iterrows():
        mask = (
            (df["Date"] >= row["Date"] - pd.Timedelta(days=7)) &
            (df["Date"] <= row["Date"] + pd.Timedelta(days=7)) &
            (np.sign(df["Direction"]) == np.sign(row["Direction"]))
        )
        unique_insiders = df.loc[mask, "Entity"].nunique()
        if unique_insiders == 1: mult = 1.0
        elif unique_insiders == 2: mult = 1.5
        else: mult = 2.0
        cluster_multipliers.append(mult)

    df["Cluster_Mult"] = cluster_multipliers
    df["Alpha_Score"] = df["Raw_Score"] * df["Cluster_Mult"]
    df["Date_Str"] = df["Date"].dt.strftime('%Y-%m-%d')
    
    return df.sort_values(by="Date", ascending=False)

def get_normalized_signal(total_alpha: float) -> tuple:
    k_constant = 0.15 
    rating_1_to_10 = 1 + (9 / (1 + np.exp(-k_constant * total_alpha)))
    
    if rating_1_to_10 >= 7.5: signal = "STRONG BUY"
    elif rating_1_to_10 >= 6.0: signal = "BUY"
    elif rating_1_to_10 <= 2.5: signal = "STRONG SHORT"
    elif rating_1_to_10 <= 4.0: signal = "SHORT"
    else: signal = "NEUTRAL"
    
    return round(rating_1_to_10, 2), signal

# =====================================================================
# MODE 1: LIVE REAL-TIME TERMINAL SCANNER
# =====================================================================
def run_live_scanner(ticker: str):
    print(f"\n[+] Executing Unified Dual-Mandate Scan for ${ticker}...")
    raw_df = get_unified_flow_data(ticker, lookback_days=365)
    
    if raw_df.empty:
        print(f"[-] Operational Failure: Zero records returned for ${ticker}.")
        return
        
    scored_df = apply_alpha_scoring_math(raw_df)
    total_raw_alpha = scored_df["Alpha_Score"].sum()
    rating, macro_signal = get_normalized_signal(total_raw_alpha)

    print("\n" + "═"*100)
    print(f" DUAL-MANDATE ALPHA MATRIX REPORT: {ticker}")
    print("═"*100)
    print(f" Tracked Institutional / Political Records: {len(scored_df)}")
    print(f" Raw Unbounded Matrix Score:                {total_raw_alpha:+.2f}")
    print(f" Normalized Alpha Rating (1-10):            {rating} / 10.0")
    print(f" Current Algorithmic Signal:                {macro_signal}")
    print("═"*100)
    
    display_df = scored_df[["Date_Str", "Entity", "Source", "Position", "Volume", "Cluster_Mult", "Alpha_Score"]].copy()
    display_df.rename(columns={"Date_Str": "Date Disclosed", "Volume": "Est. Value"}, inplace=True)
    display_df["Alpha_Score"] = display_df["Alpha_Score"].apply(lambda x: f"{x:+.2f}")
    
    print("\nChronological Execution Feed Matrix (Top 35 Scored Events):")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(display_df.head(35).to_string(index=False))

# =====================================================================
# MODE 2: 5-YEAR HISTORICAL RESEARCH & BACKTESTER
# =====================================================================
def run_5yr_backtest(ticker: str):
    print(f"\n[+] Initiating 5-Year Dual-Mandate Backtest for ${ticker}...")
    
    trades_df = get_unified_flow_data(ticker, lookback_days=1825)
    if trades_df.empty:
        print(f"[-] Operational Failure: Zero historical records found for ${ticker}.")
        return

    print(f"[+] Extracted {len(trades_df)} total transactions. Downloading daily pricing engines...")
    five_yrs_ago = (datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d")
    
    try:
        price_data = yf.download([ticker, BENCHMARK_TICKER], start=five_yrs_ago, progress=False)["Close"]
        price_data.index = pd.to_datetime(price_data.index).tz_localize(None)
    except Exception as e:
        print(f"[-] Market Data Error: Unable to fetch yfinance pricing -> {e}")
        return

    daily_groups = trades_df.groupby(["Date", "Position"])["Est_Value"].sum().unstack(fill_value=0)
    for col in ["BUY", "SELL", "SHORT"]:
        if col not in daily_groups: daily_groups[col] = 0
    
    buy_conviction = 3.0 * np.log10(daily_groups["BUY"].clip(lower=1000))
    sell_conviction = 1.0 * np.log10(daily_groups["SELL"].clip(lower=1000))
    short_conviction = 2.5 * np.log10(daily_groups["SHORT"].clip(lower=1000))
    
    daily_groups["Net_Conviction"] = buy_conviction - (sell_conviction + short_conviction)
    
    def assign_tier(score):
        if score >= 6.0: return "1. STRONG BUY"
        elif score >= 2.0: return "2. BUY"
        elif score <= -6.0: return "5. STRONG SHORT"
        elif score <= -2.0: return "4. SHORT"
        else: return "3. NEUTRAL"
        
    daily_groups["Signal_Tier"] = daily_groups["Net_Conviction"].apply(assign_tier)

    horizons = {"3-Day": 3, "2-Week": 10, "4-Week": 20, "3-Month": 63}
    results = []

    for trade_date, row in daily_groups.iterrows():
        valid_dates = price_data.index[price_data.index >= trade_date]
        if len(valid_dates) == 0:
            continue
        
        start_idx = price_data.index.get_loc(valid_dates[0])
        tier = row["Signal_Tier"]
        
        for h_label, h_days in horizons.items():
            end_idx = start_idx + h_days
            if end_idx >= len(price_data):
                continue
                
            p_start = price_data.iloc[start_idx]
            p_end = price_data.iloc[end_idx]
            
            abs_ret = (p_end[ticker] - p_start[ticker]) / p_start[ticker]
            bench_ret = (p_end[BENCHMARK_TICKER] - p_start[BENCHMARK_TICKER]) / p_start[BENCHMARK_TICKER]
            
            results.append({
                "Tier": tier,
                "Horizon": h_label,
                "Absolute_Return": abs_ret * 100,
                "Market_Alpha": (abs_ret - bench_ret) * 100
            })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        print("[-] Insufficient forward price data to complete backtest.")
        return

    summary = results_df.groupby(["Tier", "Horizon"]).agg(
        Sample_Size=("Absolute_Return", "count"),
        Avg_Absolute_Return=("Absolute_Return", "mean"),
        Avg_Market_Alpha=("Market_Alpha", "mean")
    ).reset_index()

    print("\n" + "═"*100)
    print(f" SPRINT 3: 5-YEAR HISTORICAL BACKTEST PERFORMANCE REPORT: {ticker}")
    print(f" Benchmark: S&P 500 ({BENCHMARK_TICKER}) | Model: Lynch Asymmetric Conviction")
    print("═"*100)
    
    abs_pivot = summary.pivot(index="Tier", columns="Horizon", values="Avg_Absolute_Return")[["3-Day", "2-Week", "4-Week", "3-Month"]]
    alpha_pivot = summary.pivot(index="Tier", columns="Horizon", values="Avg_Market_Alpha")[["3-Day", "2-Week", "4-Week", "3-Month"]]
    
    print("\n--- 1. ABSOLUTE FORWARD RETURNS (%) ---")
    print(abs_pivot.round(2).to_string())
    
    print("\n--- 2. MARKET-ADJUSTED ALPHA (%) [Stock Return - SPY Return] ---")
    print(alpha_pivot.round(2).to_string())
    print("═"*100)

# =====================================================================
# 6. SYSTEM ORCHESTRATION MENU
# =====================================================================
if __name__ == "__main__":
    while True:
        print("\n" + "─"*50)
        print(" MACROQUANT DUAL-MANDATE TERMINAL (FINNHUB ENGINE)")
        print("─"*50)
        print(" [1] Live Real-Time Scanner (365-Day Lookback)")
        print(" [2] 5-Year Historical Backtest (1825-Day Lookback)")
        print(" [3] Exit Engine")
        
        choice = input("\nSelect Execution Mode (1-3): ").strip()
        if choice == "3" or choice.lower() == "exit":
            break
        elif choice in ["1", "2"]:
            target = input("Enter Stock Ticker (e.g., GS, NVDA, AAPL): ").strip().upper()
            if not target:
                continue
            if choice == "1":
                run_live_scanner(target)
            else:
                run_5yr_backtest(target)
        else:
            print("[-] Invalid Selection. Please choose 1, 2, or 3.")