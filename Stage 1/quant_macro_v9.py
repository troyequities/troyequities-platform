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
# 1. ENGINE CONFIGURATION (ZERO API COSTS)
# =====================================================================
SEC_HEADERS = {
    "User-Agent": "MacroQuant Research Engine (mailto:quant.team@domain.com)", 
    "Accept-Encoding": "gzip, deflate"
}

BENCHMARK_TICKER = "SPY"

# =====================================================================
# 2. 100% FREE CONGRESSIONAL DATA LAYER (GITHUB OPEN DATA LAKES)
# =====================================================================
def fetch_free_congressional_layer(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    """
    Bypasses all API paywalls and Cloudflare blockers by pulling STOCK Act 
    disclosures directly from active 2026 open-source GitHub repositories.
    """
    print(f"[+] Querying open-source GitHub Data Lakes for ${ticker.upper()}...")
    cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    
    # Actively maintained raw JSON dumps (Immune to WAF/Cloudflare blocking)
    endpoints = [
        "https://raw.githubusercontent.com/kadoa-org/congress-trading-monitor/main/public/data/trades.json",
        "https://raw.githubusercontent.com/wisesheets/congressional-trades/main/data/all_trades.json"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }
    
    records = []
    for url in endpoints:
        repo_name = url.split('.com/')[-1].split('/')[0]
        try:
            res = requests.get(url, headers=headers, timeout=10)
            
            if res.status_code != 200:
                print(f"  [-] {repo_name} returned HTTP {res.status_code}")
                continue
                
            data = res.json()
            if not isinstance(data, list):
                continue
                
            match_count = 0
            for row in data:
                # Universal ticker extraction
                row_ticker = str(row.get("ticker", row.get("symbol", row.get("Ticker", "")))).upper().strip()
                if row_ticker != ticker.upper():
                    continue
                
                # Universal date extraction
                date_str = str(row.get("disclosure_date", row.get("transaction_date", row.get("ReportDate", ""))))[:10]
                if not date_str or date_str < cutoff_date:
                    continue
                
                # Universal entity extraction
                entity = str(row.get("representative", row.get("senator", row.get("name", "Lawmaker")))).strip()
                chamber = str(row.get("chamber", row.get("house", "Congress"))).capitalize()
                
                # Normalize Buy/Sell signal
                tx_type = str(row.get("type", row.get("transaction", ""))).upper()
                if any(w in tx_type for w in ["BUY", "PURCHASE", "P"]):
                    pos = "BUY"
                elif any(w in tx_type for w in ["SELL", "S", "SALE"]):
                    pos = "SELL"
                elif any(w in tx_type for w in ["SHORT", "PUT"]):
                    pos = "SHORT"
                else:
                    continue
                
                # Universal dollar value estimation
                val_str = str(row.get("amount", row.get("value", "15000"))).replace("$", "").replace(",", "")
                est_val = 15000.0
                if "-" in val_str:
                    try:
                        parts = [float(p.strip()) for p in val_str.split("-") if p.strip().replace('.','',1).isdigit()]
                        if len(parts) == 2:
                            est_val = sum(parts) / 2.0
                    except Exception:
                        pass
                elif val_str.replace('.','',1).isdigit():
                    est_val = float(val_str)
                
                records.append({
                    "Date": date_str,
                    "Entity": f"CONGRESS: {entity} ({chamber})"[:26],
                    "Source": f"Congress ({chamber})",
                    "Position": pos,
                    "Volume": f"${est_val:,.0f}",
                    "Est_Value": est_val
                })
                match_count += 1
            
            if match_count > 0:
                print(f"  [+] {repo_name}: Extracted {match_count} political trades.")
                
        except Exception as e:
            print(f"  [!] {repo_name} Connection Error: {str(e)[:50]}")
            pass
            
    if records:
        print(f"[+] Successfully structured {len(records)} total political trades.")
    else:
        print(f"[-] No political trades found for ${ticker.upper()} in the last {lookback_days} days.")
        
    return pd.DataFrame(records)

# =====================================================================
# 3. INSTITUTIONAL SEC EDGAR DATA LAYER
# =====================================================================
def get_cik(ticker: str) -> str:
    url = "https://www.sec.gov/files/company_tickers.json"
    res = requests.get(url, headers=SEC_HEADERS, timeout=10)
    res.raise_for_status()
    for item in res.json().values():
        if item["ticker"].upper() == ticker.upper():
            return str(item["cik_str"]).zfill(10)
    raise ValueError(f"Ticker {ticker} not found on SEC EDGAR.")

def fetch_sec_layer(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    print(f"[+] Querying SEC EDGAR Corporate Filings for ${ticker.upper()}...")
    try:
        cik = get_cik(ticker)
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        res = requests.get(url, headers=SEC_HEADERS, timeout=10)
        res.raise_for_status()
        filings = res.json()["filings"]["recent"]
        
        cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        records = []
        
        for idx, form in enumerate(filings["form"]):
            file_date = filings["filingDate"][idx]
            if file_date < cutoff_date:
                continue
            if form in ["4", "4/A"]:
                acc = filings["accessionNumber"][idx]
                clean_acc = acc.replace("-", "")
                doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{clean_acc}/{acc}.txt"
                
                try:
                    doc_res = requests.get(doc_url, headers=SEC_HEADERS, timeout=6)
                    time.sleep(0.08)  # Respect SEC EDGAR rate limits
                    if doc_res.status_code != 200:
                        continue
                    
                    xml_match = re.search(r"<ownershipDocument>.*?</ownershipDocument>", doc_res.text, re.DOTALL)
                    if not xml_match:
                        continue
                    
                    root = ET.fromstring(xml_match.group(0))
                    owner_name = root.find(".//reportingOwner/reportingOwnerId/rptOwnerName").text
                    
                    rel_node = root.find(".//reportingOwner/reportingOwnerRelationship")
                    role = "Officer"
                    if rel_node is not None:
                        if rel_node.find("isDirector") is not None and rel_node.find("isDirector").text in ['true', '1']:
                            role = "Director"
                        elif rel_node.find("isTenPercentOwner") is not None and rel_node.find("isTenPercentOwner").text in ['true', '1']:
                            role = "10% Owner"
                            
                    for tx in root.findall(".//nonDerivativeTransaction"):
                        code = tx.find(".//transactionCoding/transactionCode").text
                        if code in ["P", "S"]:
                            shares = float(tx.find(".//transactionAmounts/transactionShares/value").text or 0)
                            price = float(tx.find(".//transactionAmounts/transactionPricePerShare/value").text or 0)
                            pos_type = "BUY" if code == "P" else "SELL"
                            
                            records.append({
                                "Date": file_date,
                                "Entity": owner_name[:26],
                                "Source": f"SEC Corporate ({role})",
                                "Position": pos_type,
                                "Volume": f"{shares:,.0f} shs",
                                "Est_Value": round(shares * price, 2)
                            })
                except Exception:
                    continue
                    
        print(f"[+] Successfully extracted {len(records)} SEC Corporate transactions.")
        return pd.DataFrame(records)
        
    except Exception as e:
        print(f"[!] SEC EDGAR Warning: {e}")
        return pd.DataFrame()

# =====================================================================
# 4. UNIFIED DATA PIPELINE
# =====================================================================
def get_unified_flow_data(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    pol_df = fetch_free_congressional_layer(ticker, lookback_days)
    sec_df = fetch_sec_layer(ticker, lookback_days)
    
    master_frames = [df for df in [sec_df, pol_df] if not df.empty]
    if not master_frames:
        return pd.DataFrame()
        
    unified_df = pd.concat(master_frames, ignore_index=True)
    unified_df["Date"] = pd.to_datetime(unified_df["Date"])
    return unified_df.sort_values(by="Date", ascending=False).reset_index(drop=True)

# =====================================================================
# 5. CORE ALGORITHMIC SCORING ENGINE (PETER LYNCH ASYMMETRIC CONVICTION)
# =====================================================================
def apply_alpha_scoring_math(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    today = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))

    # 1. Exponential Time Decay (45-day half-life)
    df["Days_Old"] = (today - df["Date"]).dt.days
    decay_constant = np.log(2) / 45
    df["Decay_Weight"] = np.exp(-decay_constant * df["Days_Old"])

    # 2. Logarithmic Transaction Sizing
    df["Est_Value"] = pd.to_numeric(df["Est_Value"], errors='coerce').fillna(0)
    df["Size_Conviction"] = np.log10(df["Est_Value"].clip(lower=1000)) - 2 

    # 3. Asymmetric Lynch Conviction Multipliers
    def map_lynch_conviction(row):
        pos = row["Position"]
        source = row["Source"]
        
        if pos == "BUY":
            base = 3.0
        elif pos == "SHORT":
            base = -2.5
        else:
            base = -1.0  # SELL (Standard Liquidation)
            
        # 1.5x Macro Insight Bonus for Capitol Hill Lawmakers
        if "Congress" in source:
            base *= 1.5
            
        return base

    df["Direction"] = df.apply(map_lynch_conviction, axis=1)
    df["Raw_Score"] = df["Direction"] * df["Size_Conviction"] * df["Decay_Weight"]

    # 4. Cluster Grouping (7-day rolling window)
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
# MODE 1: LIVE REAL-TIME TERMINAL SCANNER (365-DAY HORIZON)
# =====================================================================
def run_live_scanner(ticker: str):
    print(f"\n[+] Executing Unified Dual-Mandate Scan for ${ticker} (Lookback: 365 Days)...")
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
# MODE 2: 5-YEAR HISTORICAL RESEARCH & BACKTESTER (1825-DAY HORIZON)
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
    
    # Apply Peter Lynch Asymmetric Weights (3x Buy / 1x Sell / -2.5x Short)
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
    print(f" Benchmark: S&P 500 ({BENCHMARK_TICKER}) | Model: Lynch Asymmetric Conviction (3x Buy / 1x Sell)")
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
        print(" MACROQUANT DUAL-MANDATE TERMINAL (ZERO COST ENGINE)")
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