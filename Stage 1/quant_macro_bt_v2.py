import xml.etree.ElementTree as ET
import re
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import yfinance as yf

# =====================================================================
# BACKTEST CONFIGURATION (5-YEAR HORIZON)
# =====================================================================
SEC_HEADERS = {
    "User-Agent": "MacroQuant Research Engine (research@macroquant.com)", 
    "Accept-Encoding": "gzip, deflate"
}

FIVE_YEARS_AGO = (datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d")
BENCHMARK_TICKER = "SPY"

# =====================================================================
# HISTORICAL DATA INGESTION PIPELINE (SEC + CONGRESS)
# =====================================================================
def get_cik(ticker: str) -> str:
    url = "https://www.sec.gov/files/company_tickers.json"
    res = requests.get(url, headers=SEC_HEADERS)
    res.raise_for_status()
    for item in res.json().values():
        if item["ticker"].upper() == ticker.upper():
            return str(item["cik_str"]).zfill(10)
    raise ValueError(f"Ticker {ticker} not found.")

def fetch_5yr_sec_trades(ticker: str) -> pd.DataFrame:
    try:
        cik = get_cik(ticker)
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        res = requests.get(url, headers=SEC_HEADERS)
        res.raise_for_status()
        filings = res.json()["filings"]["recent"]
        
        records = []
        for idx, form in enumerate(filings["form"]):
            file_date = filings["filingDate"][idx]
            if file_date < FIVE_YEARS_AGO:
                continue
            if form == "4":
                acc = filings["accessionNumber"][idx]
                clean_acc = acc.replace("-", "")
                doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{clean_acc}/{acc}.txt"
                
                try:
                    doc_res = requests.get(doc_url, headers=SEC_HEADERS)
                    time.sleep(0.08)
                    if doc_res.status_code != 200:
                        continue
                    
                    xml_match = re.search(r"<ownershipDocument>.*?</ownershipDocument>", doc_res.text, re.DOTALL)
                    if not xml_match:
                        continue
                    
                    root = ET.fromstring(xml_match.group(0))
                    for tx in root.findall(".//nonDerivativeTransaction"):
                        code = tx.find(".//transactionCoding/transactionCode").text
                        if code in ["P", "S"]:
                            shares = float(tx.find(".//transactionAmounts/transactionShares/value").text or 0)
                            price = float(tx.find(".//transactionAmounts/transactionPricePerShare/value").text or 0)
                            pos_type = "BUY" if code == "P" else "SELL"
                            
                            records.append({
                                "Date": file_date,
                                "Position": pos_type,
                                "Est_Value": shares * price,
                                "Source": "SEC Corporate"
                            })
                except Exception:
                    continue
        return pd.DataFrame(records)
    except Exception:
        return pd.DataFrame()

def fetch_5yr_congressional_trades(ticker: str) -> pd.DataFrame:
    endpoints = {
        "US Senate": "https://raw.githubusercontent.com/timothycarambat/senate-stock-watcher-data/master/aggregate/all_transactions.json",
        "US House": "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
    }
    
    records = []
    for chamber, url in endpoints.items():
        try:
            res = requests.get(url, timeout=5)
            if res.status_code != 200:
                continue
            for tx in res.json():
                if str(tx.get("ticker", "")).strip().upper() != ticker.upper():
                    continue
                
                raw_date = tx.get("disclosure_date", "")
                if not raw_date:
                    continue
                try:
                    if "/" in raw_date:
                        disclose_date = datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")
                    else:
                        disclose_date = raw_date
                except ValueError:
                    disclose_date = raw_date
                    
                if disclose_date < FIVE_YEARS_AGO:
                    continue
                
                tx_type = str(tx.get("type", "")).lower()
                if "purchase" in tx_type:
                    pos_type = "BUY"
                elif "short" in tx_type or "put" in tx_type:
                    pos_type = "SHORT"
                elif "sale" in tx_type:
                    pos_type = "SELL"
                else:
                    continue
                
                val_range = tx.get("amount", "Unknown Range")
                est_val = 0.0
                digits = [int(s) for s in re.findall(r'\d+', val_range.replace(',', ''))]
                if len(digits) == 2: est_val = sum(digits) / 2
                elif len(digits) == 1: est_val = float(digits[0])
                
                records.append({
                    "Date": disclose_date,
                    "Position": pos_type,
                    "Est_Value": est_val,
                    "Source": f"Congress ({chamber})"
                })
        except Exception:
            continue
    return pd.DataFrame(records)

# =====================================================================
# SPRINT 3: FORWARD RETURN & ALPHA ENGINE (LYNCH WEIGHTED)
# =====================================================================
def execute_5yr_backtest(ticker: str):
    ticker = ticker.strip().upper()
    print(f"\n[+] Initiating 5-Year Dual-Mandate Backtest for ${ticker}...")
    print(f"[+] Pulling 5-year SEC EDGAR & Congressional archives (This may take 30-60 seconds)...")
    
    sec_df = fetch_5yr_sec_trades(ticker)
    pol_df = fetch_5yr_congressional_trades(ticker)
    
    master_frames = [df for df in [sec_df, pol_df] if not df.empty]
    if not master_frames:
        print(f"[-] Operational Failure: No historical transactions extracted for ${ticker}.")
        return

    trades_df = pd.concat(master_frames, ignore_index=True)
    print(f"[+] Extracted {len(trades_df)} total transactions (SEC + Congress). Downloading daily pricing engines...")
    
    try:
        price_data = yf.download([ticker, BENCHMARK_TICKER], start=FIVE_YEARS_AGO, progress=False)["Close"]
        price_data.index = pd.to_datetime(price_data.index).tz_localize(None)
    except Exception as e:
        print(f"[-] Market Data Error: Unable to fetch yfinance data -> {e}")
        return

    # Calculate Daily Aggregates with Asymmetric Lynch Multipliers
    trades_df["Date"] = pd.to_datetime(trades_df["Date"])
    daily_groups = trades_df.groupby(["Date", "Position"])["Est_Value"].sum().unstack(fill_value=0)
    
    for col in ["BUY", "SELL", "SHORT"]:
        if col not in daily_groups: daily_groups[col] = 0
    
    # APPLYING THE PETER LYNCH ASYMMETRIC WEIGHTS:
    # BUY gets a 3.0x conviction boost. SELL gets standard 1.0x. SHORT gets 2.5x negative weight.
    buy_conviction = 3.0 * np.log10(daily_groups["BUY"].clip(lower=1000))
    sell_conviction = 1.0 * np.log10(daily_groups["SELL"].clip(lower=1000))
    short_conviction = 2.5 * np.log10(daily_groups["SHORT"].clip(lower=1000))
    
    daily_groups["Net_Conviction"] = buy_conviction - (sell_conviction + short_conviction)
    
    # Map into the 5-Tier Normalization Scale
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
            alpha_ret = abs_ret - bench_ret
            
            results.append({
                "Tier": tier,
                "Horizon": h_label,
                "Absolute_Return": abs_ret * 100,
                "Market_Alpha": alpha_ret * 100
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
    print("[+] Historical Backtest Complete. Notice how the 3x Buy Weight alters the forward distribution.")

if __name__ == "__main__":
    while True:
        target = input("\nEnter Stock Ticker for 5-Year Backtest (or 'exit' to quit): ").strip()
        if target.lower() == 'exit':
            break
        if not target:
            continue
        execute_5yr_backtest(target)