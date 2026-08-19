import xml.etree.ElementTree as ET
import re
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests

# =====================================================================
# GLOBAL CONFIGURATION & DATA HORIZON
# =====================================================================
SEC_HEADERS = {
    "User-Agent": "MacroQuant Research Engine (admin@macroquant.com)", 
    "Accept-Encoding": "gzip, deflate"
}

ONE_YEAR_AGO = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

# =====================================================================
# LEG 1: SEC INSIDER TRADING PIPELINE
# =====================================================================
def get_cik(ticker: str) -> str:
    url = "https://www.sec.gov/files/company_tickers.json"
    res = requests.get(url, headers=SEC_HEADERS)
    res.raise_for_status()
    for item in res.json().values():
        if item["ticker"].upper() == ticker.upper():
            return str(item["cik_str"]).zfill(10)
    raise ValueError(f"Ticker {ticker} not found.")

def fetch_form4_meta(cik: str) -> list:
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    res = requests.get(url, headers=SEC_HEADERS)
    res.raise_for_status()
    filings = res.json()["filings"]["recent"]
    
    form_4_meta = []
    for idx, form in enumerate(filings["form"]):
        file_date = filings["filingDate"][idx]
        if file_date < ONE_YEAR_AGO:
            continue
        if form == "4":
            form_4_meta.append({
                "accession_number": filings["accessionNumber"][idx],
                "clean_acc": filings["accessionNumber"][idx].replace("-", ""),
                "file_date": file_date
            })
    return form_4_meta

def parse_form4_xml(cik: str, acc_meta: dict) -> list:
    acc = acc_meta["accession_number"]
    clean_acc = acc_meta["clean_acc"]
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{clean_acc}/{acc}.txt"
    try:
        res = requests.get(url, headers=SEC_HEADERS)
        time.sleep(0.08) 
        if res.status_code != 200:
            return []
        
        xml_match = re.search(r"<ownershipDocument>.*?</ownershipDocument>", res.text, re.DOTALL)
        if not xml_match:
            return []
        
        root = ET.fromstring(xml_match.group(0))
        owner_name = root.find(".//reportingOwner/reportingOwnerId/rptOwnerName").text
        
        rel_node = root.find(".//reportingOwner/reportingOwnerRelationship")
        role = "Officer"
        if rel_node is not None:
            if rel_node.find("isDirector") is not None and rel_node.find("isDirector").text in ['true', '1']:
                role = "Director"
            elif rel_node.find("isTenPercentOwner") is not None and rel_node.find("isTenPercentOwner").text in ['true', '1']:
                role = "10% Owner"
                
        transactions = []
        for tx in root.findall(".//nonDerivativeTransaction"):
            code = tx.find(".//transactionCoding/transactionCode").text
            if code in ["P", "S"]:
                shares = float(tx.find(".//transactionAmounts/transactionShares/value").text or 0)
                price = float(tx.find(".//transactionAmounts/transactionPricePerShare/value").text or 0)
                pos_type = "BUY (LONG)" if code == "P" else "SELL (LIQUIDATE)"
                
                transactions.append({
                    "Date (Disclosed)": acc_meta["file_date"],
                    "Entity/Name": owner_name,
                    "Source Layer": f"SEC Corporate ({role})",
                    "Position Type": pos_type,
                    "Volume/Size": f"{shares:,.0f} shares",
                    "Est. Value": round(shares * price, 2)
                })
        return transactions
    except Exception:
        return []

def get_sec_layer(ticker: str) -> pd.DataFrame:
    try:
        cik = get_cik(ticker)
        meta = fetch_form4_meta(cik)
        records = []
        for m in meta:
            records.extend(parse_form4_xml(cik, m))
        return pd.DataFrame(records)
    except Exception:
        return pd.DataFrame()

# =====================================================================
# LEG 2: CONGRESSIONAL TRADING PIPELINE (HOUSE + SENATE)
# =====================================================================
def get_congressional_layer(ticker: str) -> pd.DataFrame:
    endpoints = {
        "US Senate": "https://raw.githubusercontent.com/timothycarambat/senate-stock-watcher-data/master/aggregate/all_transactions.json",
        "US House": "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
    }
    
    combined_trades = []
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
                    
                if disclose_date < ONE_YEAR_AGO:
                    continue
                
                tx_type = str(tx.get("type", "")).lower()
                if "purchase" in tx_type:
                    pos_type = "BUY (LONG)"
                elif "short" in tx_type or "put" in tx_type:
                    pos_type = "SHORT (BEARISH)"
                elif "sale" in tx_type:
                    pos_type = "SELL (LIQUIDATE)"
                else:
                    continue
                    
                name = tx.get("representative", tx.get("senator", "Unknown Politician"))
                val_range = tx.get("amount", "Unknown Range")
                
                est_val = 0.0
                digits = [int(s) for s in re.findall(r'\d+', val_range.replace(',', ''))]
                if len(digits) == 2: 
                    est_val = sum(digits) / 2
                elif len(digits) == 1: 
                    est_val = float(digits[0])

                combined_trades.append({
                    "Date (Disclosed)": disclose_date,
                    "Entity/Name": name,
                    "Source Layer": f"Capitol Hill ({chamber})",
                    "Position Type": pos_type,
                    "Volume/Size": val_range,
                    "Est. Value": est_val
                })
        except Exception:
            continue
            
    return pd.DataFrame(combined_trades)

# =====================================================================
# SPRINT 2: ALGORITHMIC SCORING ENGINE (WITH LYNCH WEIGHTING)
# =====================================================================
def generate_alpha_scores(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df["Date (Disclosed)"] = pd.to_datetime(df["Date (Disclosed)"])
    today = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))

    # 1. Exponential Time Decay (45-day half-life)
    df["Days_Old"] = (today - df["Date (Disclosed)"]).dt.days
    decay_constant = np.log(2) / 45
    df["Decay_Weight"] = np.exp(-decay_constant * df["Days_Old"])

    # 2. Logarithmic Transaction Sizing
    df["Est. Value"] = pd.to_numeric(df["Est. Value"], errors='coerce').fillna(0)
    df["Size_Conviction"] = np.log10(df["Est. Value"].clip(lower=1000)) - 2 

    # 3. Asymmetric Lynch Conviction Multipliers
    def map_lynch_conviction(pos):
        if "BUY" in pos:
            return 3.0   # Peter Lynch Rule: Unmistakable positive conviction
        elif "SHORT" in pos:
            return -2.5  # Active shorting carries heavy negative conviction
        else:
            return -1.0  # Standard Liquidation / Options vesting (Noise)

    df["Direction"] = df["Position Type"].apply(map_lynch_conviction)
    df["Raw_Score"] = df["Direction"] * df["Size_Conviction"] * df["Decay_Weight"]

    # 4. Cluster Grouping (7-day rolling window)
    df = df.sort_values("Date (Disclosed)").reset_index(drop=True)
    cluster_multipliers = []
    
    for i, row in df.iterrows():
        mask = (
            (df["Date (Disclosed)"] >= row["Date (Disclosed)"] - pd.Timedelta(days=7)) &
            (df["Date (Disclosed)"] <= row["Date (Disclosed)"] + pd.Timedelta(days=7)) &
            (np.sign(df["Direction"]) == np.sign(row["Direction"]))
        )
        unique_insiders = df.loc[mask, "Entity/Name"].nunique()
        
        if unique_insiders == 1:
            mult = 1.0
        elif unique_insiders == 2:
            mult = 1.5
        else:
            mult = 2.0
            
        cluster_multipliers.append(mult)

    df["Cluster_Mult"] = cluster_multipliers
    df["Alpha_Score"] = df["Raw_Score"] * df["Cluster_Mult"]
    
    df["Date (Disclosed)"] = df["Date (Disclosed)"].dt.strftime('%Y-%m-%d')
    return df.sort_values(by="Date (Disclosed)", ascending=False)

# =====================================================================
# SYSTEM CORE SCAN ORCHESTRATOR
# =====================================================================
def execute_unified_system_scan(ticker: str):
    ticker = ticker.strip().upper()
    print(f"\n[+] Executing Dual-Mandate Scan (Lynch Asymmetric Weighting) for ${ticker}...")
    
    sec_df = get_sec_layer(ticker)
    pol_df = get_congressional_layer(ticker)
    
    master_frames = [df for df in [sec_df, pol_df] if not df.empty]
    
    if not master_frames:
        print(f"\n[-] Operational Failure: Zero records returned from data layers for ${ticker}.")
        return
        
    raw_df = pd.concat(master_frames, ignore_index=True)
    scored_df = generate_alpha_scores(raw_df)
    
    # Sigmoidal Logistic Normalization (Maps Raw Alpha to a 1.0 - 10.0 Scale)
    total_raw_alpha = scored_df["Alpha_Score"].sum()
    k_constant = 0.15 
    rating_1_to_10 = 1 + (9 / (1 + np.exp(-k_constant * total_raw_alpha)))
    
    # 5-Tier Signal Categorization
    if rating_1_to_10 >= 7.5:
        macro_signal = "STRONG BUY"
    elif rating_1_to_10 >= 6.0:
        macro_signal = "BUY"
    elif rating_1_to_10 <= 2.5:
        macro_signal = "STRONG SHORT"
    elif rating_1_to_10 <= 4.0:
        macro_signal = "SHORT"
    else:
        macro_signal = "NEUTRAL"

    print("\n" + "═"*100)
    print(f" DUAL-MANDATE ALPHA MATRIX REPORT: {ticker}")
    print("═"*100)
    print(f" Total Tracked Institutional Records: {len(scored_df)}")
    print(f" Raw Unbounded Matrix Score:        {total_raw_alpha:+.2f}")
    print(f" Normalized Alpha Rating (1-10):    {rating_1_to_10:.2f} / 10.0")
    print(f" Current Algorithmic Signal:        {macro_signal}")
    print("═"*100)
    
    display_df = scored_df[[
        "Date (Disclosed)", "Entity/Name", "Source Layer", 
        "Position Type", "Est. Value", "Cluster_Mult", "Alpha_Score"
    ]].copy()
    
    display_df["Est. Value"] = display_df["Est. Value"].apply(lambda x: f"${x:,.0f}")
    display_df["Alpha_Score"] = display_df["Alpha_Score"].apply(lambda x: f"{x:+.2f}")
    
    print("\nChronological Execution Feed Matrix (Scored):")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(display_df.head(40).to_string(index=False)) 

if __name__ == "__main__":
    while True:
        target = input("\nEnter Stock Ticker (or 'exit' to quit): ").strip()
        if target.lower() == 'exit':
            break
        if not target:
            continue
        execute_unified_system_scan(target)