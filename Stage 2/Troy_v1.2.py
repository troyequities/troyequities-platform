import os
import io
import re
import time
import zipfile
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import yfinance as yf
from pypdf import PdfReader
import concurrent.futures
import sqlite3
import warnings

# --- OCR Imports ---
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

warnings.simplefilter(action='ignore', category=FutureWarning)

# =====================================================================
# 1. ENGINE CONFIGURATION 
# =====================================================================
FINNHUB_API_KEY = "d9nkj11r01qvumgam520d9nkj11r01qvumgam52g"
BENCHMARK_TICKER = "SPY"
DB_NAME = "macroquant.db"

# =====================================================================
# 2. DATABASE ARCHITECTURE (PHASE 2)
# =====================================================================
def init_db():
    """Initializes the SQLite database to store all extracted trades permanently."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS alpha_matrix_cache (
            ticker TEXT,
            trade_date TEXT,
            entity TEXT,
            source TEXT,
            position TEXT,
            volume TEXT,
            est_value REAL,
            last_updated TEXT,
            UNIQUE(ticker, trade_date, entity, source)
        )
    ''')
    conn.commit()
    conn.close()

def check_cache(ticker: str, lookback_days: int) -> pd.DataFrame:
    """Checks if the database already has today's data for this ticker."""
    conn = sqlite3.connect(DB_NAME)
    today_str = datetime.now().strftime("%Y-%m-%d")
    cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    
    query = '''
        SELECT trade_date as Date, entity as Entity, source as Source, 
               position as Position, volume as Volume, est_value as Est_Value
        FROM alpha_matrix_cache
        WHERE ticker = ? AND last_updated = ? AND trade_date >= ?
    '''
    df = pd.read_sql_query(query, conn, params=(ticker.upper(), today_str, cutoff_date))
    conn.close()
    return df

def save_to_cache(ticker: str, df: pd.DataFrame):
    """Saves newly scraped trades into the database."""
    if df.empty:
        return
        
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT OR IGNORE INTO alpha_matrix_cache 
            (ticker, trade_date, entity, source, position, volume, est_value, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (ticker.upper(), row['Date'], row['Entity'], row['Source'], 
              row['Position'], row['Volume'], row['Est_Value'], today_str))
    
    conn.commit()
    conn.close()

# =====================================================================
# 3. DYNAMIC ALIAS GENERATOR
# =====================================================================
def get_company_aliases(ticker: str) -> list:
    aliases = [ticker.upper(), " ".join(list(ticker.upper()))]
    try:
        info = yf.Ticker(ticker).info
        for key in ['shortName', 'longName']:
            name = info.get(key, '')
            if name:
                aliases.append(name)
                first_word = name.split()[0].replace(',', '')
                if len(first_word) > 2:
                    aliases.append(first_word)
    except Exception:
        pass
    unique_aliases = list(set(aliases))
    return sorted(unique_aliases, key=len, reverse=True)

# =====================================================================
# 4. SENATE DATA LAYER (ACTIVE 2026 KADOA JSON)
# =====================================================================
def fetch_open_senate_json(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    print(f"[+] Syncing Active 2026 Open-Source Senate Database for ${ticker.upper()}...")
    cutoff_date = datetime.now() - timedelta(days=lookback_days)
    url = "https://raw.githubusercontent.com/kadoa-org/congress-trading-monitor/main/public/data/trades.json"
    
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if res.status_code != 200: return pd.DataFrame()
        data = res.json()
    except Exception: return pd.DataFrame()

    records = []
    match_count = 0
    for row in data:
        row_ticker = str(row.get("ticker", row.get("symbol", ""))).upper().strip()
        if row_ticker != ticker.upper(): continue
        
        chamber = str(row.get("chamber", row.get("branch", "senate"))).lower()
        if "house" in chamber: continue
        
        raw_date = str(row.get("transaction_date", row.get("disclosure_date", "")))
        try:
            if "T" in raw_date: parsed_date = datetime.strptime(raw_date.split("T")[0], "%Y-%m-%d")
            elif "-" in raw_date: parsed_date = datetime.strptime(raw_date[:10], "%Y-%m-%d")
            elif "/" in raw_date: parsed_date = datetime.strptime(raw_date, "%m/%d/%Y")
            else: continue
        except ValueError: continue
        
        if parsed_date < cutoff_date: continue
            
        if "filer_name" in row: entity = row["filer_name"]
        elif "senator" in row: entity = row["senator"]
        elif "first_name" in row: entity = f"{row.get('first_name','')} {row.get('last_name','')}"
        else: entity = "Lawmaker"
        entity = str(entity).strip()
        
        tx_type = str(row.get("type", row.get("transaction_type", ""))).upper()
        if any(w in tx_type for w in ["BUY", "PURCHASE", "P"]): pos = "BUY"
        elif any(w in tx_type for w in ["SELL", "S", "SALE"]): pos = "SELL"
        elif any(w in tx_type for w in ["SHORT", "PUT"]): pos = "SHORT"
        else: continue
        
        val_str = str(row.get("amount", row.get("amount_range", "15000"))).replace("$", "").replace(",", "")
        est_val = 15000.0
        if "-" in val_str:
            try:
                parts = [float(p.strip()) for p in val_str.split("-") if p.strip().replace('.','',1).isdigit()]
                if len(parts) == 2: est_val = sum(parts) / 2.0
            except Exception: pass
        elif val_str.replace('.','',1).isdigit():
            est_val = float(val_str)
        
        records.append({
            "Date": parsed_date.strftime("%Y-%m-%d"),
            "Entity": f"CONGRESS: {entity} (Senate)"[:26],
            "Source": "Congress (Senate JSON)",
            "Position": pos,
            "Volume": f"${est_val:,.0f}",
            "Est_Value": est_val
        })
        match_count += 1
        
    if match_count > 0: print(f"  [+] Extracted {match_count} historical trades from active Senate database.")
    else: print(f"  [-] No recent political trades found in Senate database for ${ticker.upper()}.")
    return pd.DataFrame(records)

# =====================================================================
# 5. HOUSE PDF SCRAPER (MULTI-THREADED WITH OCR)
# =====================================================================
def fetch_local_house_pdfs(ticker: str, aliases: list, lookback_days: int = 365) -> pd.DataFrame:
    print(f"[+] Initializing Direct US House PDF Scraper for ${ticker.upper()}...")
    cutoff_date = datetime.now() - timedelta(days=lookback_days)
    current_year = datetime.now().year
    start_year = cutoff_date.year
    ptr_list = []
    
    for year in range(start_year, current_year + 1):
        cache_dir = f"house_ptr_cache_{year}"
        os.makedirs(cache_dir, exist_ok=True)
        zip_url = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.ZIP"
        xml_files = [f for f in os.listdir(cache_dir) if f.lower().endswith('.xml')]
        
        if not xml_files:
            try:
                r = requests.get(zip_url, timeout=20)
                if r.status_code == 200:
                    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                        z.extractall(cache_dir)
            except Exception: continue
            xml_files = [f for f in os.listdir(cache_dir) if f.lower().endswith('.xml')]

        if not xml_files: continue
        xml_path = os.path.join(cache_dir, xml_files[0])

        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for member in root.findall('.//Member'):
                last_name, first_name, doc_id, date_filed_str, filing_type = "Unknown", "Unknown", "", "", ""
                for child in member:
                    tag = child.tag.lower()
                    text = str(child.text).strip() if child.text else ""
                    if tag == 'last': last_name = text
                    elif tag == 'first': first_name = text
                    elif tag == 'docid': doc_id = text
                    elif tag == 'filingdate': date_filed_str = text
                    elif tag == 'filingtype': filing_type = text
                    
                if filing_type not in ["P", "PTR"]: continue
                try: date_filed = datetime.strptime(date_filed_str, "%m/%d/%Y")
                except: continue
                if date_filed >= cutoff_date:
                    ptr_list.append({"name": f"{first_name} {last_name}", "doc_id": doc_id, "date": date_filed.strftime("%Y-%m-%d"), "year": year})
        except Exception: pass

    ptr_list = sorted(ptr_list, key=lambda x: x['date'], reverse=True)
    total_ptrs = len(ptr_list)
    print(f"  [>] Found {total_ptrs} total House filings across {start_year}-{current_year}. Scanning with Multi-Threading...")
    
    escaped_aliases = [re.escape(a) for a in aliases]
    regex_pattern = rf"\b({'|'.join(escaped_aliases)})\b"
    records = []
    completed = 0
    
    def process_pdf(ptr):
        local_records = []
        pdf_year = ptr['year']
        cache_dir = f"house_ptr_cache_{pdf_year}"
        pdf_url = f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{pdf_year}/{ptr['doc_id']}.pdf"
        pdf_path = os.path.join(cache_dir, f"{ptr['doc_id']}.pdf")
        
        if not os.path.exists(pdf_path):
            try:
                pdf_res = requests.get(pdf_url, timeout=10)
                if pdf_res.status_code == 200:
                    with open(pdf_path, 'wb') as f:
                        f.write(pdf_res.content)
            except: pass
        
        if os.path.exists(pdf_path):
            try:
                reader = PdfReader(pdf_path)
                pdf_text = ""
                for page in reader.pages:
                    extracted = page.extract_text()
                    if extracted: pdf_text += extracted + " "
                
                if not pdf_text.strip() and OCR_AVAILABLE:
                    try:
                        images = convert_from_path(pdf_path, dpi=200)
                        for img in images: pdf_text += pytesseract.image_to_string(img, lang='eng') + " "
                    except Exception: pass 
                
                if re.search(regex_pattern, pdf_text, re.IGNORECASE):
                    local_records.append({
                        "Date": ptr['date'], "Entity": f"CONGRESS: {ptr['name']} (House)"[:26],
                        "Source": "Congress (Local PDF)", "Position": "BUY",
                        "Volume": "$15,000 (Est)", "Est_Value": 15000.0
                    })
            except Exception: pass
        return local_records

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_pdf, ptr): ptr for ptr in ptr_list}
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            print(f"  [~] Processing House PDF {completed} of {total_ptrs}...", end='\r', flush=True)
            try:
                result = future.result()
                if result: records.extend(result)
            except Exception: pass

    print(" " * 60, end='\r', flush=True) 
    if records: print(f"  [+] Successfully located {len(records)} House PDF disclosures.")
    else: print(f"  [-] Checked all recent House PDFs. No trades found.")
    return pd.DataFrame(records)

# =====================================================================
# 6. CORPORATE INSIDER DATA (WITH 10b5-1 HEURISTIC FILTERING)
# =====================================================================
def fetch_finnhub_insider(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    print(f"[+] Querying Finnhub Corporate Insider API for ${ticker.upper()}...")
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    url = f"https://finnhub.io/api/v1/stock/insider-transactions?symbol={ticker.upper()}&from={from_date}&to={to_date}&token={FINNHUB_API_KEY}"
    
    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200: return pd.DataFrame()
            
        data = res.json().get("data", [])
        records = []
        for row in data:
            date_str = str(row.get("filingDate", row.get("transactionDate", "")))[:10]
            if not date_str: continue
            entity = str(row.get("name", "Corporate Insider")).strip()
            change = float(row.get("change", 0))
            if change > 0: pos = "BUY"
            elif change < 0: pos = "SELL"
            else: continue
            shares = abs(change)
            price = float(row.get("transactionPrice", 0))
            if price <= 0: continue
                
            records.append({
                "Date": date_str, "Entity": entity[:26],
                "Source": "SEC Corporate", "Position": pos,
                "Volume": f"{shares:,.0f} shs", "Est_Value": round(shares * price, 2)
            })
            
        if not records: return pd.DataFrame()
        df = pd.DataFrame(records)
        print(f"  [+] Extracted {len(df)} raw Finnhub corporate transactions.")
        
        df['Is_10b5_1'] = False
        sell_mask = df['Position'] == 'SELL'
        duplicates = df[sell_mask].duplicated(subset=['Entity', 'Volume'], keep=False)
        df.loc[sell_mask, 'Is_10b5_1'] = duplicates
        
        flagged_count = df['Is_10b5_1'].sum()
        if flagged_count > 0:
            print(f"  [!] Identified & filtered {flagged_count} transactions as pre-scheduled 10b5-1 plans.")
            df.loc[df['Is_10b5_1'], 'Source'] = "SEC Corporate (10b5-1)"
            
        df.drop(columns=['Is_10b5_1'], inplace=True)
        return df
        
    except Exception: return pd.DataFrame()

# =====================================================================
# 7. UNIFIED DATA PIPELINE (WITH SQLITE CACHING)
# =====================================================================
def get_unified_flow_data(ticker: str, lookback_days: int = 365) -> pd.DataFrame:
    # 1. Check local database for instant load
    cached_df = check_cache(ticker, lookback_days)
    if not cached_df.empty:
        print(f"\n[+] INSTANT LOAD: Found recent data for ${ticker.upper()} in local database.")
        cached_df["Date"] = pd.to_datetime(cached_df["Date"])
        return cached_df.sort_values(by="Date", ascending=False).reset_index(drop=True)
        
    # 2. If no cache, run the full pipeline
    aliases = get_company_aliases(ticker)
    senate_df = fetch_open_senate_json(ticker, lookback_days)
    house_df = fetch_local_house_pdfs(ticker, aliases, lookback_days)
    sec_df = fetch_finnhub_insider(ticker, lookback_days)
    
    master_frames = [df for df in [sec_df, senate_df, house_df] if not df.empty]
    if not master_frames: return pd.DataFrame()
        
    unified_df = pd.concat(master_frames, ignore_index=True)
    
    # 3. Save to database for next time
    save_to_cache(ticker, unified_df)
    
    unified_df["Date"] = pd.to_datetime(unified_df["Date"])
    return unified_df.sort_values(by="Date", ascending=False).reset_index(drop=True)

# =====================================================================
# 8. CORE ALGORITHMIC SCORING ENGINE
# =====================================================================
def apply_alpha_scoring_math(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df

    today = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
    df["Days_Old"] = (today - df["Date"]).dt.days
    decay_constant = np.log(2) / 45
    df["Decay_Weight"] = np.exp(-decay_constant * df["Days_Old"])
    df["Est_Value"] = pd.to_numeric(df["Est_Value"], errors='coerce').fillna(0)
    df["Size_Conviction"] = np.log10(df["Est_Value"].clip(lower=1000)) - 2 

    def map_lynch_conviction(row):
        pos = row["Position"]
        source = row["Source"]
        
        if "10b5-1" in source: base = -0.1
        elif pos == "BUY": base = 3.0
        elif pos == "SHORT": base = -2.5
        else: base = -1.0 
            
        if "Congress" in source: base *= 1.5
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
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print("\nChronological Execution Feed Matrix (Top 45 Scored Events):")
    print(display_df.head(45).to_string(index=False))

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
        if len(valid_dates) == 0: continue
        start_idx = price_data.index.get_loc(valid_dates[0])
        tier = row["Signal_Tier"]
        for h_label, h_days in horizons.items():
            end_idx = start_idx + h_days
            if end_idx >= len(price_data): continue
            p_start = price_data.iloc[start_idx]
            p_end = price_data.iloc[end_idx]
            abs_ret = (p_end[ticker] - p_start[ticker]) / p_start[ticker]
            bench_ret = (p_end[BENCHMARK_TICKER] - p_start[BENCHMARK_TICKER]) / p_start[BENCHMARK_TICKER]
            results.append({
                "Tier": tier, "Horizon": h_label,
                "Absolute_Return": abs_ret * 100, "Market_Alpha": (abs_ret - bench_ret) * 100
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
# 9. SYSTEM ORCHESTRATION MENU
# =====================================================================
if __name__ == "__main__":
    init_db() # Boot up the memory bank
    while True:
        print("\n" + "─"*50)
        print(" MACROQUANT DUAL-MANDATE TERMINAL (HYBRID ENGINE)")
        print("─"*50)
        print(" [1] Live Real-Time Scanner (365-Day Lookback)")
        print(" [2] 5-Year Historical Backtest (1825-Day Lookback)")
        print(" [3] Exit Engine")
        
        choice = input("\nSelect Execution Mode (1-3): ").strip()
        if choice == "3" or choice.lower() == "exit": break
        elif choice in ["1", "2"]:
            target = input("Enter Stock Ticker (e.g., GS, NVDA, AAPL): ").strip().upper()
            if not target: continue
            if choice == "1": run_live_scanner(target)
            else: run_5yr_backtest(target)
        else: print("[-] Invalid Selection. Please choose 1, 2, or 3.")