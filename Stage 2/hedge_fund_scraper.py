import sqlite3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import time
import re

DB_NAME = "macroquant.db"

# The SEC legally requires a descriptive User-Agent for all API requests
SEC_HEADERS = {
    "User-Agent": "TroyQuant/2.0 (University of Exeter Research; Quantitative Intelligence) contact@troyquant.com",
    "Accept-Encoding": "gzip, deflate"
}

# The SEC CIK (Central Index Key) identifiers for top macro funds
TARGET_FUNDS = {
    "0001067983": "Berkshire Hathaway",
    "0001336528": "Pershing Square",
    "0001350694": "Bridgewater Associates",
    "0001568820": "Scion Asset Management",
    "0001423053": "Citadel Advisors"
}

def resolve_ticker_from_name(issuer_name: str) -> str:
    """Reverse lookups the SEC corporate name to find the actual stock ticker."""
    # Clean up standard corporate suffixes that confuse the search engine
    clean_name = issuer_name.upper().replace(" COM", "").replace(" CL A", "").replace(" INC", "").replace(" CORP", "").strip()
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={clean_name}"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        data = res.json()
        if "quotes" in data and len(data["quotes"]) > 0:
            return data["quotes"][0]["symbol"]
    except Exception:
        pass
    return "UNKNOWN"

def get_latest_13f_accession(cik: str):
    """Pings the SEC Submissions API to find the exact ID of their latest 13F filing."""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    res = requests.get(url, headers=SEC_HEADERS, timeout=10)
    if res.status_code != 200:
        return None, None
        
    data = res.json()
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    acc_nums = recent.get("accessionNumber", [])
    dates = recent.get("filingDate", [])
    
    for i, form in enumerate(forms):
        if form == "13F-HR":  # 13F Holdings Report
            return acc_nums[i], dates[i]
            
    return None, None

def parse_13f_xml(cik: str, accession_number: str):
    """Downloads the raw SEC text file, isolates the XML, and parses the portfolio."""
    acc_no_dashes = accession_number.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no_dashes}/{accession_number}.txt"
    
    res = requests.get(url, headers=SEC_HEADERS, timeout=10)
    if res.status_code != 200:
        return []
        
    raw_text = res.text
    
    # 13F text files contain multiple documents. We isolate the XML Information Table.
    start_tag = "<informationTable"
    end_tag = "</informationTable>"
    
    if start_tag not in raw_text or end_tag not in raw_text:
        return []
        
    start_idx = raw_text.find(start_tag)
    end_idx = raw_text.find(end_tag) + len(end_tag)
    xml_data = raw_text[start_idx:end_idx]
    
    # Strip XML namespaces to make parsing clean and easy
    xml_data = re.sub(r' xmlns=".*?"', '', xml_data)
    
    holdings = []
    try:
        root = ET.fromstring(xml_data)
        for info in root.findall(".//infoTable"):
            issuer_elem = info.find("nameOfIssuer")
            val_elem = info.find("value")
            
            if issuer_elem is not None and val_elem is not None:
                issuer = issuer_elem.text
                # SEC 13F values are reported in thousands of dollars
                value = float(val_elem.text) * 1000 
                
                # Consolidate multiple share classes of the same company
                existing = next((item for item in holdings if item["issuer"] == issuer), None)
                if existing:
                    existing["value"] += value
                else:
                    holdings.append({"issuer": issuer, "value": value})
                    
        # Sort by largest positions and return the top 15 to keep the database fast
        holdings.sort(key=lambda x: x["value"], reverse=True)
        return holdings[:15]
        
    except Exception as e:
        print(f"  [!] XML Parsing Error: {e}")
        return []

def run_hedge_fund_pipeline():
    print("\n" + "═"*70)
    print(" INITIATING TROY BACKGROUND WORKER: SEC EDGAR 13F PIPELINE")
    print("═"*70)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    total_positions = 0
    
    for cik, fund_name in TARGET_FUNDS.items():
        print(f"\n[>] Target: {fund_name} (CIK: {cik})")
        time.sleep(0.5) # Gentle rate-limiting for the SEC servers
        
        acc_num, file_date = get_latest_13f_accession(cik)
        if not acc_num:
            print(f"  [-] Failed to locate recent 13F-HR filing.")
            continue
            
        print(f"  [+] Located latest 13F Filing: {acc_num} (Filed: {file_date})")
        
        portfolio = parse_13f_xml(cik, acc_num)
        print(f"  [+] Extracted {len(portfolio)} top positions. Resolving tickers...")
        
        for item in portfolio:
            # Reverse lookup the ticker symbol based on the corporate name
            ticker = resolve_ticker_from_name(item['issuer'])
            if ticker == "UNKNOWN":
                continue
                
            volume_str = "Portfolio Snapshot"
            
            # Inject into the primary trading matrix
            cursor.execute('''
                INSERT OR IGNORE INTO alpha_matrix_cache 
                (ticker, trade_date, entity, source, position, volume, est_value, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (ticker, file_date, fund_name, "SEC 13F-HR", "BUY", volume_str, item['value'], today_str))
            
            total_positions += 1
            time.sleep(0.2) # Throttle Yahoo Finance API
            
    conn.commit()
    conn.close()
    
    print("\n" + "═"*70)
    print(f" 13F PIPELINE COMPLETE: Ingested {total_positions} institutional positions.")
    print("═"*70 + "\n")

if __name__ == "__main__":
    run_hedge_fund_pipeline()