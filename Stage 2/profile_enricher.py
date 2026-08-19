import sqlite3
import wikipedia
import time
import re
import warnings
from thefuzz import fuzz

# Suppress internal BeautifulSoup warnings from the Wikipedia library
warnings.filterwarnings("ignore", category=UserWarning, module='wikipedia')

# Wikipedia requires a custom User-Agent to prevent IP blocking (Fixes the JSON Decode Error)
wikipedia.set_user_agent("TroyQuant/2.0 (Quantitative Intelligence Research) contact@troyquant.com")

DB_NAME = "macroquant.db"

def clean_entity_name(raw_name):
    """Strips out SEC/Congressional tags and truncated parentheses to isolate the human or fund name."""
    # Remove anything from an opening parenthesis onward (fixes "Cleo Fields (Hou")
    name = raw_name.split('(')[0]
    name = name.replace("CONGRESS:", "")
    # Remove common corporate suffixes that confuse Wikipedia
    name = re.sub(r'\b(LLC|LP|INC|CORP|LTD|TRUST|COMPANY)\b', '', name, flags=re.IGNORECASE)
    return name.strip()

def determine_party_and_role(bio_text, raw_name):
    """Algorithmically infers political party and financial role from the Wikipedia summary."""
    bio_lower = bio_text.lower()
    party = "Unknown"
    role = "Market Participant"
    
    if "republican" in bio_lower:
        party = "Republican"
    elif "democrat" in bio_lower:
        party = "Democrat"
    elif "independent" in bio_lower:
        party = "Independent"
        
    if "senator" in bio_lower or "senate" in bio_lower:
        role = "US Senator"
    elif "representative" in bio_lower or "congress" in bio_lower:
        role = "US Representative"
    elif "hedge fund" in bio_lower or "investment" in bio_lower:
        role = "Institutional Fund"
        party = "Unknown" 
    elif "ceo" in bio_lower or "executive" in bio_lower:
        role = "Corporate Executive"
        party = "Unknown"
        
    return role, party

def run_fuzzy_enrichment():
    print("═"*70)
    print(" INITIATING TROY FUZZY ENRICHMENT ENGINE")
    print("═"*70)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Get all unique entities currently sitting in the trading matrix
    cursor.execute("SELECT DISTINCT entity FROM alpha_matrix_cache")
    entities = [row[0] for row in cursor.fetchall()]
    
    # 2. Get already enriched profiles to avoid wasting API calls
    cursor.execute("SELECT clean_name FROM entity_profiles")
    existing_profiles = [row[0] for row in cursor.fetchall()]
    
    success_count = 0
    
    for raw_name in entities:
        clean_name = clean_entity_name(raw_name)
        
        if clean_name in existing_profiles or not clean_name:
            continue
            
        print(f"\n[>] Processing: {clean_name} (Raw: {raw_name})")
        
        try:
            # Ping Wikipedia for the top 3 closest page results
            search_results = wikipedia.search(clean_name, results=3)
            if not search_results:
                print("  [-] No Wikipedia results found.")
                continue
                
            best_match = None
            highest_score = 0
            
            # --- THE FUZZY MATH ---
            for result in search_results:
                score = fuzz.token_sort_ratio(clean_name.lower(), result.lower())
                if score > highest_score:
                    highest_score = score
                    best_match = result
                    
            print(f"  [~] Best Fuzzy Match: '{best_match}' (Confidence: {highest_score}/100)")
            
            # If the mathematical confidence is high enough
            if highest_score >= 70:
                try:
                    page = wikipedia.page(best_match, auto_suggest=False)
                except wikipedia.exceptions.DisambiguationError as e:
                    # If multiple people have this name, default to the most prominent one
                    print(f"  [!] Disambiguation caught. Defaulting to primary target: {e.options[0]}")
                    try:
                        page = wikipedia.page(e.options[0], auto_suggest=False)
                    except Exception:
                        print("  [-] Could not resolve disambiguation. Skipping.")
                        continue
                
                # Extract a concise Bio
                bio = page.summary[:400] + "..." if len(page.summary) > 400 else page.summary
                
                # Extract Headshot/Logo (Skipping generic Wikipedia SVG icons)
                image_url = ""
                for img in page.images:
                    if not any(x in img.lower() for x in ['.svg', 'icon', 'logo', 'map']):
                        image_url = img
                        break
                        
                role, party = determine_party_and_role(bio, raw_name)
                
                # Save the enriched data to the SQLite database
                cursor.execute('''
                    INSERT OR REPLACE INTO entity_profiles 
                    (clean_name, original_name, role, bio, image_url, party, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (clean_name, raw_name, role, bio, image_url, party, time.strftime("%Y-%m-%d")))
                
                conn.commit()
                success_count += 1
                print(f"  [+] Profile Saved: {role} | Party: {party}")
            else:
                print(f"  [-] Match confidence {highest_score} is below threshold. Skipping to prevent bad data.")
                
        except Exception as e:
            # Catching the JSONDecodeError cleanly if Wikipedia still throttles
            if "Expecting value" in str(e):
                print("  [!] Wikipedia API Rate Limit Hit. Pausing for 5 seconds...")
                time.sleep(5)
            else:
                print(f"  [!] Error processing {clean_name}: {e}")
            
        # Increased rate-limiting to keep Wikipedia happy
        time.sleep(1.5) 
        
    conn.close()
    print("\n" + "═"*70)
    print(f" FUZZY ENRICHMENT COMPLETE: Successfully matched and injected {success_count} new institutional profiles.")
    print("═"*70 + "\n")

if __name__ == "__main__":
    run_fuzzy_enrichment()