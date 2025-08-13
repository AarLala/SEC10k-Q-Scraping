
#pip install requests pandas
#This code written is designed to get all the tickers and get save the apple 10k filings. 
#written as a test. Check 'All.py' for scaled up version.

import requests
import pandas as pd
import time
import json
import os
import logging
from requests.adapters import HTTPAdapter, Retry

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
#appropriate practices for production code
from config import USER_AGENT_EMAIL


#use emali here. preferably one connected with Firm. I don't have one, so used personal
headers = {'User-Agent': "USER_AGENT_EMAIL"}

# Retry strategy for requests
retry_strategy = Retry(
    total=5, #try 5 times, exponentially updating each time based off the number of errors. 
    backoff_factor=1,#doubles each timse.
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.headers.update(headers)

# will create folder
OUTPUT_FOLDER = 'apple_10k_filings'
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def load_company_tickers():
    url = "https://www.sec.gov/files/company_tickers.json"
    logging.info(f"Fetching company tickers from SEC")
    resp = http.get(url)
    resp.raise_for_status()
    return resp.json()

def get_apple_cik(tickers_json):
    df = pd.DataFrame.from_dict(tickers_json, orient='index')
    df['cik_str'] = df['cik_str'].astype(str).str.zfill(10)
    apple_row = df[df['ticker'] == 'AAPL']
    if apple_row.empty:
        raise ValueError("Apple ticker 'AAPL' not found in company tickers.")
    cik = apple_row.iloc[0]['cik_str']
    logging.info(f"Found Apple CIK: {cik}")
    return cik

def fetch_submission_metadata(cik):
    url = f'https://data.sec.gov/submissions/CIK{cik}.json'
    logging.info(f"Fetching submission metadata for CIK {cik}")
    resp = http.get(url)
    resp.raise_for_status()
    data = resp.json()
    if 'filings' not in data or 'recent' not in data['filings']:
        raise ValueError(f"Submission metadata missing expected 'filings.recent' structure for CIK {cik}")
    return data

def filter_10k_filings(metadata_json):
    filings = metadata_json['filings']['recent']
    forms = filings.get('form', [])
    indices = [i for i, f in enumerate(forms) if f == '10-K']
    ten_k_filings = []
    for i in indices:
        record = {k: filings[k][i] for k in filings.keys()}
        ten_k_filings.append(record)
    logging.info(f"Filtered {len(ten_k_filings)} 10-K filings")
    return ten_k_filings

def save_json(data, filepath):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    logging.info(f"Saved JSON data to {filepath}")

def download_full_filing(cik, accession_no, primary_doc, folder):
    accession_no_nodash = accession_no.replace('-', '')
    url = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_nodash}/{primary_doc}'
    filename = f"{cik}_{accession_no_nodash}_{primary_doc}"
    filepath = os.path.join(folder, filename)
    try:
        logging.info(f"Downloading full filing from {url}")
        resp = http.get(url)
        resp.raise_for_status()
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        logging.info(f"Saved full filing to {filepath}")
        time.sleep(0.2)  # polite delay
    except requests.RequestException as e:
        logging.error(f"Failed to download full filing {url}: {e}")

def create_summary_csv(ten_k_filings, folder, cik):
    # brief parsing(really coded it up for like a proof of concept). Demonstrating potential to parse in future for other information
    summary_rows = []
    for filing in ten_k_filings:
        summary_rows.append({
            'accessionNumber': filing.get('accessionNumber', ''),
            'filingDate': filing.get('filingDate', ''),
            'form': filing.get('form', ''),
            'primaryDocument': filing.get('primaryDocument', ''),
            'filingHref': f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{filing.get('accessionNumber', '').replace('-', '')}/{filing.get('primaryDocument', '')}"
        })
    df = pd.DataFrame(summary_rows)
    csv_path = os.path.join(folder, f"{cik}_10k_summary.csv")
    df.to_csv(csv_path, index=False)
    logging.info(f"Saved parsed summary CSV to {csv_path}")

def main():
    tickers_json = load_company_tickers()
    cik = get_apple_cik(tickers_json)
    submission_metadata = fetch_submission_metadata(cik)
    
    raw_metadata_path = os.path.join(OUTPUT_FOLDER, f"{cik}_submission_metadata.json")
    save_json(submission_metadata, raw_metadata_path)
    
    ten_k_filings = filter_10k_filings(submission_metadata)
    
    # Save 10-K metadata JSON
    ten_k_metadata_path = os.path.join(OUTPUT_FOLDER, f"{cik}_10k_metadata.json")
    save_json(ten_k_filings, ten_k_metadata_path)
    
    for filing in ten_k_filings:
        download_full_filing(cik, filing['accessionNumber'], filing['primaryDocument'], OUTPUT_FOLDER)
    
    # will create file, #Test
    create_summary_csv(ten_k_filings, OUTPUT_FOLDER, cik)
    
    logging.info("All done!")

if __name__ == "__main__":
    main()
