#pip install requests pandas tqdm


import requests
import pandas as pd
import time
import json
import os
import logging
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
import threading
from collections import defaultdict
import concurrent.futures
from queue import Queue
import math

'''basic logging. All data will be printed and stored in sec_scraper.log'''
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sec_scraper.log')
    ]
)

# my email is hidden inside the config.py file. 
from config import USER_AGENT_EMAIL



#Updated USER_AGENT_EMAIL with your email. SEC site recommends using work, but I used personal email.
headers = {'User-Agent': USER_AGENT_EMAIL}

''' this code will try 5 times, each time expoennetialy more time for the request, before moving on. Not fully sure if
this is the most efficient approach, however, it was similar to what I read from other similar scrapers
'''
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)

# SEC caps max of 10 requests/second. This approach is more conservative and maxes to 5.
#Just for safety, i read SEC sometimes does IP bans incase of abusing the requests, so ensured the code is well under the limit
class RateLimiter:
    def __init__(self, max_requests_per_second=5):  
        self.max_requests = max_requests_per_second
        self.tokens = max_requests_per_second
        self.updated_at = time.time()
        self.lock = threading.Lock()
    
    def wait_for_token(self):
        with self.lock:
            now = time.time()
            time_passed = now - self.updated_at
            self.tokens = min(self.max_requests, self.tokens + time_passed * self.max_requests)
            self.updated_at = now
            
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.max_requests
                time.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1

rate_limiter = RateLimiter(max_requests_per_second=5)

def create_session():
    """Create a requests session with retry strategy"""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.headers.update(headers)
    return session

# Outpt configss
OUTPUT_FOLDER = 'all_10k_filings'
SUMMARY_FOLDER = os.path.join(OUTPUT_FOLDER, 'summaries')
METADATA_FOLDER = os.path.join(OUTPUT_FOLDER, 'metadata')
FILINGS_FOLDER = os.path.join(OUTPUT_FOLDER, 'filings')

# Create  nesessaru folders
for folder in [OUTPUT_FOLDER, SUMMARY_FOLDER, METADATA_FOLDER, FILINGS_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# cotes for progress tracking
class ProgressCounters:
    def __init__(self):
        self.lock = threading.Lock()
        self.processed_companies = 0
        self.total_10k_filings = 0
        self.successful_downloads = 0
        self.failed_downloads = 0
        self.companies_with_10k = 0
        self.companies_without_10k = 0
        self.api_requests_made = 0

counters = ProgressCounters()

def load_company_tickers():
    """Load all company tickers from SEC API"""
    url = "https://www.sec.gov/files/company_tickers.json"
    logging.info("Fetching all company tickers from SEC")
    
    session = create_session()
    rate_limiter.wait_for_token()
    
    resp = session.get(url)
    resp.raise_for_status()
    tickers_data = resp.json()
    
    with counters.lock:
        counters.api_requests_made += 1
    
    # convert to df for future potential processng
    df = pd.DataFrame.from_dict(tickers_data, orient='index')
    df['cik_str'] = df['cik_str'].astype(str).str.zfill(10)
    
    logging.info(f"Loaded {len(df)} company tickers")
    return df

def fetch_submission_metadata(cik, ticker, company_name, session):
    """Fetch submission metadata for a given CIK"""
    url = f'https://data.sec.gov/submissions/CIK{cik}.json'
    
    try:
        # Rate limiting before request
        rate_limiter.wait_for_token()
        
        resp = session.get(url)
        resp.raise_for_status()
        data = resp.json()
        
        with counters.lock:
            counters.api_requests_made += 1
        
        if 'filings' not in data or 'recent' not in data['filings']:
            logging.warning(f"No recent filings found for {ticker} ({company_name})")
            return None
            
        return data
        
    except requests.RequestException as e:
        logging.error(f"Failed to fetch metadata for {ticker} ({company_name}): {e}")
        return None

def filter_10k_filings(metadata_json):
    """Extract 10-K filings from submission metadata"""
    if not metadata_json:
        return []
        
    filings = metadata_json['filings']['recent']
    forms = filings.get('form', [])
    indices = [i for i, f in enumerate(forms) if f == '10-K']
    
    ten_k_filings = []
    for i in indices:
        record = {k: filings[k][i] for k in filings.keys()}
        ten_k_filings.append(record)
    
    return ten_k_filings

def save_json(data, filepath):
    """Save data as JSON file"""
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        logging.error(f"Failed to save JSON to {filepath}: {e}")
        return False

def download_full_filing(cik, accession_no, primary_doc, ticker, session):
    """Download the full filing document"""
    accession_no_nodash = accession_no.replace('-', '')
    url = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_nodash}/{primary_doc}'
    
    # Create company-specific folder
    company_folder = os.path.join(FILINGS_FOLDER, ticker)
    os.makedirs(company_folder, exist_ok=True)
    
    filename = f"{cik}_{accession_no_nodash}_{primary_doc}"
    filepath = os.path.join(company_folder, filename)
    
    try:
        # Rate limiting before request
        rate_limiter.wait_for_token()
        
        resp = session.get(url)
        resp.raise_for_status()
        
        with counters.lock:
            counters.api_requests_made += 1
        
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        
        with counters.lock:
            counters.successful_downloads += 1
        
        return True
        
    except requests.RequestException as e:
        logging.error(f"Failed to download {ticker} filing {url}: {e}")
        with counters.lock:
            counters.failed_downloads += 1
        return False

def create_summary_csv(ten_k_filings, ticker, cik, company_name):
    """Create summary CSV for a company's 10-K filings"""
    if not ten_k_filings:
        return False
        
    summary_rows = []
    for filing in ten_k_filings:
        summary_rows.append({
            'ticker': ticker,
            'company_name': company_name,
            'cik': cik,
            'accessionNumber': filing.get('accessionNumber', ''),
            'filingDate': filing.get('filingDate', ''),
            'reportDate': filing.get('reportDate', ''),
            'form': filing.get('form', ''),
            'primaryDocument': filing.get('primaryDocument', ''),
            'filingHref': f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{filing.get('accessionNumber', '').replace('-', '')}/{filing.get('primaryDocument', '')}"
        })
    
    df = pd.DataFrame(summary_rows)
    csv_path = os.path.join(SUMMARY_FOLDER, f"{ticker}_{cik}_10k_summary.csv")
    
    try:
        df.to_csv(csv_path, index=False)
        return True
    except Exception as e:
        logging.error(f"Failed to save CSV for {ticker}: {e}")
        return False

def process_company(company_info, pbar):
    """Process a single company's 10-K filings with thread-local session"""
    ticker, cik, company_name = company_info
    ''' w/o code would take about ~6 hrs to run, with it takes about 2 hrs to finish. Definetly useful'''
    # Create thread-local session
    session = create_session()
    
    # Update progress bar description (thread-safe)
    pbar.set_description(f"Processing {ticker}")
    
    try:
        # Fetch submission metadata
        submission_metadata = fetch_submission_metadata(cik, ticker, company_name, session)
        
        if submission_metadata:
            # Save raw metadata
            metadata_path = os.path.join(METADATA_FOLDER, f"{ticker}_{cik}_metadata.json")
            save_json(submission_metadata, metadata_path)
            
            # Filter 10-K filings
            ten_k_filings = filter_10k_filings(submission_metadata)
            
            if ten_k_filings:
                with counters.lock:
                    counters.companies_with_10k += 1
                    counters.total_10k_filings += len(ten_k_filings)
                
                # Save 10-K specific metadata
                ten_k_path = os.path.join(METADATA_FOLDER, f"{ticker}_{cik}_10k_metadata.json")
                save_json(ten_k_filings, ten_k_path)
                
                # Create summary CSV
                create_summary_csv(ten_k_filings, ticker, cik, company_name)
                
                # Download all 10-K filings for this company
                for filing in ten_k_filings:
                    download_full_filing(
                        cik, 
                        filing['accessionNumber'], 
                        filing['primaryDocument'], 
                        ticker,
                        session
                    )
                    
                logging.info(f"Processed {ticker}: {len(ten_k_filings)} 10-K filings")
            else:
                with counters.lock:
                    counters.companies_without_10k += 1
                logging.info(f"No 10-K filings found for {ticker}")
        
        with counters.lock:
            counters.processed_companies += 1
            
        return True
        
    except Exception as e:
        logging.error(f"Error processing {ticker}: {e}")
        with counters.lock:
            counters.processed_companies += 1
        return False
    
    finally:
        session.close()

def create_master_summary(companies_df):
    """Create a master summary of all processed companies"""
    logging.info("Creating master summary...")
    
    # Collect all individual summary files
    all_summaries = []
    summary_files = [f for f in os.listdir(SUMMARY_FOLDER) if f.endswith('_10k_summary.csv')]
    
    for summary_file in tqdm(summary_files, desc="Compiling master summary"):
        try:
            df = pd.read_csv(os.path.join(SUMMARY_FOLDER, summary_file))
            all_summaries.append(df)
        except Exception as e:
            logging.error(f"Failed to read {summary_file}: {e}")
    
    if all_summaries:
        master_df = pd.concat(all_summaries, ignore_index=True)
        master_df = master_df.sort_values(['ticker', 'filingDate'])
        
        master_path = os.path.join(OUTPUT_FOLDER, 'master_10k_summary.csv')
        master_df.to_csv(master_path, index=False)
        logging.info(f"Master summary saved to {master_path} with {len(master_df)} total 10-K filings")
        
        # Create statistics summary
        stats = {
            'total_companies_processed': counters.processed_companies,
            'companies_with_10k_filings': counters.companies_with_10k,
            'companies_without_10k_filings': counters.companies_without_10k,
            'total_10k_filings_found': counters.total_10k_filings,
            'successful_downloads': counters.successful_downloads,
            'failed_downloads': counters.failed_downloads,
            'unique_companies_in_master': master_df['ticker'].nunique(),
            'total_api_requests_made': counters.api_requests_made,
            'date_range': {
                'earliest_filing': master_df['filingDate'].min(),
                'latest_filing': master_df['filingDate'].max()
            }
        }
        
        stats_path = os.path.join(OUTPUT_FOLDER, 'processing_statistics.json')
        save_json(stats, stats_path)
        
        return master_df, stats
    
    return None, None

def main():
    """Main execution function with multithreading"""
    start_time = time.time()
    
    logging.info("=== Starting multithreaded 10-K scraper ===")
    
    # Load all company tickers
    companies_df = load_company_tickers()
    total_companies = len(companies_df)
    
    # Determine optimal thread count
    # Conservative approach: limit threads to ensure we don't overwhelm SEC servers
    max_threads = min(8, max(2, total_companies // 100))  # Scale with company count but cap at 8
    logging.info(f"Processing {total_companies} companies with {max_threads} threads...")
    logging.info(f"Rate limit: {rate_limiter.max_requests} requests/second")
    
    # Prepare company data for processing
    company_data = [
        (row['ticker'], row['cik_str'], row['title']) 
        for _, row in companies_df.iterrows()
    ]
    
    # Process companies with multithreading
    with tqdm(total=total_companies, desc="Processing companies") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Submit all tasks
            future_to_company = {
                executor.submit(process_company, company_info, pbar): company_info[0]
                for company_info in company_data
            }
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_company):
                ticker = future_to_company[future]
                try:
                    result = future.result()
                    pbar.update(1)
                except Exception as exc:
                    logging.error(f"Company {ticker} generated an exception: {exc}")
                    pbar.update(1)
    
    # Create master summary
    master_df, stats = create_master_summary(companies_df)
    
    # Final logging
    elapsed_time = time.time() - start_time
    
    logging.info("=== Processing Complete ===")
    logging.info(f"Total execution time: {elapsed_time:.2f} seconds")
    logging.info(f"Average time per company: {elapsed_time/total_companies:.2f} seconds")
    logging.info(f"Companies processed: {counters.processed_companies}")
    logging.info(f"Companies with 10-K filings: {counters.companies_with_10k}")
    logging.info(f"Total 10-K filings found: {counters.total_10k_filings}")
    logging.info(f"Successful downloads: {counters.successful_downloads}")
    logging.info(f"Failed downloads: {counters.failed_downloads}")
    logging.info(f"Total API requests made: {counters.api_requests_made}")
    logging.info(f"Average API requests per second: {counters.api_requests_made/elapsed_time:.2f}")
    
    if stats:
        print("\n" + "="*60)
        print("PROCESSING SUMMARY")
        print("="*60)
        for key, value in stats.items():
            if key != 'date_range':
                if isinstance(value, int):
                    print(f"{key.replace('_', ' ').title()}: {value:,}")
                else:
                    print(f"{key.replace('_', ' ').title()}: {value}")
        
        if 'date_range' in stats:
            print(f"Filing Date Range: {stats['date_range']['earliest_filing']} to {stats['date_range']['latest_filing']}")
        
        print(f"\nPerformance Metrics:")
        print(f"Execution Time: {elapsed_time:.2f} seconds")
        print(f"Average Time Per Company: {elapsed_time/total_companies:.2f} seconds")
        print(f"API Request Rate: {counters.api_requests_made/elapsed_time:.2f} req/sec")
        print(f"Thread Count Used: {max_threads}")
        
        print(f"\nFiles saved to: {OUTPUT_FOLDER}")
        print(f"Master summary: {os.path.join(OUTPUT_FOLDER, 'master_10k_summary.csv')}")

if __name__ == "__main__":
    main()