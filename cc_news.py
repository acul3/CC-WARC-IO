import json
import os
import requests
from warcio.archiveiterator import ArchiveIterator
import trafilatura
import fasttext
import time
from multiprocessing import Pool, cpu_count
from requests.exceptions import HTTPError
from tqdm import tqdm
root = "/home/acul/data_nvme6/cc_news/"
# Function to download a file from a URL
def download_file(url, filename):
    print(f"downloading {filename}")
    number_of_retries = 5
    delay = 60  # delay in seconds (1 minute)

    for i in range(number_of_retries):
        
        try:
            r = requests.get(url)
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while downloading file from {url}: {e}")
        
        if r.status_code == 200:
            with open(filename, "wb") as f:
                f.write(r.content)
            break  # Exit the loop if the download is successful

        else:
            print(f"Error downloading, status code: {r.status_code}")
            if i < number_of_retries - 1:  # Hold off the wait for the last attempt
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Wait for a certain amount of time before reattempting
                delay += 60  # Increase the delay for the next attempt
            else:
                print(f"Failed to download the file after {number_of_retries} attempts.")
                with open("unsucess_2.txt", "a") as f:
                    f.write(url + "\n")

FASTTEXT_MODEL_PATH = 'lid.176.bin'

# Load fastText model for language detection
model = fasttext.load_model(FASTTEXT_MODEL_PATH)

def process_warc_url(warc_url):
    # Step 1: Download WARC file
    if warc_url.startswith("crawl-data"):
        warc_url = "https://data.commoncrawl.org/" + warc_url
    warc_filename = warc_url.split('/')[-1]

    # Initialize JSONL data
    jsonl_data = []
    jsonl_filename = warc_filename.replace('.warc.gz', '.jsonl')

    if not os.path.exists(f"{root}{jsonl_filename}"):
        print(f"file not exist {root}{jsonl_filename}")
        download_file(warc_url, warc_filename)
        if os.path.exists(f"{root}{warc_filename}"):
        # Step 2: Read using warcio
            try:
                with open(warc_filename, 'rb') as stream:
                    for record in ArchiveIterator(stream):
                        if record.rec_type == 'response':
                            # Step 3: Extract content using trafilatura
                            try:
                                content = trafilatura.extract(record.content_stream().read())
                            except Exception as e:
                                print(f"trafilatura failed: {e}")
                                content = None

                            # Step 4: Detect language of the content using fastText
                            if content:
                                try:
                                    lang_predictions = model.predict(content.replace("\n",""))
                                    lang = lang_predictions[0][0].replace('__label__', '')
                                except Exception as e:
                                    print(f"FastText prediction failed: {e}")
                                    lang = "none"

                                # Step 5: Check if language is Indonesian
                                if lang == 'id':
                                    # Step 6: Create a dictionary from data
                                    url = record.rec_headers.get_header('WARC-Target-URI')
                                    jsonl_data.append({
                                        'text': content,
                                        'url': url,
                                    })
            except Exception as e:
                with open("unsucess_2.txt", 'a') as f:
                    f.write(warc_url + "\n")
                print(f"An error occurred while reading WARC for file {warc_url}: {e}")
                return

    # Step 7: Save data with JSONL under the same name
        if jsonl_data:
            with open(jsonl_filename, 'w') as f:
                for entry in jsonl_data:
                    f.write(json.dumps(entry) + '\n')
        else:
            with open("no_id.txt", 'a') as f:
                    f.write(warc_url + "\n")

    # Step 8: Remove WARC file if finished
    if os.path.exists(warc_filename):
        os.remove(warc_filename)

# List of WARC URLs to download
with open('/home/acul/data_nvme6/cc_news/warc.txt', 'r') as f:
    warc_urls = [line.strip() for line in f.readlines()]

# Initialize multiprocessing pool
num_workers = 10
with Pool(num_workers) as pool:
    for _ in tqdm(pool.imap_unordered(process_warc_url, warc_urls), total=len(warc_urls)):
        pass
