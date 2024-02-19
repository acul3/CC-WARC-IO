import json
import os
import requests
from warcio.archiveiterator import ArchiveIterator
import trafilatura
import fasttext
import time
from multiprocessing import Pool
import logging
from pathlib import Path

# Configuration
DATA_ROOT = "/home/acul/data_nvme6/cc_news/"
FASTTEXT_MODEL_PATH = 'lid.176.bin'
WARC_FILE_LIST = Path(DATA_ROOT) / 'warc.txt'
UNSUCCESSFUL_DOWNLOADS_FILE = Path(DATA_ROOT) / 'unsucess_2.txt'
NO_ID_FILE = Path(DATA_ROOT) / 'no_id.txt'
LANGUAGE_TARGET = 'id'
NUM_WORKERS = 10

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load fastText model for language detection
model = fasttext.load_model(FASTTEXT_MODEL_PATH)

def download_file(url, filepath):
    logging.info(f"Downloading {filepath}")
    number_of_retries = 5
    delay = 60  # delay in seconds

    for attempt in range(number_of_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            with open(filepath, "wb") as file:
                file.write(response.content)
            return True
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP Error for {url}: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception for {url}: {e}")

        if attempt < number_of_retries - 1:
            logging.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            delay += 60
        else:
            logging.error(f"Failed to download {url} after {number_of_retries} attempts.")
            with open(UNSUCCESSFUL_DOWNLOADS_FILE, "a") as file:
                file.write(url + "\n")
            return False

def process_warc_file(warc_path, output_path):
    jsonl_data = []
    try:
        with open(warc_path, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    try:
                        content = trafilatura.extract(record.content_stream().read())
                        if content:
                            lang_predictions = model.predict(content.replace("\n", ""))
                            lang = lang_predictions[0][0].replace('__label__', '')
                            if lang == LANGUAGE_TARGET:
                                url = record.rec_headers.get_header('WARC-Target-URI')
                                jsonl_data.append({
                                    'text': content,
                                    'url': url,
                                })
                    except Exception as e:
                        logging.error(f"Error processing record: {e}")
    except Exception as e:
        logging.error(f"Error reading WARC file {warc_path}: {e}")
        with open(UNSUCCESSFUL_DOWNLOADS_FILE, 'a') as file:
            file.write(str(warc_path) + "\n")

    if jsonl_data:
        with open(output_path, 'w') as file:
            for entry in jsonl_data:
                file.write(json.dumps(entry) + '\n')
    else:
        with open(NO_ID_FILE, 'a') as file:
            file.write(str(warc_path) + "\n")

def process_warc_url(warc_url):
    if warc_url.startswith("crawl-data"):
        warc_url = f"https://data.commoncrawl.org/{warc_url}"
    warc_filename = Path(warc_url.split('/')[-1])
    jsonl_filename = warc_filename.with_suffix('.jsonl')

    warc_path = Path(DATA_ROOT) / warc_filename
    jsonl_path = Path(DATA_ROOT) / jsonl_filename

    if not jsonl_path.exists():
        if download_file(warc_url, warc_path):
            process_warc_file(warc_path, jsonl_path)
            warc_path.unlink()  # Remove WARC file after processing

def main():
    with open(WARC_FILE_LIST, 'r') as file:
        warc_urls = [line.strip() for line in file.readlines()]

    with Pool(NUM_WORKERS) as pool:
        pool.map(process_warc_url, warc_urls)

if __name__ == "__main__":
    main()
