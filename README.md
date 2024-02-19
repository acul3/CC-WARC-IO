# Common Crawl WARC Processor

This script automates the downloading and processing of WARC (Web ARChive) files from the Common Crawl dataset. It extracts content from these archives, performs language detection using fastText, and filters content based on the specified target language (default is Indonesian). Extracted content is then saved in a JSONL format for further analysis or processing.

## Features

- **Automatic Downloading:** Downloads WARC files from specified URLs.
- **Content Extraction:** Uses `trafilatura` to extract web page content from WARC files.
- **Language Detection:** Employs `fastText` to detect the language of extracted content.
- **Customizable Filtering:** Filters content by language (configurable to any language supported by `fastText`).
- **Multiprocessing Support:** Utilizes multiple CPU cores for faster processing of multiple WARC files simultaneously.

## Dependencies

- Python 3.x
- `requests`
- `warcio`
- `trafilatura`
- `fasttext`
- `tqdm` (optional, for progress bar support)

## Setup

1. **Clone the Repository:**

git clone <repository-url>
cd <repository-folder>


2. **Install Required Python Packages:**


3. **Download fastText Pre-trained Model:**

Download the fastText pre-trained language detection model (`lid.176.bin`) from the official fastText website and place it in the script's directory or specify its path in the script configuration.

## Configuration

Edit the script or create a `.env` file (if your script is set up to use environment variables) to customize the following settings:

- `DATA_ROOT`: The root directory where WARC files will be downloaded and processed.
- `FASTTEXT_MODEL_PATH`: Path to the fastText model file for language detection.
- `LANGUAGE_TARGET`: Target language code for filtering content (default is `id` for Indonesian).
- `NUM_WORKERS`: Number of worker processes for multiprocessing.

## Usage

Run the script with the following command:

python cc_news.py


Ensure you have a file named `warc.txt` in your `DATA_ROOT` directory with one WARC file URL per line that you wish to download and process.

## Contributing

Contributions to improve the script or add new features are welcome. Please submit a pull request or open an issue to discuss your ideas.

## License

Specify your license here or indicate if the project is open-source under a specific license agreement.


