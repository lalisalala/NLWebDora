import requests
import json
import os
import logging
import time
from bs4 import BeautifulSoup

# Setup logging
LOGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../logs"))
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOGS_DIR, "extract_ckan_metadata.log"))
    ]
)
logger = logging.getLogger(__name__)

# CKAN API endpoints
BASE_URL = "https://data.london.gov.uk/api/"
PACKAGE_LIST_URL = BASE_URL + "3/action/package_list"
PACKAGE_SHOW_URL = BASE_URL + "3/action/package_show"

HEADERS = {"User-Agent": "PortalGPT/1.0"}

def clean_html(text):
    return BeautifulSoup(text, "html.parser").get_text()

def fetch_dataset_list():
    try:
        response = requests.get(PACKAGE_LIST_URL, headers=HEADERS)
        response.raise_for_status()
        return response.json().get("result", [])
    except Exception as e:
        logger.error(f"Error fetching dataset list: {e}")
        return []

def fetch_metadata(dataset_id):
    try:
        url = f"{BASE_URL}dataset/{dataset_id}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching metadata for {dataset_id}: {e}")
        return {}

def preprocess_metadata(metadata):
    if not metadata:
        return {}

    resources = []
    extras = metadata.get("extras", {})

    smallest_geography = metadata.get("london_smallest_geography") or extras.get("smallest_geography", "Not specified")

    geospatial_coverage = {
        "bounding_box": metadata.get("london_bounding_box", "Unknown"),
        "smallest_geography": smallest_geography
    }

    if isinstance(metadata.get("resources"), dict):
        for res_id, res in metadata["resources"].items():
            slug = metadata.get("slug", metadata.get("id"))
            corrected_url = f"https://data.london.gov.uk/download/{slug}/{res_id}/{res['title'].replace(' ', '%20')}"
            resources.append({
                "title": res.get("title", "Unnamed Resource"),
                "url": corrected_url,
                "format": res.get("format", "Unknown"),
                "temporal_coverage_from": res.get("temporal_coverage_from"),
                "temporal_coverage_to": res.get("temporal_coverage_to"),
                "size": res.get("check_size", "Unknown"),
                "mimetype": res.get("check_mimetype", "Unknown"),
            })

    licence_data = metadata.get("readonly", {}).get("licence", {}).get("title") or metadata.get("licence", "Unknown License")

    return {
        "id": metadata.get("id", "unknown"),
        "title": metadata.get("title", "Unnamed Dataset"),
        "summary": clean_html(metadata.get("description", "")),
        "publisher": metadata.get("maintainer", "Unknown Publisher"),
        "tags": metadata.get("tags", []),
        "metadata_created": metadata.get("createdAt", "Unknown"),
        "metadata_modified": metadata.get("updatedAt", "Unknown"),
        "temporal_coverage_from": metadata.get("temporal_coverage_from"),
        "temporal_coverage_to": metadata.get("temporal_coverage_to"),
        "geospatial_coverage": geospatial_coverage,
        "resources": resources,
        "license": licence_data,
        "landing_page": f"https://data.london.gov.uk/dataset/{metadata.get('id', '')}"
    }

def fetch_and_write_jsonl(output_file="data/london_datasets_for_ingestion.jsonl"):
    dataset_ids = fetch_dataset_list()
    if not dataset_ids:
        logger.error("No datasets found.")
        return

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        for idx, dataset_id in enumerate(dataset_ids):
            logger.info(f"[{idx+1}/{len(dataset_ids)}] Fetching: {dataset_id}")
            raw_meta = fetch_metadata(dataset_id)
            if raw_meta:
                processed = preprocess_metadata(raw_meta)
                json_str = json.dumps(processed, ensure_ascii=False).replace('\n', ' ')
                f.write(f"{processed['landing_page']}\t{json_str}\n")
            time.sleep(0.1)  # throttle to avoid rate limits

    logger.info(f"✅ Metadata written to: {output_file}")

if __name__ == "__main__":
    fetch_and_write_jsonl()
