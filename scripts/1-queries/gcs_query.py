"""
Questions:
1. Are search results being directed in the right place?
2. Am I supposed to be using requests here?
3. I don't think I'm taking in the API key correctly
"""

# Standard library
import argparse
import os
import time
from typing import List

# Third-party
import pandas as pd
import requests

# Constants
API_KEY = os.getenv("GOOGLE_API_KEY")
CX = os.getenv("CUSTOM_SEARCH_ENGINE_ID")
BASE_URL = ""


# Fetch search results from Google Custom Search API
# Returns a list of search result items
def get_search_results(query: str, num_records: int, start: int) -> List[dict]:
    """
    Parameters:
    query (str): search query
    num_records (int): number of records to fetch
    start (int): start index for records
    """
    params = {
        "key": API_KEY,
        "cx": CX,
        "q": query,
        "num": num_records,
        "start": start,
    }

    # GET request to Google Custom Search API
    response = requests.get(BASE_URL, params=params)

    # Check for request errors
    response.raise_for_status()
    return response.json().get("items", [])


# Parses command-line arguments, returns parsed arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="Google Custom Search Script")
    parser.add_argument(
        "--query", type=str, required=True, help="Search query"
    )
    parser.add_argument(
        "--records", type=int, default=1, help="Number of records per query"
    )
    parser.add_argument(
        "--pages", type=int, default=1, help="Number of pages to query"
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    query = args.query
    records_per_query = args.records
    pages = args.pages

    all_results = []

    for page in range(pages):
        start_index = page * records_per_query + 1
        try:
            results = get_search_results(query, records_per_query, start_index)
            all_results.extend(results)
            time.sleep(1)  # Ensure script completes within ~1s
        except Exception as e:
            print(f"Error fetching results: {e}")
            break

    # Convert results to DataFrame to save as CSV
    df = pd.DataFrame(all_results)
    data_directory = os.path.join(os.getcwd(), "../../data/1-fetched")
    os.makedirs(data_directory, exist_ok=True)
    file_name = os.path.join(
        data_directory, "google_custom_search_results.csv"
    )

    df.to_csv(file_name, index=False)


if __name__ == "__main__":
    main()
