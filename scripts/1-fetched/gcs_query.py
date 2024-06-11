# Standard library
import argparse
import os
import sys
import time
import traceback
from typing import List

# Third-party
import googleapiclient.discovery
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.errors import HttpError

# Constants
API_KEY = os.getenv("GOOGLE_API_KEY")
CX = os.getenv("CUSTOM_SEARCH_ENGINE_ID")
BASE_URL = "https://www.googleapis.com/customsearch/v1"


# Setup paths and LOGGER using shared library
sys.path.append(".")
# First-party/Local
import quantify  # noqa: E402

PATH_REPO_ROOT, PATH_WORK_DIR, PATH_DOTENV, LOGGER = quantify.setup(__file__)

# Load environment variables
load_dotenv(PATH_DOTENV)

# Log the start of the script execution
LOGGER.info("Script execution started.")


def get_search_service():
    """
    Creates and returns the Google Custom Search API service.
    """
    return googleapiclient.discovery.build(
        "customsearch", "v1", developerKey=API_KEY
    )


def fetch_results(
    query: str, records_per_query: int, pages: int
) -> List[dict]:
    """
    Fetch search results from Google Custom Search API.
    """
    service = get_search_service()
    all_results = []

    for page in range(pages):
        start_index = page * records_per_query + 1
        try:
            results = (
                service.cse()
                .list(q=query, cx=CX, num=records_per_query, start=start_index)
                .execute()
            )
            all_results.extend(results.get("items", []))
            time.sleep(1)  # Ensure script completes within ~1 second
        except HttpError as e:
            LOGGER.error(f"Error fetching results: {e}")
            break

    return all_results


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
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

    all_results = fetch_results(query, records_per_query, pages)

    # Create new directory structure for year and quarter
    year = time.strftime("%Y")
    quarter = (int(time.strftime("%m")) - 1) // 3 + 1
    data_directory = os.path.join(
        PATH_REPO_ROOT, "data", "1-fetched", f"{year}Q{quarter}"
    )
    os.makedirs(data_directory, exist_ok=True)

    # Convert results to DataFrame to save as CSV
    df = pd.DataFrame(all_results)
    file_name = os.path.join(
        data_directory, "google_custom_search_results.csv"
    )

    df.to_csv(file_name, index=False)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
