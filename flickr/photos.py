"""
Fetching photo information from Flickr API for photos under
each Creative Commons license and saving the data into a JSON file
"""

# Standard library
import json
import logging
import os
import os.path
import sys

# Third-party
import flickrapi
from dotenv import load_dotenv

# Get the current working directory
CWD = os.path.dirname(os.path.abspath(__file__))
# Load environment variables
dotenv_path = os.path.join(os.path.dirname(CWD), ".env")
load_dotenv(dotenv_path)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logfile.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main():
    # Initialize Flickr API instance
    flickr = flickrapi.FlickrAPI(
        os.getenv("FLICKR_API_KEY"),
        os.getenv("FLICKR_API_SECRET"),
        format="json",
    )

    # Dictionary to store photo data for each Creative Commons license
    dic = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 9: 0, 10: 0}
    # Use search method to retrieve photo info for each license
    # and store it in the dictionary
    for i in dic.keys():
        photosJson = flickr.photos.search(license=i, per_page=500)
        dic[i] = [json.loads(photosJson.decode("utf-8"))]
    # Save the dictionary containing photo data to a JSON file
    with open(os.path.join(CWD, "photos.json"), "w") as json_file:
        json.dump(dic, json_file)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
    except KeyboardInterrupt:
        logger.info("Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        logger.exception("Unhandled exception:")
        sys.exit(1)
