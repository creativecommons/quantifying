#!/usr/bin/env python
"""
Script to fetch photo information from Flickr API, process the data,
and save it into multiple CSV files and a JSON file.
"""

# Standard library
import argparse
import csv
import json
import os
import sys
import time
import traceback

# Third-party
import flickrapi
import pandas as pd
from dotenv import load_dotenv

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup paths, and LOGGER using quantify.setup()
LOGGER, PATHS = shared.setup(__file__)

# Load environment variables
load_dotenv(PATHS["dotenv"])

# Global variable: Number of retries for error handling
RETRIES = 0

# Log the start of the script execution
LOGGER.info("Script execution started.")

# PATHS["data_phase"], "flickr_fetched",

# Flickr API rate limits
FLICKR_API_CALLS_PER_HOUR = 3600
SECONDS_PER_HOUR = 3600
API_CALL_INTERVAL = SECONDS_PER_HOUR / FLICKR_API_CALLS_PER_HOUR


def parse_arguments():
    """
    Parses command-line arguments, returns parsed arguments.
    """
    LOGGER.info("Parsing command-line arguments")
    parser = argparse.ArgumentParser(description="Google Custom Search Script")
    parser.add_argument(
        "--records", type=int, default=1, help="Number of records per query"
    )
    parser.add_argument(
        "--pages", type=int, default=1, help="Number of pages to query"
    )
    parser.add_argument(
        "--licenses", type=int, default=1, help="Number of licenses to query"
    )
    return parser.parse_args()


def to_df(datalist, namelist):
    """
    Transform data into a DataFrame.

    Args:
    - datalist (list): List of lists containing data.
    - namelist (list): List of column names.

    Returns:
    - df (DataFrame): DataFrame constructed from the data.
    """
    LOGGER.info("Transforming data into a DataFrame.")
    df = pd.DataFrame(datalist).transpose()
    df.columns = namelist
    return df


def df_to_csv(temp_list, name_list, temp_csv, final_csv):
    """
    Save data to temporary CSV and then merge it with final CSV.

    Args:
    - temp_list (list): csv that is used for saving data every 100 seconds.
    - name_list (list): List of column names.
    - temp_csv (str): Temporary CSV file path.
    - final_csv (str): Final CSV file path.
    """
    LOGGER.info("Saving data to temporary CSV and merging with final CSV.")
    df = to_df(temp_list, name_list)
    df.to_csv(temp_csv, index=False)
    # Merge temporary CSV with final CSV, ignoring index to avoid duplication
    if os.path.exists(final_csv):
        df_final = pd.read_csv(final_csv)
        df = pd.concat([df_final, df], ignore_index=True)
    df.to_csv(final_csv, index=False)


def creat_lisoflis(size):
    """
    Create one list of list [[],[],[]] to save all the columns with
    each column as a list.

    Args:
    - size (int): Size of the list of lists.

    Returns:
    - temp_list (list): List of empty lists.
    """
    LOGGER.info("Creating list of lists for data storage.")
    temp_list = [[] for _ in range(size)]
    return temp_list


def clean_saveas_csv(old_csv_str, new_csv_str):
    """
    Clean empty columns and save CSV to a new file.

    Args:
    - old_csv_str (str): Path to the old CSV file.
    - new_csv_str (str): Path to the new CSV file.
    """
    LOGGER.info("Cleaning empty columns and saving CSV to a new file.")
    data = pd.read_csv(old_csv_str, low_memory=False)
    data = data.loc[:, ~data.columns.str.contains("^Unnamed")]
    data.to_csv(new_csv_str, index=False)


def query_helper1(raw, part, detail, temp_list, index):
    """
    Helper function 1 for querying data.

    Args:
    - raw (dict): Raw data from API.
    - part (str): Part of the data.
    - detail (str): Detail to be queried.
    - temp_list (list): List to store queried data.
    - index (int): Index of the data in temp_list.
    """
    queried_raw = raw["photo"][part][detail]
    temp_list[index].append(queried_raw)


def query_helper2(raw, part, temp_list, index):
    """
    Helper function 2 for querying data.

    Args:
    - raw (dict): Raw data from API.
    - part (str): Part of the data.
    - temp_list (list): List to store queried data.
    - index (int): Index of the data in temp_list.
    """
    queried_raw = raw["photo"][part]
    temp_list[index].append(queried_raw)


def query_data(raw_data, name_list, data_list):
    """
    Query useful data from raw pulled data and store it in lists.

    Args:
    - raw_data (dict): Raw data from API.
    - name_list (list): List of column names.
    - data_list (list): List of lists to store data.
    """
    LOGGER.info(
        "Querying useful data from raw pulled data and storing it in lists."
    )
    for a in range(len(name_list)):
        if (0 <= a < 4) or a == 9:
            query_helper2(raw_data, name_list[a], data_list, a)
        elif a in [4, 5]:
            query_helper1(raw_data, "owner", name_list[a], data_list, a)
        elif a in [6, 7, 10]:
            query_helper1(raw_data, name_list[a], "_content", data_list, a)
        elif a == 8:
            query_helper1(raw_data, "dates", "taken", data_list, a)
        if a == 11:
            tags = raw_data["photo"]["tags"]["tag"]
            data_list[a].append([tag["raw"] for tag in tags] if tags else [])


def page1_reset(final_csv, raw_data):
    """
    Reset page count and update total picture count.

    Args:
    - final_csv (str): Path to the final CSV file.
    - raw_data (dict): Raw data from API call.

    Returns:
    - int: Total number of pages.
    """
    LOGGER.info("Resetting page count and updating total picture count.")
    if os.path.exists(final_csv):
        data = pd.read_csv(final_csv, low_memory=False)
        data.drop(data.columns, axis=1, inplace=True)
        data.to_csv(final_csv, index=False)
    return raw_data["photos"]["pages"]


def handle_rate_limiting():
    """
    Handle rate limiting by pausing execution
    to avoid hitting the API rate limit.
    """
    LOGGER.info(
        f"Sleeping for {API_CALL_INTERVAL} seconds to handle rate limiting."
    )
    time.sleep(API_CALL_INTERVAL)


def process_data():
    final_csv_path = os.path.join(
        PATHS["data_phase"], "flickr_fetched", "final.csv"
    )
    record_txt_path = os.path.join(
        PATHS["data_phase"], "flickr_fetched", "rec.txt"
    )
    hs_csv_path = os.path.join(PATHS["data_phase"], "flickr_fetched", "hs.csv")

    # Ensure files exist
    if not os.path.exists(record_txt_path):
        with open(record_txt_path, "w") as f:
            f.write("1 1 1")  # Start from page 1, license 1, total pages 1

    if not os.path.exists(final_csv_path):
        with open(final_csv_path, "w") as f:
            pass  # Create an empty final.csv

    if not os.path.exists(hs_csv_path):
        with open(hs_csv_path, "w") as f:
            pass  # Create an empty hs.csv

    flickr = flickrapi.FlickrAPI(
        os.getenv("FLICKR_API_KEY"),
        os.getenv("FLICKR_API_SECRET"),
        format="json",
    )
    license_list = [1, 2, 3, 4, 5, 6, 9, 10]
    name_list = [
        "id",
        "dateuploaded",
        "isfavorite",
        "license",
        "realname",
        "location",
        "title",
        "description",
        "dates",
        "views",
        "comments",
        "tags",
    ]
    temp_list = creat_lisoflis(len(name_list))

    # Dictionary to store photo data for each Creative Commons license
    photo_data_dict = {license_num: [] for license_num in license_list}

    with open(record_txt_path) as f:
        readed = f.read().split(" ")
        j = int(readed[0])
        i = int(readed[1])
        total = int(readed[2])

    while i in license_list:
        while j <= total:
            try:
                photosJson = flickr.photos.search(
                    license=i, per_page=100, page=j
                )
                handle_rate_limiting()
                photos = json.loads(photosJson.decode("utf-8"))
                id_list = [x["id"] for x in photos["photos"]["photo"]]

                if j == 1:
                    total = page1_reset(final_csv_path, photos)

                for index in range(len(id_list)):
                    detailJson = flickr.photos.getInfo(
                        license=i, photo_id=id_list[index]
                    )
                    handle_rate_limiting()
                    photos_detail = json.loads(detailJson.decode("utf-8"))
                    LOGGER.info(
                        f"{index} id out of {len(id_list)} in "
                        f"license {i}, page {j} out of {total}"
                    )
                    query_data(photos_detail, name_list, temp_list)
                    photo_data_dict[i].append(photos_detail)

                j += 1
                LOGGER.info(
                    f"Page {j} out of {total} in license "
                    f"{i} with retry number {RETRIES}"
                )
                df_to_csv(temp_list, name_list, hs_csv_path, final_csv_path)
                with open(record_txt_path, "w") as f:
                    f.write(f"{j} {i} {total}")
                temp_list = creat_lisoflis(len(name_list))

                if j > total:
                    license_i_path = os.path.join(
                        PATHS["data_phase"],
                        "flickr_fetched",
                        f"cleaned_license{i}.csv",
                    )
                    clean_saveas_csv(final_csv_path, license_i_path)
                    i += 1
                    j = 1
                    while i not in license_list:
                        i += 1
                    with open(record_txt_path, "w") as f:
                        f.write(f"{j} {i} {total}")
                    temp_list = creat_lisoflis(len(name_list))
                    break

            except flickrapi.exceptions.FlickrError as e:
                if "rate limit" in str(e).lower():
                    LOGGER.warning("Rate limit reached, sleeping for an hour.")
                    time.sleep(SECONDS_PER_HOUR)
                    continue
                else:
                    LOGGER.error(f"Flickr API error: {e}")
                    raise

    # Save the dictionary containing photo data to a JSON file
    with open(
        os.path.join(PATHS["data_phase"], "flickr_fetched", "photos.json"), "w"
    ) as json_file:
        json.dump(photo_data_dict, json_file)


def save_license_totals():
    LOGGER.info("Saving license totals.")
    license_counts = {}
    for i in [1, 2, 3, 4, 5, 6, 9, 10]:
        df = pd.read_csv(
            os.path.join(
                PATHS["data_phase"],
                "flickr_fetched",
                f"cleaned_license{i}.csv",
            )
        )
        license_counts[i] = len(df)

    license_total_path = os.path.join(
        PATHS["data_phase"], "flickr_fetched", "license_total.csv"
    )
    with open(license_total_path, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["License", "Total"])
        for license, total in license_counts.items():
            writer.writerow([license, total])


def main():
    # Fetch and merge changes
    shared.fetch_and_merge(PATHS["repo"])

    process_data()
    save_license_totals()
    LOGGER.info("Script execution completed successfully.")

    # Add and commit changes
    shared.add_and_commit(
        PATHS["repo"], PATHS["data_phase"], "Add and commit new reports"
    )

    # Push changes
    shared.push_changes(PATHS["repo"])


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
        if e.exit_code == 0:
            LOGGER.info(e.message)
        else:
            LOGGER.error(e.message)
        sys.exit(e.exit_code)
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
