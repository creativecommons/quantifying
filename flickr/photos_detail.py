"""
The following is a version of api call
optimized with taking up less memory
and no duplicate data (ideally)
step1: API call
step2: save useful data in the format of [[], []]
step3: saving lists of data to DataFrame
"""

# Standard library
import json
import os
import sys
import time
import traceback

# Third-party
import flickrapi
import pandas as pd
from dotenv import load_dotenv

sys.path.append(".")
# First-party/Local
import quantify  # noqa: E402

# Setup paths, and LOGGER using quantify.setup()
_, PATH_WORK_DIR, PATH_DOTENV, _, LOGGER = quantify.setup(__file__)

# Load environment variables
load_dotenv(PATH_DOTENV)

# Global variable: Number of retries for error handling
RETRIES = 0


# LOG the start of the script execution
LOGGER.info("Script execution started.")


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

    df = [pd.DataFrame() for ind in range(len(datalist))]
    df = pd.DataFrame(datalist).transpose()
    df.columns = namelist
    return df


def df_to_csv(temp_list, name_list, temp_csv, final_csv):
    """
    Save data to temporary CSV and then merge it with final CSV.

    Args:
    - temp_list (list): csv that is used for saving data every 100 seconds.
    It is set to prevent data from losing when script stops
    - name_list (list): List of column names.
    - temp_csv (str): Temporary CSV file path.
    - final_csv (str): Final CSV file path.
    """
    LOGGER.info("Saving data to temporary CSV and merging with final CSV.")

    df = to_df(temp_list, name_list)
    df.to_csv(temp_csv)
    # Merge temporary CSV with final CSV, ignoring index to avoid duplication
    df = pd.concat(map(pd.read_csv, [temp_csv, final_csv]), ignore_index=True)
    df.to_csv(final_csv)


def creat_lisoflis(size):
    """
    Create one list of list [[],[],[]] to save all the columns with
    each column as a list

    Args:
    - size (int): Size of the list of lists.

    Returns:
    - temp_list (list): List of empty lists.
    """
    LOGGER.info("Saving all the columns with each column as a list")

    temp_list = [[] for i in range(size)]
    return temp_list


def clean_saveas_csv(old_csv_str, new_csv_str):
    """
    Clean empty columns and save CSV to a new file.

    Args:
    - old_csv_str (str): Path to the old CSV file.
    - new_csv_str (str): Path to the new CSV file.
    """
    LOGGER.info("Cleaning empty columns and save CSV to a new file.")

    data = pd.read_csv(old_csv_str, low_memory=False)
    for col in list(data.columns):
        if "Unnamed" in col:
            data.drop(col, inplace=True, axis=1)
    data.to_csv(new_csv_str)


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
    yield queried_raw


def query_helper2(raw, part, temp_list, index):
    """
    Helper function 2 for querying data.

    Args:
    - raw (dict): Raw data from API.
    - part (str): Part of the data.
    - temp_list (list): List to store queried data.
    - index (int): Index of the data in temp_list.
    """
    # part should be string
    queried_raw = raw["photo"][part]
    yield queried_raw


def query_data(raw_data, name_list, data_list):
    """
    Query useful data from raw pulled data and store it in lists.
    In our case useful data is supposed to be this
    name list: ["id", "dateuploaded", "isfavorite",
    "license", "realname", "location", "title",
    "description", "dates", "views", "comments", "tags"]

    Args:
    - raw_data (dict): Raw data from API.
    - name_list (list): List of column names.
    - data_list (list): List of lists to store data.
    """

    LOGGER.info(
        "Querying useful data from raw pulled data and storing it in lists."
    )

    for a in range(0, len(name_list)):
        if (0 <= a < 4) or a == 9:
            temp = query_helper2(raw_data, name_list[a], data_list, a)
            data_list[a].append(next(temp))
        elif a == 4 or a == 5:
            temp = query_helper1(raw_data, "owner", name_list[a], data_list, a)
            data_list[a].append(next(temp))
        elif a == 6 or a == 7 or a == 10:
            temp = query_helper1(
                raw_data, name_list[a], "_content", data_list, a
            )
            data_list[a].append(next(temp))
        elif a == 8:
            temp = query_helper1(raw_data, name_list[a], "taken", data_list, a)
            data_list[a].append(next(temp))
        # Each photo ID can have multiple tags,
        # so we save the tags for each ID as a list.
        # Further cleaning or analysis may be required for this data column.
        if a == 11:
            tags = raw_data["photo"]["tags"]["tag"]
            if tags:
                data_list[a].append(
                    [tags[num]["raw"] for num in range(len(tags))]
                )
            else:
                temp = query_helper1(
                    raw_data, name_list[a], "tag", data_list, a
                )
                data_list[a].append(next(temp))


def page1_reset(final_csv, raw_data):
    """
    Reset page count and update total picture count.

    Args:
    - final_csv (str): Path to the final CSV file.
    - raw_data (dict): Raw data from API call.

    Returns:
    - int: Total number of pages.
    """
    LOGGER.info("Reset page count and update total picture count.")

    data = pd.read_csv(final_csv, low_memory=False)
    for col in list(data.columns):
        data.drop(col, inplace=True, axis=1)
        data.to_csv(final_csv)
    return raw_data["photos"]["pages"]


def main():
    final_csv_path = os.path.join(PATH_WORK_DIR, "final.csv")
    record_txt_path = os.path.join(PATH_WORK_DIR, "rec.txt")
    hs_csv_path = os.path.join(PATH_WORK_DIR, "hs.csv")

    # Initialize Flickr API instance
    flickr = flickrapi.FlickrAPI(
        os.getenv("FLICKR_API_KEY"),
        os.getenv("FLICKR_API_SECRET"),
        format="json",
    )
    # List of Creative Commons licenses
    license_list = [1, 2, 3, 4, 5, 6, 9, 10]

    # List of column names for the final CSV
    # name_list is the header of final table
    # temp_list is in the form of list within list, which saves the actual data
    # each internal list is a column: ie. temp_list[0] saves the data of id
    # number
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
    # Read current page, license, and total from record text file
    # Resumes iteration from the last processed page if the script
    # encounters errors or stops.
    with open(record_txt_path) as f:
        readed = f.read().split(" ")
        j = int(readed[0])  # Current page
        i = int(readed[1])  # Current license
        total = int(readed[2])  # Total number of pages
    while i in license_list:
        # Iterate through pages
        while j <= total:
            # Use search method to pull photo ids for each license
            photosJson = flickr.photos.search(license=i, per_page=100, page=j)
            time.sleep(1)
            photos = json.loads(photosJson.decode("utf-8"))
            id = [x["id"] for x in photos["photos"]["photo"]]

            # Reset total and clear final CSV if on the first page
            if j == 1:
                total = page1_reset(final_csv_path, photos)

            # Use getInfo method to get detailed photo info from photo ids
            # Query data and save into temp_list as columns of final dataset
            for index in range(0, len(id)):
                detailJson = flickr.photos.getInfo(
                    license=i, photo_id=id[index]
                )
                time.sleep(1)
                photos_detail = json.loads(detailJson.decode("utf-8"))
                LOGGER.info(
                    f"{index} id out of {len(id)} in license {i}, "
                    f"page {j} out of {total}"
                )

                # query process of useful data
                query_data(photos_detail, name_list, temp_list)

            j += 1
            LOGGER.info(
                f"Page {j} out of {total} in license {i}"
                f"with retry number {RETRIES}"
            )

            # save data to csv
            df_to_csv(temp_list, name_list, hs_csv_path, final_csv_path)
            # Update current page in record text file
            with open(record_txt_path, "w") as f:
                f.write(f"{j} {i} {total}")

            # Clear temp_list everytime after saving the data into
            # the csv file to prevent duplication
            temp_list = creat_lisoflis(len(name_list))

            # If reached max limit of pages, reset j to 1 and
            # update i to the license in the dictionary
            if j == total + 1 or j > total:
                license_i_path = os.path.join(PATH_WORK_DIR, f"license{i}.csv")
                clean_saveas_csv(final_csv_path, license_i_path)
                i += 1
                j = 1
                while i not in license_list:
                    i += 1
                with open(record_txt_path, "w") as f:
                    f.write(f"{j} {i} {total}")

                # Clear temp_list before rerun to prevent duplication
                temp_list = creat_lisoflis(len(name_list))
                break


if __name__ == "__main__":
    RETRIES = 0  # Initialize RETRIES counter
    while True:
        try:
            main()
        except SystemExit as e:
            LOGGER.error(f"System exit with code: {e.code}")
            sys.exit(e.code)
        except KeyboardInterrupt:
            LOGGER.info("(130) Halted via KeyboardInterrupt.")
            sys.exit(130)
        except Exception:
            RETRIES += 1
            LOGGER.exception(
                f"(1) Unhandled exception: {traceback.format_exc()}"
            )
            if RETRIES <= 20:
                continue
            else:
                sys.exit(1)
