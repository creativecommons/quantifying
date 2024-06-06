"""
This is to clean the data pulled by the photos_detail.py script so as to
further delete useless columns and reorganize the dataset as this form:

|       locations                  | amount |   time     | license | content_categories | highest_comment | total_view |  # noqa: E501
| -------------------------------- | -----: | ---------- | ------: | ------------------ | --------------: | ---------: |  # noqa: E501
| Minneapolis, United States       |     20 | 2022-10-22 |       4 | football, life     |             105 |     100000 |  # noqa: E501
| São José do Rio Preto SP, Brasil |     30 | 2022-10-22 |       4 | football, life     |              50 |     300000 |  # noqa: E501
...

Note:
content_categories will be got from basic NLP on the tags column
"""

# Standard library
import sys
import traceback

# Third-party
import pandas as pd

sys.path.append(".")
# First-party/Local
import quantify  # noqa: E402

# Setup only LOGGER using quantify.setup()
_, _, _, _, LOGGER = quantify.setup(__file__)


def drop_empty_column(csv_path, new_csv_path):
    """
    Drops columns with 'Unnamed' in the name from the CSV file.
    Args:
    - csv_path (str): Path to the original CSV file.
    - new_csv_path (str): Path to save the cleaned CSV file.
    """
    LOGGER.info("Dropping 'Unnamed' columns from the CSV file.")

    df = pd.read_csv(csv_path)
    for col in df.columns:
        if "Unnamed" in col:
            data = df.drop(col, axis=1)
            LOGGER.info(f"Dropping column {col}")
    data.to_csv(new_csv_path)
    LOGGER.info("Dropping empty columns completed.")


def drop_duplicate_id(csv_path, new_csv_path):
    """
    Drops duplicate rows based on the 'id' column from the CSV file.

    Args:
    - csv_path (str): Path to the original CSV file.
    - new_csv_path (str): Path to save the cleaned CSV file.
    """
    LOGGER.info(
        "Dropping duplicate rows based on the 'id' column from the CSV file."
    )

    df = pd.read_csv(csv_path)
    data = df.drop_duplicates(subset=["id"])
    data.to_csv(new_csv_path)
    LOGGER.info("Dropping duplicates completed.")


def save_new_data(csv_path, column_name_list, new_csv_path):
    """
    Saves specified columns from the original CSV file to a new CSV file.

    Args:
    - csv_path (str): Path to the original CSV file.
    - column_name_list (list of str): List of column names to be saved
    (belongs to the existing column names from original csv)
    - new_csv_path (str): Path to save the new CSV file.
    """
    LOGGER.info("Saving columns from the original CSV to a new CSV.")

    df = pd.read_csv(csv_path)
    new_df = pd.DataFrame()
    for col in column_name_list:
        new_df[col] = list(df[col])
        LOGGER.info(f"Saving column {col}")
    new_df.to_csv(new_csv_path)
    LOGGER.info("Saving new data to new csv")


def main():
    drop_empty_column("hs.csv", "dataset/cleaned_license10.csv")
    drop_duplicate_id(
        "dataset/cleaned_license10.csv", "dataset/cleaned_license10.csv"
    )
    save_new_data(
        "dataset/cleaned_license10.csv",
        [
            "location",
            "dates",
            "license",
            "description",
            "tags",
            "views",
            "comments",
        ],
        "dataset/cleaned_license10.csv",
    )


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
