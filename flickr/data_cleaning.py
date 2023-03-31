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

# Import Standard library
import sys
import traceback

# Import Third-party
import pandas as pd


def drop_empty_column(csv_path, new_csv_path):  # attribute is string
    df = pd.read_csv(csv_path)
    for col in df.columns:  # to get the column list
        if "Unnamed" in col:
            data = df.drop(col, axis=1)
            print("Dropping column", col)
    data.to_csv(new_csv_path)
    print("Dropping empty columns")


def drop_duplicate_id(csv_path, new_csv_path):  # attribute is string
    df = pd.read_csv(csv_path)
    data = df.drop_duplicates(subset=["id"])
    data.to_csv(new_csv_path)
    print("Dropping duplicates")


def save_new_data(csv_path, column_name_list, new_csv_path):  # attribute is string
    """
    column_name_list must belongs to the
    existing column names from original csv
    csv_path is the path of original csv
    This function generate a new dataframe
    to save final data with useful columns
    """
    df = pd.read_csv(csv_path)
    new_df = pd.DataFrame()
    for col in column_name_list:
        new_df[col] = list(df[col])
        print("Saving column", col)
    new_df.to_csv(new_csv_path)
    print("Saving new data to new csv")


def main():
    drop_empty_column("final.csv", "dataset/cleaned_license10.csv")
    drop_duplicate_id("dataset/cleaned_license10.csv", "dataset/cleaned_license10.csv")
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
        sys.exit(e.code)
    except KeyboardInterrupt:
        print("INFO (130) Halted via KeyboardInterrupt.", file=sys.stderr)
        sys.exit(130)
    except Exception:
        print("ERROR (1) Unhandled exception:", file=sys.stderr)
        print(traceback.print_exc(), file=sys.stderr)
    sys.exit(1)
