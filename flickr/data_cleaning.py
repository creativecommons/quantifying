"""
This is to clean the data pulled
by the photos_detail.py script
so as to further delete useless columns
and reorganize the dataset as this form:
       locations                   |  amount  |     time      |  license  |  content_categories  | highest_comment | total_view
Minneapolis, United States         |    20    |   2022-10-22  |     4     |    football, life    |      105        |    100000
São José do Rio Preto SP, Brasil   |    30    |   2022-10-22  |     4     |    football, life    |       50        |    300000
...
Note:
content_categories will be got from basic NLP on the tags column
"""

# Standard library
import sys
import traceback

# Third-party
import pandas as pd


def drop_empty_column(csv_path, new_csv_path):  # attribute is string
    df = pd.read_csv(csv_path)
    for col in df.columns:  # to get the column list
        if "Unnamed" in col:
            data = df.drop(col, axis=1)
    data.to_csv(new_csv_path)


def drop_duplicate_id(csv_path, new_csv_path):  # attribute is string
    df = pd.read_csv(csv_path)
    data = df.drop_duplicates(subset=['id'])
    data.to_csv(new_csv_path)


"""
column_name_list must belongs to the
existing column names from original csv
csv_path is the path of original csv
This function generate a new dataframe
to save final data with useful columns
"""


def save_new_data(csv_path, column_name_list, new_csv_path):  # attribute is string
    df = pd.read_csv(csv_path)
    new_df = pd.DataFrame()
    for col in column_name_list:
        new_df[col] = list(df[col])
    new_df.to_csv(new_csv_path)


def main():
    drop_empty_column("hs.csv", "cleaned_hs.csv")
    drop_duplicate_id("hs.csv", "cleaned_hs.csv")
    save_new_data("hs.csv", ["location", "id", "dates", "license", "tags", "views", "comments"], "cleaned_hs.csv")

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