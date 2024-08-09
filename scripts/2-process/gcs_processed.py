#!/usr/bin/env python
"""
This file is dedicated to processing Google Custom Search data
for analysis and comparison between quarters.
"""
# Standard library
import os
import sys
import traceback

# import pandas as pd

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# def load_quarter_data(quarter):
#     """
#     Load data for a specific quarter.
#     """
#     file_path = os.path.join(PATHS["data"], f"{quarter}",
#       "1-fetch", "gcs_fetched.csv")
#     if not os.path.exists(file_path):
#         LOGGER.error(f"Data file for quarter {quarter} not found.")
#         return None
#     return pd.read_csv(file_path)


# def compare_data(current_quarter, previous_quarter):
#     """
#     Compare data between two quarters.
#     """
#     current_data = load_quarter_data(current_quarter)
#     previous_data = load_quarter_data(previous_quarter)

#     if current_data is None or previous_data is None:
#         return

#     # Process the data to compare by country
#     compare_by_country(current_data, previous_data,
#   current_quarter, previous_quarter)

#     # Process the data to compare by license
#     compare_by_license(current_data, previous_data,
#       current_quarter, previous_quarter)

#     # Process the data to compare by language
#     compare_by_language(current_data, previous_data,
#       current_quarter, previous_quarter)


# def compare_by_country(current_data, previous_data,
#       current_quarter, previous_quarter):
#     """
#     Compare the number of webpages licensed by country between two quarters.
#     """
#     LOGGER.info(f"Comparing data by country between
#       {current_quarter} and {previous_quarter}.")

#     # Get the list of country columns dynamically
#     columns = [col.strip() for col in current_data.columns.tolist()]
#     start_index = columns.index("United States")
#     end_index = columns.index("Japan") + 1

#     countries = columns[start_index:end_index]

#     current_country_data = current_data[countries].sum()
#     previous_country_data = previous_data[countries].sum()

#     comparison = pd.DataFrame({
#         'Country': countries,
#         f'{current_quarter}': current_country_data.values,
#         f'{previous_quarter}': previous_country_data.values,
#         'Difference': current_country_data.values
#            - previous_country_data.values
#     })

#     LOGGER.info(f"Country comparison:\n{comparison}")

#     # Visualization code to be added here


# def compare_by_license(current_data, previous_data,
#   current_quarter, previous_quarter):
#     """
#     Compare the number of webpages licensed by license type
#   between two quarters.
#     """
#     LOGGER.info(f"Comparing data by license type
#       between {current_quarter} and {previous_quarter}.")

#     current_license_data =
#       current_data.groupby('LICENSE TYPE').sum().sum(axis=1)
#     previous_license_data =
#       previous_data.groupby('LICENSE TYPE').sum().sum(axis=1)

#     comparison = pd.DataFrame({
#         'License Type': current_license_data.index,
#         f'{current_quarter}': current_license_data.values,
#         f'{previous_quarter}': previous_license_data.values,
#         'Difference': current_license_data.values
#           - previous_license_data.values
#     })

#     LOGGER.info(f"License type comparison:\n{comparison}")

#     # Visualization code to be added here


# def compare_by_language(current_data, previous_data,
#           current_quarter, previous_quarter):
#     """
#     Compare the number of webpages licensed by language between two quarters.
#     """
#     LOGGER.info(f"Comparing data by language between
#                   {current_quarter} and {previous_quarter}.")

#     # Get the list of language columns dynamically
#     columns = [col.strip() for col in current_data.columns.tolist()]
#     start_index = columns.index("English")
#     languages = columns[start_index:]

#     current_language_data = current_data[languages].sum()
#     previous_language_data = previous_data[languages].sum()

#     comparison = pd.DataFrame({
#         'Language': languages,
#         f'{current_quarter}': current_language_data.values,
#         f'{previous_quarter}': previous_language_data.values,
#         'Difference': current_language_data.values
#           - previous_language_data.values
#     })

#     LOGGER.info(f"Language comparison:\n{comparison}")


# def parse_arguments():
#     """
#     Parses command-line arguments, returns parsed arguments.
#     """
#     LOGGER.info("Parsing command-line arguments")
#     parser = argparse.ArgumentParser(
#       description="Google Custom Search Comparison Report")
#     parser.add_argument(
#         "--current_quarter", type=str, required=True,
#       help="Current quarter for comparison (e.g., 2024Q3)"
#     )
#     parser.add_argument(
#         "--previous_quarter", type=str, required=True,
#           help="Previous quarter for comparison (e.g., 2024Q2)"
#     )
#     return parser.parse_args()


def main():
    raise shared.QuantifyingException("No current code for Phase 2", 0)

    # # Fetch and merge changes
    # shared.fetch_and_merge(PATHS["repo"])

    # # Add and commit changes
    # shared.add_and_commit(PATHS["repo"], "Fetched and updated new data")

    # # Push changes
    # shared.push_changes(PATHS["repo"])


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
        if e.exit_code == 0:
            LOGGER.info(e.message)
        else:
            LOGGER.error(e.message)
        sys.exit(e.code)
    except SystemExit as e:
        LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except Exception:
        LOGGER.exception(f"(1) Unhandled exception: {traceback.format_exc()}")
        sys.exit(1)
