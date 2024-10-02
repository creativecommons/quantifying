#!/usr/bin/env python
"""
This file is dedicated to processing Youtube data
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
#       "1-fetch", "youtube_fetched")
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

#     Process data to compare totals


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
    # shared.add_and_commit(
    #     PATHS["repo"], PATHS["data_phase"], "Fetched and updated new data"
    # )

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
