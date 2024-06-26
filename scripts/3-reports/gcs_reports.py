#!/usr/bin/env python
"""
This file is dedicated to visualizing and analyzing the data collected.
"""
# Standard library
import os
import sys
import traceback

# Third-party
import matplotlib.pyplot as plt
import pandas as pd

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)


def load_data():
    """
    Load the collected data from the CSV file.
    """
    file_path = os.path.join(PATHS["data_phase"], "gcs_fetched.csv")
    if not os.path.exists(file_path):
        LOGGER.error(f"Data file not found: {file_path}")
        return pd.DataFrame()

    data = pd.read_csv(file_path)
    LOGGER.info(f"Data loaded from {file_path}")
    return data


def process_data(data):
    """
    Process the data to prepare it for visualization.
    """
    # are we supposed to take from phase 2?
    return data


def visualize_data(data):
    """
    Create visualizations for the data.
    """
    plt.figure(figsize=(10, 6))

    # Example - fix later
    license_counts = data["LICENSE TYPE"].value_counts()
    license_counts.plot(kind="bar")
    plt.title("License Counts")
    plt.xlabel("License Type")
    plt.ylabel("Count")
    plt.show()

    LOGGER.info("Visualization created.")


def main():
    data = load_data()
    if data.empty:
        return

    processed_data = process_data(data)
    visualize_data(processed_data)


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
