#!/usr/bin/env python
# Standard library
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Third-party
import pandas as pd  # noqa: E402

# First-party/Local
import shared  # noqa: E402
from plot import combined_plot as plot  # noqa: E402

LOGGER, PATHS = shared.setup(__file__)
SECTION = "GCS Report"


def plot_retired_tools(args):
    """
    Create plots for retired CC legal tool totals and percentages
    """
    LOGGER.info(plot_retired_tools.__doc__.strip())
    file_path = shared.path_join(
        PATHS["data_2-process"],
        "gcs_status_retired_totals.csv",
    )
    LOGGER.info(f"data file: {file_path.replace(PATHS['repo'], '.')}")
    name_label = "CC legal tool"
    data = pd.read_csv(file_path)

    if len(data) == 0:
        LOGGER.info(
            "No retired tools data available, skipping plot generation"
        )
        return

    data.set_index(name_label, inplace=True)
    data.sort_values(name_label, ascending=False, inplace=True)

    title = "Retired CC legal tools"
    plt = plot.combined_plot(
        args=args,
        data=data,
        title=title,
        name_label=name_label,
        data_label="Count",
        bar_xscale="log",
    )

    image_path = shared.path_join(
        PATHS["data_phase"], "gcs_status_retired_tools.png"
    )
    LOGGER.info(f"image file: {image_path.replace(PATHS['repo'], '.')}")

    if args.enable_save:
        # Create the directory if it does not exist
        os.makedirs(PATHS["data_phase"], exist_ok=True)
        plt.savefig(image_path)

    shared.update_readme(
        args,
        SECTION,
        title,
        image_path,
        "Plots showing retired Creative Commons (CC) legal tools total and"
        " percentages.",
        "For more information on retired legal tools, see [Retired Legal Tools"
        " - Creative Commons](https://creativecommons.org/retiredlicenses/).",
    )


if __name__ == "__main__":
    # Standard library
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--enable-save", action="store_true")
    parser.add_argument("--show-plots", action="store_true")
    args = parser.parse_args()

    # Set quarter for the plot annotation
    args.quarter = "2025Q4"

    plot_retired_tools(args)
