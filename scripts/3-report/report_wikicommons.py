#!/usr/bin/env python
"""
Generate reports and visualizations for WikiCommons data.
"""
# Standard library
import argparse
import os
import sys
import textwrap
import traceback

# Third-party
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
from matplotlib import colormaps
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
QUARTER = os.path.basename(PATHS["data_quarter"])
SECTION = "WikiCommons"
INPUT_FILE = shared.path_join(PATHS["data_2-process"], "wikicommons_2_processed.csv")
OUTPUT_FILE = shared.path_join(PATHS["data_phase"], "wikicommons_summary.png")


def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarter",
        default=QUARTER,
        help=f"Data quarter in format YYYYQx (default: {QUARTER})",
    )
    parser.add_argument(
        "--show-plots",
        action="store_true",
        help="Show generated plots (default: False)",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results (default: False)",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions such as fetch, merge, add, commit, and push"
        " (default: False)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview summary table without generating charts",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    if args.quarter != QUARTER:
        global PATHS
        PATHS = shared.paths_update(LOGGER, PATHS, QUARTER, args.quarter)
    args.logger = LOGGER
    args.paths = PATHS
    return args


def load_processed_data(args):
    """
    Load and validate the processed WikiCommons data.
    
    Args:
        args: Parsed arguments
        
    Returns:
        pd.DataFrame: Loaded and validated data
    """
    LOGGER.info("Loading processed WikiCommons data")
    
    input_file = shared.path_join(args.paths["data_2-process"], "wikicommons_2_processed.csv")
    
    if not os.path.exists(input_file):
        raise shared.QuantifyingException(
            f"Processed data file not found: {input_file}", 1
        )
    
    try:
        data = pd.read_csv(input_file)
        LOGGER.info(f"Loaded {len(data)} rows from {input_file}")
        
        # Validate expected columns
        expected_columns = [
            "LICENSE", "LICENSE_TYPE", "LICENSE_VERSION", "BASE_LICENSE",
            "FILE_COUNT", "FILE_PERCENTAGE", "PAGE_COUNT", "PAGE_PERCENTAGE",
            "IS_FREE_CULTURAL", "IS_PUBLIC_DOMAIN"
        ]
        
        if not all(col in data.columns for col in expected_columns):
            missing_cols = set(expected_columns) - set(data.columns)
            raise shared.QuantifyingException(
                f"Missing expected columns: {missing_cols}", 1
            )
        
        # Validate data types
        data["FILE_COUNT"] = pd.to_numeric(data["FILE_COUNT"], errors="coerce")
        data["FILE_PERCENTAGE"] = pd.to_numeric(data["FILE_PERCENTAGE"], errors="coerce")
        data["PAGE_COUNT"] = pd.to_numeric(data["PAGE_COUNT"], errors="coerce")
        data["PAGE_PERCENTAGE"] = pd.to_numeric(data["PAGE_PERCENTAGE"], errors="coerce")
        
        # Check for any NaN values after conversion
        if data[["FILE_COUNT", "FILE_PERCENTAGE", "PAGE_COUNT", "PAGE_PERCENTAGE"]].isnull().any().any():
            LOGGER.warning("Found non-numeric values in count columns")
        
        # Filter out TOTAL row for analysis
        analysis_data = data[data["LICENSE"] != "TOTAL"].copy()
        
        LOGGER.info(f"Data validation completed. Analysis data shape: {analysis_data.shape}")
        return data, analysis_data
        
    except Exception as e:
        raise shared.QuantifyingException(f"Error loading processed data: {e}", 1)


def create_bar_chart(args, data):
    """
    Create a horizontal bar chart showing file counts by license.
    
    Args:
        args: Parsed arguments
        data (pd.DataFrame): Analysis data (excluding TOTAL row)
    """
    LOGGER.info("Creating WikiCommons file count bar chart")
    
    # Filter out zero-count licenses for cleaner visualization
    chart_data = data[data["FILE_COUNT"] > 0].copy()
    
    # Sort by file count descending
    chart_data = chart_data.sort_values("FILE_COUNT", ascending=True)
    
    # Set up the plot
    plt.rcParams.update({"font.family": "sans-serif", "figure.dpi": 300})
    
    # Calculate figure height based on number of licenses
    height = max(6, len(chart_data) * 0.4)
    fig, ax = plt.subplots(figsize=(12, height))
    
    # Use a color palette
    colors = colormaps["tab10"].colors
    
    # Create horizontal bar chart
    bars = ax.barh(
        range(len(chart_data)),
        chart_data["FILE_COUNT"],
        color=[colors[i % len(colors)] for i in range(len(chart_data))]
    )
    
    # Customize the chart
    ax.set_yticks(range(len(chart_data)))
    ax.set_yticklabels(chart_data["LICENSE"], fontsize=10)
    ax.set_xlabel("Number of Files", fontsize=12, fontweight="bold")
    ax.set_title("WikiCommons Files by Creative Commons License", fontsize=14, fontweight="bold", pad=20)
    
    # Format x-axis with commas
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"{x:,.0f}"))
    
    # Add value labels on bars
    for i, (bar, count, percentage) in enumerate(zip(bars, chart_data["FILE_COUNT"], chart_data["FILE_PERCENTAGE"])):
        width = bar.get_width()
        ax.text(
            width + width * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{count:,.0f} ({percentage:.1f}%)",
            ha="left",
            va="center",
            fontsize=9,
            fontweight="bold"
        )
    
    # Invert y-axis to show highest counts at top
    ax.invert_yaxis()
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the chart
    if args.enable_save:
        os.makedirs(args.paths["data_phase"], exist_ok=True)
        plt.savefig(OUTPUT_FILE, dpi=300, bbox_inches="tight")
        LOGGER.info(f"Chart saved to: {OUTPUT_FILE}")
    
    # Show plot if requested
    if args.show_plots:
        plt.show()
    else:
        plt.close()


def print_summary_table(args, data, analysis_data):
    """
    Print a comprehensive summary table to console.
    
    Args:
        args: Parsed arguments
        data (pd.DataFrame): Full data including TOTAL row
        analysis_data (pd.DataFrame): Analysis data excluding TOTAL row
    """
    LOGGER.info("Generating WikiCommons summary table")
    
    # Get totals from TOTAL row
    total_row = data[data["LICENSE"] == "TOTAL"].iloc[0]
    total_files = int(total_row["FILE_COUNT"])
    total_pages = int(total_row["PAGE_COUNT"])
    
    # Calculate Free Cultural Works statistics
    free_cultural_data = analysis_data[analysis_data["IS_FREE_CULTURAL"] == True]
    free_cultural_files = free_cultural_data["FILE_COUNT"].sum()
    free_cultural_percentage = (free_cultural_files / total_files * 100) if total_files > 0 else 0
    
    # Calculate Public Domain statistics
    public_domain_data = analysis_data[analysis_data["IS_PUBLIC_DOMAIN"] == True]
    public_domain_files = public_domain_data["FILE_COUNT"].sum()
    public_domain_percentage = (public_domain_files / total_files * 100) if total_files > 0 else 0
    
    # Get top 5 licenses by file count
    top_5 = analysis_data.nlargest(5, "FILE_COUNT")
    
    # Print summary
    print("\n" + "=" * 80)
    print("WIKICOMMONS CREATIVE COMMONS LICENSE SUMMARY")
    print("=" * 80)
    print(f"Quarter: {args.quarter}")
    print(f"Data Source: WikiCommons (commons.wikimedia.org)")
    print(f"Total Licenses Analyzed: {len(analysis_data)}")
    print("")
    
    print("TOP 5 LICENSES BY FILE COUNT:")
    print("-" * 50)
    for i, (_, row) in enumerate(top_5.iterrows(), 1):
        print(f"{i}. {row['LICENSE']:<20} {row['FILE_COUNT']:>12,} files ({row['FILE_PERCENTAGE']:>5.1f}%)")
    
    print("")
    print("OVERALL STATISTICS:")
    print("-" * 50)
    print(f"Total Files:           {total_files:>12,}")
    print(f"Total Pages:          {total_pages:>12,}")
    print(f"Unique Licenses:      {len(analysis_data):>12}")
    print("")
    
    print("LICENSE CATEGORIES:")
    print("-" * 50)
    print(f"Free Cultural Works:  {free_cultural_files:>12,} files ({free_cultural_percentage:>5.1f}%)")
    print(f"Public Domain Tools: {public_domain_files:>12,} files ({public_domain_percentage:>5.1f}%)")
    
    # License version breakdown
    version_breakdown = analysis_data.groupby("LICENSE_VERSION")["FILE_COUNT"].sum().sort_values(ascending=False)
    print("")
    print("LICENSE VERSION BREAKDOWN:")
    print("-" * 50)
    for version, count in version_breakdown.items():
        percentage = (count / total_files * 100) if total_files > 0 else 0
        print(f"Version {version:<6} {count:>12,} files ({percentage:>5.1f}%)")
    
    # License type breakdown
    type_breakdown = analysis_data.groupby("BASE_LICENSE")["FILE_COUNT"].sum().sort_values(ascending=False)
    print("")
    print("LICENSE TYPE BREAKDOWN:")
    print("-" * 50)
    for license_type, count in type_breakdown.items():
        percentage = (count / total_files * 100) if total_files > 0 else 0
        print(f"{license_type:<12} {count:>12,} files ({percentage:>5.1f}%)")
    
    print("=" * 80)
    print("")


def update_readme(args, data):
    """
    Update the README with WikiCommons report information.
    
    Args:
        args: Parsed arguments
        data (pd.DataFrame): Full data including TOTAL row
    """
    if not args.enable_save:
        return
    
    LOGGER.info("Updating README with WikiCommons report")
    
    # Get totals
    total_row = data[data["LICENSE"] == "TOTAL"].iloc[0]
    total_files = int(total_row["FILE_COUNT"])
    
    # Get top license
    analysis_data = data[data["LICENSE"] != "TOTAL"]
    top_license = analysis_data.loc[analysis_data["FILE_COUNT"].idxmax()]
    
    # Create report text
    report_text = (
        f"WikiCommons data represents Creative Commons licensed media files "
        f"(images, videos, audio) from Wikimedia Commons.\n\n"
        f"**The results indicate there are a total of {total_files:,} CC-licensed "
        f"media files in WikiCommons.**\n\n"
        f"The most popular license is {top_license['LICENSE']} with "
        f"{top_license['FILE_COUNT']:,} files ({top_license['FILE_PERCENTAGE']:.1f}%).\n\n"
        f"Thank you to the WikiCommons community for providing this valuable "
        f"collection of openly licensed media!"
    )
    
    # Update README
    shared.update_readme(
        args,
        SECTION,
        "Overview",
        OUTPUT_FILE,
        "WikiCommons Creative Commons License Distribution",
        report_text,
    )


def main():
    """
    Main function to orchestrate WikiCommons report generation.
    """
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    
    try:
        # Load processed data
        full_data, analysis_data = load_processed_data(args)
        
        # Print summary table
        print_summary_table(args, full_data, analysis_data)
        
        # Handle dry-run mode
        if args.dry_run:
            LOGGER.info("DRY RUN MODE - Summary table displayed, no charts generated")
            return
        
        # Create bar chart
        create_bar_chart(args, analysis_data)
        
        # Update README
        update_readme(args, full_data)
        
        # Git operations
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Add WikiCommons report for {QUARTER}",
        )
        shared.git_push_changes(args, PATHS["repo"])
        
        LOGGER.info("WikiCommons report generation completed successfully")
        
    except shared.QuantifyingException as e:
        LOGGER.error(f"Report generation failed: {e.message}")
        sys.exit(e.exit_code)
    except Exception as e:
        traceback_formatted = textwrap.indent(
            highlight(
                traceback.format_exc(),
                PythonTracebackLexer(),
                TerminalFormatter(),
            ),
            "    ",
        )
        LOGGER.critical(f"Unexpected error during report generation:\n{traceback_formatted}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
    except SystemExit as e:
        if e.code != 0:
            LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except Exception:
        traceback_formatted = textwrap.indent(
            highlight(
                traceback.format_exc(),
                PythonTracebackLexer(),
                TerminalFormatter(),
            ),
            "    ",
        )
        LOGGER.critical(f"(1) Unhandled exception:\n{traceback_formatted}")
        sys.exit(1)
