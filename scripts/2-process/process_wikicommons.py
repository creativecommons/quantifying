#!/usr/bin/env python
"""
Process WikiCommons data for analysis and reporting.
"""
# Standard library
import argparse
import csv
import os
import sys
import textwrap
import traceback

# Third-party
import pandas as pd
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
INPUT_FILE = shared.path_join(PATHS["data_1-fetch"], "wikicommons_1_count.csv")
OUTPUT_FILE = shared.path_join(PATHS["data_phase"], "wikicommons_2_processed.csv")

# License normalization mapping
LICENSE_NORMALIZATION = {
    "CC-BY-4.0": "CC BY 4.0",
    "CC-BY-SA-4.0": "CC BY-SA 4.0",
    "CC-BY-NC-4.0": "CC BY-NC 4.0",
    "CC-BY-NC-SA-4.0": "CC BY-NC-SA 4.0",
    "CC-BY-NC-ND-4.0": "CC BY-NC-ND 4.0",
    "CC-BY-ND-4.0": "CC BY-ND 4.0",
    "CC-BY-3.0": "CC BY 3.0",
    "CC-BY-SA-3.0": "CC BY-SA 3.0",
    "CC-BY-NC-3.0": "CC BY-NC 3.0",
    "CC-BY-NC-SA-3.0": "CC BY-NC-SA 3.0",
    "CC-BY-NC-ND-3.0": "CC BY-NC-ND 3.0",
    "CC-BY-ND-3.0": "CC BY-ND 3.0",
    "CC-BY-2.5": "CC BY 2.5",
    "CC-BY-SA-2.5": "CC BY-SA 2.5",
    "CC-BY-NC-2.5": "CC BY-NC 2.5",
    "CC-BY-NC-SA-2.5": "CC BY-NC-SA 2.5",
    "CC-BY-NC-ND-2.5": "CC BY-NC-ND 2.5",
    "CC-BY-ND-2.5": "CC BY-ND 2.5",
    "CC-BY-2.0": "CC BY 2.0",
    "CC-BY-SA-2.0": "CC BY-SA 2.0",
    "CC-BY-NC-2.0": "CC BY-NC 2.0",
    "CC-BY-NC-SA-2.0": "CC BY-NC-SA 2.0",
    "CC-BY-NC-ND-2.0": "CC BY-NC-ND 2.0",
    "CC-BY-ND-2.0": "CC BY-ND 2.0",
    "CC-BY-1.0": "CC BY 1.0",
    "CC-BY-SA-1.0": "CC BY-SA 1.0",
    "CC-BY-NC-1.0": "CC BY-NC 1.0",
    "CC-BY-NC-SA-1.0": "CC BY-NC-SA 1.0",
    "CC-BY-NC-ND-1.0": "CC BY-NC-ND 1.0",
    "CC-BY-ND-1.0": "CC BY-ND 1.0",
    "CC0-1.0": "CC0 1.0",
    "PDM-1.0": "PDM 1.0",
}


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
        help="Print first few processed rows instead of writing to disk",
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


def load_input_data(args):
    """
    Load and validate the input WikiCommons data.
    
    Returns:
        pd.DataFrame: Loaded and validated data
    """
    LOGGER.info("Loading WikiCommons input data")
    
    input_file = shared.path_join(args.paths["data_1-fetch"], "wikicommons_1_count.csv")
    
    if not os.path.exists(input_file):
        raise shared.QuantifyingException(
            f"Input file not found: {input_file}", 1
        )
    
    try:
        data = pd.read_csv(input_file)
        LOGGER.info(f"Loaded {len(data)} rows from {input_file}")
        
        # Validate expected columns
        expected_columns = ["LICENSE", "FILE_COUNT", "PAGE_COUNT"]
        if not all(col in data.columns for col in expected_columns):
            missing_cols = set(expected_columns) - set(data.columns)
            raise shared.QuantifyingException(
                f"Missing expected columns: {missing_cols}", 1
            )
        
        # Validate data types
        data["FILE_COUNT"] = pd.to_numeric(data["FILE_COUNT"], errors="coerce")
        data["PAGE_COUNT"] = pd.to_numeric(data["PAGE_COUNT"], errors="coerce")
        
        # Check for any NaN values after conversion
        if data[["FILE_COUNT", "PAGE_COUNT"]].isnull().any().any():
            LOGGER.warning("Found non-numeric values in count columns, filling with 0")
            data[["FILE_COUNT", "PAGE_COUNT"]] = data[["FILE_COUNT", "PAGE_COUNT"]].fillna(0)
        
        LOGGER.info(f"Data validation completed. Shape: {data.shape}")
        return data
        
    except Exception as e:
        raise shared.QuantifyingException(f"Error loading input data: {e}", 1)


def normalize_license_names(data):
    """
    Normalize license names to standard format.
    
    Args:
        data (pd.DataFrame): Input data with LICENSE column
        
    Returns:
        pd.DataFrame: Data with normalized license names
    """
    LOGGER.info("Normalizing license names")
    
    # Create a copy to avoid modifying original
    processed_data = data.copy()
    
    # Apply normalization mapping
    processed_data["LICENSE_NORMALIZED"] = processed_data["LICENSE"].map(
        LICENSE_NORMALIZATION
    )
    
    # Handle any unmapped licenses
    unmapped_licenses = processed_data[
        processed_data["LICENSE_NORMALIZED"].isnull()
    ]["LICENSE"].unique()
    
    if len(unmapped_licenses) > 0:
        LOGGER.warning(f"Found unmapped licenses: {unmapped_licenses}")
        # For unmapped licenses, use the original name
        processed_data["LICENSE_NORMALIZED"] = processed_data["LICENSE_NORMALIZED"].fillna(
            processed_data["LICENSE"]
        )
    
    # Replace the original LICENSE column with normalized version
    processed_data["LICENSE"] = processed_data["LICENSE_NORMALIZED"]
    processed_data = processed_data.drop("LICENSE_NORMALIZED", axis=1)
    
    LOGGER.info(f"License normalization completed. Unique licenses: {len(processed_data['LICENSE'].unique())}")
    return processed_data


def extract_license_metadata(data):
    """
    Extract metadata from license names (version, type, etc.).
    
    Args:
        data (pd.DataFrame): Data with normalized LICENSE column
        
    Returns:
        pd.DataFrame: Data with additional metadata columns
    """
    LOGGER.info("Extracting license metadata")
    
    processed_data = data.copy()
    
    # Extract version
    processed_data["LICENSE_VERSION"] = processed_data["LICENSE"].str.extract(r"(\d+\.\d+)")
    
    # Extract license type (BY, BY-SA, BY-NC, etc.)
    processed_data["LICENSE_TYPE"] = processed_data["LICENSE"].str.replace(r"\s+\d+\.\d+", "", regex=True)
    
    # Extract base license (BY, BY-SA, BY-NC, BY-NC-SA, BY-NC-ND, BY-ND)
    processed_data["BASE_LICENSE"] = processed_data["LICENSE_TYPE"].str.replace(r"^CC\s+", "", regex=True)
    
    # Determine if it's a Free Cultural Work
    free_cultural_licenses = ["BY", "BY-SA"]
    processed_data["IS_FREE_CULTURAL"] = processed_data["BASE_LICENSE"].isin(free_cultural_licenses)
    
    # Determine if it's a public domain tool
    processed_data["IS_PUBLIC_DOMAIN"] = processed_data["LICENSE_TYPE"].isin(["CC0", "PDM"])
    
    LOGGER.info("License metadata extraction completed")
    return processed_data


def aggregate_data(data):
    """
    Aggregate data by license type and version.
    
    Args:
        data (pd.DataFrame): Data with metadata columns
        
    Returns:
        pd.DataFrame: Aggregated data
    """
    LOGGER.info("Aggregating data by license type and version")
    
    # Group by license type and version, sum the counts
    aggregated = data.groupby(["LICENSE_TYPE", "LICENSE_VERSION", "BASE_LICENSE"]).agg({
        "FILE_COUNT": "sum",
        "PAGE_COUNT": "sum",
        "IS_FREE_CULTURAL": "first",
        "IS_PUBLIC_DOMAIN": "first"
    }).reset_index()
    
    # Reconstruct the full license name
    aggregated["LICENSE"] = aggregated["LICENSE_TYPE"] + " " + aggregated["LICENSE_VERSION"]
    
    # Reorder columns
    aggregated = aggregated[[
        "LICENSE", "LICENSE_TYPE", "LICENSE_VERSION", "BASE_LICENSE",
        "FILE_COUNT", "PAGE_COUNT", "IS_FREE_CULTURAL", "IS_PUBLIC_DOMAIN"
    ]]
    
    LOGGER.info(f"Aggregation completed. {len(aggregated)} unique license combinations")
    return aggregated


def add_derived_metrics(data):
    """
    Add derived metrics and summary statistics.
    
    Args:
        data (pd.DataFrame): Aggregated data
        
    Returns:
        pd.DataFrame: Data with derived metrics
    """
    LOGGER.info("Adding derived metrics")
    
    processed_data = data.copy()
    
    # Calculate total files across all licenses
    total_files = processed_data["FILE_COUNT"].sum()
    total_pages = processed_data["PAGE_COUNT"].sum()
    
    # Add percentage columns
    processed_data["FILE_PERCENTAGE"] = (processed_data["FILE_COUNT"] / total_files * 100).round(2)
    processed_data["PAGE_PERCENTAGE"] = (processed_data["PAGE_COUNT"] / total_pages * 100).round(2)
    
    # Add totals row
    totals_row = pd.DataFrame({
        "LICENSE": ["TOTAL"],
        "LICENSE_TYPE": ["TOTAL"],
        "LICENSE_VERSION": ["ALL"],
        "BASE_LICENSE": ["ALL"],
        "FILE_COUNT": [total_files],
        "PAGE_COUNT": [total_pages],
        "IS_FREE_CULTURAL": [False],
        "IS_PUBLIC_DOMAIN": [False],
        "FILE_PERCENTAGE": [100.0],
        "PAGE_PERCENTAGE": [100.0]
    })
    
    # Combine data with totals
    processed_data = pd.concat([processed_data, totals_row], ignore_index=True)
    
    LOGGER.info(f"Derived metrics added. Total files: {total_files:,}, Total pages: {total_pages:,}")
    return processed_data


def sort_and_finalize_data(data):
    """
    Sort data and finalize column order.
    
    Args:
        data (pd.DataFrame): Data with all processing complete
        
    Returns:
        pd.DataFrame: Finalized data
    """
    LOGGER.info("Sorting and finalizing data")
    
    processed_data = data.copy()
    
    # Sort by file count descending, but put TOTAL at the end
    totals_mask = processed_data["LICENSE"] == "TOTAL"
    non_totals = processed_data[~totals_mask].sort_values("FILE_COUNT", ascending=False)
    totals = processed_data[totals_mask]
    
    processed_data = pd.concat([non_totals, totals], ignore_index=True)
    
    # Final column order
    final_columns = [
        "LICENSE",
        "LICENSE_TYPE", 
        "LICENSE_VERSION",
        "BASE_LICENSE",
        "FILE_COUNT",
        "FILE_PERCENTAGE",
        "PAGE_COUNT", 
        "PAGE_PERCENTAGE",
        "IS_FREE_CULTURAL",
        "IS_PUBLIC_DOMAIN"
    ]
    
    processed_data = processed_data[final_columns]
    
    LOGGER.info("Data sorting and finalization completed")
    return processed_data


def save_processed_data(args, data):
    """
    Save the processed data to CSV file.
    
    Args:
        args: Parsed arguments
        data (pd.DataFrame): Processed data to save
    """
    if not args.enable_save:
        LOGGER.info("Saving disabled, skipping file write")
        return
    
    LOGGER.info("Saving processed WikiCommons data")
    
    # Create output directory
    os.makedirs(args.paths["data_phase"], exist_ok=True)
    
    # Save to CSV with proper formatting
    output_file = shared.path_join(args.paths["data_phase"], "wikicommons_2_processed.csv")
    
    try:
        data.to_csv(
            output_file,
            index=False,
            quoting=csv.QUOTE_ALL,
            lineterminator="\n"
        )
        LOGGER.info(f"Processed data saved to: {output_file}")
        
    except Exception as e:
        raise shared.QuantifyingException(f"Error saving processed data: {e}", 1)


def print_summary(args, data):
    """
    Print summary statistics of the processed data.
    
    Args:
        args: Parsed arguments
        data (pd.DataFrame): Processed data
    """
    LOGGER.info("Generating processing summary")
    
    # Calculate summary statistics
    total_files = data[data["LICENSE"] != "TOTAL"]["FILE_COUNT"].sum()
    total_pages = data[data["LICENSE"] != "TOTAL"]["PAGE_COUNT"].sum()
    unique_licenses = len(data[data["LICENSE"] != "TOTAL"])
    
    # Top licenses by file count
    top_licenses = data[data["LICENSE"] != "TOTAL"].head(5)
    
    # Free cultural works statistics
    free_cultural_data = data[data["IS_FREE_CULTURAL"] == True]
    free_cultural_files = free_cultural_data["FILE_COUNT"].sum()
    free_cultural_percentage = (free_cultural_files / total_files * 100) if total_files > 0 else 0
    
    # Public domain statistics
    public_domain_data = data[data["IS_PUBLIC_DOMAIN"] == True]
    public_domain_files = public_domain_data["FILE_COUNT"].sum()
    public_domain_percentage = (public_domain_files / total_files * 100) if total_files > 0 else 0
    
    LOGGER.info("=" * 60)
    LOGGER.info("WIKICOMMONS DATA PROCESSING SUMMARY")
    LOGGER.info("=" * 60)
    LOGGER.info(f"Quarter: {args.quarter}")
    LOGGER.info(f"Total licenses processed: {unique_licenses}")
    LOGGER.info(f"Total files counted: {total_files:,}")
    LOGGER.info(f"Total pages counted: {total_pages:,}")
    LOGGER.info("")
    LOGGER.info("TOP 5 LICENSES BY FILE COUNT:")
    for _, row in top_licenses.iterrows():
        LOGGER.info(f"  {row['LICENSE']}: {row['FILE_COUNT']:,} files ({row['FILE_PERCENTAGE']:.1f}%)")
    LOGGER.info("")
    LOGGER.info("LICENSE CATEGORIES:")
    LOGGER.info(f"  Free Cultural Works: {free_cultural_files:,} files ({free_cultural_percentage:.1f}%)")
    LOGGER.info(f"  Public Domain Tools: {public_domain_files:,} files ({public_domain_percentage:.1f}%)")
    LOGGER.info("=" * 60)


def main():
    """
    Main function to orchestrate WikiCommons data processing.
    """
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    
    try:
        # Load and validate input data
        raw_data = load_input_data(args)
        
        # Process the data
        processed_data = raw_data
        processed_data = normalize_license_names(processed_data)
        processed_data = extract_license_metadata(processed_data)
        processed_data = aggregate_data(processed_data)
        processed_data = add_derived_metrics(processed_data)
        processed_data = sort_and_finalize_data(processed_data)
        
        # Handle dry-run mode
        if args.dry_run:
            LOGGER.info("DRY RUN MODE - Showing first 10 processed rows:")
            LOGGER.info("=" * 80)
            print(processed_data.head(10).to_string(index=False))
            LOGGER.info("=" * 80)
            LOGGER.info("Dry run completed - no files written")
            return
        
        # Save processed data
        save_processed_data(args, processed_data)
        
        # Print summary
        print_summary(args, processed_data)
        
        # Git operations
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Process WikiCommons data for {QUARTER}",
        )
        shared.git_push_changes(args, PATHS["repo"])
        
    except shared.QuantifyingException as e:
        LOGGER.error(f"Processing failed: {e.message}")
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
        LOGGER.critical(f"Unexpected error during processing:\n{traceback_formatted}")
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
