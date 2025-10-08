#!/usr/bin/env python
"""
Process Open Textbook Library (OTL) education dataset.
Fetches, processes, and reports on CC-licensed educational resources.
"""

# Standard library
import argparse
import csv
import os
import sys
from collections import Counter

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
OTL_CSV_PATH = os.path.join(
    PATHS["repo"], "pre-automation", "education", "datasets", "OTL.csv"
)
OUTPUT_DIR = os.path.join(PATHS["data_quarter"], "1-fetch")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "otl_education_report.csv")


def fetch_data():
    """Read OTL.csv file (already exists locally)."""
    LOGGER.info("Reading OTL.csv dataset")

    if not os.path.exists(OTL_CSV_PATH):
        raise shared.QuantifyingException(
            f"OTL.csv not found at {OTL_CSV_PATH}"
        )

    with open(OTL_CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data = list(reader)

    LOGGER.info(f"Loaded {len(data)} records from OTL.csv")
    return data


def process_data(data):
    """Process and analyze the OTL data for CC licenses."""
    LOGGER.info("Processing OTL data for CC licenses")

    cc_resources = []
    license_counts = Counter()
    subject_counts = Counter()

    for row in data:
        license_text = row.get("License", "").strip()

        # Filter for CC licenses
        if any(
            cc_term in license_text.lower()
            for cc_term in ["attribution", "cc", "creative commons"]
        ):
            cc_resources.append(
                {
                    "otl_id": row.get("OTL ID", "").strip(),
                    "title": row.get("Title", "").strip(),
                    "license": license_text,
                    "subject1": row.get("Subject 1", "").strip(),
                    "subject2": row.get("Subject 2", "").strip(),
                    "publisher": row.get("Publisher", "").strip(),
                    "year": row.get("Copyright Year", "").strip(),
                }
            )

            license_counts[license_text] += 1
            if row.get("Subject 1"):
                subject_counts[row.get("Subject 1").strip()] += 1

    LOGGER.info(f"Found {len(cc_resources)} CC-licensed resources")
    return cc_resources, license_counts, subject_counts


def generate_report(cc_resources, license_counts, subject_counts):
    """Generate CSV report of CC-licensed educational resources."""
    LOGGER.info(f"Generating report to {OUTPUT_FILE}")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Write detailed CC resources
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "OTL_ID",
                "TITLE",
                "LICENSE",
                "SUBJECT1",
                "SUBJECT2",
                "PUBLISHER",
                "YEAR",
            ]
        )

        for resource in cc_resources:
            writer.writerow(
                [
                    resource["otl_id"],
                    resource["title"],
                    resource["license"],
                    resource["subject1"],
                    resource["subject2"],
                    resource["publisher"],
                    resource["year"],
                ]
            )

    # Write summary stats
    summary_file = OUTPUT_FILE.replace(".csv", "_summary.csv")
    with open(summary_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow(["METRIC", "VALUE"])
        writer.writerow(["TOTAL_CC_RESOURCES", len(cc_resources)])
        writer.writerow([""])

        writer.writerow(["LICENSE_TYPE", "COUNT"])
        for license_type, count in license_counts.most_common():
            writer.writerow([license_type, count])

        writer.writerow([""])
        writer.writerow(["SUBJECT", "COUNT"])
        for subject, count in subject_counts.most_common(10):
            writer.writerow([subject, count])

    LOGGER.info(f"Report generated: {OUTPUT_FILE}")
    LOGGER.info(f"Summary generated: {summary_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Process OTL education dataset"
    )
    parser.add_argument(
        "--enable-git", action="store_true", help="Enable git operations"
    )
    args = parser.parse_args()

    try:
        # Fetch data
        data = fetch_data()

        # Process data
        cc_resources, license_counts, subject_counts = process_data(data)

        # Generate report
        generate_report(cc_resources, license_counts, subject_counts)

        # Git operations
        if args.enable_git:
            shared.git_add_and_commit(
                args,
                PATHS["repo"],
                OUTPUT_DIR,
                "Add OTL education dataset report",
            )

        LOGGER.info("OTL processing completed successfully")

    except Exception as e:
        LOGGER.error(f"Error processing OTL data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
