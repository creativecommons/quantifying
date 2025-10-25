"""
ArXiv category code to user-friendly name converter.
Called by arxiv_fetch.py to convert category codes to readable names.
"""
# Standard library
import csv
import logging
import os
import sys

# Third-party
import yaml

# Add scripts directory to path to import shared module
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

logger = logging.getLogger(__name__)


def load_category_mapping(data_dir):
    """Load category code to label mapping from YAML file."""
    mapping_file = os.path.join(data_dir, "arxiv_category_map.yaml")

    if not os.path.exists(mapping_file):
        logger.warning(f"Category mapping file not found: {mapping_file}")
        return {}

    try:
        with open(mapping_file, "r") as f:
            return yaml.safe_load(f) or {}
    except (yaml.YAMLError, IOError) as e:
        logger.error(f"Failed to load category mapping from {mapping_file}: {e}")
        return {}


def convert_categories_to_friendly_names(input_file, output_file, data_dir):
    """
    Convert category codes in CSV to user-friendly names.

    Args:
        input_file: Path to input CSV with category codes
        output_file: Path to output CSV with friendly names
        data_dir: Directory containing arxiv_category_map.yaml
    """
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    # Load category mapping
    category_mapping = load_category_mapping(data_dir)
    logger.info(f"Loaded {len(category_mapping)} category mappings")

    try:
        with (
            open(input_file, "r") as infile,
            open(output_file, "w", newline="") as outfile,
        ):
            reader = csv.DictReader(infile)

            # Create new fieldnames with both code and label
            fieldnames = []
            for field in reader.fieldnames:
                fieldnames.append(field)
                if field == "CATEGORY":
                    fieldnames.append("CATEGORY_LABEL")

            writer = csv.DictWriter(outfile, fieldnames=fieldnames, dialect="unix")
            writer.writeheader()

            for row in reader:
                if "CATEGORY" in row:
                    category_code = row["CATEGORY"]
                    # Convert code to label, fallback to uppercase first part
                    category_label = category_mapping.get(
                        category_code,
                        (
                            category_code.split(".")[0].upper()
                            if category_code and "." in category_code
                            else category_code
                        ),
                    )
                    row["CATEGORY_LABEL"] = category_label

                writer.writerow(row)
        
        logger.info(f"Successfully converted categories: {input_file} -> {output_file}")
    
    except (IOError, csv.Error) as e:
        logger.error(f"Failed to process CSV files: {e}")
