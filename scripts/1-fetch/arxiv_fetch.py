#!/usr/bin/env python
"""
Fetch ArXiv papers with CC license information using OAI-PMH API.

This script uses ArXiv's OAI-PMH interface to harvest papers with structured
license metadata, providing more accurate CC license detection than text-based
pattern matching. Focuses on recent years where CC licensing is more common.
"""
# Standard library
import argparse
import csv
import os
import sys
import textwrap
import time
import traceback
import xml.etree.ElementTree as ET  # XML parsing for OAI-PMH responses
from collections import Counter, defaultdict
from datetime import datetime  # Date calculations for harvesting ranges
from operator import itemgetter

# Third-party
import requests
import yaml
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
# API Configuration - Updated to use OAI-PMH for structured license data
BASE_URL = "https://oaipmh.arxiv.org/oai"
# Implementation choice: Set to 1000 CC-licensed papers for balanced collection
# This is NOT an ArXiv API requirement - ArXiv only requires "responsible" use
# The 3-second delays between requests ensure compliance with OAI-PMH practices
DEFAULT_FETCH_LIMIT = 1000  # Default total CC-licensed papers to fetch
DEFAULT_YEARS_BACK = 5  # Default years to look back from current year

# CSV Headers
HEADER_AUTHOR_BUCKET = ["TOOL_IDENTIFIER", "AUTHOR_BUCKET", "COUNT"]
HEADER_CATEGORY_REPORT = [
    "TOOL_IDENTIFIER",
    "CATEGORY_CODE",
    "CATEGORY_LABEL",
    "COUNT",
]
HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]

# License mapping for structured data from OAI-PMH
LICENSE_MAPPING = {
    "http://creativecommons.org/licenses/by/3.0/": "CC BY 3.0",
    "http://creativecommons.org/licenses/by/4.0/": "CC BY 4.0",
    "http://creativecommons.org/licenses/by-nc/3.0/": "CC BY-NC 3.0",
    "http://creativecommons.org/licenses/by-nc/4.0/": "CC BY-NC 4.0",
    "http://creativecommons.org/licenses/by-nc-nd/3.0/": "CC BY-NC-ND 3.0",
    "http://creativecommons.org/licenses/by-nc-nd/4.0/": "CC BY-NC-ND 4.0",
    "http://creativecommons.org/licenses/by-nc-sa/3.0/": "CC BY-NC-SA 3.0",
    "http://creativecommons.org/licenses/by-nc-sa/4.0/": "CC BY-NC-SA 4.0",
    "http://creativecommons.org/licenses/by-nd/3.0/": "CC BY-ND 3.0",
    "http://creativecommons.org/licenses/by-nd/4.0/": "CC BY-ND 4.0",
    "http://creativecommons.org/licenses/by-sa/3.0/": "CC BY-SA 3.0",
    "http://creativecommons.org/licenses/by-sa/4.0/": "CC BY-SA 4.0",
    "http://creativecommons.org/licenses/publicdomain": "CC CERTIFICATION 1.0 US",
    "http://creativecommons.org/publicdomain/zero/1.0/": "CC0 1.0",
    "http://creativecommons.org/share-your-work/public-domain/cc0/": "CC0",
}

# ArXiv Categories - manually curated from ArXiv official taxonomy
# Source: https://arxiv.org/category_taxonomy
CATEGORIES = {
    # Computer Science
    "cs.AI": "Artificial Intelligence",
    "cs.AR": "Hardware Architecture",
    "cs.CC": "Computational Complexity",
    "cs.CE": "Computational Engineering, Finance, and Science",
    "cs.CG": "Computational Geometry",
    "cs.CL": "Computation and Language",
    "cs.CR": "Cryptography and Security",
    "cs.CV": "Computer Vision and Pattern Recognition",
    "cs.CY": "Computers and Society",
    "cs.DB": "Databases",
    "cs.DC": "Distributed, Parallel, and Cluster Computing",
    "cs.DL": "Digital Libraries",
    "cs.DM": "Discrete Mathematics",
    "cs.DS": "Data Structures and Algorithms",
    "cs.ET": "Emerging Technologies",
    "cs.FL": "Formal Languages and Automata Theory",
    "cs.GL": "General Literature",
    "cs.GR": "Graphics",
    "cs.GT": "Computer Science and Game Theory",
    "cs.HC": "Human-Computer Interaction",
    "cs.IR": "Information Retrieval",
    "cs.IT": "Information Theory",
    "cs.LG": "Machine Learning",
    "cs.LO": "Logic in Computer Science",
    "cs.MA": "Multiagent Systems",
    "cs.MM": "Multimedia",
    "cs.MS": "Mathematical Software",
    "cs.NA": "Numerical Analysis",
    "cs.NE": "Neural and Evolutionary Computing",
    "cs.NI": "Networking and Internet Architecture",
    "cs.OH": "Other Computer Science",
    "cs.OS": "Operating Systems",
    "cs.PF": "Performance",
    "cs.PL": "Programming Languages",
    "cs.RO": "Robotics",
    "cs.SC": "Symbolic Computation",
    "cs.SD": "Sound",
    "cs.SE": "Software Engineering",
    "cs.SI": "Social and Information Networks",
    "cs.SY": "Systems and Control",
    # Mathematics
    "math.AC": "Commutative Algebra",
    "math.AG": "Algebraic Geometry",
    "math.AP": "Analysis of PDEs",
    "math.AT": "Algebraic Topology",
    "math.CA": "Classical Analysis and ODEs",
    "math.CO": "Combinatorics",
    "math.CT": "Category Theory",
    "math.CV": "Complex Variables",
    "math.DG": "Differential Geometry",
    "math.DS": "Dynamical Systems",
    "math.FA": "Functional Analysis",
    "math.GM": "General Mathematics",
    "math.GN": "General Topology",
    "math.GR": "Group Theory",
    "math.GT": "Geometric Topology",
    "math.HO": "History and Overview",
    "math.IT": "Information Theory",
    "math.KT": "K-Theory and Homology",
    "math.LO": "Logic",
    "math.MG": "Metric Geometry",
    "math.MP": "Mathematical Physics",
    "math.NA": "Numerical Analysis",
    "math.NT": "Number Theory",
    "math.OA": "Operator Algebras",
    "math.OC": "Optimization and Control",
    "math.PR": "Probability",
    "math.QA": "Quantum Algebra",
    "math.RA": "Rings and Algebras",
    "math.RT": "Representation Theory",
    "math.SG": "Symplectic Geometry",
    "math.SP": "Spectral Theory",
    "math.ST": "Statistics Theory",
    # Physics
    "physics.acc-ph": "Accelerator Physics",
    "physics.ao-ph": "Atmospheric and Oceanic Physics",
    "physics.app-ph": "Applied Physics",
    "physics.atm-clus": "Atomic and Molecular Clusters",
    "physics.atom-ph": "Atomic Physics",
    "physics.bio-ph": "Biological Physics",
    "physics.chem-ph": "Chemical Physics",
    "physics.class-ph": "Classical Physics",
    "physics.comp-ph": "Computational Physics",
    "physics.data-an": "Data Analysis, Statistics and Probability",
    "physics.ed-ph": "Physics Education",
    "physics.flu-dyn": "Fluid Dynamics",
    "physics.gen-ph": "General Physics",
    "physics.geo-ph": "Geophysics",
    "physics.hist-ph": "History and Philosophy of Physics",
    "physics.ins-det": "Instrumentation and Detectors",
    "physics.med-ph": "Medical Physics",
    "physics.optics": "Optics",
    "physics.plasm-ph": "Plasma Physics",
    "physics.pop-ph": "Popular Physics",
    "physics.soc-ph": "Physics and Society",
    "physics.space-ph": "Space Physics",
    # Statistics
    "stat.AP": "Applications",
    "stat.CO": "Computation",
    "stat.ME": "Methodology",
    "stat.ML": "Machine Learning",
    "stat.OT": "Other Statistics",
    "stat.TH": "Statistics Theory",
    # Quantitative Biology
    "q-bio.BM": "Biomolecules",
    "q-bio.CB": "Cell Behavior",
    "q-bio.GN": "Genomics",
    "q-bio.MN": "Molecular Networks",
    "q-bio.NC": "Neurons and Cognition",
    "q-bio.OT": "Other Quantitative Biology",
    "q-bio.PE": "Populations and Evolution",
    "q-bio.QM": "Quantitative Methods",
    "q-bio.SC": "Subcellular Processes",
    "q-bio.TO": "Tissues and Organs",
    # Economics
    "econ.EM": "Econometrics",
    "econ.GN": "General Economics",
    "econ.TH": "Theoretical Economics",
    # Electrical Engineering
    "eess.AS": "Audio and Speech Processing",
    "eess.IV": "Image and Video Processing",
    "eess.SP": "Signal Processing",
    "eess.SY": "Systems and Control",
    # High Energy Physics
    "hep-ex": "High Energy Physics - Experiment",
    "hep-lat": "High Energy Physics - Lattice",
    "hep-ph": "High Energy Physics - Phenomenology",
    "hep-th": "High Energy Physics - Theory",
    # Other Physics
    "astro-ph": "Astrophysics",
    "astro-ph.CO": "Cosmology and Nongalactic Astrophysics",
    "astro-ph.EP": "Earth and Planetary Astrophysics",
    "astro-ph.GA": "Astrophysics of Galaxies",
    "astro-ph.HE": "High Energy Astrophysical Phenomena",
    "astro-ph.IM": "Instrumentation and Methods for Astrophysics",
    "astro-ph.SR": "Solar and Stellar Astrophysics",
    "cond-mat.dis-nn": "Disordered Systems and Neural Networks",
    "cond-mat.mes-hall": "Mesoscale and Nanoscale Physics",
    "cond-mat.mtrl-sci": "Materials Science",
    "cond-mat.other": "Other Condensed Matter",
    "cond-mat.quant-gas": "Quantum Gases",
    "cond-mat.soft": "Soft Condensed Matter",
    "cond-mat.stat-mech": "Statistical Mechanics",
    "cond-mat.str-el": "Strongly Correlated Electrons",
    "cond-mat.supr-con": "Superconductivity",
    "gr-qc": "General Relativity and Quantum Cosmology",
    "nlin.AO": "Adaptation and Self-Organizing Systems",
    "nlin.CD": "Chaotic Dynamics",
    "nlin.CG": "Cellular Automata and Lattice Gases",
    "nlin.PS": "Pattern Formation and Solitons",
    "nlin.SI": "Exactly Solvable and Integrable Systems",
    "nucl-ex": "Nuclear Experiment",
    "nucl-th": "Nuclear Theory",
    "quant-ph": "Quantum Physics",
}

# File Paths
FILE_ARXIV_AUTHOR_BUCKET = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_4_count_by_author_bucket.csv"
)
FILE_ARXIV_CATEGORY_REPORT = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_2_count_by_category_report.csv"
)
FILE_ARXIV_COUNT = shared.path_join(PATHS["data_1-fetch"], "arxiv_1_count.csv")
FILE_ARXIV_YEAR = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_3_count_by_year.csv"
)
# records metadata for each run for audit, reproducibility, and provenance
FILE_PROVENANCE = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_provenance.yaml"
)

# Runtime variables
QUARTER = os.path.basename(PATHS["data_quarter"])


# parsing arguments function
def parse_arguments():
    """Parse command-line options, returns parsed argument namespace.

    Note: The --limit parameter sets the total number of papers to fetch.
    The --years-back parameter limits harvesting to recent years where
    CC licensing is more common.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FETCH_LIMIT,
        help=(
            f"Total limit of papers to fetch "
            f"(default: {DEFAULT_FETCH_LIMIT}). "
            f"Note: Uses OAI-PMH API for structured license data."
        ),
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=DEFAULT_YEARS_BACK,
        help=(
            f"Number of years back from current year to harvest "
            f"(default: {DEFAULT_YEARS_BACK}). "
            f"Reduces dataset size and focuses on recent CC-licensed papers."
        ),
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving results",
    )
    parser.add_argument(
        "--enable-git",
        action="store_true",
        help="Enable git actions (fetch, merge, add, commit, and push)",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def initialize_data_file(file_path, headers):
    """Initialize CSV file with headers if it doesn't exist."""
    if not os.path.isfile(file_path):
        with open(file_path, "w", encoding="utf-8", newline="\n") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=headers, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files(args):
    """Initialize all data files used by this script.

    Creates the data directory and initializes empty CSVs with headers.
    """
    if not args.enable_save:
        return

    os.makedirs(PATHS["data_1-fetch"], exist_ok=True)
    initialize_data_file(FILE_ARXIV_COUNT, HEADER_COUNT)
    initialize_data_file(FILE_ARXIV_CATEGORY_REPORT, HEADER_CATEGORY_REPORT)
    initialize_data_file(FILE_ARXIV_YEAR, HEADER_YEAR)
    initialize_data_file(FILE_ARXIV_AUTHOR_BUCKET, HEADER_AUTHOR_BUCKET)


def extract_license_from_xml(record_xml):
    """
    Extract CC license information from OAI-PMH XML record.
    Returns normalized license identifier or specific error indicator.
    """
    try:
        root = ET.fromstring(record_xml)

        # Find license element in arXiv namespace
        license_element = root.find(".//{http://arxiv.org/OAI/arXiv/}license")

        if license_element is not None and license_element.text:
            license_url = license_element.text.strip()

            # Check exact mapping first
            if license_url in LICENSE_MAPPING:
                return LICENSE_MAPPING[license_url]

            # Validate CC URLs more strictly
            if "creativecommons.org/licenses/" in license_url.lower():
                return f"CC (unmapped): {license_url}"
            elif "creativecommons.org" in license_url.lower():
                return f"CC (ambiguous): {license_url}"

            return "Non-CC"

        return "No license field"

    except ET.ParseError as e:
        LOGGER.error(f"XML parsing failed: {e}")
        return "XML parse error"
    except Exception as e:
        LOGGER.error(f"License extraction failed: {e}")
        return "Extraction error"


def extract_metadata_from_xml(record_xml):
    """
    Extract paper metadata from OAI-PMH XML record.

    Returns dict with category, year, author_count, and license info.
    """
    try:
        root = ET.fromstring(record_xml)

        # Extract category (primary category from categories field)
        categories_elem = root.find(
            ".//{http://arxiv.org/OAI/arXiv/}categories"
        )
        category = "Unknown"
        if categories_elem is not None and categories_elem.text:
            # Take first category as primary
            category = categories_elem.text.strip().split()[0]

        # Extract year from created date
        created_elem = root.find(".//{http://arxiv.org/OAI/arXiv/}created")
        year = "Unknown"
        if created_elem is not None and created_elem.text:
            try:
                year = created_elem.text.strip()[:4]  # Extract year
            except (AttributeError, IndexError) as e:
                LOGGER.warning(
                    f"Failed to extract year from '{created_elem.text}': {e}"
                )
                year = "Unknown"

        # Extract author count
        authors = root.findall(".//{http://arxiv.org/OAI/arXiv/}author")
        author_count = len(authors) if authors else 0

        # Extract license
        license_info = extract_license_from_xml(record_xml)

        return {
            "category": category,
            "year": year,
            "author_count": author_count,
            "license": license_info,
        }

    except Exception as e:
        LOGGER.error(f"Metadata extraction error: {e}")
        return {
            "category": "Unknown",
            "year": "Unknown",
            "author_count": 0,
            "license": "Unknown",
        }


def bucket_author_count(author_count):
    """Convert author count to predefined buckets: "1", "2", "3", "4", "5+"."""
    if author_count <= 4:
        return str(author_count)
    return "5+"


def save_count_data(
    license_counts, category_counts, year_counts, author_counts
):
    """
    Save all collected data to CSV files.

    """
    # license_counts: {license: count}
    # category_counts: {license: {category_code: count}}
    # year_counts: {license: {year: count}}
    # author_counts: {license: {author_count: count}}

    # Save license counts
    data = []
    for license_name, count in license_counts.items():
        data.append({"TOOL_IDENTIFIER": license_name, "COUNT": count})
    data.sort(key=itemgetter("TOOL_IDENTIFIER"))
    with open(
        FILE_ARXIV_COUNT, "w", encoding="utf-8", newline="\n"
    ) as file_handle:
        writer = csv.DictWriter(
            file_handle, fieldnames=HEADER_COUNT, dialect="unix"
        )
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    # Save category report with labels
    data = []
    for license_name, categories in category_counts.items():
        for code, count in categories.items():
            label = CATEGORIES.get(code, code)
            data.append(
                {
                    "TOOL_IDENTIFIER": license_name,
                    "CATEGORY_CODE": code,
                    "CATEGORY_LABEL": label,
                    "COUNT": count,
                }
            )
    data.sort(key=itemgetter("TOOL_IDENTIFIER", "CATEGORY_CODE"))
    with open(
        FILE_ARXIV_CATEGORY_REPORT, "w", encoding="utf-8", newline="\n"
    ) as file_handle:
        writer = csv.DictWriter(
            file_handle, fieldnames=HEADER_CATEGORY_REPORT, dialect="unix"
        )
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    # Save year counts
    data = []
    for license_name, years in year_counts.items():
        for year, count in years.items():
            data.append(
                {"TOOL_IDENTIFIER": license_name, "YEAR": year, "COUNT": count}
            )
    data.sort(key=itemgetter("TOOL_IDENTIFIER", "YEAR"))
    with open(
        FILE_ARXIV_YEAR, "w", encoding="utf-8", newline="\n"
    ) as file_handle:
        writer = csv.DictWriter(
            file_handle, fieldnames=HEADER_YEAR, dialect="unix"
        )
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    # Save author buckets summary
    data = []
    for license_name, author_count_data in author_counts.items():
        # build buckets across licenses
        bucket_counts = Counter()
        for author_count, count in author_count_data.items():
            bucket = bucket_author_count(author_count)
            bucket_counts[bucket] += count
        for bucket, count in bucket_counts.items():
            data.append(
                {
                    "TOOL_IDENTIFIER": license_name,
                    "AUTHOR_BUCKET": bucket,
                    "COUNT": count,
                }
            )
    data.sort(key=itemgetter("TOOL_IDENTIFIER", "AUTHOR_BUCKET"))
    with open(
        FILE_ARXIV_AUTHOR_BUCKET, "w", encoding="utf-8", newline="\n"
    ) as file_handle:
        writer = csv.DictWriter(
            file_handle, fieldnames=HEADER_AUTHOR_BUCKET, dialect="unix"
        )
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def query_arxiv(args):
    """
    Main function to query ArXiv OAI-PMH API and collect CC license data.

    Uses structured license metadata from OAI-PMH instead of text search.
    Harvests papers from recent years to focus on CC-licensed content.
    """

    LOGGER.info("Beginning to fetch results from ArXiv OAI-PMH API")
    session = shared.get_session()

    # Calculate date range for harvesting
    current_year = datetime.now().year
    start_year = current_year - args.years_back
    from_date = f"{start_year}-01-01"

    LOGGER.info(
        f"Harvesting papers from {from_date} onwards "
        f"({args.years_back} years back)"
    )

    # Data structures for counting
    license_counts = defaultdict(int)
    category_counts = defaultdict(lambda: defaultdict(int))
    year_counts = defaultdict(lambda: defaultdict(int))
    author_counts = defaultdict(lambda: defaultdict(int))

    total_fetched = 0
    resumption_token = None

    while total_fetched < args.limit:
        try:
            # Build OAI-PMH request URL
            if resumption_token:
                # Continue with resumption token
                query_params = {
                    "verb": "ListRecords",
                    "resumptionToken": resumption_token,
                }
            else:
                # Initial request with date range
                query_params = {
                    "verb": "ListRecords",
                    "metadataPrefix": "arXiv",
                    "from": from_date,
                }

            # Make API request
            LOGGER.info(f"Fetching batch starting from record {total_fetched}")
            response = session.get(BASE_URL, params=query_params, timeout=60)
            response.raise_for_status()

            # Parse XML response
            root = ET.fromstring(response.content)

            # Check for errors
            error_element = root.find(
                ".//{http://www.openarchives.org/OAI/2.0/}error"
            )
            if error_element is not None:
                raise shared.QuantifyingException(
                    f"OAI-PMH Error: {error_element.text}", 1
                )

            # Process records
            records = root.findall(
                ".//{http://www.openarchives.org/OAI/2.0/}record"
            )
            batch_cc_count = 0

            for record in records:
                if total_fetched >= args.limit:
                    break

                # Convert record to string for metadata extraction
                record_xml = ET.tostring(record, encoding="unicode")
                metadata = extract_metadata_from_xml(record_xml)

                # Only process CC-licensed papers
                if (
                    metadata["license"] != "Unknown"
                    and metadata["license"].startswith("CC")
                ):
                    license_info = metadata["license"]
                    category = metadata["category"]
                    year = metadata["year"]
                    author_count = metadata["author_count"]

                    # Count by license
                    license_counts[license_info] += 1

                    # Count by category and license
                    category_counts[license_info][category] += 1

                    # Count by year and license
                    year_counts[license_info][year] += 1

                    # Count by author count and license
                    author_counts[license_info][author_count] += 1

                    total_fetched += 1
                    batch_cc_count += 1

            LOGGER.info(
                f"Batch completed: {batch_cc_count} CC-licensed papers found"
            )

            # Check for resumption token
            resumption_element = root.find(
                ".//{http://www.openarchives.org/OAI/2.0/}resumptionToken"
            )
            if resumption_element is not None and resumption_element.text:
                resumption_token = resumption_element.text
                LOGGER.info("Continuing with resumption token...")
            else:
                LOGGER.info("No more records available")
                break

            # OAI-PMH recommends delays between requests
            time.sleep(3)

        except requests.HTTPError as e:
            raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
        except requests.RequestException as e:
            raise shared.QuantifyingException(f"Request Exception: {e}", 1)
        except ET.ParseError as e:
            raise shared.QuantifyingException(f"XML Parse Error: {e}", 1)
        except Exception as e:
            raise shared.QuantifyingException(f"Unexpected error: {e}", 1)

    # Save results
    if args.enable_save:
        save_count_data(
            license_counts, category_counts, year_counts, author_counts
        )

    # Save provenance
    provenance_data = {
        "total_fetched": total_fetched,
        "from_date": from_date,
        "years_back": args.years_back,
        "limit": args.limit,
        "quarter": QUARTER,
        "script": os.path.basename(__file__),
        "api_endpoint": BASE_URL,
        "method": "OAI-PMH structured license harvesting",
    }

    # Write provenance YAML for auditing
    try:
        with open(
            FILE_PROVENANCE, "w", encoding="utf-8", newline="\n"
        ) as file_handle:
            yaml.dump(
                provenance_data,
                file_handle,
                default_flow_style=False,
                indent=2,
            )
    except Exception as e:
        LOGGER.error(f"Failed to write provenance file: {e}")
        raise shared.QuantifyingException(
            f"Provenance file write failed: {e}", 1
        )

    LOGGER.info(f"Total papers with CC licenses fetched: {total_fetched}")
    LOGGER.info(f"License distribution: {dict(license_counts)}")


def main():
    """Main function."""
    LOGGER.info("Script execution started.")
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    initialize_all_data_files(args)
    query_arxiv(args)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new ArXiv CC license data for {QUARTER}",
    )
    shared.git_push_changes(args, PATHS["repo"])


if __name__ == "__main__":
    try:
        main()
    except shared.QuantifyingException as e:
        if e.exit_code == 0:
            LOGGER.info(e.message)
        else:
            LOGGER.error(e.message)
        sys.exit(e.exit_code)
    except SystemExit as e:
        if e.code != 0:
            LOGGER.error(f"System exit with code: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        LOGGER.info("(130) Halted via KeyboardInterrupt.")
        sys.exit(130)
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
