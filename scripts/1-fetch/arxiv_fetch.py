#!/usr/bin/env python
"""
Fetch ArXiv articles with CC license information using OAI-PMH API.
OAI-PMH: Open Archives Initiative Protocol for Metadata Havesting.
"""
# Standard library
import argparse
import csv
import os
import sys
import textwrap
import time
import traceback
from collections import Counter, defaultdict
from copy import copy
from datetime import datetime, timezone
from operator import itemgetter

# Third-party
import requests
import yaml
from lxml import etree
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
BASE_URL = "https://oaipmh.arxiv.org/oai"
# Defaults should result in quick operation (not complete operation)
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
DEFAULT_FETCH_LIMIT = 1000
DEFAULT_YEARS_BACK = 5
# CSV file paths
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
FILE_PROVENANCE = shared.path_join(
    PATHS["data_1-fetch"], "arxiv_provenance.yaml"
)
# CSV headers
HEADER_AUTHOR_BUCKET = ["TOOL_IDENTIFIER", "AUTHOR_BUCKET", "COUNT"]
HEADER_CATEGORY_REPORT = [
    "TOOL_IDENTIFIER",
    "CATEGORY_CODE",
    "CATEGORY_LABEL",
    "COUNT",
]
HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])


# parsing arguments function
def parse_arguments():
    """Parse command-line options, returns parsed argument namespace.

    Note: The --limit parameter sets the total number of articles to fetch.
    The --years-back parameter limits harvesting to recent years where
    CC licensing is more common.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
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
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FETCH_LIMIT,
        help=(
            "Limit number of fetched articles (default:"
            f" {DEFAULT_FETCH_LIMIT}). Use a value of -1 to remove limit."
        ),
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=DEFAULT_YEARS_BACK,
        help=(
            "Number of years back from current year to fetch (default:"
            f" {DEFAULT_YEARS_BACK}). Use a value of -1 to specify 2008-02-05"
            " (first date a CC licensed article was added)."
        ),
    )

    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    # Restrict args.years_back to earliest datetime and initialize
    # args.from_date
    #
    # Survey of records indicated the first CC licenced article was added on
    # 2008-02-05
    earliest_date = datetime(2008, 2, 5, tzinfo=timezone.utc)
    this_year = datetime.now(timezone.utc).year
    if args.years_back == -1:
        arg_date = earliest_date
    else:
        start_year = this_year - args.years_back
        arg_date = datetime(start_year, 1, 1, tzinfo=timezone.utc)
        if arg_date < earliest_date:
            arg_date = earliest_date
    args.from_date = arg_date.strftime("%Y-%m-%d")
    args.years_back = this_year - arg_date.year

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


def get_license_mapping():
    global LICENSE_MAPPING
    LOGGER.info("Loading CC Legal Tool metadata for license mapping")
    file_path = shared.path_join(PATHS["data"], "cc-legal-tools.csv")
    license_mapping = {}
    with open(file_path, "r", encoding="utf-8") as file_obj:
        rows = csv.DictReader(file_obj, dialect="unix")
        for row in rows:
            simple_url = row["CANONICAL_URL"].replace("https://", "")
            simple_url = simple_url.rstrip("/")
            identifier = row["IDENTIFIER"]
            license_mapping[simple_url] = identifier

    # Add legacy entry
    simple_url = "creativecommons.org/licenses/publicdomain"
    license_mapping[simple_url] = "CERTIFICATION 1.0 US"

    LICENSE_MAPPING = dict(
        sorted(license_mapping.items(), key=lambda item: item[1])
    )


def extract_record_license(record):
    """
    Extract CC license information from OAI-PMH XML record.
    Returns normalized license identifier or specific error indicator.
    """
    # Find license element in arXiv namespace
    license_element = record.find(".//{http://arxiv.org/OAI/arXiv/}license")

    if license_element is not None and license_element.text:
        license_url = license_element.text.strip()
        simple_url = copy(license_url).replace("http://", "")
        simple_url = simple_url.replace("https://", "")
        simple_url = simple_url.rstrip("/")
        # Check exact mapping first
        if simple_url in LICENSE_MAPPING:
            return LICENSE_MAPPING[simple_url]
        # Validate CC URLs more strictly
        elif "creativecommons.org" in license_url.lower():
            return f"CC (ambiguous): {license_url}"
        else:
            return "Non-CC"
    else:
        return "No license field"


def extract_record_metadata(record):
    """
    Extract paper metadata from OAI-PMH XML record.

    Returns dict with author_count, category, year, and license info.
    """

    # Extract license first to avoid unnecessary work
    license_info = extract_record_license(record)
    if not license_info.startswith("CC"):
        return {}

    #  # Extract added on
    #  added_on_elem = record.find(
    #      ".//{http://www.openarchives.org/OAI/2.0/}datestamp"
    #  )
    #  if added_on_elem is not None and added_on_elem.text:
    #      added_on = added_on_elem.text.strip()

    # Extract author count
    authors = record.findall(".//{http://arxiv.org/OAI/arXiv/}author")
    author_count = len(authors) if authors else 0

    # Extract category (primary category from categories field)
    categories_elem = record.find(".//{http://arxiv.org/OAI/arXiv/}categories")
    if categories_elem is not None and categories_elem.text:
        # Take first category as primary
        category = categories_elem.text.strip().split()[0]
    else:
        category = "Unknown"

    # Extract year from 1) updated, 2) created
    updated_elem = record.find(".//{http://arxiv.org/OAI/arXiv/}updated")
    if updated_elem is not None and updated_elem.text:
        try:
            year = updated_elem.text.strip()[:4]  # Extract year
        except (AttributeError, IndexError) as e:
            LOGGER.error(
                f"Failed to extract year from '{updated_elem.text}': {e}"
            )
            year = "Unknown"
    else:
        created_elem = record.find(".//{http://arxiv.org/OAI/arXiv/}created")
        if created_elem is not None and created_elem.text:
            try:
                year = created_elem.text.strip()[:4]  # Extract year
            except (AttributeError, IndexError) as e:
                LOGGER.error(
                    f"Failed to extract year from '{created_elem.text}': {e}"
                )
                year = "Unknown"
        else:
            year = "Unknown"

    metadata = {
        #  "added_on": added_on,
        "author_count": author_count,
        "category": category,
        "license": license_info,
        "year": year,
    }
    return metadata


def bucket_author_count(author_count):
    """
    Convert author count to predefined buckets: "1", "2", "3", "4", "5+".
    """
    if author_count <= 4:
        return str(author_count)
    return "5+"


def query_arxiv(args, session):
    """
    Query ArXiv OAI-PMH API and return information about CC licensed articles.
    """
    LOGGER.info(
        f"Querying articles from {args.from_date} onwards ({args.years_back}"
        " years back)"
    )

    # Data structures for counting
    license_counts = defaultdict(int)
    category_counts = defaultdict(lambda: defaultdict(int))
    year_counts = defaultdict(lambda: defaultdict(int))
    author_counts = defaultdict(lambda: defaultdict(int))

    batch = 1
    total_fetched = 0
    cc_articles_found = 0
    #  min_added_on = False
    resumption_token = None

    # Proceed is set to False when limit reached or end of records (missing
    # resumption token)
    proceed = True
    while proceed:
        if resumption_token:
            # Continue with resumption token
            query_params = {
                "verb": "ListRecords",
                "resumptionToken": resumption_token,
            }
            verb = "resuming"
        else:
            # Initial request with date range
            query_params = {
                "verb": "ListRecords",
                "metadataPrefix": "arXiv",
                "from": args.from_date,
            }
            verb = "starting"

        # Make API request
        LOGGER.info(
            f"Fetching batch {batch} {verb} from record {total_fetched}"
        )
        batch += 1

        try:
            # Build OAI-PMH request URL
            response = session.get(BASE_URL, params=query_params, timeout=60)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
        except requests.RequestException as e:
            raise shared.QuantifyingException(f"Request Exception: {e}", 1)

        root = etree.fromstring(response.content)

        # Check for errors
        error_element = root.find(
            ".//{http://www.openarchives.org/OAI/2.0/}error"
        )
        if error_element is not None:
            raise shared.QuantifyingException(
                f"OAI-PMH Error: {error_element.text}", 1
            )

        # Process batch of article records
        records = root.findall(
            ".//{http://www.openarchives.org/OAI/2.0/}record"
        )
        batch_cc_count = 0
        for record in records:
            if args.limit > 0 and args.limit <= total_fetched:
                proceed = False
                break
            total_fetched += 1

            metadata = extract_record_metadata(record)
            if not metadata:  # Only true for CC licensed articles
                continue

            #  added_on = metadata["added_on"]
            #  if not min_added_on or added_on < min_added_on:
            #      min_added_on = added_on

            license_info = metadata["license"]

            # Count by author count and license
            author_count = metadata["author_count"]
            author_counts[license_info][author_count] += 1

            # Count by category and license
            category = metadata["category"]
            category_counts[license_info][category] += 1

            # Count by license
            license_counts[license_info] += 1

            # Count by year and license
            year = metadata["year"]
            year_counts[license_info][year] += 1

            batch_cc_count += 1
            cc_articles_found += 1

        #  if min_added_on:
        #      LOGGER.info(f"  Earliest CC article addition: {min_added_on}")

        LOGGER.info(
            f"  Batch CC licensed articles: {batch_cc_count}, Total"
            f" CC-licensed articles: {cc_articles_found}"
        )

        # Check for resumption token
        resumption_element = root.find(
            ".//{http://www.openarchives.org/OAI/2.0/}resumptionToken"
        )
        if not proceed:
            break
        elif resumption_element is not None and resumption_element.text:
            resumption_token = resumption_element.text
        else:
            LOGGER.info("No more records available")
            proceed = False
            break

        # OAI-PMH requires a 3 second delay between requests
        # https://info.arxiv.org/help/api/tou.html#rate-limits
        time.sleep(3)

    data = {
        "author_counts": author_counts,
        "category_counts": category_counts,
        "license_counts": license_counts,
        "year_counts": year_counts,
    }
    return data, cc_articles_found


def rows_to_csv(args, fieldnames, rows, file_path):
    if not args.enable_save:
        return args

    with open(file_path, "w", encoding="utf-8", newline="\n") as file_handle:
        writer = csv.DictWriter(
            file_handle, fieldnames=fieldnames, dialect="unix"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_data(args, data):
    """
    Write fetched data to CSV files.
    """
    # Save author buckets report
    # fetched_data["author_counts"]: {license: {author_count: count}}
    rows = []
    for license_name, author_count_data in data["author_counts"].items():
        # build buckets across licenses
        bucket_counts = Counter()
        for author_count, count in author_count_data.items():
            bucket = bucket_author_count(author_count)
            bucket_counts[bucket] += count
        for bucket, count in bucket_counts.items():
            rows.append(
                {
                    "TOOL_IDENTIFIER": license_name,
                    "AUTHOR_BUCKET": bucket,
                    "COUNT": count,
                }
            )
    rows.sort(key=itemgetter("TOOL_IDENTIFIER", "AUTHOR_BUCKET"))
    rows_to_csv(args, HEADER_AUTHOR_BUCKET, rows, FILE_ARXIV_AUTHOR_BUCKET)

    # Save category report with labels
    # fetched_data["category_counts"]: {license: {category_code: count}}
    rows = []
    for license_name, categories in data["category_counts"].items():
        for code, count in categories.items():
            label = CATEGORIES.get(code, code)
            rows.append(
                {
                    "TOOL_IDENTIFIER": license_name,
                    "CATEGORY_CODE": code,
                    "CATEGORY_LABEL": label,
                    "COUNT": count,
                }
            )
    rows.sort(key=itemgetter("TOOL_IDENTIFIER", "CATEGORY_CODE"))
    rows_to_csv(args, HEADER_CATEGORY_REPORT, rows, FILE_ARXIV_CATEGORY_REPORT)

    # Save license counts report
    # fetched_data["license_counts"]: {license: count}
    rows = []
    for license_name, count in data["license_counts"].items():
        rows.append({"TOOL_IDENTIFIER": license_name, "COUNT": count})
    rows.sort(key=itemgetter("TOOL_IDENTIFIER"))
    rows_to_csv(args, HEADER_COUNT, rows, FILE_ARXIV_COUNT)

    # Save year count report
    # fetched_data["year_counts"]: {license: {year: count}}
    rows = []
    for license_name, years in data["year_counts"].items():
        for year, count in years.items():
            rows.append(
                {"TOOL_IDENTIFIER": license_name, "YEAR": year, "COUNT": count}
            )
    rows.sort(key=itemgetter("TOOL_IDENTIFIER", "YEAR"))
    rows_to_csv(args, HEADER_YEAR, rows, FILE_ARXIV_YEAR)


def write_provence(args, cc_articles_found):
    """
    Write provenance information to YAML file.
    """
    if not args.enable_save:
        return args

    # Save provenance
    desc = "Open Archives Initiative Protocol for Metadata Havesting (OAI-PMH)"
    provenance_data = {
        "api_description": desc,
        "api_endpoint": BASE_URL,
        "arguments": {
            "from_date": args.from_date,
            "limit": args.limit,
            "years_back": args.years_back,
        },
        "cc_articles_found": cc_articles_found,
        "quarter": QUARTER,
        "script": os.path.basename(__file__),
    }

    # Write provenance YAML for auditing
    with open(
        FILE_PROVENANCE, "w", encoding="utf-8", newline="\n"
    ) as file_handle:
        yaml.dump(
            provenance_data,
            file_handle,
            default_flow_style=False,
            indent=2,
        )


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    initialize_all_data_files(args)
    get_license_mapping()
    session = shared.get_session()
    data, cc_articles_found = query_arxiv(args, session)
    write_data(args, data)
    write_provence(args, cc_articles_found)
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
