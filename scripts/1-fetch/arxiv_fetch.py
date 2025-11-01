#!/usr/bin/env python
"""
Fetch ArXiv papers with CC license information and generate count reports.
"""
# Standard library
import argparse
import csv
import os
import re
import sys
import textwrap
import time
import traceback
import urllib.parse
from collections import Counter, defaultdict
from operator import itemgetter

# Third-party
import feedparser
import requests
import yaml
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# Constants
# API Configuration
BASE_URL = "http://export.arxiv.org/api/query?"
DEFAULT_FETCH_LIMIT = 800  # Default total papers to fetch

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

# Search Queries
SEARCH_QUERIES = [
    'all:"creative commons"',
    'all:"CC BY"',
    'all:"CC-BY"',
    'all:"CC BY-NC"',
    'all:"CC-BY-NC"',
    'all:"CC BY-SA"',
    'all:"CC-BY-SA"',
    'all:"CC BY-ND"',
    'all:"CC-BY-ND"',
    'all:"CC BY-NC-SA"',
    'all:"CC-BY-NC-SA"',
    'all:"CC BY-NC-ND"',
    'all:"CC-BY-NC-ND"',
    'all:"CC0"',
    'all:"CC 0"',
    'all:"CC-0"',
]

# Compiled regex patterns for CC license detection
CC_PATTERNS = [
    (re.compile(r"\bCC[-\s]?0\b", re.IGNORECASE), "CC0"),
    (
        re.compile(r"\bCC[-\s]?BY[-\s]?NC[-\s]?ND\b", re.IGNORECASE),
        "CC BY-NC-ND",
    ),
    (
        re.compile(r"\bCC[-\s]?BY[-\s]?NC[-\s]?SA\b", re.IGNORECASE),
        "CC BY-NC-SA",
    ),
    (re.compile(r"\bCC[-\s]?BY[-\s]?ND\b", re.IGNORECASE), "CC BY-ND"),
    (re.compile(r"\bCC[-\s]?BY[-\s]?SA\b", re.IGNORECASE), "CC BY-SA"),
    (re.compile(r"\bCC[-\s]?BY[-\s]?NC\b", re.IGNORECASE), "CC BY-NC"),
    (re.compile(r"\bCC[-\s]?BY\b", re.IGNORECASE), "CC BY"),
    (
        re.compile(r"\bCREATIVE\s+COMMONS\b", re.IGNORECASE),
        "UNKNOWN CC legal tool",
    ),
]

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

    Note: The --limit parameter sets the total number of papers to fetch
    across all search queries, not per query. ArXiv API recommends
    maximum of 30000 results per session for optimal performance.
    """
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FETCH_LIMIT,
        help=(
            f"Total limit of papers to fetch across all search queries "
            f"(default: {DEFAULT_FETCH_LIMIT}). Maximum recommended: 30000. "
            f"Note: Individual queries limited to 500 results "
            f"(implementation choice). "
            f"See ArXiv API documentation: "
            f"https://info.arxiv.org/help/api/user-manual.html"
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


def get_requests_session():
    """Create request session with retry logic"""
    retry_strategy = Retry(
        total=5,
        backoff_factor=10,
        status_forcelist=shared.STATUS_FORCELIST,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": shared.USER_AGENT})
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session


def normalize_license_text(raw_text):
    """
    Convert raw license text to standardized CC license identifiers.

    Uses regex patterns to identify CC licenses from paper text.
    Returns specific license (e.g., "CC BY", "CC0") or "Unknown".
    """
    if not raw_text:
        return "Unknown"

    for pattern, license_type in CC_PATTERNS:
        if pattern.search(raw_text):
            return license_type

    return "Unknown"


def extract_license_info(entry):
    """
    Extract CC license information from ArXiv paper entry.

    Checks rights field first, then summary field for license patterns.
    Returns normalized license identifier or "Unknown".
    """
    # checking through the rights field first then summary
    if hasattr(entry, "rights") and entry.rights:
        license_info = normalize_license_text(entry.rights)
        if license_info != "Unknown":
            return license_info
    if hasattr(entry, "summary") and entry.summary:
        license_info = normalize_license_text(entry.summary)
        if license_info != "Unknown":
            return license_info
    return "Unknown"


def extract_category_from_entry(entry):
    """Extract primary category from ArXiv entry."""
    if (
        hasattr(entry, "arxiv_primary_category")
        and entry.arxiv_primary_category
    ):
        return entry.arxiv_primary_category.get("term", "Unknown")
    if hasattr(entry, "tags") and entry.tags:
        # Get first category from tags
        for tag in entry.tags:
            if hasattr(tag, "term"):
                return tag.term
    return "Unknown"


def extract_year_from_entry(entry):
    """Extract publication year from ArXiv entry."""
    if hasattr(entry, "published") and entry.published:
        try:
            return entry.published[:4]  # Extract year from date string
        except (AttributeError, IndexError) as e:
            LOGGER.debug(
                f"Failed to extract year from '{entry.published}': {e}"
            )
    return "Unknown"


def extract_author_count_from_entry(entry):
    """Extract number of authors from ArXiv entry."""
    if hasattr(entry, "authors") and entry.authors:
        try:
            return len(entry.authors)
        except Exception as e:
            LOGGER.debug(f"Failed to count authors from entry.authors: {e}")
    if hasattr(entry, "author") and entry.author:
        return 1
    return "Unknown"


def bucket_author_count(n):
    """
    Convert author count to predefined buckets for analysis.

    Buckets: "1", "2", "3", "4", "5+", "Unknown"
    Reduces granularity for better statistical analysis.
    """
    if n == 1:
        return "1"
    if n == 2:
        return "2"
    if n == 3:
        return "3"
    if n == 4:
        return "4"
    if n >= 5:
        return "5+"
    return "Unknown"


def save_count_data(
    license_counts, category_counts, year_counts, author_counts
):
    """
    Save all collected data to CSV files.

    """
    # license_counts: {license: count}
    # category_counts: {license: {category_code: count}}
    # year_counts: {license: {year: count}}
    # author_counts: {license: {author_count(int|None): count}}

    # Save license counts
    data = []
    for lic, c in license_counts.items():
        data.append({"TOOL_IDENTIFIER": lic, "COUNT": c})
    data.sort(key=itemgetter("TOOL_IDENTIFIER"))
    with open(FILE_ARXIV_COUNT, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_COUNT, dialect="unix")
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    # Save category report with labels
    data = []
    for lic, cats in category_counts.items():
        for code, c in cats.items():
            label = CATEGORIES.get(code, code)
            data.append(
                {
                    "TOOL_IDENTIFIER": lic,
                    "CATEGORY_CODE": code,
                    "CATEGORY_LABEL": label,
                    "COUNT": c,
                }
            )
    data.sort(key=itemgetter("TOOL_IDENTIFIER", "CATEGORY_CODE"))
    with open(
        FILE_ARXIV_CATEGORY_REPORT, "w", encoding="utf-8", newline="\n"
    ) as fh:
        writer = csv.DictWriter(
            fh, fieldnames=HEADER_CATEGORY_REPORT, dialect="unix"
        )
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    # Save year counts
    data = []
    for lic, years in year_counts.items():
        for year, c in years.items():
            data.append({"TOOL_IDENTIFIER": lic, "YEAR": year, "COUNT": c})
    data.sort(key=itemgetter("TOOL_IDENTIFIER", "YEAR"))
    with open(FILE_ARXIV_YEAR, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_YEAR, dialect="unix")
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    # Save author buckets summary
    data = []
    for lic, acs in author_counts.items():
        # build buckets across licenses
        bucket_counts = Counter()
        for ac, c in acs.items():
            b = bucket_author_count(ac)
            bucket_counts[b] += c
        for b, c in bucket_counts.items():
            data.append(
                {"TOOL_IDENTIFIER": lic, "AUTHOR_BUCKET": b, "COUNT": c}
            )
    data.sort(key=itemgetter("TOOL_IDENTIFIER", "AUTHOR_BUCKET"))
    with open(
        FILE_ARXIV_AUTHOR_BUCKET, "w", encoding="utf-8", newline="\n"
    ) as fh:
        writer = csv.DictWriter(
            fh, fieldnames=HEADER_AUTHOR_BUCKET, dialect="unix"
        )
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def query_arxiv(args):
    """
    Main function to query ArXiv API and collect CC license data.

    """

    LOGGER.info("Beginning to fetch results from ArXiv API")
    session = get_requests_session()

    results_per_iteration = 50

    search_queries = SEARCH_QUERIES

    # Data structures for counting
    license_counts = defaultdict(int)
    category_counts = defaultdict(lambda: defaultdict(int))
    year_counts = defaultdict(lambda: defaultdict(int))
    author_counts = defaultdict(lambda: defaultdict(int))

    total_fetched = 0

    for search_query in search_queries:
        if total_fetched >= args.limit:
            break

        LOGGER.info(f"Searching for: {search_query}")
        papers_found_for_query = 0

        for start in range(
            0,
            min(args.limit - total_fetched, 500),
            results_per_iteration,
        ):
            encoded_query = urllib.parse.quote_plus(search_query)
            query = (
                f"search_query={encoded_query}&start={start}"
                f"&max_results={results_per_iteration}"
            )

            papers_found_in_batch = 0

            try:
                LOGGER.info(
                    f"Fetching results {start} - "
                    f"{start + results_per_iteration}"
                )
                response = session.get(BASE_URL + query, timeout=30)
                response.raise_for_status()
                feed = feedparser.parse(response.content)

                for entry in feed.entries:
                    if total_fetched >= args.limit:
                        break

                    license_info = extract_license_info(entry)

                    if license_info != "Unknown":

                        category = extract_category_from_entry(entry)
                        year = extract_year_from_entry(entry)
                        author_count = extract_author_count_from_entry(entry)

                        # Count by license
                        license_counts[license_info] += 1

                        # Count by category and license
                        category_counts[license_info][category] += 1

                        # Count by year and license
                        year_counts[license_info][year] += 1

                        # Count by author count and license
                        author_counts[license_info][author_count] += 1

                        total_fetched += 1
                        papers_found_in_batch += 1
                        papers_found_for_query += 1

                # arXiv recommends a 3-seconds delay between consecutive
                # api calls for efficiency
                time.sleep(3)
            except requests.HTTPError as e:
                raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
            except requests.RequestException as e:
                raise shared.QuantifyingException(f"Request Exception: {e}", 1)
            except KeyError as e:
                raise shared.QuantifyingException(f"KeyError: {e}", 1)

            if papers_found_in_batch == 0:
                break

        LOGGER.info(
            f"Query '{search_query}' completed: "
            f"{papers_found_for_query} papers found"
        )

    # Save results
    if args.enable_save:
        save_count_data(
            license_counts, category_counts, year_counts, author_counts
        )

    # save provenance
    provenance_data = {
        "total_fetched": total_fetched,
        "queries": search_queries,
        "limit": args.limit,
        "quarter": QUARTER,
        "script": os.path.basename(__file__),
    }

    # write provenance YAML for auditing
    try:
        with open(FILE_PROVENANCE, "w", encoding="utf-8", newline="\n") as fh:
            yaml.dump(provenance_data, fh, default_flow_style=False, indent=2)
    except Exception as e:
        LOGGER.warning("Failed to write provenance file: %s", e)

    LOGGER.info(f"Total CC licensed papers fetched: {total_fetched}")


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
