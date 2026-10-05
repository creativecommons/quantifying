#!/usr/bin/env python
"""
Fetch arXiv articles that use a CC legal tool using the OAI-PMH API.
OAI-PMH: Open Archives Initiative Protocol for Metadata Havesting.

Note: This fetch script is ready to fetch data, but is not ready for
automation. It currently requires approximately 6 hours to execute.
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
DEFAULT_FETCH_LIMIT = 4500  # Fetch 3 batches of 1,500 articles each
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
    "CATEGORY_NAME",
    "COUNT",
]
HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]
QUARTER = os.path.basename(PATHS["data_quarter"])
SUBSUMED_CATEGORIES = {
    # https://arxiv.org/archive/alg-geom
    # "The alg-geom archive has been subsumed into Algebraic Geometry
    # (math.AG)."
    "alg-geom": "math.AG",
    # https://arxiv.org/archive/chao-dyn
    # "The chao-dyn archive has been subsumed into Chaotic Dynamics (nlin.CD)."
    "chao-dyn": "nlin.CD",
    # https://arxiv.org/archive/dg-ga
    # "The dg-ga archive has been subsumed into Differential Geometry
    # (math.DG)."
    "dg-ga": "math.DG",
    # https://arxiv.org/archive/solv-int
    # "The solv-int archive has been subsumed into Exactly Solvable and
    # Integrable Systems (nlin.SI)."
    "solv-int": "nlin.SI",
    # https://arxiv.org/archive/q-alg
    # "The q-alg archive has been subsumed into Quantum Algebra (math.QA)."
    "q-alg": "math.QA",
}


# parsing arguments function
def parse_arguments():
    """
    Parse command-line options, returns parsed argument namespace.
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
            f" {DEFAULT_FETCH_LIMIT}). Use a value of -1 to fetch all articles"
            " (remove limit)."
        ),
    )
    parser.add_argument(
        "--show-added",
        action="store_true",
        help="Log additional information about when articles were added",
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def get_identifier_mapping():
    global IDENTIER_MAPPING
    LOGGER.info("Loading CC Legal Tool metadata for CC identifer mapping")
    file_path = shared.path_join(PATHS["data"], "cc-legal-tools.csv")
    identifier_mapping = {}
    with open(file_path, "r", encoding="utf-8") as file_obj:
        rows = csv.DictReader(file_obj, dialect="unix")
        for row in rows:
            simple_url = row["CANONICAL_URL"].replace("https://", "")
            simple_url = simple_url.rstrip("/")
            identifier = row["IDENTIFIER"]
            identifier_mapping[simple_url] = identifier

    # Add legacy entry
    simple_url = "creativecommons.org/licenses/publicdomain"
    identifier_mapping[simple_url] = "CERTIFICATION 1.0 US"

    IDENTIER_MAPPING = dict(
        sorted(identifier_mapping.items(), key=lambda item: item[1])
    )


def query_category_mapping(args, session):
    """
    Query to establish mapping of category codes and names.

    Also see https://arxiv.org/category_taxonomy
    """
    global CATEGORY_MAPPING

    params = {"verb": "ListSets"}
    try:
        response = session.get(BASE_URL, params=params, timeout=60)
        response.raise_for_status()
    except requests.HTTPError as e:
        raise shared.QuantifyingException(f"HTTP Error: {e}", 1)
    except requests.RequestException as e:
        raise shared.QuantifyingException(f"Request Exception: {e}", 1)

    root = etree.fromstring(response.content)
    CATEGORY_MAPPING = {}
    sets = root.findall(".//{http://www.openarchives.org/OAI/2.0/}set")
    for set_ in sets:
        spec, name = set_.getchildren()
        # Ensure category code (key) matches code used in articles
        spec_list = spec.text.split(":")
        if len(spec_list) > 1:
            # Remove parent category and replace colon with period
            # 3 part examples:
            #     match:math:AC       => math.AC
            #     physics:astro-ph:CO => astro-ph.CO
            # 2 part examples
            #     physics:astro-ph    => astro-ph
            #     physics:quant-ph    => quant-ph
            spec_text = ".".join(spec_list[1:])
        else:
            spec_text = spec.text
        CATEGORY_MAPPING[spec_text] = name.text
    CATEGORY_MAPPING = dict(sorted(CATEGORY_MAPPING.items()))


def extract_record_cc_legal_tool_identifier(record):
    """
    Extract CC legal tool identifier from OAI-PMH XML record.

    Returns normalized legal tool identifier or specific error indicator.
    """
    # Find license element in arXiv namespace
    license_element = record.find(".//{http://arxiv.org/OAI/arXiv/}license")

    if license_element is not None and license_element.text:
        license_url = license_element.text.strip()
        simple_url = copy(license_url).replace("http://", "")
        simple_url = simple_url.replace("https://", "")
        simple_url = simple_url.rstrip("/")
        # Check exact mapping first
        if simple_url in IDENTIER_MAPPING:
            identifer = IDENTIER_MAPPING[simple_url]
        # Validate CC URLs more strictly
        elif "creativecommons.org" in license_url.lower():
            identifer = f"CC (ambiguous): {license_url}"
        else:
            identifer = "N/A: non-CC"
    else:
        identifer = "N/A: article missing license field"

    return identifer


def extract_record_metadata(args, record):
    """
    Extract paper metadata from OAI-PMH XML record.

    Returns metadata dictionary.
    """
    metadata = {}

    # Extract identifer first to avoid unnecessary work
    identifer = extract_record_cc_legal_tool_identifier(record)
    if not identifer.startswith("CC"):
        return {}
    # metadata value set below to ensure natural order of keys

    if args.show_added:
        # Extract added on
        added_on_elem = record.find(
            ".//{http://www.openarchives.org/OAI/2.0/}datestamp"
        )
        if added_on_elem is not None and added_on_elem.text:
            metadata["added_on"] = added_on_elem.text.strip()
        else:
            metadata["added_on"] = False

    # Extract author count
    authors = record.findall(".//{http://arxiv.org/OAI/arXiv/}author")
    metadata["author_count"] = len(authors) if authors else 0

    # Extract categories
    categories_elem = record.find(".//{http://arxiv.org/OAI/arXiv/}categories")
    if categories_elem is not None and categories_elem.text:
        metadata["categories"] = categories_elem.text.strip().split()
        for index, code in enumerate(metadata["categories"]):
            metadata["categories"][index] = SUBSUMED_CATEGORIES.get(code, code)
        metadata["categories"] = list(set(metadata["categories"]))
        metadata["categories"].sort()
    else:
        metadata["categories"] = False

    # Set identifer
    metadata["identifer"] = identifer

    # Extract year from 1) updated, 2) created
    updated_elem = record.find(".//{http://arxiv.org/OAI/arXiv/}updated")
    if updated_elem is not None and updated_elem.text:
        try:
            metadata["year"] = updated_elem.text.strip()[:4]  # Extract year
        except (AttributeError, IndexError) as e:
            LOGGER.error(
                f"Failed to extract year from '{updated_elem.text}': {e}"
            )
            metadata["year"] = "Unknown"
    else:
        created_elem = record.find(".//{http://arxiv.org/OAI/arXiv/}created")
        if created_elem is not None and created_elem.text:
            try:
                metadata["year"] = created_elem.text.strip()[
                    :4
                ]  # Extract year
            except (AttributeError, IndexError) as e:
                LOGGER.error(
                    f"Failed to extract year from '{created_elem.text}': {e}"
                )
                metadata["year"] = "Unknown"
        else:
            metadata["year"] = "Unknown"

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
    Query arXiv OAI-PMH API starting from addition date 2008-02-05 and return
    information about articles using a CC legal tool.

    2008-02-05 was the first date that articles using a CC legal tool were
    added to arXiv.
    """
    if args.limit == -1:
        count_desc = "all"
    else:
        count_desc = f"a maximum of {args.limit}"
    LOGGER.info(
        f"Fetching {count_desc} articles starting form add date 2008-02-05"
    )

    # Data structures for counting
    tool_counts = defaultdict(int)
    category_counts = defaultdict(lambda: defaultdict(int))
    year_counts = defaultdict(lambda: defaultdict(int))
    author_counts = defaultdict(lambda: defaultdict(int))

    batch = 1
    total_fetched = 0
    cc_articles_found = 0
    if args.show_added:
        cc_articles_added = []
    resumption_token = None

    # Proceed is set to False when limit reached or end of records (missing
    # resumption token)
    proceed = True
    while proceed:
        if args.limit > 0 and args.limit <= total_fetched:
            proceed = False
            break

        if resumption_token:
            # Continue with resumption token
            params = {
                "verb": "ListRecords",
                "resumptionToken": resumption_token,
            }
            verb = "resuming"
        else:
            # Initial request with date range
            params = {
                "verb": "ListRecords",
                "metadataPrefix": "arXiv",
                "from": "2008-02-05",  # First addition of articles using CC
            }
            verb = "starting"

        # Make API request
        LOGGER.info(
            f"Fetching batch {batch} {verb} from record {total_fetched}"
        )
        batch += 1

        try:
            # Build OAI-PMH request URL
            response = session.get(BASE_URL, params=params, timeout=60)
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

            metadata = extract_record_metadata(args, record)
            if not metadata:  # Only true for articles using a CC legal tool
                continue

            if args.show_added and metadata["added_on"]:
                cc_articles_added.append(metadata["added_on"])
            identifer = metadata["identifer"]

            # Count by author count and identifer
            author_count = metadata["author_count"]
            author_counts[identifer][author_count] += 1

            # Count by category and identifer
            categories = metadata["categories"]
            if metadata["categories"]:
                for category in categories:
                    category_counts[identifer][category] += 1

            # Count by identifer
            tool_counts[identifer] += 1

            # Count by year and identifer
            year = metadata["year"]
            year_counts[identifer][year] += 1

            batch_cc_count += 1
            cc_articles_found += 1

        if args.show_added and cc_articles_added:
            cc_articles_added = list(set(cc_articles_added))
            cc_articles_added.sort()
            LOGGER.info(f"  CC articles added: {', '.join(cc_articles_added)}")

        LOGGER.info(
            f"  Batch CC legal tool articles: {batch_cc_count}, Total"
            f" CC legal tool articles: {cc_articles_found}"
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
        "tool_counts": tool_counts,
        "year_counts": year_counts,
    }
    return data, cc_articles_found


def write_data(args, data):
    """
    Write fetched data to CSV files.
    """
    # Save author buckets report
    # fetched_data["author_counts"]: {identifer: {author_count: count}}
    rows = []
    for identifier, author_count_data in data["author_counts"].items():
        # build buckets across CC legal tool identifiers
        bucket_counts = Counter()
        for author_count, count in author_count_data.items():
            bucket = bucket_author_count(author_count)
            bucket_counts[bucket] += count
        # add rows
        for bucket, count in bucket_counts.items():
            rows.append(
                {
                    "TOOL_IDENTIFIER": identifier,
                    "AUTHOR_BUCKET": bucket,
                    "COUNT": count,
                }
            )
    rows.sort(key=itemgetter("TOOL_IDENTIFIER", "AUTHOR_BUCKET"))
    shared.rows_to_csv(
        args, FILE_ARXIV_AUTHOR_BUCKET, HEADER_AUTHOR_BUCKET, rows
    )

    # Save category report
    # fetched_data["category_counts"]: {identifer: {category_code: count}}
    rows = []
    for identifier, categories in data["category_counts"].items():
        for code, count in categories.items():
            # map category codes to names
            name = CATEGORY_MAPPING.get(code, code)
            # append row
            rows.append(
                {
                    "TOOL_IDENTIFIER": identifier,
                    "CATEGORY_CODE": code,
                    "CATEGORY_NAME": name,
                    "COUNT": count,
                }
            )
    rows.sort(key=itemgetter("TOOL_IDENTIFIER", "CATEGORY_CODE"))
    shared.rows_to_csv(
        args, FILE_ARXIV_CATEGORY_REPORT, HEADER_CATEGORY_REPORT, rows
    )

    # Save tool counts report
    # fetched_data["tool_counts"]: {identfier: count}
    rows = []
    for identifier, count in data["tool_counts"].items():
        rows.append({"TOOL_IDENTIFIER": identifier, "COUNT": count})
    rows.sort(key=itemgetter("TOOL_IDENTIFIER"))
    shared.rows_to_csv(args, FILE_ARXIV_COUNT, HEADER_COUNT, rows)

    # Save year count report
    # fetched_data["year_counts"]: {identifer: {year: count}}
    rows = []
    for identifier, years in data["year_counts"].items():
        for year, count in years.items():
            rows.append(
                {"TOOL_IDENTIFIER": identifier, "YEAR": year, "COUNT": count}
            )
    rows.sort(key=itemgetter("TOOL_IDENTIFIER", "YEAR"))
    shared.rows_to_csv(args, FILE_ARXIV_YEAR, HEADER_YEAR, rows)


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
        "cc_articles_found": cc_articles_found,
        "fetch_limit": args.limit,
        "from_add_date": "2008-02-05",
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
    get_identifier_mapping()
    session = shared.get_session()
    query_category_mapping(args, session)
    data, cc_articles_found = query_arxiv(args, session)
    write_data(args, data)
    write_provence(args, cc_articles_found)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new arXiv data for {QUARTER}",
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
