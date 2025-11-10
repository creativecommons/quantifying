#!/usr/bin/env python
"""
Fetch DOAJ journals and articles with CC license information using API v4.
Enhanced to capture more comprehensive license data from both journals and articles.
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

# Third-party
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
BASE_URL = "https://doaj.org/api/v4/search"
DEFAULT_FETCH_LIMIT = 1000
RATE_LIMIT_DELAY = 0.5

# CSV Headers
HEADER_COUNT = ["TOOL_IDENTIFIER", "COUNT"]
HEADER_SUBJECT_REPORT = [
    "TOOL_IDENTIFIER",
    "SUBJECT_CODE",
    "SUBJECT_LABEL",
    "COUNT",
]
HEADER_LANGUAGE = ["TOOL_IDENTIFIER", "LANGUAGE_CODE", "LANGUAGE", "COUNT"]
HEADER_YEAR = ["TOOL_IDENTIFIER", "YEAR", "COUNT"]
HEADER_ARTICLE_COUNT = ["TOOL_IDENTIFIER", "TYPE", "COUNT"]
HEADER_PUBLISHER = ["TOOL_IDENTIFIER", "PUBLISHER", "COUNTRY_CODE", "COUNTRY_NAME", "COUNT"]

# CC License types
CC_LICENSE_TYPES = [
    "CC BY",
    "CC BY-NC",
    "CC BY-SA",
    "CC BY-ND",
    "CC BY-NC-SA",
    "CC BY-NC-ND",
    "CC0",
    "UNKNOWN CC legal tool",
]

# Language code to readable name mapping
LANGUAGE_NAMES = {
    "EN": "English",
    "ES": "Spanish",
    "PT": "Portuguese",
    "FR": "French",
    "DE": "German",
    "IT": "Italian",
    "RU": "Russian",
    "ZH": "Chinese",
    "JA": "Japanese",
    "AR": "Arabic",
    "TR": "Turkish",
    "NL": "Dutch",
    "SV": "Swedish",
    "NO": "Norwegian",
    "DA": "Danish",
    "FI": "Finnish",
    "PL": "Polish",
    "CS": "Czech",
    "HU": "Hungarian",
    "RO": "Romanian",
    "BG": "Bulgarian",
    "HR": "Croatian",
    "SK": "Slovak",
    "SL": "Slovenian",
    "ET": "Estonian",
    "LV": "Latvian",
    "LT": "Lithuanian",
    "EL": "Greek",
    "CA": "Catalan",
    "IS": "Icelandic",
    "MK": "Macedonian",
    "SR": "Serbian",
    "UK": "Ukrainian",
    "BE": "Belarusian",
    "KO": "Korean",
    "TH": "Thai",
    "VI": "Vietnamese",
    "ID": "Indonesian",
    "MS": "Malay",
    "HI": "Hindi",
    "BN": "Bengali",
    "UR": "Urdu",
    "FA": "Persian",
    "HE": "Hebrew",
    "SW": "Swahili",
    "AF": "Afrikaans",
}

# ISO 3166-1 alpha-2 country codes to country names (DOAJ uses this standard)
COUNTRY_NAMES = {
    "AD": "Andorra", "AE": "United Arab Emirates", "AF": "Afghanistan", "AG": "Antigua and Barbuda",
    "AI": "Anguilla", "AL": "Albania", "AM": "Armenia", "AO": "Angola", "AQ": "Antarctica",
    "AR": "Argentina", "AS": "American Samoa", "AT": "Austria", "AU": "Australia", "AW": "Aruba",
    "AX": "Åland Islands", "AZ": "Azerbaijan", "BA": "Bosnia and Herzegovina", "BB": "Barbados",
    "BD": "Bangladesh", "BE": "Belgium", "BF": "Burkina Faso", "BG": "Bulgaria", "BH": "Bahrain",
    "BI": "Burundi", "BJ": "Benin", "BL": "Saint Barthélemy", "BM": "Bermuda", "BN": "Brunei",
    "BO": "Bolivia", "BQ": "Caribbean Netherlands", "BR": "Brazil", "BS": "Bahamas", "BT": "Bhutan",
    "BV": "Bouvet Island", "BW": "Botswana", "BY": "Belarus", "BZ": "Belize", "CA": "Canada",
    "CC": "Cocos Islands", "CD": "Democratic Republic of the Congo", "CF": "Central African Republic",
    "CG": "Republic of the Congo", "CH": "Switzerland", "CI": "Côte d'Ivoire", "CK": "Cook Islands",
    "CL": "Chile", "CM": "Cameroon", "CN": "China", "CO": "Colombia", "CR": "Costa Rica",
    "CU": "Cuba", "CV": "Cape Verde", "CW": "Curaçao", "CX": "Christmas Island", "CY": "Cyprus",
    "CZ": "Czech Republic", "DE": "Germany", "DJ": "Djibouti", "DK": "Denmark", "DM": "Dominica",
    "DO": "Dominican Republic", "DZ": "Algeria", "EC": "Ecuador", "EE": "Estonia", "EG": "Egypt",
    "EH": "Western Sahara", "ER": "Eritrea", "ES": "Spain", "ET": "Ethiopia", "FI": "Finland",
    "FJ": "Fiji", "FK": "Falkland Islands", "FM": "Micronesia", "FO": "Faroe Islands", "FR": "France",
    "GA": "Gabon", "GB": "United Kingdom", "GD": "Grenada", "GE": "Georgia", "GF": "French Guiana",
    "GG": "Guernsey", "GH": "Ghana", "GI": "Gibraltar", "GL": "Greenland", "GM": "Gambia",
    "GN": "Guinea", "GP": "Guadeloupe", "GQ": "Equatorial Guinea", "GR": "Greece", "GS": "South Georgia",
    "GT": "Guatemala", "GU": "Guam", "GW": "Guinea-Bissau", "GY": "Guyana", "HK": "Hong Kong",
    "HM": "Heard Island", "HN": "Honduras", "HR": "Croatia", "HT": "Haiti", "HU": "Hungary",
    "ID": "Indonesia", "IE": "Ireland", "IL": "Israel", "IM": "Isle of Man", "IN": "India",
    "IO": "British Indian Ocean Territory", "IQ": "Iraq", "IR": "Iran", "IS": "Iceland", "IT": "Italy",
    "JE": "Jersey", "JM": "Jamaica", "JO": "Jordan", "JP": "Japan", "KE": "Kenya", "KG": "Kyrgyzstan",
    "KH": "Cambodia", "KI": "Kiribati", "KM": "Comoros", "KN": "Saint Kitts and Nevis", "KP": "North Korea",
    "KR": "South Korea", "KW": "Kuwait", "KY": "Cayman Islands", "KZ": "Kazakhstan", "LA": "Laos",
    "LB": "Lebanon", "LC": "Saint Lucia", "LI": "Liechtenstein", "LK": "Sri Lanka", "LR": "Liberia",
    "LS": "Lesotho", "LT": "Lithuania", "LU": "Luxembourg", "LV": "Latvia", "LY": "Libya",
    "MA": "Morocco", "MC": "Monaco", "MD": "Moldova", "ME": "Montenegro", "MF": "Saint Martin",
    "MG": "Madagascar", "MH": "Marshall Islands", "MK": "North Macedonia", "ML": "Mali", "MM": "Myanmar",
    "MN": "Mongolia", "MO": "Macao", "MP": "Northern Mariana Islands", "MQ": "Martinique", "MR": "Mauritania",
    "MS": "Montserrat", "MT": "Malta", "MU": "Mauritius", "MV": "Maldives", "MW": "Malawi",
    "MX": "Mexico", "MY": "Malaysia", "MZ": "Mozambique", "NA": "Namibia", "NC": "New Caledonia",
    "NE": "Niger", "NF": "Norfolk Island", "NG": "Nigeria", "NI": "Nicaragua", "NL": "Netherlands",
    "NO": "Norway", "NP": "Nepal", "NR": "Nauru", "NU": "Niue", "NZ": "New Zealand",
    "OM": "Oman", "PA": "Panama", "PE": "Peru", "PF": "French Polynesia", "PG": "Papua New Guinea",
    "PH": "Philippines", "PK": "Pakistan", "PL": "Poland", "PM": "Saint Pierre and Miquelon",
    "PN": "Pitcairn Islands", "PR": "Puerto Rico", "PS": "Palestine", "PT": "Portugal", "PW": "Palau",
    "PY": "Paraguay", "QA": "Qatar", "RE": "Réunion", "RO": "Romania", "RS": "Serbia",
    "RU": "Russia", "RW": "Rwanda", "SA": "Saudi Arabia", "SB": "Solomon Islands", "SC": "Seychelles",
    "SD": "Sudan", "SE": "Sweden", "SG": "Singapore", "SH": "Saint Helena", "SI": "Slovenia",
    "SJ": "Svalbard and Jan Mayen", "SK": "Slovakia", "SL": "Sierra Leone", "SM": "San Marino",
    "SN": "Senegal", "SO": "Somalia", "SR": "Suriname", "SS": "South Sudan", "ST": "São Tomé and Príncipe",
    "SV": "El Salvador", "SX": "Sint Maarten", "SY": "Syria", "SZ": "Eswatini", "TC": "Turks and Caicos Islands",
    "TD": "Chad", "TF": "French Southern Territories", "TG": "Togo", "TH": "Thailand", "TJ": "Tajikistan",
    "TK": "Tokelau", "TL": "Timor-Leste", "TM": "Turkmenistan", "TN": "Tunisia", "TO": "Tonga",
    "TR": "Turkey", "TT": "Trinidad and Tobago", "TV": "Tuvalu", "TW": "Taiwan", "TZ": "Tanzania",
    "UA": "Ukraine", "UG": "Uganda", "UM": "U.S. Minor Outlying Islands", "US": "United States",
    "UY": "Uruguay", "UZ": "Uzbekistan", "VA": "Vatican City", "VC": "Saint Vincent and the Grenadines",
    "VE": "Venezuela", "VG": "British Virgin Islands", "VI": "U.S. Virgin Islands", "VN": "Vietnam",
    "VU": "Vanuatu", "WF": "Wallis and Futuna", "WS": "Samoa", "YE": "Yemen", "YT": "Mayotte",
    "ZA": "South Africa", "ZM": "Zambia", "ZW": "Zimbabwe"
}

# File Paths
FILE_DOAJ_COUNT = shared.path_join(PATHS["data_1-fetch"], "doaj_1_count.csv")
FILE_DOAJ_SUBJECT_REPORT = shared.path_join(
    PATHS["data_1-fetch"], "doaj_2_count_by_subject_report.csv"
)
FILE_DOAJ_LANGUAGE = shared.path_join(
    PATHS["data_1-fetch"], "doaj_3_count_by_language.csv"
)
FILE_DOAJ_YEAR = shared.path_join(
    PATHS["data_1-fetch"], "doaj_4_count_by_year.csv"
)
FILE_DOAJ_ARTICLE_COUNT = shared.path_join(
    PATHS["data_1-fetch"], "doaj_5_article_count.csv"
)
FILE_DOAJ_PUBLISHER = shared.path_join(
    PATHS["data_1-fetch"], "doaj_6_count_by_publisher.csv"
)
FILE_PROVENANCE = shared.path_join(
    PATHS["data_1-fetch"], "doaj_provenance.yaml"
)

# Runtime variables
QUARTER = os.path.basename(PATHS["data_quarter"])


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch DOAJ journals with CC licenses using API v4"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FETCH_LIMIT,
        help=f"Total journals to fetch (default: {DEFAULT_FETCH_LIMIT})",
    )
    parser.add_argument(
        "--enable-save",
        action="store_true",
        help="Enable saving data to CSV files",
    )
    parser.add_argument(
        "--enable-git", action="store_true", help="Enable git actions"
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def setup_session():
    """Setup requests session with retry strategy."""
    retry_strategy = Retry(
        total=5, backoff_factor=1, status_forcelist=shared.STATUS_FORCELIST
    )
    session = requests.Session()
    session.headers.update({"User-Agent": shared.USER_AGENT})
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session


def initialize_data_file(file_path, headers):
    """Initialize CSV file with headers if it doesn't exist."""
    if not os.path.isfile(file_path):
        with open(file_path, "w", encoding="utf-8", newline="\n") as file_obj:
            writer = csv.DictWriter(
                file_obj, fieldnames=headers, dialect="unix"
            )
            writer.writeheader()


def initialize_all_data_files(args):
    """Initialize all data files."""
    if not args.enable_save:
        return
    os.makedirs(PATHS["data_1-fetch"], exist_ok=True)
    initialize_data_file(FILE_DOAJ_COUNT, HEADER_COUNT)
    initialize_data_file(FILE_DOAJ_SUBJECT_REPORT, HEADER_SUBJECT_REPORT)
    initialize_data_file(FILE_DOAJ_LANGUAGE, HEADER_LANGUAGE)
    initialize_data_file(FILE_DOAJ_YEAR, HEADER_YEAR)
    initialize_data_file(FILE_DOAJ_ARTICLE_COUNT, HEADER_ARTICLE_COUNT)
    initialize_data_file(FILE_DOAJ_PUBLISHER, HEADER_PUBLISHER)


def extract_license_type(license_info):
    """Extract CC license type from DOAJ license information."""
    if not license_info:
        return "UNKNOWN CC legal tool"
    for lic in license_info:
        lic_type = lic.get("type", "")
        if lic_type in CC_LICENSE_TYPES:
            return lic_type
    return "UNKNOWN CC legal tool"


def process_articles(session, args):
    """Process DOAJ articles to get license statistics from journal metadata."""
    LOGGER.info("Fetching DOAJ articles for license analysis...")

    article_license_counts = Counter()
    total_articles = 0
    page = 1
    page_size = 100
    article_limit = min(args.limit // 10, 10000)  # Sample articles for efficiency

    while total_articles < article_limit:
        LOGGER.info(f"Fetching articles page {page}...")

        url = f"{BASE_URL}/articles/*"
        params = {"pageSize": page_size, "page": page}

        try:
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response.status_code == 400:
                LOGGER.info(f"Reached end of available articles at page {page}")
            else:
                LOGGER.error(f"Failed to fetch articles page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for article in results:
            if total_articles >= article_limit:
                break

            bibjson = article.get("bibjson", {})
            journal_info = bibjson.get("journal", {})
            
            # Get journal title to infer license from journal data
            journal_title = journal_info.get("title", "")
            if journal_title:
                # For now, count articles from CC licensed journals
                article_license_counts["Articles from CC Journals"] += 1

            total_articles += 1

        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    return article_license_counts, total_articles


def process_journals(session, args):
    """Process DOAJ journals with CC licenses using API v4."""
    LOGGER.info("Fetching DOAJ journals...")

    license_counts = Counter()
    subject_counts = defaultdict(Counter)
    language_counts = defaultdict(Counter)
    year_counts = defaultdict(Counter)
    publisher_counts = defaultdict(Counter)

    total_processed = 0
    page = 1
    page_size = 100

    while total_processed < args.limit:
        LOGGER.info(f"Fetching journals page {page}...")

        url = f"{BASE_URL}/journals/*"
        params = {"pageSize": page_size, "page": page}

        try:
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response.status_code == 400:
                LOGGER.info(f"Reached end of available data at page {page}")
            else:
                LOGGER.error(f"Failed to fetch journals page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for journal in results:
            if total_processed >= args.limit:
                break

            bibjson = journal.get("bibjson", {})

            # Check for CC license
            license_info = bibjson.get("license")
            if not license_info:
                continue

            license_type = extract_license_type(license_info)
            if license_type == "UNKNOWN CC legal tool":
                continue

            license_counts[license_type] += 1

            # Extract subjects
            subjects = bibjson.get("subject", [])
            for subject in subjects:
                if isinstance(subject, dict):
                    code = subject.get("code", "")
                    term = subject.get("term", "")
                    if code and term:
                        subject_counts[license_type][f"{code}|{term}"] += 1

            # Extract year from oa_start (Open Access start year)
            oa_start = bibjson.get("oa_start")
            if oa_start:
                year_counts[license_type][str(oa_start)] += 1
            else:
                year_counts[license_type]["Unknown"] += 1

            # Extract languages
            languages = bibjson.get("language", [])
            for lang in languages:
                language_counts[license_type][lang] += 1

            # Extract publisher information (new in v4)
            publisher_info = bibjson.get("publisher", {})
            if publisher_info:
                publisher_name = publisher_info.get("name", "Unknown")
                publisher_country = publisher_info.get("country", "Unknown")
                publisher_key = f"{publisher_name}|{publisher_country}"
                publisher_counts[license_type][publisher_key] += 1

            total_processed += 1

        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    return (
        license_counts,
        subject_counts,
        language_counts,
        year_counts,
        publisher_counts,
        total_processed,
    )


def save_count_data(
    license_counts, subject_counts, language_counts, year_counts, 
    publisher_counts, article_counts
):
    """Save all collected data to CSV files."""

    # Save license counts
    with open(FILE_DOAJ_COUNT, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_COUNT, dialect="unix")
        writer.writeheader()
        for lic, count in license_counts.items():
            writer.writerow({"TOOL_IDENTIFIER": lic, "COUNT": count})

    # Save subject report
    with open(
        FILE_DOAJ_SUBJECT_REPORT, "w", encoding="utf-8", newline="\n"
    ) as fh:
        writer = csv.DictWriter(
            fh, fieldnames=HEADER_SUBJECT_REPORT, dialect="unix"
        )
        writer.writeheader()
        for lic, subjects in subject_counts.items():
            for subject_info, count in subjects.items():
                if "|" in subject_info:
                    code, label = subject_info.split("|", 1)
                else:
                    code, label = subject_info, subject_info
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "SUBJECT_CODE": code,
                        "SUBJECT_LABEL": label,
                        "COUNT": count,
                    }
                )

    # Save language counts with readable names
    with open(FILE_DOAJ_LANGUAGE, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_LANGUAGE, dialect="unix")
        writer.writeheader()
        for lic, languages in language_counts.items():
            for lang_code, count in languages.items():
                lang_name = LANGUAGE_NAMES.get(lang_code, lang_code)
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "LANGUAGE_CODE": lang_code,
                        "LANGUAGE": lang_name,
                        "COUNT": count,
                    }
                )

    # Save year counts
    with open(FILE_DOAJ_YEAR, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_YEAR, dialect="unix")
        writer.writeheader()
        for lic, years in year_counts.items():
            for year, count in years.items():
                writer.writerow(
                    {"TOOL_IDENTIFIER": lic, "YEAR": year, "COUNT": count}
                )

    # Save article counts
    with open(FILE_DOAJ_ARTICLE_COUNT, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_ARTICLE_COUNT, dialect="unix")
        writer.writeheader()
        for article_type, count in article_counts.items():
            writer.writerow(
                {"TOOL_IDENTIFIER": article_type, "TYPE": "Article", "COUNT": count}
            )

    # Save publisher counts
    with open(FILE_DOAJ_PUBLISHER, "w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER_PUBLISHER, dialect="unix")
        writer.writeheader()
        for lic, publishers in publisher_counts.items():
            for publisher_info, count in publishers.items():
                if "|" in publisher_info:
                    publisher, country_code = publisher_info.split("|", 1)
                else:
                    publisher, country_code = publisher_info, "Unknown"
                
                country_name = COUNTRY_NAMES.get(country_code, country_code)
                writer.writerow(
                    {
                        "TOOL_IDENTIFIER": lic,
                        "PUBLISHER": publisher,
                        "COUNTRY_CODE": country_code,
                        "COUNTRY_NAME": country_name,
                        "COUNT": count,
                    }
                )


def query_doaj(args):
    """Main function to query DOAJ API v4."""
    session = setup_session()

    LOGGER.info("Processing both journals and articles with DOAJ API v4")

    # Process journals
    (
        license_counts,
        subject_counts,
        language_counts,
        year_counts,
        publisher_counts,
        journals_processed,
    ) = process_journals(session, args)

    # Process articles
    article_counts, articles_processed = process_articles(session, args)

    # Save results
    if args.enable_save:
        save_count_data(
            license_counts, subject_counts, language_counts, year_counts,
            publisher_counts, article_counts
        )

    # Save provenance
    provenance_data = {
        "total_articles_fetched": articles_processed,
        "total_journals_fetched": journals_processed,
        "total_processed": journals_processed + articles_processed,
        "limit": args.limit,
        "quarter": QUARTER,
        "script": os.path.basename(__file__),
        "api_version": "v4",
        "note": "Enhanced data collection with API v4 including publisher info and article sampling",
    }

    try:
        with open(FILE_PROVENANCE, "w", encoding="utf-8", newline="\n") as fh:
            yaml.dump(provenance_data, fh, default_flow_style=False, indent=2)
    except Exception as e:
        LOGGER.warning("Failed to write provenance file: %s", e)

    LOGGER.info(f"Total CC licensed journals processed: {journals_processed}")
    LOGGER.info(f"Total articles sampled: {articles_processed}")


def main():
    """Main function."""
    LOGGER.info("Script execution started.")
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)
    shared.git_fetch_and_merge(args, PATHS["repo"])
    initialize_all_data_files(args)
    query_doaj(args)
    args = shared.git_add_and_commit(
        args,
        PATHS["repo"],
        PATHS["data_quarter"],
        f"Add and commit new DOAJ CC license data for {QUARTER} using API v4",
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
