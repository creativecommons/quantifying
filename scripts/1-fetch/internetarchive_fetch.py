#!/usr/bin/env python
"""
Fetch open content data from Internet Archive using the Python interface.
"""

# Standard library
import argparse
import csv
import os
import re
import sys
import textwrap
import traceback
import unicodedata
from collections import Counter
from urllib.parse import urlparse

# Third-party
import babel
import iso639
from internetarchive import ArchiveSession
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
FILE1_COUNT = os.path.join(PATHS["data_phase"], "internetarchive_1_count.csv")
FILE2_LANGUAGE = os.path.join(
    PATHS["data_phase"], "internetarchive_2_count_by_language.csv"
)
HEADER1 = ["LICENSE", "COUNT"]
HEADER2 = ["LICENSE", "LANGUAGE", "COUNT"]
LANGUAGE_ALIAS_MAP = {
    "american english": "English",
    "american": "English",
    "anglais": "English",
    "castellano": "Spanish",
    "chinese sub": "Chinese",
    "egligh": "English",
    "eglish": "English",
    "en_us es_es": "Multiple languages",
    "english & chinese subbed": "Multiple languages",
    "english (us)": "English",
    "english - american": "English",
    "english_handwritten": "English",
    "engrish": "English",
    "enlgish": "English",
    "france": "French",
    "hwbrew": "Hebrew",
    "ilokano": "Ilokano",
    "indian english": "English",
    "mandarin": "Chinese",
    "multi": "Multiple Languages",
    "multilanguage": "Multiple languages",
    "multiple": "Multiple Languages",
    "polska": "Polish",
    "português e espanhol": "Multiple languages",
    "sgn": "Sign languages",
    "spain": "Spanish",
    "uk english": "English",
    "us english": "English",
    "us-en": "English",
    "viẹetnamese": "Vietnamese",
    "український": "Ukrainian",
}
LANGUAGE_NAME_MAP = {}  # Populated by create_language_name_map()
LANGUAGE_NOISE_WORDS = [
    "-handwritten",
    "-spoken",
    "=",
    "english patch",
    "hand write",
    "hand written",
    "hand-written",
    "handwritten",
    "instrumental",
    "language",
    "no speech",
    "no spoken word",
    "no voice",
    "simple",
    "spoken",
    "sub-titles",
    "subbed",
    "subtitle",
    "subtitles?",
    "universal",
    "with subtitles?",
]
LIMIT_DEFAULT = 100000
QUARTER = os.path.basename(PATHS["data_quarter"])


def parse_arguments():
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=LIMIT_DEFAULT,
        help=f"Limit total results (default: {LIMIT_DEFAULT})",
    )
    parser.add_argument(
        "--enable-save", action="store_true", help="Enable saving results"
    )
    parser.add_argument(
        "--enable-git", action="store_true", help="Enable git actions"
    )
    args = parser.parse_args()
    if not args.enable_save and args.enable_git:
        parser.error("--enable-git requires --enable-save")
    return args


def load_license_mapping():
    """Loads and normalizes the license mapping from CSV."""
    license_mapping = {}
    file_path = shared.path_join(
        PATHS["data"], "license_url_to_identifier_mapping.csv"
    )
    with open(file_path, "r", encoding="utf-8") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            raw_url = row["LICENSE_URL"]
            label = row["LICENSE"].strip()
            normalized_url = normalize_license(raw_url, license_mapping=None)
            if normalized_url:
                license_mapping[normalized_url] = label
    return license_mapping


def create_language_name_map():
    global LANGUAGE_NAME_MAP
    for locale_code in babel.localedata.locale_identifiers():
        locale = babel.Locale.parse(locale_code)
        # Localized/native language name
        name = normalize_key(locale.display_name)
        if not name:
            continue
        LANGUAGE_NAME_MAP[name.lower()] = locale.language
        # English language name
        name = normalize_key(locale.english_name)
        if not name:
            continue
        LANGUAGE_NAME_MAP[name.lower()] = locale.language
    LANGUAGE_NAME_MAP = dict(sorted(LANGUAGE_NAME_MAP.items()))


def normalize_license(licenseurl, license_mapping=None):
    """Normalize licenseurl and map to standard license label."""
    if not isinstance(licenseurl, str) or not licenseurl.strip():
        return None

    # Parse and clean
    parsed = urlparse(
        licenseurl.strip()
        .lower()
        .replace("http://", "https://")
        .replace("www.", "")
    )
    path = parsed.path.rstrip("/")
    path = re.sub(r"/(deed|legalcode)(\.[a-zA-Z_-]+)?$|[/'\"\W]+$", "", path)

    # Reconstruct normalized URL
    normalized_url = f"https://{parsed.netloc}{path}"

    # Lookup mapped label
    label = (
        license_mapping.get(normalized_url, "UNKNOWN")
        if license_mapping
        else normalized_url
    )

    return label


def normalize_key(key_string):
    """
    Normalize string for dictionary keys:
        NFKD, remove diacritics, punctuation, collapse spaces, lowercase
    """
    if not key_string:
        return ""
    key_string = str(key_string)
    key_string = unicodedata.normalize("NFKD", key_string)
    key_string = "".join(
        ch for ch in key_string if not unicodedata.combining(ch)
    )
    key_string = re.sub(
        r"[^\w\s\+\-/]", " ", key_string, flags=re.UNICODE
    )  # keep + / - for splits
    if re.fullmatch(r"[a-zA-Z]{2,3}[-_][a-zA-Z]{2,3}", key_string.strip()):
        key_string = key_string.replace("_", "-")
    return key_string.strip().lower()


def strip_noise(language):
    """
    Strip common noise like "subtitles", "subtitle", "(English)",
    "english patch", "handwritten", etc.
    """

    # Helper to find words with flexible boundaries
    def word_regex(word):
        return r"(\b|(?<=[\-_]))" + re.escape(word) + r"\b"

    # Combine all noise words into one regex
    combined_regex = r"|".join(word_regex(w) for w in LANGUAGE_NOISE_WORDS)

    language = re.sub(combined_regex, " ", language, flags=re.I)

    # Original regex for symbols
    language = re.sub(r"[()\"\']", " ", language)
    return language


def is_multi_language(raw_language):
    """Detects multi-language strings."""
    return bool(
        re.search(
            r",|;|\band\b|\bor\b|\bwith\b|\/|&\s+",
            raw_language,
            flags=re.IGNORECASE,
        )
    )


def normalize_language(raw_language):
    raw = str(raw_language).strip()
    if not raw:
        return "Undetermined"

    # 1st: check multi-language
    if is_multi_language(raw):
        return "Multiple languages"

    # Prep for subsequent checks by striping noise and normalizing
    cleaned = normalize_key(strip_noise(raw))
    if cleaned in LANGUAGE_NAME_MAP:
        cleaned = LANGUAGE_NAME_MAP[cleaned]

    # 2nd: check language alias map
    alias_map = {normalize_key(k): v for k, v in LANGUAGE_ALIAS_MAP.items()}
    if cleaned in alias_map:
        return alias_map[cleaned]

    for language in [raw, cleaned]:
        if not language:
            continue

        # 3rd: check ISO639
        try:
            name = iso639.Language.match(language).name
            return name
        except iso639.language.LanguageNotFoundError:
            pass

        # 4th: check Babel
        try:
            language_locale = language.replace("-", "_")
            locale = babel.Locale.parse(language_locale, sep="_")
            name = locale.get_language_name("en")
            return name
        except babel.core.UnknownLocaleError:
            pass
        except ValueError:
            pass

    return "Undetermined"


def query_internet_archive(args, session, license_mapping):
    license_counter = Counter()
    language_counter = Counter()
    unmapped_licenseurl_counter = Counter()
    unmapped_language_counter = Counter()

    LOGGER.info("Beginning fetch.")
    # Use search_items for simpler pagination management
    response = session.search_items(
        query="licenseurl:*creativecommons.org*",
        fields=["licenseurl", "language"],
        params={"count": 10000},
        request_kwargs={"timeout": 30},
    )
    found = response.num_found
    LOGGER.info(
        f"Found {found:,} results. Processing a maximum of" f" {args.limit:,}."
    )

    total_processed = 0
    for result in response:
        if result.get("error"):
            raise shared.QuantifyingException(result.get("error"), 1)
        # Extract and normalize license URL
        licenseurl = result.get("licenseurl", "")
        if isinstance(licenseurl, list):
            licenseurl = licenseurl[0] if licenseurl else "UNKNOWN"
        if not licenseurl:
            licenseurl = "UNKNOWN"
        normalized_url = normalize_license(licenseurl, license_mapping)
        if normalized_url == "UNKNOWN":
            unmapped_licenseurl_counter[licenseurl] += 1
        else:
            license_counter[(normalized_url)] += 1

            # Extract and normalize language
            raw_language = result.get("language", "Undetermined")
            if isinstance(raw_language, list):
                raw_language = (
                    raw_language[0] if raw_language else "Undetermined"
                )

            normalized_lang = normalize_language(raw_language)
            if normalized_lang == "Undetermined":
                unmapped_language_counter[raw_language] += 1

            language_counter[(normalized_url, normalized_lang)] += 1
        total_processed += 1
        if not total_processed % 10000:
            LOGGER.info(
                f"Processed {total_processed:,} items so far:"
                f" {len(license_counter):,} unique legal tools,"
                f" {len(language_counter):,} unique languages."
            )
        if total_processed >= args.limit:
            LOGGER.warning("Aborting fetch. Limit reached.")
            break

    LOGGER.info(
        f"Finished fetching {total_processed:,} items:"
        f" {len(license_counter):,} unique legal tools,"
        f" {len(language_counter):,} unique languages."
    )

    if unmapped_licenseurl_counter:
        LOGGER.warning(
            "Number of unmapped legal tools: "
            f"{sum(unmapped_licenseurl_counter.values()):,}"
        )
        for license, count in unmapped_licenseurl_counter.items():
            LOGGER.warning(f"  Unmapped legal tools: {license}: {count:,}")

    if unmapped_language_counter:
        LOGGER.warning(
            "Number of unmapped languages: "
            f"{sum(unmapped_language_counter.values()):,}"
        )
        for lang, count in unmapped_language_counter.items():
            cleaned = normalize_key(strip_noise(lang))
            LOGGER.warning(
                f"  Unmapped language: {lang} (cleaned: {cleaned}): {count:,}"
            )

    return license_counter, language_counter


def write_csv(file_path, header, rows):
    with open(file_path, "w", encoding="utf-8", newline="\n") as file_obj:
        writer = csv.writer(file_obj, dialect="unix")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
    LOGGER.info(f"Wrote {len(rows)} rows to {file_path}")


def write_all(args, license_counter, language_counter):
    if not args.enable_save:
        return args

    os.makedirs(PATHS["data_phase"], exist_ok=True)

    # Sort license data by license name
    sorted_license_rows = sorted(
        [(license, count) for license, count in license_counter.items()],
        key=lambda x: x[0],
    )

    # Sort language data by license then language
    sorted_language_rows = sorted(
        [
            (license, language, count)
            for (license, language), count in language_counter.items()
        ],
        key=lambda x: (x[0], x[1]),
    )

    write_csv(
        FILE1_COUNT,
        HEADER1,
        sorted_license_rows,
    )
    write_csv(
        FILE2_LANGUAGE,
        HEADER2,
        sorted_language_rows,
    )

    return args


def main():
    args = parse_arguments()
    shared.paths_log(LOGGER, PATHS)

    session = shared.get_session(
        accept_header="application/json", session=ArchiveSession()
    )

    license_mapping = load_license_mapping()
    create_language_name_map()

    license_data, language_data = query_internet_archive(
        args, session, license_mapping
    )

    if args.enable_save:
        write_all(args, license_data, language_data)

    if args.enable_git:
        args = shared.git_add_and_commit(
            args,
            PATHS["repo"],
            PATHS["data_quarter"],
            f"Add Internet Archive data for {QUARTER}",
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
