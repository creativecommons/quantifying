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
from time import sleep
from urllib.parse import urlparse

# Third-party
from babel import Locale
from internetarchive import ArchiveSession
from iso639 import Lang
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonTracebackLexer
from urllib3.util.retry import Retry

# Add parent directory so shared can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# First-party/Local
import shared  # noqa: E402

# Setup
LOGGER, PATHS = shared.setup(__file__)

# CSV paths
FILE1_COUNT = os.path.join(PATHS["data_phase"], "internetarchive_1_count.csv")
FILE2_LANGUAGE = os.path.join(
    PATHS["data_phase"], "internetarchive_2_count_by_language.csv"
)

# CSV headers
HEADER1 = ["LICENSE", "COUNT"]
HEADER2 = ["LICENSE", "LANGUAGE", "COUNT"]

QUARTER = os.path.basename(PATHS["data_quarter"])

ISO639_CACHE = {}


def parse_arguments():
    LOGGER.info("Parsing command-line options")
    parser = argparse.ArgumentParser(description=__doc__)
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


def get_archive_session():
    retry_strategy = Retry(
        total=5,
        backoff_factor=10,
        status_forcelist=shared.STATUS_FORCELIST,
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter_kwargs = {
        "max_retries": retry_strategy,
    }
    session = ArchiveSession(http_adapter_kwargs=adapter_kwargs)
    session.headers.update(
        {"User-Agent": shared.USER_AGENT, "Accept": "application/json"}
    )
    return session


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


def normalize_key(s):
    """Normalize string for dictionary keys:
    NFKD, remove diacritics, punctuation, collapse spaces, lowercase."""
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(
        r"[^\w\s\+\-/]", " ", s, flags=re.UNICODE
    )  # keep + / - for splits
    if re.fullmatch(r"[a-zA-Z]{2,3}[-_][a-zA-Z]{2,3}", s.strip()):
        s = s.replace("_", "-")
    return s.strip().lower()


def iso639_lookup(term):
    """Return a Language object or None;
    cache results.
    Accepts raw user input."""
    if not term:
        return None
    key = term.strip().lower().replace("_", "-")
    if key in ISO639_CACHE:
        return ISO639_CACHE[key]

    # Try direct code match
    try:
        result = Lang(key)
        ISO639_CACHE[key] = result
        return result
    except Exception:
        pass

    # fallback to title-case name lookup
    try:
        result = Lang(term.strip().title())
        if result:
            ISO639_CACHE[key] = result
            return result
    except Exception:
        pass

    return None


# strip common noise like "subtitles", "subtitle",
# "(English)", "english patch", "handwritten", etc
def strip_noise(s):
    # Helper to find words with flexible boundaries
    def word_regex(word):
        return r"(\b|(?<=[\-_]))" + re.escape(word) + r"\b"

    noise_words = [
        "subtitles?",
        "subtitle",
        "sub-titles",
        "subbed",
        "with subtitles?",
        "english patch",
        "handwritten",
        "hand write",
        "hand-written",
        "hand written",
        "-handwritten",
        "no voice",
        "no spoken word",
        "no speech",
        "instrumental",
        "universal",
        "language",
        "=",
        "simple",
        "spoken",
        "-spoken",
    ]

    # Combine all noise words into one regex
    combined_regex = r"|".join(word_regex(w) for w in noise_words)

    s = re.sub(combined_regex, " ", s, flags=re.I)

    # Original regex for symbols
    s = re.sub(r"[()\"\']", " ", s)
    return s


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
    if not raw_language:
        return "Undetermined"

    raw = str(raw_language).strip()

    # check multi-language
    if is_multi_language(raw):
        return "Multiple languages"

    # strip noise and normalize
    cleaned = normalize_key(strip_noise(raw))

    # --- Try ISO639 first ---
    lang_obj = iso639_lookup(raw) or iso639_lookup(cleaned)
    if lang_obj and getattr(lang_obj, "name", None):
        return lang_obj.name

    # Try Babel
    for cand in [raw, cleaned]:
        if not cand:
            continue
        try:
            cand_locale = cand.replace("-", "_")
            locale = Locale.parse(cand_locale, sep="_")
            return locale.get_language_name("en")
        except Exception:
            pass

    # --- Try Alias Map ---
    ALIAS_MAP = {
        "engrish": "English",
        "english_handwritten": "English",
        "enlgish": "English",
        "american english": "English",
        "english - american": "English",
        "american": "English",
        "uk english": "English",
        "eglish": "English",
        "egligh": "English",
        "english (us)": "English",
        "us-en": "English",
        "sgn": "Sign languages",
        "anglais": "English",
        "us english": "English",
        "indian english": "English",
        "hwbrew": "Hebrew",
        "polska": "Polish",
        "bosanski": "Bosnian",
        "український": "Ukrainian",
        "chinese sub": "Chinese",
        "spain": "Spanish",
        "português e espanhol": "Multiple languages",
        "русский": "Russian",
        "deutsch": "German",
        "france": "French",
        "francais": "French",
        "italiano": "Italian",
        "ilokano": "Ilokano",
        "viẹetnamese": "Vietnamese",
        "português": "Portuguese",
        "pt_br": "Portuguese",
        "espanol": "Spanish",
        "castellano": "Spanish",
        "greek": "Greek",
        "mandarin": "Chinese",
        "nederlands": "Dutch",
        "swahili": "Swahili",
        "no language (english)": "Undetermined",
        "whatever we play it to be": "Undetermined",
        "en_us es_es": "Multiple languages",
        "english & chinese subbed": "Multiple languages",
        "n/a": "Undetermined",
        "none": "Undetermined",
        "unknown": "Undetermined",
        "no speech": "Undetermined",
        "no spoken language": "Undetermined",
        "multi": "Multiple Languages",
        "multilanguage": "Multiple languages",
        "multiple": "Multiple Languages",
        "music": "Undetermined",
    }
    ALIAS_MAP = {normalize_key(k): v for k, v in ALIAS_MAP.items()}

    if cleaned in ALIAS_MAP:
        return ALIAS_MAP[cleaned]

    return "Undetermined"


def query_internet_archive(args):
    license_counter = Counter()
    language_counter = Counter()
    unmapped_licenseurl_counter = Counter()
    unmapped_language_counter = Counter()

    fields = ["licenseurl", "language"]
    query = "creativecommons.org"
    license_mapping = load_license_mapping()

    rows = 1000000
    total_rows = 0
    total_processed = 0
    max_retries = 3

    session = get_archive_session()
    while True:
        # Loop until no more results are returned by the API
        LOGGER.info(f"Fetching {rows} items starting at {total_rows}...")
        results = None

        for attempt in range(max_retries):
            try:
                # Use search_items for simpler pagination management
                search = session.search_items(
                    query,
                    fields=fields,
                    params={"rows": rows, "start": total_rows},
                    request_kwargs={"timeout": 120},
                )

                # Convert to list to iterate over
                results = list(search)
                total_rows += len(results)
                break

            except Exception as e:
                wait_time = 2**attempt
                LOGGER.warning(
                    f"API request failed (Attempt {attempt+1}/{max_retries}). "
                    f"Waiting {wait_time}s.Error: {e}"
                    f"\n{traceback.format_exc()}"
                )
                sleep(wait_time)
        else:
            raise shared.QuantifyingException(
                f"Failed to fetch data after {max_retries} attempts.", 1
            )

        if not results:
            LOGGER.info("No more results. Ending pagination.")
            break

        for result in results:
            # Extract and normalize license URL
            licenseurl = result.get("licenseurl", "")
            if isinstance(licenseurl, list):
                licenseurl = licenseurl[0] if licenseurl else "UNKNOWN"
            if not licenseurl:
                licenseurl = "UNKNOWN"

            normalized_url = normalize_license(licenseurl, license_mapping)
            if normalized_url == "UNKNOWN":
                unmapped_licenseurl_counter[licenseurl] += 1
                continue  # Skip

            # Extract and normalize language
            raw_language = result.get("language", "Undetermined")
            if isinstance(raw_language, list):
                raw_language = (
                    raw_language[0] if raw_language else "Undetermined"
                )

            normalized_lang = normalize_language(raw_language)
            if normalized_lang == "Undetermined":
                unmapped_language_counter[raw_language] += 1

            license_counter[(normalized_url)] += 1
            language_counter[(normalized_url, normalized_lang)] += 1
            total_processed += 1

        LOGGER.info(
            f"Processed {len(results)} new items, total: {total_processed}"
        )
        LOGGER.info(f"Total items processed so far: {total_processed}")
        LOGGER.info(
            f"Unique licenses: {len(license_counter)}|"
            f"Languages:{len(language_counter)}"
        )

        # If the results is less than the requested rows, implies the end
        if len(results) < rows:
            LOGGER.info(
                "Fewer results returned than requested. Pagination complete."
            )
            break

    LOGGER.info(
        "Finished processing.\n"
        "Number of unmapped licenses: "
        f"{sum(unmapped_licenseurl_counter.values())}"
    )

    # Log unmapped languages once at the end
    if unmapped_licenseurl_counter:
        for license, count in unmapped_licenseurl_counter.items():
            LOGGER.warning(f"Unmapped llicense: {license} : {count}")

    LOGGER.info(
        "\n Number of unmapped languages: "
        f"{sum(unmapped_language_counter.values())}"
    )
    if unmapped_language_counter:
        for lang, count in unmapped_language_counter.items():
            cleaned = normalize_key(strip_noise(lang))
            LOGGER.warning(
                f"Unmapped language: {lang} (cleaned: {cleaned}): {count}"
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

    license_data, language_data = query_internet_archive(args)

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
