# Data Sources

This project uses data from various sources that are openly licensed or in the
public domain. Below are the sources and their respective information:


## CC Legal Tools

**Description:** A `.txt` file provided by Timid Robot containing all legal
tool paths.

**API documentation link:**
- [`google_custom_search/legal-tool-paths.txt`][tools-paths]: a list of all
  current Creative Commons (CC) legal tool paths
- [`data/prioritized-tool-urls.txt`][prioritized-tool-urls]: a prioritized list
  of all current CC legal tool URLs

**API information:**
- No API key required
- No query limits

[tools-paths]:data/legal-tool-paths.txt
[prioritized-tool-urls]: data/prioritized-tool-urls.txt


## GCS (Google Custom Search) JSON API

**Description:** The Custom Search JSON API allows user-defined detailed query
and access towards related query data using a programmable search engine.

**Admin links:**
- [Programmable Search - All search engines][gcs-admin]
- [APIs & Services – APIs & Services – Google Cloud console][google-api-admin]

**API documentation links:**
- [Custom Search JSON API Reference | Programmable Search Engine | Google
  Developers][google-json]
- [Google API Python Client Library][google-api-python]
  - [Google API Client Library for Python Docs |
    google-api-python-client][google-api-python]
    - _Reference documentation for the core library
      [googleapiclient][googleapiclient]._
      - See: googleapiclient.discovery > build
    - _[Library reference documentation by API][gcs-library-ref]_
      - See Custom Search v1 [cse()][gcs-cse]
- [Method: cse.list | Custom Search JSON API | Google Developers][cse-list]
- [XML API reference appendices][reference-appendix]

**API information:**
- API key required
- Query limit: 100 queries per day
- Data available through JSON format

**Notes:**
- The data from Google Custom Search will only cover 50+ general, most
  significant categories of CC License for data collection quota constraint.
  As an additional note, the order of precedence of license the collected
  data's first column is sorted due to intermediate data analysis progress.

[gcs-admin]: https://programmablesearchengine.google.com/controlpanel/all
[google-api-admin]: https://console.cloud.google.com/apis/dashboard
[google-json]: https://developers.google.com/custom-search/v1/reference/rest
[google-api-python]: https://github.com/googleapis/google-api-python-client
[googleapiclient]: http://googleapis.github.io/google-api-python-client/docs/epy/index.html
[gcs-library-ref]: https://googleapis.github.io/google-api-python-client/docs/dyn/
[gcs-cse]: https://googleapis.github.io/google-api-python-client/docs/dyn/customsearch_v1.cse.html
[cse-list]: https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list
[reference-appendix]: https://developers.google.com/custom-search/docs/xml_results_appendices


## GitHub

**Description:** A development platform for hosting and managing code.

**API documentation link:**
- [GitHub REST API v3](https://docs.github.com/en/rest)

**API information:**
- API key not required but recommended by GitHub
- Query limit: 60 requests per hour if unauthenticated,
  5000 requests per hour if authenticated
- Data available through JSON format

## Wikipedia

**Description:** The Wikipedia API allows users to query statistics of pages,
categories, revisions from a public API endpoint. We have included two urls in
the project: The `WIKIPEDIA_BASE_URL` AND `WIKIPEDIA_MATRIX_URL`. The
`WIKIPEDIA_BASE_URL` provides access to articles, categories, and metadata from
the English version of Wikipedia. It runs on the MediaWiki Action API, but this
instance only provides English Wikipedia data. Then the `WIKIPEDIA_MATRIX_URL`
provides access to information of all wikimedia projects including the different
language edition of wikipedia. It runs on the Meta-Wiki API.

**API documentation link:**
[WIKIPEDIA_BASE_URL documentation](https://en.wikipedia.org/w/api.php)
[WIKIPEDIA_BASE_URL reference page](https://www.mediawiki.org/wiki/API:Main_page)
[WIKIPEDIA_MATRIX_URL documentation](https://meta.wikimedia.org/w/api.php)
[WIKIPEDIA_MATRIX_URL reference page](https://www.mediawiki.org/wiki/API:Sitematrix)

**API information:**
- No API key required
- Query limit: It is rate-limited only to prevent abuse
- Data available through XML or JSON format

## Internet Archive

**Description:**
The Internet Archive is a nonprofit digital library offering free access to millions of digital materials including books, movies, software, music, and websites. This project uses the Internet Archive’s Session and Search API to fetch metadata of items that reference Creative Commons licenses.

**API documentation link:**
- [InternetArchive: A Python Interface to archive.org](https://internetarchive.readthedocs.io/en/stable/internetarchive.html)
- [The Internet Archive Python Library](https://archive.org/developers/internetarchive/)
- [The Internet Archive Search API reference](https://archive.org/advancedsearch.php)
- [A Python interface to archive.org.](https://pypi.org/project/internetarchive/)
- [Internet Archive Python client; Session + Search Items](https://github.com/jjjake/internetarchive/tree/master/internetarchive)

**API information:**
- No API key required
- Pagination supported via rows and start parameters
- Python access via internetarchive library (search_items, ArchiveSession)
- Query limit: None specified, but rate-limiting may apply (1000000 max at a time)
- Data available through JSON format
- Retry logic and session management implemented for reliability

**Notes:**
- This project queries for items containing `text:creativecommons.org` in their metadata.
- The script extracts and normalizes license URLs and language codes
- In summary, it queries licenseurl and language fields for all items containing "creativecommons.org" in their metadata
- Aggregated counts are saved to CSV files for licenses and languages.
- License normalization uses a canonical mapping defined in `license_url_to_identifier_mapping.csv`.
- Language normalization using Babel and [iso-639](https://github.com/jacksonllee/iso639), see also [iso-639 standards](https://www.loc.gov/standards/iso639-2/), you can also checkout [iso639-2](https://www.loc.gov/standards/iso639-2/php/English_list.php)
