# Data Sources

This project uses data from various sources that are openly licensed or in the
public domain. Below are the sources and their respective information:


## arXiv

**Description:** arXiv is a free distribution service and an open-access archive for scholarly articles in physics, mathematics, computer science, quantitative biology, quantitative finance, statistics, electrical engineering and systems science, and economics. All arXiv articles are available under various open licenses or are in the public domain.

**API documentation link:**
- [arXiv API User Manual](https://arxiv.org/help/api/user-manual)
- [arXiv API Reference](https://arxiv.org/help/api)
- [arXiv OAI-PMH Interface](https://arxiv.org/help/oa/index)
- [Base URL (Standard API)](http://export.arxiv.org/api/query)
- [Base URL (OAI-PMH)](https://oaipmh.arxiv.org/oai)
- [arXiv Subject Classifications](https://arxiv.org/category_taxonomy)
- [Terms of Use for arXiv APIs](https://info.arxiv.org/help/api/tou.html)

**API information:**
- No API key required for either interface
- Query limit: No official limit, but requests should be made responsibly
- **Standard API**: Data available through Atom XML format, supports search by various fields
- **OAI-PMH Interface** (used by `arxiv_fetch.py`): 
  - Structured metadata harvesting with resumption tokens
  - Better license metadata extraction for CC-licensed papers
  - Recommended 3-second delays between requests
  - Supports date-based filtering for bulk harvesting
- Metadata includes comprehensive licensing information for each paper


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


## Europeana

**Description:**
The **Europeana Search API** provides access to digital cultural heritage metadata records aggregated from museums, libraries, and archives across Europe. This project uses the API to fetch aggregated counts of cultural heritage records by data provider, rights statement, and theme.

**Official API Documentation:**
- [Search API Documentation](https://europeana.atlassian.net/wiki/spaces/EF/pages/2385739812/Search+API+Documentation)
  - Themes are listed in the Search API Request Parameter accordion

**API information:**
- API key required
- Minimum 0.003 seconds between queries
- Query parameters allow:
  - Full-text searching (`query`)
  - Retrieving metadata facets (`profile=facets`)
  - Filtering by data provider, rights statement, and theme
- Data available through JSON format
- Offset-based pagination


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


## Openverse

**Description:** Openverse is a search engine for openly licensed media,
including images and audio. It provides access to over 700 million works from
more than 20 sources, all of which are under Creative Commons licenses or in the
public domain. The API allows querying for media by source, license type, and
other parameters. Because anonymous Openverse API access returns a maximum of
~240 result count per source-license combination, the `openverse_fetch.py`
script currently provides approximate counts. It does not include pagination or
license_version breakdown.

**API documentation link:**
- [Openverse API Documentation](https://api.openverse.org/v1/)
- [Openverse API Reference](https://wordpress.org/openverse/api/)
- [Base URL](https://api.openverse.org/v1)
- [Openverse Frontend](https://openverse.org/)

**API information:**
- No API key required for basic access
- Query limit: Rate-limited to prevent abuse (anonymous access provides ~240 results per source-license combination)
- Data available through JSON format
- Supports filtering by source, license, media type (images, audio)
- Media types: `images`, `audio`
- Supported licenses: `by`, `by-nc`, `by-nc-nd`, `by-nc-sa`, `by-nd`, `by-sa`, `cc0`, `nc-sampling+`, `pdm`, `sampling+`


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
