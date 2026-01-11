# Getty Images as a Data Source

## Overview

The Getty Images API provides access to information about a very large
collection of visual content, including images, illustrations, and videos
from Getty Images and iStock.

Instead of providing the actual media files, the API focuses on returning
metadata. This metadata describes the content, its creator, and how it is
licensed or restricted for use. Because of this, the API can be useful for
studying patterns in licensed creative works without storing or distributing
the media itself.

---

## Proposed Queries

Using the Getty Images API, several types of queries could be explored to
understand creative content at a high level.

For example, it is possible to:

- Search for visual content using keywords or subject terms to understand
  common themes or topics.
- Filter content by asset type, such as images, illustrations, or videos.
- Query assets based on licensing or usage restrictions to compare different
  license models.
- Retrieve metadata for specific assets using their unique identifiers.

These types of queries help describe how creative works are organised,
categorised, and licensed across a large content library.

---

## Data Returned

When a query is made, the Getty Images API returns structured metadata about
each asset. This metadata typically includes:

- A unique identifier for the asset
- A title or short description of the content
- Information about the creator or contributor
- Details about the license model or usage restrictions
- Keywords or categories associated with the content
- The type of asset, such as image or video

This information provides context about the content and how it can be used,
without requiring access to the actual media files.

---

## Relevance to Quantifying the Commons

The metadata provided by the Getty Images API could support the goals of
Quantifying the Commons by enabling analysis of licensed creative works at
scale.

For example, this data could help examine trends such as the distribution of
license types, the kinds of creative content being produced, or how content
is categorised over time. By focusing on metadata rather than media files,
the project can analyse creative ecosystems while respecting usage and
licensing constraints.
