---
name: DOAJ Issue Template
about: Report issues related to DOAJ data collection and API integration
title: '[DOAJ] '
labels: ['doaj', 'data-collection']
assignees: ''
---

## Problem
<!-- Brief description of the issue -->

## Description
<!-- Detailed explanation of what happened, what was expected, and what actually occurred -->

## Query Strategy
<!-- Describe the search parameters and approach used -->
- **Limit**: 
- **Date Back Filter**: 
- **License Types**: 
- **Search Scope**: 

## API Information

### Rate Limits
- **Current Delay**: 0.5 seconds between requests
- **Observed Rate**: 
- **API Response Time**: 

### Relevant API Links
- **Base URL**: https://doaj.org/api/v4/search
- **Journals Endpoint**: https://doaj.org/api/v4/search/journals/*
- **Articles Endpoint**: https://doaj.org/api/v4/search/articles/*
- **API Documentation**: https://doaj.org/api/docs

### Useful Search Fields
- **License Information**: `bibjson.license[].type`
- **Publisher Details**: `bibjson.publisher.name`, `bibjson.publisher.country`
- **Open Access Start**: `bibjson.oa_start`
- **Subject Classification**: `bibjson.subject[].code`, `bibjson.subject[].term`
- **Language**: `bibjson.language[]`
- **Last Updated**: `last_updated`
- **Created Date**: `created_date`

### API Limitations
- **Articles**: No direct license information available
- **Pagination**: Maximum pageSize appears to be 100
- **Historical Data**: License information may reflect current status, not original licensing
- **Date Filtering**: Only available through oa_start field, not license adoption date

## Additional Context
<!-- Any other relevant information, logs, screenshots, or context -->

### Environment
- **Script Version**: 
- **API Version**: v4
- **Date Back Default**: 2002 (to avoid CC license false positives)

### Logs
```
<!-- Paste relevant log output here -->
```

### Data Files Affected
- [ ] doaj_1_count.csv
- [ ] doaj_2_count_by_subject_report.csv
- [ ] doaj_3_count_by_language.csv
- [ ] doaj_4_count_by_year.csv
- [ ] doaj_5_count_by_publisher.csv
- [ ] doaj_provenance.yaml

### Reproducibility
<!-- Steps to reproduce the issue -->
1. 
2. 
3. 

### Potential Impact
<!-- How this affects commons quantification analysis -->
