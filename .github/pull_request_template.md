# Add DOAJ (Directory of Open Access Journals) Data Source Integration

## Description

This PR adds comprehensive DOAJ API v4 integration to the quantifying commons project, enabling collection and analysis of Creative Commons licensed academic journals. The implementation includes two main components:

1. **`scripts/1-fetch/doaj_fetch.py`** - Main data collection script for DOAJ journals
2. **`dev/generate_country_codes.py`** - Utility for programmatic ISO country code generation

### Key Features
- ✅ DOAJ API v4 integration with enhanced metadata collection
- ✅ Creative Commons license analysis (BY, NC, SA, ND combinations)
- ✅ Publisher and geographic distribution analysis
- ✅ Temporal filtering to prevent CC license false positives (default: ≥2002)
- ✅ Automatic country code generation and mapping
- ✅ Comprehensive error handling and data validation
- ✅ Self-contained execution with auto-dependency resolution

## Technical Details

### API Integration
- **Endpoint**: `https://doaj.org/api/v4/search/journals/*`
- **Rate Limiting**: 0.5 seconds between requests
- **Pagination**: 100 journals per page with automatic pagination
- **Error Handling**: Comprehensive exception handling without swallowing errors

### Data Quality Measures
- **Date Filtering**: Default `--date-back=2002` to avoid retroactive CC license false positives
- **License Validation**: Only processes journals with valid CC license declarations
- **Country Mapping**: ISO 3166-1 alpha-2 codes automatically mapped to readable names

### Output Files Generated
```
data/2025Q4/1-fetch/
├── doaj_1_count.csv                    # License type counts
├── doaj_2_count_by_subject_report.csv  # Subject classification analysis
├── doaj_3_count_by_language.csv        # Language distribution
├── doaj_4_count_by_year.csv           # Temporal analysis by oa_start year
├── doaj_5_count_by_publisher.csv      # Publisher and country analysis
├── doaj_provenance.yaml               # Execution metadata and audit trail
└── iso_country_codes.yaml             # Auto-generated country mapping
```

## Query Strategy

### License Extraction
```python
def extract_license_type(license_info):
    """Extract CC license type from DOAJ license information."""
    if not license_info:
        return "UNKNOWN CC legal tool"
    for lic in license_info:
        lic_type = lic.get("type", "")
        if lic_type in CC_LICENSE_TYPES:
            return lic_type
    return "UNKNOWN CC legal tool"
```

### Date Filtering Implementation
```python
# Apply date-back filter if specified
if args.date_back and oa_start and oa_start < args.date_back:
    continue
```

### Publisher Analysis
```python
# Extract publisher information (new in v4)
publisher_info = bibjson.get("publisher", {})
if publisher_info:
    publisher_name = publisher_info.get("name", "Unknown")
    publisher_country = publisher_info.get("country", "Unknown")
    publisher_key = f"{publisher_name}|{publisher_country}"
    publisher_counts[license_type][publisher_key] += 1
```

### Auto-Dependency Resolution
```python
# Generate country codes file if it doesn't exist
if not os.path.isfile(country_file):
    LOGGER.info("Country codes file not found, generating it...")
    generate_script = shared.path_join(PATHS["repo"], "dev", "generate_country_codes.py")
    subprocess.run([sys.executable, generate_script], check=True)
```

## Test

### Basic Execution
```bash
# Run with default settings (date-back=2002, limit=1000)
pipenv run ./scripts/1-fetch/doaj_fetch.py --enable-save

# Run with custom parameters
pipenv run ./scripts/1-fetch/doaj_fetch.py --limit 50 --date-back 2020 --enable-save

# Test country code generation
pipenv run ./dev/generate_country_codes.py
```

### Validation Commands
```bash
# Check output files
ls -la data/2025Q4/1-fetch/doaj_*

# Verify country mapping
head -10 data/iso_country_codes.yaml

# Check provenance
cat data/2025Q4/1-fetch/doaj_provenance.yaml

# Static analysis
pipenv run pre-commit run --files scripts/1-fetch/doaj_fetch.py
```

### Expected Output
```
INFO - Processing DOAJ journals with DOAJ API v4
INFO - Fetching journals page 1...
INFO - Total CC licensed journals processed: 50
INFO - Articles: 0 (DOAJ API doesn't provide license info for articles)
```

## Data Quality Note

**Important**: DOAJ data represents journal-level licensing policies, not individual article licenses. This data should be interpreted as indicators of institutional commitment to CC licensing rather than precise counts of CC-licensed articles. The `--date-back=2002` default prevents false positives from journals that retroactively adopted CC licenses.

## Checklist

- [x] Scripts are executable and properly documented
- [x] Static analysis passes (Black, Flake8, isort)
- [x] Comprehensive error handling implemented
- [x] Auto-dependency resolution working
- [x] Country code mapping functional
- [x] Date filtering prevents false positives
- [x] Output files generated correctly
- [x] Provenance tracking implemented
- [x] GitHub issue template created
- [x] Documentation updated
