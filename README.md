# Healthcare Eligibility Pipeline

A production-ready, configuration-driven data pipeline for ingesting member eligibility files from multiple healthcare partners into a unified, standardized format.

## Features

- **Configuration-Driven Design**: Add new partners without code changes
- **Robust Data Transformation**: Handles various date formats, phone formats, and text normalization
- **Comprehensive Validation**: Validates required fields and flags malformed data
- **Detailed Logging**: Full audit trail of processing steps and errors
- **Extensible Architecture**: Clean separation of concerns for easy maintenance

## Project Structure

```
healthcare_pipeline/
├── config/
│   └── partners.yaml       # Partner configuration file
├── data/
│   ├── input/              # Input files from partners
│   │   ├── acme.txt
│   │   └── bettercare.csv
│   └── output/             # Unified output files
├── src/
│   └── pipeline.py         # Main pipeline code
├── tests/
│   └── test_pipeline.py    # Unit tests
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.9+
- PyYAML library

### Installation

```bash
# Install dependencies
pip install pyyaml

# Run the pipeline
python src/pipeline.py \
    --config config/partners.yaml \
    --input data/input \
    --output data/output/unified_eligibility.csv
```

### Expected Output

```
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | ============================================================
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | Starting Healthcare Eligibility Pipeline
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | ============================================================
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | Processing partner: acme_health (ACME)
2025-01-12 10:00:00 | INFO     | eligibility_pipeline |   Completed: 2/2 rows (100.0% success rate)
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | Processing partner: better_care (BCARE)
2025-01-12 10:00:00 | INFO     | eligibility_pipeline |   Completed: 2/2 rows (100.0% success rate)
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | Writing 4 records to: data/output/unified_eligibility.csv
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | ============================================================
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | Pipeline Complete - Summary
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | ============================================================
2025-01-12 10:00:00 | INFO     | eligibility_pipeline | Total records written: 4
```

## Output Schema

| Field | Type | Transformation | Required |
|-------|------|----------------|----------|
| `external_id` | string | Direct mapping | Yes |
| `first_name` | string | Title Case | No |
| `last_name` | string | Title Case | No |
| `dob` | date | ISO-8601 (YYYY-MM-DD) | No |
| `email` | string | Lowercase | No |
| `phone` | string | Format: XXX-XXX-XXXX | No |
| `partner_code` | string | From config | Yes |

## Adding a New Partner

Adding a new partner requires **only a configuration update**—no code changes needed.

### Step 1: Add Partner Configuration

Edit `config/partners.yaml` and add a new partner entry:

```yaml
partners:
  # ... existing partners ...

  new_partner:
    # Partner identification
    partner_code: "NEWP"
    description: "New Partner Healthcare"
    
    # File settings
    file_pattern: "newpartner*.csv"    # Glob pattern to match files
    delimiter: ","                      # Field delimiter
    encoding: "utf-8"                   # File encoding
    has_header: true                    # Whether file has header row
    
    # Column mapping: source_column -> standard_field
    column_mapping:
      member_id: "external_id"          # Map partner's ID column
      given_name: "first_name"          # Map to first_name
      family_name: "last_name"          # Map to last_name
      birthdate: "dob"                  # Map to dob
      contact_email: "email"            # Map to email
      contact_phone: "phone"            # Map to phone
    
    # Date parsing format (strptime format)
    date_format: "%d-%m-%Y"             # e.g., "25-12-1990"
```

### Step 2: Place Input File

Drop the partner's file in the `data/input/` directory. The filename must match the `file_pattern` specified in the configuration.

### Step 3: Run Pipeline

```bash
python src/pipeline.py -c config/partners.yaml -i data/input -o data/output/unified.csv
```

## Configuration Reference

### Partner Configuration Options

| Option | Required | Description |
|--------|----------|-------------|
| `partner_code` | Yes | Unique identifier added to output records |
| `description` | No | Human-readable partner description |
| `file_pattern` | Yes | Glob pattern to match input files |
| `delimiter` | Yes | Field delimiter character |
| `encoding` | No | File encoding (default: utf-8) |
| `has_header` | No | Whether file has header row (default: true) |
| `column_mapping` | Yes | Map of source columns to standard fields |
| `date_format` | Yes | strptime format string for date parsing |

### Common Date Formats

| Format | Pattern | Example |
|--------|---------|---------|
| US Format | `%m/%d/%Y` | 03/15/1955 |
| ISO Format | `%Y-%m-%d` | 1955-03-15 |
| European | `%d/%m/%Y` | 15/03/1955 |
| Compact | `%Y%m%d` | 19550315 |

## CLI Options

```
usage: pipeline.py [-h] -c CONFIG -i INPUT -o OUTPUT [--include-invalid] [-v]

Healthcare Eligibility Pipeline

options:
  -h, --help            Show this help message and exit
  -c, --config CONFIG   Path to the YAML configuration file
  -i, --input INPUT     Directory containing input files
  -o, --output OUTPUT   Path for the unified output CSV file
  --include-invalid     Include invalid rows in output (default: skip them)
  -v, --verbose         Enable verbose (DEBUG) logging
```

## Validation Rules

The pipeline validates each record and handles errors gracefully:

### Required Fields
- `external_id`: Must be present and non-empty. Records without this field are flagged and optionally skipped.

### Optional Validations (Warnings)
- **Date**: Must parse according to configured format
- **Email**: Basic format validation (contains @, valid domain)
- **Phone**: Checks for expected digit count

### Error Handling

- **Malformed rows**: Logged and skipped (configurable)
- **Invalid dates**: Logged as warning, field left empty
- **Missing columns**: Mapped to empty string
- **Encoding errors**: Uses configured encoding with fallback

## Running Tests

```bash
cd healthcare_pipeline
python -m pytest tests/ -v
```

Or using unittest directly:

```bash
python -m unittest tests.test_pipeline -v
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    EligibilityPipeline                          │
│                    (Orchestrator)                               │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
    ┌─────────────────┐ ┌───────────────┐ ┌─────────────────┐
    │ PartnerConfig   │ │ FileReader    │ │ RecordProcessor │
    │ (Data Model)    │ │ (I/O)         │ │ (Transform)     │
    └─────────────────┘ └───────────────┘ └─────────────────┘
                              │               │
                              ▼               ▼
                        ┌───────────────┐ ┌─────────────────┐
                        │ DataTransform │ │ RecordValidator │
                        │ (Utilities)   │ │ (Validation)    │
                        └───────────────┘ └─────────────────┘
```

## Design Decisions

1. **YAML Configuration**: Chosen for readability and support for comments, making it easy for non-developers to understand and modify.

2. **Dataclasses**: Used for type safety and self-documenting code structure.

3. **Generator-based Reading**: Memory efficient for large files—processes row by row.

4. **Separation of Concerns**: Each class has a single responsibility, making testing and maintenance straightforward.

5. **Comprehensive Logging**: All processing steps are logged for debugging and audit purposes.

## Scaling Considerations

For production workloads with larger files:

- **PySpark Integration**: The architecture supports easy migration to PySpark DataFrames
- **Parallel Processing**: Multiple files can be processed concurrently
- **Streaming**: Generator-based design supports streaming large files
- **Cloud Storage**: File readers can be extended for S3/GCS/ADLS


