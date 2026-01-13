"""
Healthcare Eligibility Pipeline
================================
A configuration-driven data ingestion pipeline for processing member eligibility
files from multiple healthcare partners into a unified, standardized format.
"""

from __future__ import annotations

import csv
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("eligibility_pipeline")


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class ValidationResult:
    """Result of a row validation check."""
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class ProcessingStats:
    """Statistics for pipeline processing."""
    total_rows: int = 0
    successful_rows: int = 0
    failed_rows: int = 0
    validation_errors: list[dict[str, Any]] = field(default_factory=list)

    def add_success(self) -> None:
        self.total_rows += 1
        self.successful_rows += 1

    def add_failure(self, row_num: int, errors: list[str], raw_data: dict) -> None:
        self.total_rows += 1
        self.failed_rows += 1
        self.validation_errors.append({
            "row_number": row_num,
            "errors": errors,
            "raw_data": raw_data,
        })

    @property
    def success_rate(self) -> float:
        return (self.successful_rows / self.total_rows * 100) if self.total_rows > 0 else 0.0


@dataclass
class PartnerConfig:
    """Configuration for a single partner."""
    partner_code: str
    description: str
    file_pattern: str
    delimiter: str
    encoding: str
    has_header: bool
    column_mapping: dict[str, str]
    date_format: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PartnerConfig:
        """Create a PartnerConfig from a dictionary."""
        return cls(
            partner_code=data["partner_code"],
            description=data.get("description", ""),
            file_pattern=data["file_pattern"],
            delimiter=data["delimiter"],
            encoding=data.get("encoding", "utf-8"),
            has_header=data.get("has_header", True),
            column_mapping=data["column_mapping"],
            date_format=data["date_format"],
        )


@dataclass
class StandardizedRecord:
    """A standardized eligibility record."""
    external_id: str
    first_name: str
    last_name: str
    dob: str
    email: str
    phone: str
    partner_code: str

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary."""
        return {
            "external_id": self.external_id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "dob": self.dob,
            "email": self.email,
            "phone": self.phone,
            "partner_code": self.partner_code,
        }


# =============================================================================
# Transformers
# =============================================================================

class DataTransformer:
    """Handles all data transformation logic."""

    @staticmethod
    def to_title_case(value: str | None) -> str:
        """Convert string to title case, handling edge cases."""
        if not value or not isinstance(value, str):
            return ""
        return value.strip().title()

    @staticmethod
    def to_lowercase(value: str | None) -> str:
        """Convert string to lowercase."""
        if not value or not isinstance(value, str):
            return ""
        return value.strip().lower()

    @staticmethod
    def format_date(value: str | None, input_format: str) -> str:
        """
        Parse date from input format and return ISO-8601 format (YYYY-MM-DD).
        
        Args:
            value: The date string to parse
            input_format: strptime format string for parsing
            
        Returns:
            ISO-8601 formatted date string, or empty string if parsing fails
        """
        if not value or not isinstance(value, str):
            return ""
        
        value = value.strip()
        if not value:
            return ""
        
        try:
            parsed_date = datetime.strptime(value, input_format)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError as e:
            logger.warning(f"Failed to parse date '{value}' with format '{input_format}': {e}")
            return ""

    @staticmethod
    def format_phone(value: str | None) -> str:
        """
        Format phone number as XXX-XXX-XXXX.
        
        Handles various input formats:
        - 5551234567
        - 555-123-4567
        - 555.123.4567
        - (555) 123-4567
        """
        if not value or not isinstance(value, str):
            return ""
        
        # Remove all non-numeric characters
        digits = re.sub(r"\D", "", value.strip())
        
        # Handle 10-digit phone numbers
        if len(digits) == 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        
        # Handle 11-digit numbers (with country code)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        
        # Return original if format is unexpected
        logger.warning(f"Unexpected phone format: '{value}' ({len(digits)} digits)")
        return value.strip()


# =============================================================================
# Validators
# =============================================================================

class RecordValidator:
    """Validates eligibility records."""

    @staticmethod
    def validate(record: dict[str, str], row_num: int) -> ValidationResult:
        """
        Validate a record and return validation result.
        
        Args:
            record: The record to validate
            row_num: Row number for error reporting
            
        Returns:
            ValidationResult with status and any errors/warnings
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Required field: external_id
        external_id = record.get("external_id", "").strip()
        if not external_id:
            errors.append("Missing required field: external_id")

        # Validate date format (if present)
        dob = record.get("dob", "")
        if dob and not RecordValidator._is_valid_iso_date(dob):
            warnings.append(f"Invalid date format for dob: '{dob}'")

        # Validate email format (if present)
        email = record.get("email", "")
        if email and not RecordValidator._is_valid_email(email):
            warnings.append(f"Invalid email format: '{email}'")

        # Validate phone format (if present)
        phone = record.get("phone", "")
        if phone and not RecordValidator._is_valid_phone(phone):
            warnings.append(f"Phone may have unexpected format: '{phone}'")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    @staticmethod
    def _is_valid_iso_date(value: str) -> bool:
        """Check if value is a valid ISO-8601 date."""
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    @staticmethod
    def _is_valid_email(value: str) -> bool:
        """Basic email format validation."""
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return bool(re.match(pattern, value))

    @staticmethod
    def _is_valid_phone(value: str) -> bool:
        """Check if phone matches XXX-XXX-XXXX format."""
        pattern = r"^\d{3}-\d{3}-\d{4}$"
        return bool(re.match(pattern, value))


# =============================================================================
# File Reader
# =============================================================================

class PartnerFileReader:
    """Reads partner files based on configuration."""

    def __init__(self, config: PartnerConfig):
        self.config = config

    def read(self, file_path: Path) -> Iterator[dict[str, str]]:
        """
        Read records from a partner file.
        
        Args:
            file_path: Path to the input file
            
        Yields:
            Dictionary for each row with original column names as keys
        """
        logger.info(f"Reading file: {file_path}")
        
        with open(file_path, "r", encoding=self.config.encoding) as f:
            reader = csv.DictReader(f, delimiter=self.config.delimiter)
            
            for row in reader:
                yield row


# =============================================================================
# Record Processor
# =============================================================================

class RecordProcessor:
    """Processes raw records into standardized format."""

    def __init__(self, config: PartnerConfig):
        self.config = config
        self.transformer = DataTransformer()

    def process(self, raw_record: dict[str, str]) -> dict[str, str]:
        """
        Transform a raw record into standardized format.
        
        Args:
            raw_record: Raw record with partner-specific column names
            
        Returns:
            Standardized record dictionary
        """
        # Map columns using configuration
        mapped = self._map_columns(raw_record)
        
        # Apply transformations
        return {
            "external_id": mapped.get("external_id", "").strip(),
            "first_name": self.transformer.to_title_case(mapped.get("first_name")),
            "last_name": self.transformer.to_title_case(mapped.get("last_name")),
            "dob": self.transformer.format_date(mapped.get("dob"), self.config.date_format),
            "email": self.transformer.to_lowercase(mapped.get("email")),
            "phone": self.transformer.format_phone(mapped.get("phone")),
            "partner_code": self.config.partner_code,
        }

    def _map_columns(self, raw_record: dict[str, str]) -> dict[str, str]:
        """Map partner columns to standard field names."""
        mapped: dict[str, str] = {}
        
        for source_col, target_field in self.config.column_mapping.items():
            if source_col in raw_record:
                mapped[target_field] = raw_record[source_col]
        
        return mapped


# =============================================================================
# Pipeline Orchestrator
# =============================================================================

class EligibilityPipeline:
    """
    Main pipeline orchestrator for processing eligibility files.
    
    This class coordinates the reading, transformation, validation,
    and output of eligibility data from multiple partners.
    """

    # Standard output field order
    OUTPUT_FIELDS = [
        "external_id",
        "first_name",
        "last_name",
        "dob",
        "email",
        "phone",
        "partner_code",
    ]

    def __init__(self, config_path: Path):
        """
        Initialize the pipeline with a configuration file.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = config_path
        self.partner_configs: dict[str, PartnerConfig] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load and parse the configuration file."""
        logger.info(f"Loading configuration from: {self.config_path}")
        
        with open(self.config_path, "r") as f:
            config_data = yaml.safe_load(f)
        
        for partner_id, partner_data in config_data.get("partners", {}).items():
            self.partner_configs[partner_id] = PartnerConfig.from_dict(partner_data)
            logger.info(f"  Loaded partner config: {partner_id} ({partner_data['partner_code']})")

    def process_partner(
        self,
        partner_id: str,
        input_path: Path,
        skip_invalid: bool = True,
    ) -> tuple[list[dict[str, str]], ProcessingStats]:
        """
        Process a single partner file.
        
        Args:
            partner_id: The partner identifier from configuration
            input_path: Path to the input file
            skip_invalid: If True, skip invalid rows; if False, include them
            
        Returns:
            Tuple of (processed records, processing statistics)
        """
        if partner_id not in self.partner_configs:
            raise ValueError(f"Unknown partner: {partner_id}")

        config = self.partner_configs[partner_id]
        reader = PartnerFileReader(config)
        processor = RecordProcessor(config)
        stats = ProcessingStats()
        records: list[dict[str, str]] = []

        logger.info(f"Processing partner: {partner_id} ({config.partner_code})")

        for row_num, raw_record in enumerate(reader.read(input_path), start=2):  # Start at 2 (after header)
            try:
                # Transform the record
                processed = processor.process(raw_record)
                
                # Validate
                validation = RecordValidator.validate(processed, row_num)
                
                if validation.is_valid:
                    records.append(processed)
                    stats.add_success()
                    
                    # Log warnings if any
                    for warning in validation.warnings:
                        logger.warning(f"Row {row_num}: {warning}")
                else:
                    stats.add_failure(row_num, validation.errors, raw_record)
                    
                    for error in validation.errors:
                        logger.error(f"Row {row_num}: {error}")
                    
                    if not skip_invalid:
                        records.append(processed)

            except Exception as e:
                logger.error(f"Row {row_num}: Unexpected error - {e}")
                stats.add_failure(row_num, [str(e)], raw_record)

        logger.info(
            f"  Completed: {stats.successful_rows}/{stats.total_rows} rows "
            f"({stats.success_rate:.1f}% success rate)"
        )

        return records, stats

    def run(
        self,
        input_dir: Path,
        output_path: Path,
        skip_invalid: bool = True,
    ) -> dict[str, ProcessingStats]:
        """
        Run the full pipeline for all configured partners.
        
        Args:
            input_dir: Directory containing input files
            output_path: Path for the unified output file
            skip_invalid: If True, skip invalid rows; if False, include them
            
        Returns:
            Dictionary mapping partner_id to ProcessingStats
        """
        logger.info("=" * 60)
        logger.info("Starting Healthcare Eligibility Pipeline")
        logger.info("=" * 60)

        all_records: list[dict[str, str]] = []
        all_stats: dict[str, ProcessingStats] = {}

        for partner_id, config in self.partner_configs.items():
            # Find matching files
            pattern = config.file_pattern
            matching_files = list(input_dir.glob(pattern))

            if not matching_files:
                logger.warning(f"No files found for partner {partner_id} (pattern: {pattern})")
                continue

            for file_path in matching_files:
                records, stats = self.process_partner(partner_id, file_path, skip_invalid)
                all_records.extend(records)
                all_stats[partner_id] = stats

        # Write unified output
        self._write_output(all_records, output_path)

        # Summary
        logger.info("=" * 60)
        logger.info("Pipeline Complete - Summary")
        logger.info("=" * 60)
        
        total_records = len(all_records)
        logger.info(f"Total records written: {total_records}")
        
        for partner_id, stats in all_stats.items():
            logger.info(
                f"  {partner_id}: {stats.successful_rows} successful, "
                f"{stats.failed_rows} failed"
            )

        return all_stats

    def _write_output(self, records: list[dict[str, str]], output_path: Path) -> None:
        """Write records to a CSV file."""
        logger.info(f"Writing {len(records)} records to: {output_path}")
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.OUTPUT_FIELDS)
            writer.writeheader()
            writer.writerows(records)


# =============================================================================
# CLI Entry Point
# =============================================================================

def main() -> int:
    """Main entry point for the pipeline."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Healthcare Eligibility Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --config config/partners.yaml --input data/input --output data/output/unified.csv
  python pipeline.py -c config/partners.yaml -i ./data -o ./output.csv --include-invalid
        """,
    )
    
    parser.add_argument(
        "-c", "--config",
        type=Path,
        required=True,
        help="Path to the YAML configuration file",
    )
    
    parser.add_argument(
        "-i", "--input",
        type=Path,
        required=True,
        help="Directory containing input files",
    )
    
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Path for the unified output CSV file",
    )
    
    parser.add_argument(
        "--include-invalid",
        action="store_true",
        help="Include invalid rows in output (default: skip them)",
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        pipeline = EligibilityPipeline(args.config)
        pipeline.run(
            input_dir=args.input,
            output_path=args.output,
            skip_invalid=not args.include_invalid,
        )
        return 0

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 1
    except yaml.YAMLError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
