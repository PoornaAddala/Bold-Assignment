"""
Unit Tests for Healthcare Eligibility Pipeline
===============================================
Comprehensive test suite covering all pipeline components.
"""

import csv
import tempfile
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline import (
    DataTransformer,
    RecordValidator,
    PartnerConfig,
    RecordProcessor,
    EligibilityPipeline,
    ProcessingStats,
)


class TestDataTransformer(unittest.TestCase):
    """Tests for DataTransformer class."""

    def test_to_title_case_basic(self):
        """Test basic title case conversion."""
        self.assertEqual(DataTransformer.to_title_case("john"), "John")
        self.assertEqual(DataTransformer.to_title_case("JANE"), "Jane")
        self.assertEqual(DataTransformer.to_title_case("alice johnson"), "Alice Johnson")

    def test_to_title_case_edge_cases(self):
        """Test title case with edge cases."""
        self.assertEqual(DataTransformer.to_title_case(""), "")
        self.assertEqual(DataTransformer.to_title_case(None), "")
        self.assertEqual(DataTransformer.to_title_case("  john  "), "John")

    def test_to_lowercase(self):
        """Test lowercase conversion."""
        self.assertEqual(DataTransformer.to_lowercase("JOHN.DOE@EMAIL.COM"), "john.doe@email.com")
        self.assertEqual(DataTransformer.to_lowercase(""), "")
        self.assertEqual(DataTransformer.to_lowercase(None), "")

    def test_format_date_mm_dd_yyyy(self):
        """Test date formatting from MM/DD/YYYY."""
        result = DataTransformer.format_date("03/15/1955", "%m/%d/%Y")
        self.assertEqual(result, "1955-03-15")

    def test_format_date_yyyy_mm_dd(self):
        """Test date formatting from YYYY-MM-DD (already ISO)."""
        result = DataTransformer.format_date("1965-08-10", "%Y-%m-%d")
        self.assertEqual(result, "1965-08-10")

    def test_format_date_invalid(self):
        """Test date formatting with invalid input."""
        result = DataTransformer.format_date("invalid", "%m/%d/%Y")
        self.assertEqual(result, "")
        
        result = DataTransformer.format_date("", "%m/%d/%Y")
        self.assertEqual(result, "")
        
        result = DataTransformer.format_date(None, "%m/%d/%Y")
        self.assertEqual(result, "")

    def test_format_phone_10_digits(self):
        """Test phone formatting with 10 digits."""
        self.assertEqual(DataTransformer.format_phone("5551234567"), "555-123-4567")
        self.assertEqual(DataTransformer.format_phone("5554445555"), "555-444-5555")

    def test_format_phone_with_separators(self):
        """Test phone formatting with various separators."""
        self.assertEqual(DataTransformer.format_phone("555-222-3333"), "555-222-3333")
        self.assertEqual(DataTransformer.format_phone("555.222.3333"), "555-222-3333")
        self.assertEqual(DataTransformer.format_phone("(555) 222-3333"), "555-222-3333")

    def test_format_phone_with_country_code(self):
        """Test phone formatting with US country code."""
        self.assertEqual(DataTransformer.format_phone("15551234567"), "555-123-4567")

    def test_format_phone_edge_cases(self):
        """Test phone formatting edge cases."""
        self.assertEqual(DataTransformer.format_phone(""), "")
        self.assertEqual(DataTransformer.format_phone(None), "")


class TestRecordValidator(unittest.TestCase):
    """Tests for RecordValidator class."""

    def test_valid_record(self):
        """Test validation of a valid record."""
        record = {
            "external_id": "ABC123",
            "first_name": "John",
            "last_name": "Doe",
            "dob": "1955-03-15",
            "email": "john@example.com",
            "phone": "555-123-4567",
        }
        result = RecordValidator.validate(record, 1)
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.errors), 0)

    def test_missing_external_id(self):
        """Test validation fails when external_id is missing."""
        record = {
            "external_id": "",
            "first_name": "John",
        }
        result = RecordValidator.validate(record, 1)
        self.assertFalse(result.is_valid)
        self.assertIn("Missing required field: external_id", result.errors)

    def test_invalid_email_warning(self):
        """Test validation warns on invalid email."""
        record = {
            "external_id": "ABC123",
            "email": "not-an-email",
        }
        result = RecordValidator.validate(record, 1)
        self.assertTrue(result.is_valid)  # Still valid (email not required)
        self.assertTrue(any("email" in w.lower() for w in result.warnings))


class TestPartnerConfig(unittest.TestCase):
    """Tests for PartnerConfig class."""

    def test_from_dict(self):
        """Test creating config from dictionary."""
        data = {
            "partner_code": "TEST",
            "description": "Test Partner",
            "file_pattern": "test*.csv",
            "delimiter": ",",
            "encoding": "utf-8",
            "has_header": True,
            "column_mapping": {"id": "external_id"},
            "date_format": "%Y-%m-%d",
        }
        config = PartnerConfig.from_dict(data)
        self.assertEqual(config.partner_code, "TEST")
        self.assertEqual(config.delimiter, ",")


class TestRecordProcessor(unittest.TestCase):
    """Tests for RecordProcessor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.acme_config = PartnerConfig(
            partner_code="ACME",
            description="Acme Health",
            file_pattern="acme*.txt",
            delimiter="|",
            encoding="utf-8",
            has_header=True,
            column_mapping={
                "MBI": "external_id",
                "FNAME": "first_name",
                "LNAME": "last_name",
                "DOB": "dob",
                "EMAIL": "email",
                "PHONE": "phone",
            },
            date_format="%m/%d/%Y",
        )

    def test_process_acme_record(self):
        """Test processing an Acme Health record."""
        processor = RecordProcessor(self.acme_config)
        
        raw = {
            "MBI": "1234567890A",
            "FNAME": "john",
            "LNAME": "DOE",
            "DOB": "03/15/1955",
            "EMAIL": "JOHN.DOE@EMAIL.COM",
            "PHONE": "5551234567",
        }
        
        result = processor.process(raw)
        
        self.assertEqual(result["external_id"], "1234567890A")
        self.assertEqual(result["first_name"], "John")
        self.assertEqual(result["last_name"], "Doe")
        self.assertEqual(result["dob"], "1955-03-15")
        self.assertEqual(result["email"], "john.doe@email.com")
        self.assertEqual(result["phone"], "555-123-4567")
        self.assertEqual(result["partner_code"], "ACME")


class TestProcessingStats(unittest.TestCase):
    """Tests for ProcessingStats class."""

    def test_success_tracking(self):
        """Test tracking successful rows."""
        stats = ProcessingStats()
        stats.add_success()
        stats.add_success()
        
        self.assertEqual(stats.total_rows, 2)
        self.assertEqual(stats.successful_rows, 2)
        self.assertEqual(stats.failed_rows, 0)
        self.assertEqual(stats.success_rate, 100.0)

    def test_failure_tracking(self):
        """Test tracking failed rows."""
        stats = ProcessingStats()
        stats.add_success()
        stats.add_failure(2, ["Missing external_id"], {"raw": "data"})
        
        self.assertEqual(stats.total_rows, 2)
        self.assertEqual(stats.successful_rows, 1)
        self.assertEqual(stats.failed_rows, 1)
        self.assertEqual(stats.success_rate, 50.0)


class TestIntegration(unittest.TestCase):
    """Integration tests for the full pipeline."""

    def setUp(self):
        """Set up temporary test files."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = Path(self.temp_dir) / "config.yaml"
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_path = Path(self.temp_dir) / "output.csv"
        
        self.input_dir.mkdir()

        # Create test configuration
        config_content = """
partners:
  test_partner:
    partner_code: "TEST"
    description: "Test Partner"
    file_pattern: "test*.csv"
    delimiter: ","
    encoding: "utf-8"
    has_header: true
    column_mapping:
      id: "external_id"
      fname: "first_name"
      lname: "last_name"
      birth_date: "dob"
      email_addr: "email"
      phone_num: "phone"
    date_format: "%Y-%m-%d"
"""
        self.config_path.write_text(config_content)

        # Create test input file
        input_file = self.input_dir / "test_data.csv"
        input_file.write_text(
            "id,fname,lname,birth_date,email_addr,phone_num\n"
            "ID001,john,DOE,1990-01-15,JOHN@TEST.COM,5551234567\n"
            "ID002,jane,SMITH,1985-06-20,jane@test.com,555-987-6543\n"
        )

    def test_full_pipeline(self):
        """Test running the full pipeline."""
        pipeline = EligibilityPipeline(self.config_path)
        stats = pipeline.run(self.input_dir, self.output_path)

        # Verify output file exists
        self.assertTrue(self.output_path.exists())

        # Verify output content
        with open(self.output_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.assertEqual(len(rows), 2)
        
        # Check first record transformations
        self.assertEqual(rows[0]["external_id"], "ID001")
        self.assertEqual(rows[0]["first_name"], "John")
        self.assertEqual(rows[0]["last_name"], "Doe")
        self.assertEqual(rows[0]["dob"], "1990-01-15")
        self.assertEqual(rows[0]["email"], "john@test.com")
        self.assertEqual(rows[0]["phone"], "555-123-4567")
        self.assertEqual(rows[0]["partner_code"], "TEST")

    def test_pipeline_with_invalid_rows(self):
        """Test pipeline handling of invalid rows."""
        # Create file with invalid row (missing external_id)
        input_file = self.input_dir / "test_invalid.csv"
        input_file.write_text(
            "id,fname,lname,birth_date,email_addr,phone_num\n"
            "ID001,john,doe,1990-01-15,john@test.com,5551234567\n"
            ",jane,smith,1985-06-20,jane@test.com,5559876543\n"  # Missing ID
        )

        # Update config to match new file pattern
        config_content = """
partners:
  test_partner:
    partner_code: "TEST"
    file_pattern: "test_invalid*.csv"
    delimiter: ","
    has_header: true
    column_mapping:
      id: "external_id"
      fname: "first_name"
      lname: "last_name"
      birth_date: "dob"
      email_addr: "email"
      phone_num: "phone"
    date_format: "%Y-%m-%d"
"""
        self.config_path.write_text(config_content)

        pipeline = EligibilityPipeline(self.config_path)
        stats = pipeline.run(self.input_dir, self.output_path, skip_invalid=True)

        # Should only have 1 valid row
        with open(self.output_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.assertEqual(len(rows), 1)
        self.assertEqual(stats["test_partner"].failed_rows, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
