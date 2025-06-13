from unittest.mock import Mock, patch

import pandas as pd
import pytest

from app.anthropic.column_matcher import ColumnMapping, ColumnMatchingResponse
from app.enrichment.enrichment_calculation_models import (
    ColumnMappingCalculation,
    EnrichmentReportCalculation,
)
from app.enrichment.enrichment_calculator import EnrichmentStatisticsCalculator


class TestEnrichmentStatisticsCalculator:
    """Comprehensive test suite for enrichment business logic."""

    def setup_method(self):
        """Set up test fixtures."""
        self.calculator = EnrichmentStatisticsCalculator()

    def test_calculate_statistics_basic_scenario(self):
        """Test basic statistics calculation with simple data."""
        # Create test data
        df = pd.DataFrame(
            {
                "name_crm": ["John", "Jane", "Bob"],
                "email_crm": ["john@example.com", "jane@example.com", None],
                "name_export": ["John Smith", "Jane Doe", "Bob Wilson"],
                "email_export": [
                    "john@example.com",
                    "jane@example.com",
                    "bob@example.com",
                ],
            }
        )

        # Create mock column matching result
        mappings = [
            ColumnMapping(
                crm_column="name_crm",
                export_column="name_export",
                confidence=0.9,
                reasoning="Name similarity",
                is_many_to_one=False,
            ),
            ColumnMapping(
                crm_column="email_crm",
                export_column="email_export",
                confidence=1.0,
                reasoning="Email match",
                is_many_to_one=False,
            ),
        ]

        column_matching_result = ColumnMatchingResponse(
            mappings=mappings, unmapped_crm_columns=[], unmapped_export_columns=[]
        )

        crm_columns = ["name_crm", "email_crm"]
        export_columns = ["name_export", "email_export"]

        # Calculate statistics
        result = self.calculator.calculate_statistics(
            df, column_matching_result, crm_columns, export_columns
        )

        # Verify basic report structure
        assert result.total_rows == 3
        assert result.total_crm_columns == 2
        assert result.total_export_columns == 2
        assert result.export_columns_created == 2
        assert result.new_columns_count == 0  # No unmapped export columns
        assert result.many_to_one_count == 0  # No many-to-one mappings
        assert result.columns_reduced_by_merging == 0
        assert len(result.column_mappings) == 2

    def test_global_statistics_with_many_to_one(self):
        """Test global statistics calculation with many-to-one mappings."""
        # Create mappings with many-to-one relationships
        mappings = [
            ColumnMapping(
                crm_column="linkedin1",
                export_column="linkedin_export",
                confidence=0.8,
                reasoning="LinkedIn consolidation",
                is_many_to_one=True,
                additional_crm_columns=["linkedin2", "linkedin3"],
            ),
            ColumnMapping(
                crm_column="email_crm",
                export_column="email_export",
                confidence=1.0,
                reasoning="Direct match",
                is_many_to_one=False,
            ),
        ]

        column_matching_result = ColumnMatchingResponse(
            mappings=mappings,
            unmapped_crm_columns=["unused_crm"],
            unmapped_export_columns=["new_export1", "new_export2"],
        )

        report = EnrichmentReportCalculation(
            total_rows=100,
            total_crm_columns=5,
            total_export_columns=4,
            new_columns_count=0,
            many_to_one_count=0,
            columns_reduced_by_merging=0,
            records_modified_count=0,
            export_columns_created=4,
        )

        # Test the global statistics calculation
        self.calculator._calculate_global_statistics(
            report,
            column_matching_result,
            ["linkedin1", "linkedin2", "linkedin3", "email_crm", "unused_crm"],
            ["linkedin_export", "email_export", "new_export1", "new_export2"],
        )

        # Verify calculations
        assert report.new_columns_count == 2  # 2 unmapped export columns
        assert report.many_to_one_count == 1  # 1 many-to-one mapping
        # Columns reduced: (1 main + 2 additional) - 1 resulting = 2
        assert report.columns_reduced_by_merging == 2

    def test_column_comparison_stats_all_scenarios(self):
        """Test column comparison statistics with all data scenarios."""
        # Create comprehensive test data
        df = pd.DataFrame(
            {
                "crm_col": ["same", "different_crm", None, "removed", ""],
                "export_col": ["same", "different_export", "added", None, "filled"],
            }
        )

        column_mapping = ColumnMappingCalculation(
            enrichment_report_id="test",
            crm_column="crm_col",
            export_column="export_col",
            confidence=1.0,
            reasoning="Test mapping",
        )

        modified_records = set()

        # Calculate comparison stats
        stats = self.calculator._calculate_column_comparison_stats(
            df, column_mapping, modified_records
        )

        # Verify row-by-row comparisons
        assert stats.good_data == 1  # 'same' in both
        assert stats.fixed_data == 1  # 'different_crm' vs 'different_export'
        assert stats.added_new_data == 2  # None -> 'added' and '' -> 'filled'
        assert stats.discarded_invalid_data == 1  # 'removed' -> None

        # Verify that all categories add up to total rows
        total_categorized = (
            stats.good_data
            + stats.fixed_data
            + stats.added_new_data
            + stats.discarded_invalid_data
        )
        assert total_categorized == len(df)  # Should equal 5 rows

        # Verify modified records tracking
        assert len(modified_records) == 4  # Rows 1, 2, 3, 4 were modified
        assert 0 not in modified_records  # Row 0 ('same') unchanged
        assert 1 in modified_records  # Row 1 (fixed_data)
        assert 2 in modified_records  # Row 2 (added_new_data)
        assert 3 in modified_records  # Row 3 (discarded_invalid_data)
        assert 4 in modified_records  # Row 4 (added_new_data)

        # Verify correct values calculations
        assert stats.correct_values_before == 1  # Only 'same' was unchanged
        assert stats.correct_values_after == 4  # All non-null export values

    def test_column_comparison_stats_missing_columns(self):
        """Test column comparison when columns are missing."""
        df = pd.DataFrame({"other_col": [1, 2, 3]})

        column_mapping = ColumnMappingCalculation(
            enrichment_report_id="test",
            crm_column="missing_crm",
            export_column="missing_export",
            confidence=1.0,
            reasoning="Test mapping",
        )

        modified_records = set()
        stats = self.calculator._calculate_column_comparison_stats(
            df, column_mapping, modified_records
        )

        # Should return empty stats when columns don't exist
        assert stats.good_data == 0
        assert stats.fixed_data == 0
        assert stats.added_new_data == 0
        assert stats.discarded_invalid_data == 0
        assert len(modified_records) == 0

    def test_column_comparison_stats_null_export_column(self):
        """Test column comparison when export column is None."""
        df = pd.DataFrame({"crm_col": [1, 2, 3]})

        column_mapping = ColumnMappingCalculation(
            enrichment_report_id="test",
            crm_column="crm_col",
            export_column=None,  # No export mapping
            confidence=0.5,
            reasoning="No match found",
        )

        modified_records = set()
        stats = self.calculator._calculate_column_comparison_stats(
            df, column_mapping, modified_records
        )

        # Should return empty stats when no export column
        assert stats.good_data == 0
        assert stats.fixed_data == 0
        assert stats.added_new_data == 0
        assert stats.discarded_invalid_data == 0
        assert len(modified_records) == 0

    def test_percentage_calculations(self):
        """Test correct percentage calculations."""
        df = pd.DataFrame(
            {
                "crm_col": ["a", "b", "c", "d", "e"],  # 5 rows
                "export_col": ["a", "x", "c", None, "y"],  # 4 non-null values
            }
        )

        column_mapping = ColumnMappingCalculation(
            enrichment_report_id="test",
            crm_column="crm_col",
            export_column="export_col",
            confidence=1.0,
            reasoning="Test mapping",
        )

        modified_records = set()
        stats = self.calculator._calculate_column_comparison_stats(
            df, column_mapping, modified_records
        )

        # 2 unchanged records ('a' and 'c') out of 5 total
        assert stats.correct_percentage_before == 40.0
        # 4 non-null export values out of 5 total
        assert stats.correct_percentage_after == 80.0

    def test_empty_string_handling(self):
        """Test handling of empty strings vs None values."""
        df = pd.DataFrame(
            {
                "crm_col": ["", "  ", None, "value"],
                "export_col": [None, "", "filled", "value"],
            }
        )

        column_mapping = ColumnMappingCalculation(
            enrichment_report_id="test",
            crm_column="crm_col",
            export_column="export_col",
            confidence=1.0,
            reasoning="Test mapping",
        )

        modified_records = set()
        stats = self.calculator._calculate_column_comparison_stats(
            df, column_mapping, modified_records
        )

        # Both empty/whitespace and None should be treated as "no value"
        assert (
            stats.good_data == 3
        )  # Rows 0,1,3: empty->None, whitespace->empty, value->value
        assert stats.added_new_data == 1  # Row 2: None -> 'filled'
        assert stats.fixed_data == 0
        assert stats.discarded_invalid_data == 0

        # Verify all categories add up to total rows
        total_categorized = (
            stats.good_data
            + stats.fixed_data
            + stats.added_new_data
            + stats.discarded_invalid_data
        )
        assert total_categorized == len(df)  # Should equal 4 rows

    @patch("app.enrichment.enrichment_calculator.analyze_inconsistency")
    def test_calculate_column_formats_integration(self, mock_analyze):
        """Test format calculation integration with analyze_inconsistency."""
        # Mock the analyze_inconsistency function
        mock_analyze.return_value = {"test_col": Mock(type="email", format_count=2)}

        column_data = pd.Series(["test@example.com", "user@test.com"], name="test_col")

        data_type, format_count = self.calculator._calculate_column_formats(
            column_data, is_export_column=False
        )

        assert data_type == "email"
        assert format_count == 2
        mock_analyze.assert_called_once()

    def test_calculate_column_formats_empty_data(self):
        """Test format calculation with empty data."""
        empty_series = pd.Series([], dtype=object, name="empty_col")

        data_type, format_count = self.calculator._calculate_column_formats(
            empty_series, is_export_column=False
        )

        assert data_type is None
        assert format_count == 1

    def test_calculate_column_formats_null_data(self):
        """Test format calculation with all null data."""
        null_series = pd.Series([None, None, None], name="null_col")

        data_type, format_count = self.calculator._calculate_column_formats(
            null_series, is_export_column=False
        )

        assert data_type is None
        assert format_count == 1

    @patch("app.enrichment.enrichment_calculator.phonenumbers")
    def test_phone_column_detection_with_phonenumbers(self, mock_phonenumbers):
        """Test phone column detection when phonenumbers library is available."""
        # Mock phonenumbers functionality
        mock_parse = Mock()
        mock_phonenumbers.parse.return_value = mock_parse
        mock_phonenumbers.is_valid_number.return_value = True

        phone_data = pd.Series(
            ["+1-555-123-4567", "+1-555-987-6543", "+1-555-111-2222"]
        )

        result = self.calculator._is_phone_column_with_valid_numbers(phone_data)

        assert result is True
        # Should have called parse and is_valid_number for each phone
        assert mock_phonenumbers.parse.call_count == 3
        assert mock_phonenumbers.is_valid_number.call_count == 3

    def test_phone_column_detection_without_phonenumbers(self):
        """Test phone column detection when phonenumbers library is not available."""
        with patch("app.enrichment.enrichment_calculator.phonenumbers", None):
            phone_data = pd.Series(["+1-555-123-4567", "+1-555-987-6543"])

            result = self.calculator._is_phone_column_with_valid_numbers(phone_data)

            assert result is False

    def test_phone_column_detection_mixed_data(self):
        """Test phone column detection with mixed valid/invalid data."""
        with patch(
            "app.enrichment.enrichment_calculator.phonenumbers"
        ) as mock_phonenumbers:
            # Mock to return True for valid-looking numbers, False for others
            def mock_parse_side_effect(number, region):
                if "invalid" in number:
                    raise Exception("Invalid number")
                return Mock()

            def mock_is_valid_side_effect(parsed):
                return True  # Assume parsed numbers are valid

            mock_phonenumbers.parse.side_effect = mock_parse_side_effect
            mock_phonenumbers.is_valid_number.side_effect = mock_is_valid_side_effect

            # Mix of valid and invalid phone numbers
            mixed_data = pd.Series(
                [
                    "+1-555-123-4567",  # Valid
                    "+1-555-987-6543",  # Valid
                    "invalid-phone",  # Invalid
                    "+1-555-111-2222",  # Valid
                    "not-a-phone",  # Invalid
                ]
            )

            result = self.calculator._is_phone_column_with_valid_numbers(mixed_data)

            # Should return False because only 60% are valid (< 80% threshold)
            assert result is False

    def test_phone_column_detection_non_phone_data(self):
        """Test phone column detection with clearly non-phone data."""
        non_phone_data = pd.Series(["apple", "banana", "cherry", "date"])

        result = self.calculator._is_phone_column_with_valid_numbers(non_phone_data)

        assert result is False

    def test_is_valid_phone_number_edge_cases(self):
        """Test individual phone number validation with edge cases."""
        with patch(
            "app.enrichment.enrichment_calculator.phonenumbers"
        ) as mock_phonenumbers:
            # Test with valid number
            mock_phonenumbers.parse.return_value = Mock()
            mock_phonenumbers.is_valid_number.return_value = True

            result = self.calculator._is_valid_phone_number("+1-555-123-4567")
            assert result is True

            # Test with invalid number (exception during parsing)
            mock_phonenumbers.parse.side_effect = Exception("Parse error")

            result = self.calculator._is_valid_phone_number("invalid")
            assert result is False

            # Test when phonenumbers is None
            with patch("app.enrichment.enrichment_calculator.phonenumbers", None):
                result = self.calculator._is_valid_phone_number("+1-555-123-4567")
                assert result is False

    def test_records_modified_tracking_across_mappings(self):
        """Test that modified records are tracked correctly across multiple mappings."""
        df = pd.DataFrame(
            {
                "crm1": ["same", "change1", None],
                "export1": ["same", "changed1", "added1"],
                "crm2": ["same2", None, "change2"],
                "export2": ["same2", "added2", "changed2"],
            }
        )

        mappings = [
            ColumnMapping(
                crm_column="crm1",
                export_column="export1",
                confidence=1.0,
                reasoning="Test 1",
                is_many_to_one=False,
            ),
            ColumnMapping(
                crm_column="crm2",
                export_column="export2",
                confidence=1.0,
                reasoning="Test 2",
                is_many_to_one=False,
            ),
        ]

        column_matching_result = ColumnMatchingResponse(
            mappings=mappings, unmapped_crm_columns=[], unmapped_export_columns=[]
        )

        result = self.calculator.calculate_statistics(
            df, column_matching_result, ["crm1", "crm2"], ["export1", "export2"]
        )

        # All rows should be marked as modified
        # Row 0: same in both mappings (unchanged)
        # Row 1: change1->changed1 in mapping1, None->added2 in mapping2
        # Row 2: None->added1 in mapping1, change2->changed2 in mapping2
        assert result.records_modified_count == 2  # Rows 1 and 2

    def test_zero_division_protection(self):
        """Test protection against zero division in percentage calculations."""
        # Empty dataframe
        df = pd.DataFrame({"crm_col": [], "export_col": []})

        column_mapping = ColumnMappingCalculation(
            enrichment_report_id="test",
            crm_column="crm_col",
            export_column="export_col",
            confidence=1.0,
            reasoning="Test mapping",
        )

        modified_records = set()
        stats = self.calculator._calculate_column_comparison_stats(
            df, column_mapping, modified_records
        )

        # Should handle empty data gracefully
        assert stats.correct_percentage_before == 0.0
        assert stats.correct_percentage_after == 0.0

    def test_complex_many_to_one_calculation(self):
        """Test complex many-to-one calculations with multiple scenarios."""
        mappings = [
            # First many-to-one: 3 CRM columns -> 1 export
            ColumnMapping(
                crm_column="linkedin1",
                export_column="linkedin_consolidated",
                confidence=0.8,
                reasoning="LinkedIn consolidation",
                is_many_to_one=True,
                additional_crm_columns=["linkedin2", "linkedin3"],
            ),
            # Second many-to-one: 2 CRM columns -> 1 export
            ColumnMapping(
                crm_column="phone1",
                export_column="phone_consolidated",
                confidence=0.9,
                reasoning="Phone consolidation",
                is_many_to_one=True,
                additional_crm_columns=["phone2"],
            ),
            # Regular one-to-one mapping
            ColumnMapping(
                crm_column="email",
                export_column="email_export",
                confidence=1.0,
                reasoning="Direct match",
                is_many_to_one=False,
            ),
        ]

        column_matching_result = ColumnMatchingResponse(
            mappings=mappings,
            unmapped_crm_columns=[],
            unmapped_export_columns=["new_field"],
        )

        report = EnrichmentReportCalculation(
            total_rows=100,
            total_crm_columns=6,  # 3 linkedin + 2 phone + 1 email
            total_export_columns=4,  # 1 consolidated linkedin + 1 consolidated phone + 1 email + 1 new
            new_columns_count=0,
            many_to_one_count=0,
            columns_reduced_by_merging=0,
            records_modified_count=0,
            export_columns_created=4,
        )

        self.calculator._calculate_global_statistics(
            report,
            column_matching_result,
            ["linkedin1", "linkedin2", "linkedin3", "phone1", "phone2", "email"],
            [
                "linkedin_consolidated",
                "phone_consolidated",
                "email_export",
                "new_field",
            ],
        )

        assert report.many_to_one_count == 2  # Two many-to-one mappings
        assert report.new_columns_count == 1  # One unmapped export column
        # Columns reduced: (3 + 2) CRM columns reduced to 2 export columns = 3 reduction
        assert report.columns_reduced_by_merging == 3

    def test_data_type_consistency_in_comparison_stats(self):
        """Test that data types are correctly detected and stored in comparison stats."""
        df = pd.DataFrame(
            {
                "crm_email": ["test@example.com", "user@test.com"],
                "export_email": ["test@example.com", "user@test.com"],
            }
        )

        with patch.object(self.calculator, "_calculate_column_formats") as mock_format:
            # Mock different return values for CRM vs export columns
            mock_format.side_effect = [
                ("email", 1),  # CRM column
                ("email", 1),  # Export column
            ]

            column_mapping = ColumnMappingCalculation(
                enrichment_report_id="test",
                crm_column="crm_email",
                export_column="export_email",
                confidence=1.0,
                reasoning="Test mapping",
            )

            modified_records = set()
            stats = self.calculator._calculate_column_comparison_stats(
                df, column_mapping, modified_records
            )

            # Verify format detection was called for both columns
            assert mock_format.call_count == 2
            assert stats.crm_data_type == "email"
            assert stats.export_data_type == "email"
            assert stats.crm_format_count == 1
            assert stats.export_format_count == 1

    def test_integration_with_real_data_patterns(self):
        """Test with realistic data patterns that might occur in production."""
        # Simulate real-world messy data
        df = pd.DataFrame(
            {
                "company_name_crm": [
                    "Apple Inc.",
                    "Google LLC",
                    "",
                    "Microsoft Corp",
                    None,
                ],
                "company_name_export": [
                    "Apple Inc.",
                    "Google",
                    "Facebook",
                    "Microsoft Corporation",
                    "Tesla",
                ],
                "phone_crm": [
                    "+1-408-996-1010",
                    "650-253-0000",
                    "",
                    "+1-425-882-8080",
                    None,
                ],
                "phone_export": [
                    "+14089961010",
                    "6502530000",
                    "+1-650-543-4800",
                    "4258828080",
                    "+1-512-555-1234",
                ],
            }
        )

        mappings = [
            ColumnMapping(
                crm_column="company_name_crm",
                export_column="company_name_export",
                confidence=0.8,
                reasoning="Company name match",
                is_many_to_one=False,
            ),
            ColumnMapping(
                crm_column="phone_crm",
                export_column="phone_export",
                confidence=0.9,
                reasoning="Phone number match",
                is_many_to_one=False,
            ),
        ]

        column_matching_result = ColumnMatchingResponse(
            mappings=mappings, unmapped_crm_columns=[], unmapped_export_columns=[]
        )

        result = self.calculator.calculate_statistics(
            df,
            column_matching_result,
            ["company_name_crm", "phone_crm"],
            ["company_name_export", "phone_export"],
        )

        # Verify realistic results
        assert result.total_rows == 5
        assert result.records_modified_count >= 1  # At least some modifications
        assert len(result.column_mappings) == 2

        # Check that each mapping has comparison stats
        for mapping in result.column_mappings:
            assert mapping.comparison_stats is not None
            stats = mapping.comparison_stats
            # Sum of all comparison categories should equal total rows
            total_categorized = (
                stats.good_data
                + stats.fixed_data
                + stats.added_new_data
                + stats.discarded_invalid_data
            )
            assert (
                total_categorized == result.total_rows
            )  # Must always equal total rows


# Additional test class for edge cases and error conditions
class TestEnrichmentCalculatorEdgeCases:
    """Test edge cases and error conditions."""

    def setup_method(self):
        self.calculator = EnrichmentStatisticsCalculator()

    def test_malformed_column_matching_response(self):
        """Test handling of malformed column matching responses."""
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Empty mappings
        empty_response = ColumnMatchingResponse(
            mappings=[], unmapped_crm_columns=["col1"], unmapped_export_columns=["col2"]
        )

        result = self.calculator.calculate_statistics(
            df, empty_response, ["col1"], ["col2"]
        )

        assert result.total_rows == 3
        assert len(result.column_mappings) == 0
        assert result.records_modified_count == 0

    def test_unicode_and_special_characters(self):
        """Test handling of unicode and special characters in data."""
        df = pd.DataFrame(
            {
                "crm_col": ["café", "北京", "émoji 😀", "normal"],
                "export_col": ["café", "北京", "emoji 😀", "normal"],
            }
        )

        mapping = ColumnMapping(
            crm_column="crm_col",
            export_column="export_col",
            confidence=1.0,
            reasoning="Unicode test",
            is_many_to_one=False,
        )

        column_matching_result = ColumnMatchingResponse(
            mappings=[mapping], unmapped_crm_columns=[], unmapped_export_columns=[]
        )

        result = self.calculator.calculate_statistics(
            df, column_matching_result, ["crm_col"], ["export_col"]
        )

        # Should handle unicode characters without errors
        assert result.total_rows == 4
        stats = result.column_mappings[0].comparison_stats
        assert stats is not None
        assert stats.good_data == 3  # café, 北京, normal should match
        assert stats.fixed_data == 1  # émoji vs emoji difference

    def test_very_large_dataset_simulation(self):
        """Test behavior with a simulated large dataset structure."""
        # Create a larger dataset to test performance characteristics
        import numpy as np

        size = 1000
        # Create arrays without None, then manually add some None values
        crm_values = np.random.choice(["A", "B", "C"], size)
        export_values = np.random.choice(["A", "B", "C", "D"], size)

        # Convert to list and add some None values
        crm_list = crm_values.tolist()
        export_list = export_values.tolist()

        # Replace some values with None (about 10%)
        null_indices = np.random.choice(size, size // 10, replace=False)
        for idx in null_indices:
            crm_list[idx] = None
            export_list[idx] = None

        df = pd.DataFrame(
            {
                "crm_col": crm_list,
                "export_col": export_list,
            }
        )

        mapping = ColumnMapping(
            crm_column="crm_col",
            export_column="export_col",
            confidence=1.0,
            reasoning="Large dataset test",
            is_many_to_one=False,
        )

        column_matching_result = ColumnMatchingResponse(
            mappings=[mapping], unmapped_crm_columns=[], unmapped_export_columns=[]
        )

        result = self.calculator.calculate_statistics(
            df, column_matching_result, ["crm_col"], ["export_col"]
        )

        # Should handle large dataset without errors
        assert result.total_rows == size
        stats = result.column_mappings[0].comparison_stats
        assert stats is not None
        # All categories should sum to reasonable totals
        total_categorized = (
            stats.good_data
            + stats.fixed_data
            + stats.added_new_data
            + stats.discarded_invalid_data
        )
        assert total_categorized <= size


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
