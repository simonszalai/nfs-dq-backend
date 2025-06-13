from typing import List, Optional, Set

import pandas as pd

from app.anthropic.column_matcher import ColumnMatchingResponse
from app.enrichment.enrichment_calculation_models import (
    ColumnComparisonStatsCalculation,
    ColumnMappingCalculation,
    EnrichmentReportCalculation,
)
from app.inconsistency import analyze_inconsistency

# Add phone number imports
try:
    import phonenumbers
    from phonenumbers import NumberParseException
except ImportError:
    phonenumbers = None
    NumberParseException = Exception


class EnrichmentStatisticsCalculator:
    """Main class for calculating enrichment statistics from column mappings."""

    def __init__(self):
        pass

    def calculate_statistics(
        self,
        df: pd.DataFrame,
        column_matching_result: ColumnMatchingResponse,
        crm_columns: List[str],
        export_columns: List[str],
    ) -> EnrichmentReportCalculation:
        """Calculate all enrichment statistics.

        Args:
            df: DataFrame containing both CRM and export data
            column_matching_result: Result from ColumnMatcher
            crm_columns: List of CRM column names
            export_columns: List of export column names

        Returns:
            EnrichmentReportCalculation with all calculated statistics
        """
        # Create the enrichment report with basic information
        report = EnrichmentReportCalculation(
            total_rows=len(df),
            total_crm_columns=len(crm_columns),
            total_export_columns=len(export_columns),
            new_columns_count=0,  # Will be calculated
            many_to_one_count=0,  # Will be calculated
            columns_reduced_by_merging=0,  # Will be calculated
            records_modified_count=0,  # Will be calculated
            export_columns_created=len(export_columns),
        )

        # Calculate global statistics
        self._calculate_global_statistics(
            report, column_matching_result, crm_columns, export_columns
        )

        # Create column mappings and their comparison stats
        column_mappings = []
        all_comparison_stats = []
        modified_records = set()

        for mapping in column_matching_result.mappings:
            # Create ColumnMapping record
            column_mapping = ColumnMappingCalculation(
                enrichment_report_id=report.id,
                crm_column=mapping.crm_column,
                export_column=mapping.export_column,
                is_many_to_one=mapping.is_many_to_one,
                additional_crm_columns=mapping.additional_crm_columns,
                confidence=mapping.confidence,
                reasoning=mapping.reasoning,
            )
            column_mappings.append(column_mapping)

            # Calculate comparison stats for this mapping
            if mapping.export_column:  # Only if there's a mapped export column
                comparison_stats = self._calculate_column_comparison_stats(
                    df, column_mapping, modified_records
                )
                comparison_stats.column_mapping_id = column_mapping.id
                all_comparison_stats.append(comparison_stats)

        # Update records modified count
        report.records_modified_count = len(modified_records)

        # Set relationships
        report.column_mappings = column_mappings
        for i, mapping in enumerate(column_mappings):
            if i < len(all_comparison_stats):
                mapping.comparison_stats = all_comparison_stats[i]

        return report

    def _calculate_global_statistics(
        self,
        report: EnrichmentReportCalculation,
        column_matching_result: ColumnMatchingResponse,
        crm_columns: List[str],
        export_columns: List[str],
    ) -> None:
        """Calculate global statistics and update the report."""

        # Count new columns (export columns not matching any CRM)
        report.new_columns_count = len(column_matching_result.unmapped_export_columns)

        # Count many-to-one relationships
        many_to_one_mappings = [
            m for m in column_matching_result.mappings if m.is_many_to_one
        ]
        report.many_to_one_count = len(many_to_one_mappings)

        # Calculate columns reduced by merging
        total_crm_columns_in_many_to_one = 0
        for mapping in many_to_one_mappings:
            # Count main column + additional columns
            total_crm_columns_in_many_to_one += 1  # Main column
            if mapping.additional_crm_columns:
                total_crm_columns_in_many_to_one += len(mapping.additional_crm_columns)

        # Reduced count = total CRM columns involved - number of resulting export columns
        report.columns_reduced_by_merging = (
            total_crm_columns_in_many_to_one - report.many_to_one_count
        )

    def _calculate_column_comparison_stats(
        self,
        df: pd.DataFrame,
        column_mapping: ColumnMappingCalculation,
        modified_records: Set[int],
    ) -> ColumnComparisonStatsCalculation:
        """Calculate row-by-row comparison statistics for a column mapping."""

        crm_col = column_mapping.crm_column
        export_col = column_mapping.export_column

        if not export_col or crm_col not in df.columns or export_col not in df.columns:
            return ColumnComparisonStatsCalculation(column_mapping_id=column_mapping.id)

        # Get column data
        crm_data = df[crm_col]
        export_data = df[export_col]

        # Initialize stats
        stats = ColumnComparisonStatsCalculation(column_mapping_id=column_mapping.id)

        for idx in df.index:
            crm_val = crm_data.iloc[idx]
            export_val = export_data.iloc[idx]

            crm_has_value = pd.notna(crm_val) and str(crm_val).strip() != ""
            export_has_value = pd.notna(export_val) and str(export_val).strip() != ""

            if crm_has_value and not export_has_value:
                # CRM has value, export doesn't -> DISCARDED
                stats.discarded_invalid_data += 1
                modified_records.add(idx)
            elif not crm_has_value and export_has_value:
                # CRM doesn't have value, export has -> ADDED
                stats.added_new_data += 1
                modified_records.add(idx)
            elif crm_has_value and export_has_value:
                # Both have values - check if they're the same
                if str(crm_val).strip() == str(export_val).strip():
                    # GOOD
                    stats.good_data += 1
                else:
                    # FIXED
                    stats.fixed_data += 1
                    modified_records.add(idx)
            elif not crm_has_value and not export_has_value:
                # Both are null/empty - count as good data (consistent empty data)
                stats.not_found += 1

        # Verify that the four categories add up to the total number of rows
        total_categorized = (
            stats.good_data
            + stats.fixed_data
            + stats.added_new_data
            + stats.discarded_invalid_data
            + stats.not_found
        )
        assert total_categorized == len(df), (
            f"Column statistics don't add up to total rows. "
            f"Total categorized: {total_categorized}, Total rows: {len(df)}, "
            f"Column: {crm_col} -> {export_col}"
        )

        # Calculate correct values and percentages
        # "Correct before" = records that are exactly the same before and after (unchanged)
        stats.correct_values_before = stats.good_data
        # "Correct after" = any non-null value in export column (as per user requirement)
        non_null_count = int(
            df[export_col].apply(lambda x: pd.notna(x) and str(x).strip() != "").sum()
        )
        correct_count = stats.good_data + stats.fixed_data + stats.added_new_data
        assert correct_count == non_null_count, (
            f"Correct count doesn't match non-null count. "
            f"Correct count: {correct_count}, Non-null count: {non_null_count}, "
            f"Column: {crm_col} -> {export_col}"
        )
        stats.correct_values_after = correct_count

        total_rows = len(df)
        if total_rows > 0:
            stats.correct_percentage_before = float(
                (stats.correct_values_before / total_rows) * 100
            )
            stats.correct_percentage_after = float(
                (stats.correct_values_after / total_rows) * 100
            )

        # Get format information - calculate manually for better accuracy
        stats.crm_data_type, stats.crm_format_count = self._calculate_column_formats(
            crm_data, is_export_column=False
        )
        stats.export_data_type, stats.export_format_count = (
            self._calculate_column_formats(export_data, is_export_column=True)
        )

        return stats

    def _calculate_column_formats(
        self, column_data: pd.Series, is_export_column: bool = False
    ) -> tuple[Optional[str], int]:
        """Calculate data type and format count for a column using the same logic as initial report."""
        # Remove null values and convert to string
        clean_data = column_data.dropna().astype(str).str.strip()

        if clean_data.empty:
            return None, 1

        # Only apply phone number validation for export columns
        if is_export_column and self._is_phone_column_with_valid_numbers(clean_data):
            return "phone", 1

        col_name = str(column_data.name) if column_data.name is not None else "temp"

        # Use the same inconsistency analysis as the initial report
        temp_df = pd.DataFrame({col_name: column_data})
        format_analysis = analyze_inconsistency(temp_df)

        if col_name in format_analysis:
            # Use exactly what analyze_inconsistency returns
            return format_analysis[col_name].type, format_analysis[
                col_name
            ].format_count

        return "string", 1

    def _is_phone_column_with_valid_numbers(self, clean_data: pd.Series) -> bool:
        """Check if column contains phone numbers and if all are valid according to their country codes."""
        if phonenumbers is None or clean_data.empty:
            return False

        # Basic phone number pattern check first (to avoid expensive validation on non-phone data)
        import re

        phone_pattern = re.compile(r"[\+]?[1-9]?[\d\s\-\(\)\.]{7,20}")
        potential_phones = clean_data.apply(lambda x: bool(phone_pattern.match(str(x))))

        # If less than 80% look like phone numbers, not a phone column
        if potential_phones.sum() / len(clean_data) < 0.8:
            return False

        # Now validate each potential phone number
        valid_count = 0
        total_count = len(clean_data)

        for phone_str in clean_data:
            if self._is_valid_phone_number(phone_str):
                valid_count += 1

        # If at least 80% are valid phone numbers, treat as phone column with unified format
        return (valid_count / total_count) >= 0.8

    def _is_valid_phone_number(self, s: str, region: str = "US") -> bool:
        """Check if a string is a valid phone number using phonenumbers library."""
        if phonenumbers is None:
            return False
        try:
            # Try parsing without region first (for international numbers)
            try:
                parsed = phonenumbers.parse(s, None)
            except Exception:
                # Fall back to parsing with default region
                parsed = phonenumbers.parse(s, region)

            return phonenumbers.is_valid_number(parsed)
        except Exception:
            return False

    # Remove the custom format counting methods since we'll use analyze_inconsistency
    # which already handles format counting correctly
