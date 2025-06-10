from typing import List, Optional, Set

import pandas as pd

from app.anthropic.column_matcher import ColumnMatchingResponse
from app.enrichment.enrichment_calculation_models import (
    ColumnComparisonStatsCalculation,
    ColumnMappingCalculation,
    EnrichmentReportCalculation,
)
from app.inconsistency import analyze_inconsistency


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

        # Row-by-row comparison
        unchanged_records = 0  # Records that are exactly the same before and after

        for idx in df.index:
            crm_val = crm_data.iloc[idx]
            export_val = export_data.iloc[idx]

            crm_has_value = pd.notna(crm_val) and str(crm_val).strip() != ""
            export_has_value = pd.notna(export_val) and str(export_val).strip() != ""

            if crm_has_value and not export_has_value:
                # CRM has value, export doesn't -> discarded invalid data
                stats.discarded_invalid_data += 1
                modified_records.add(idx)
            elif not crm_has_value and export_has_value:
                # CRM doesn't have value, export has -> added new data
                stats.added_new_data += 1
                modified_records.add(idx)
            elif crm_has_value and export_has_value:
                # Both have values - check if they're the same
                if str(crm_val).strip() == str(export_val).strip():
                    stats.good_data += 1
                    unchanged_records += 1  # Count as unchanged/correct
                else:
                    stats.fixed_data += 1
                    modified_records.add(idx)
            elif not crm_has_value and not export_has_value:
                # Both are null/empty - count as unchanged
                unchanged_records += 1

        # Calculate correct values and percentages
        # "Correct before" = records that are exactly the same before and after (unchanged)
        stats.correct_values_before = unchanged_records
        # "Correct after" = any non-null value in export column (as per user requirement)
        stats.correct_values_after = export_data.notna().sum()

        total_rows = len(df)
        if total_rows > 0:
            stats.correct_percentage_before = (
                stats.correct_values_before / total_rows
            ) * 100
            stats.correct_percentage_after = (
                stats.correct_values_after / total_rows
            ) * 100

        # Get format information - calculate manually for better accuracy
        stats.crm_data_type, stats.crm_format_count = self._calculate_column_formats(
            crm_data
        )
        stats.export_data_type, stats.export_format_count = (
            self._calculate_column_formats(export_data)
        )

        return stats

    def _calculate_column_formats(
        self, column_data: pd.Series
    ) -> tuple[Optional[str], int]:
        """Calculate data type and format count for a column manually for better accuracy."""
        # Remove null values and convert to string
        clean_data = column_data.dropna().astype(str).str.strip()

        if clean_data.empty:
            return None, 1

        # Use existing inconsistency analysis for basic type detection
        from app.inconsistency import analyze_inconsistency

        temp_df = pd.DataFrame({column_data.name or "temp": column_data})
        format_analysis = analyze_inconsistency(temp_df)

        col_name = column_data.name or "temp"
        if col_name in format_analysis:
            detected_type = format_analysis[col_name].type

            # For certain types, manually count unique formats for better accuracy
            if detected_type == "phone":
                unique_formats = self._count_phone_formats(clean_data)
                return detected_type, unique_formats
            elif detected_type == "date":
                unique_formats = self._count_date_formats(clean_data)
                return detected_type, unique_formats
            elif detected_type in ["url", "email"]:
                # For URLs and emails, count unique patterns
                unique_patterns = len(clean_data.unique())
                # Cap at reasonable number to avoid too many "formats"
                return detected_type, min(unique_patterns, 10)
            else:
                # For other types, use the original analysis
                return detected_type, format_analysis[col_name].format_count

        return "string", 1

    def _count_phone_formats(self, phone_data: pd.Series) -> int:
        """Count unique phone number formats more accurately."""
        format_patterns = set()

        for phone in phone_data:
            if pd.isna(phone) or str(phone).strip() == "":
                continue

            phone_str = str(phone).strip()

            # Create a format signature based on structure
            format_signature = []

            # Check for country code
            if phone_str.startswith("+"):
                format_signature.append("country_code")
            elif phone_str.startswith("00"):
                format_signature.append("intl_prefix")
            else:
                format_signature.append("domestic")

            # Check for parentheses around area code
            if "(" in phone_str and ")" in phone_str:
                format_signature.append("area_parens")
            else:
                format_signature.append("no_parens")

            # Check for separators
            separators = []
            if "-" in phone_str:
                separators.append("dash")
            if "." in phone_str:
                separators.append("dot")
            if " " in phone_str:
                separators.append("space")

            if separators:
                format_signature.append(f"sep_{'_'.join(separators)}")
            else:
                format_signature.append("no_sep")

            # Check for extensions
            if any(ext in phone_str.lower() for ext in ["ext", "x", "#"]):
                format_signature.append("has_ext")

            format_patterns.add("|".join(format_signature))

        return max(1, len(format_patterns))

    def _count_date_formats(self, date_data: pd.Series) -> int:
        """Count unique date formats more accurately."""
        format_patterns = set()

        for date_val in date_data:
            if pd.isna(date_val) or str(date_val).strip() == "":
                continue

            date_str = str(date_val).strip()

            # Create format signature based on separators and structure
            format_signature = []

            # Check separators
            if "/" in date_str:
                format_signature.append("slash_sep")
            elif "-" in date_str:
                format_signature.append("dash_sep")
            elif "." in date_str:
                format_signature.append("dot_sep")
            else:
                format_signature.append("no_sep")

            # Check if has time component
            if ":" in date_str:
                format_signature.append("has_time")
            else:
                format_signature.append("date_only")

            # Check for timezone
            if (
                date_str.endswith("Z")
                or "+" in date_str[-6:]
                or date_str.count("-") > 2
            ):
                format_signature.append("has_tz")

            format_patterns.add("|".join(format_signature))

        return max(1, len(format_patterns))
