import os
from typing import List

from sqlmodel import Session, SQLModel, create_engine, select

from app.enrichment.enrichment_calculation_models import EnrichmentReportCalculation
from app.enrichment.models import (
    ColumnComparisonStats,
    ColumnMapping,
    EnrichmentReport,
)
from app.initial.utils import generate_token_from_company_name


def save_enrichment_report_to_database(
    enrichment_report: EnrichmentReportCalculation,
    filename: str,
) -> None:
    """
    Save an enrichment report and all its related data to the database.

    This function is idempotent - it will delete any existing enrichment report
    with the same filename before saving the new one.

    Args:
        enrichment_report: The EnrichmentReportCalculation object to save
        filename: The filename (e.g., clay_export.csv) to generate token from
    """
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("Warning: DATABASE_URL not set. Skipping database save.")
        return

    engine = create_engine(DATABASE_URL)

    # Create tables if they don't exist
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        # Generate token from filename for idempotent operations
        token = generate_token_from_company_name(filename)

        # First, check if a report with this token already exists and delete it
        existing_report = session.exec(
            select(EnrichmentReport).where(EnrichmentReport.token == token)
        ).first()

        if existing_report:
            print(
                f"Found existing enrichment report for {filename}, removing old data..."
            )

            # Delete all comparison stats for column mappings of this report
            for mapping in existing_report.column_mappings:
                if mapping.comparison_stats:
                    session.delete(mapping.comparison_stats)
                session.delete(mapping)

            # Delete the report itself
            session.delete(existing_report)
            session.commit()
            print("✓ Removed old enrichment report data")

        # Create the database enrichment report
        db_report = EnrichmentReport(
            id=enrichment_report.id,
            token=token,
            filename=filename,
            created_at=enrichment_report.created_at,
            total_rows=enrichment_report.total_rows,
            total_crm_columns=enrichment_report.total_crm_columns,
            total_export_columns=enrichment_report.total_export_columns,
            new_columns_count=enrichment_report.new_columns_count,
            many_to_one_count=enrichment_report.many_to_one_count,
            columns_reduced_by_merging=enrichment_report.columns_reduced_by_merging,
            records_modified_count=enrichment_report.records_modified_count,
            export_columns_created=enrichment_report.export_columns_created,
        )

        # Save report
        session.add(db_report)
        session.flush()  # Flush to get the report ID for foreign keys

        # Create and save column mappings
        db_mappings: List[ColumnMapping] = []
        for mapping in enrichment_report.column_mappings:
            db_mapping = ColumnMapping(
                id=mapping.id,
                enrichment_report_id=db_report.id,
                crm_column=mapping.crm_column,
                export_column=mapping.export_column,
                is_many_to_one=mapping.is_many_to_one,
                additional_crm_columns=mapping.additional_crm_columns,
                confidence=mapping.confidence,
                reasoning=mapping.reasoning,
            )
            db_mappings.append(db_mapping)
            session.add(db_mapping)

        session.flush()  # Flush to get the mapping IDs for foreign keys

        # Create and save comparison stats
        db_stats: List[ColumnComparisonStats] = []
        for i, mapping in enumerate(enrichment_report.column_mappings):
            if mapping.comparison_stats:
                stats = mapping.comparison_stats
                db_stat = ColumnComparisonStats(
                    id=stats.id,
                    column_mapping_id=db_mappings[i].id,
                    discarded_invalid_data=stats.discarded_invalid_data,
                    added_new_data=stats.added_new_data,
                    fixed_data=stats.fixed_data,
                    good_data=stats.good_data,
                    not_found=stats.not_found,
                    correct_values_before=stats.correct_values_before,
                    correct_values_after=stats.correct_values_after,
                    correct_percentage_before=stats.correct_percentage_before,
                    correct_percentage_after=stats.correct_percentage_after,
                    crm_data_type=stats.crm_data_type,
                    crm_format_count=stats.crm_format_count,
                    export_data_type=stats.export_data_type,
                    export_format_count=stats.export_format_count,
                )
                db_stats.append(db_stat)
                session.add(db_stat)

        # Commit all changes
        session.commit()
        print(f"✓ Enrichment analysis saved to database with report ID: {db_report.id}")
        print(f"✓ Report token: {db_report.token}")
        print(f"✓ Saved {len(db_mappings)} column mappings")
        print(f"✓ Saved {len(db_stats)} comparison statistics")
