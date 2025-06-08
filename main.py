import argparse
import os
import sys
import uuid
from typing import List

import pandas as pd
from sqlmodel import Session, SQLModel, create_engine

from app.anthropic import DataQualityAnalyzer

# Import all required modules
from app.drive import get_drive_client
from app.global_date_formats import count_unique_date_formats
from app.inconsistency import analyze_inconsistency
from app.load_data import load_hubspot_files
from app.models import FieldModel, GlobalIssue, Report, Severity, Warning
from app.population import analyze_population
from app.utils import generate_token_from_company_name

SPARSE_THRESHOLD = 0.25


def main(company_name=None):
    """
    Main function to analyze HubSpot data quality for a given company.

    Args:
        company_name: Name of the company to analyze. If None, defaults to 'nofluffselling'
    """
    # Default company name if not provided
    if company_name is None:
        company_name = "nofluffselling"

    print(f"Starting analysis for company: {company_name}")

    # Step 1: Connect to Google Drive
    print("Connecting to Google Drive...")
    drive = get_drive_client()

    # Step 2: Load HubSpot files
    print("Loading HubSpot files...")
    all_companies = load_hubspot_files(drive)

    # Check if company exists
    if company_name not in all_companies:
        print(f"Error: Company '{company_name}' not found in the loaded data.")
        print(f"Available companies: {list(all_companies.keys())}")
        return

    # Get the company's DataFrame
    hubspot_df: pd.DataFrame = all_companies[company_name]

    # Define column configuration
    column_config = {
        "critical_columns": {
            "company_info": {
                "Website": "Website URL",
                "LinkedIn": "LinkedIn Company Page",
                "Industry": "Industry",
            },
            "financial_data": {
                "Annual Revenue": "Annual Revenue",
                "Funding Stage": "Total Money Raised",
                "Last Funding Date": "Recent Deal Close Date",
            },
            "size_and_structure": {
                "Employee Count": "Number of Employees",
                "CEO Name": "Company owner",
                "Office Locations": "Country/Region",
            },
        }
    }

    # Duplicates allowed, in the end it will be converted to set
    cols_with_issues: List[str] = []

    # Pre-generate report id
    report_id = str(uuid.uuid4())

    # 1. Analyze column population
    population_results = analyze_population(hubspot_df)

    # 2. Classify columns
    classified_cols = analyze_inconsistency(hubspot_df)

    # 3. Create DB Field Records from results of 1. and 2.
    fields: List[FieldModel] = []
    for column_name in population_results.keys():
        population_result = population_results[column_name]
        classified_col = classified_cols[column_name]

        field = FieldModel(
            report_id=report_id,
            column_name=column_name,
            populated_count=population_result.populated_count,
            inferred_type=classified_col.type,
            format_count=classified_col.format_count,
        )

        if population_result.populated_count < (SPARSE_THRESHOLD * len(hubspot_df)):
            cols_with_issues.append(column_name)

        if classified_col.format_count > 1:
            cols_with_issues.append(column_name)

        fields.append(field)

    # 4. Analyze data quality with AI
    analyzer = DataQualityAnalyzer()
    warnings = analyzer.analyze_dataframe(hubspot_df)

    # 5. Create DB Warning Records from results of 4.
    db_warnings: List[Warning] = []
    for col in warnings.column_warnings.keys():
        col_warnings = warnings.column_warnings[col]

        if len(col_warnings) > 0:
            cols_with_issues.append(col)

        for warning in col_warnings:
            db_warnings.append(Warning(**warning.model_dump()))

    # 6. Analyze global issues
    unique_date_format_count = count_unique_date_formats(classified_cols, hubspot_df)

    # 7. Create DB Global Issue Record from results of 6.
    global_issues: List[GlobalIssue] = []
    global_issues.append(
        GlobalIssue(
            report_id=report_id,
            type="date_format_across_cols",
            title="Date formats across columns",
            description=f"Found {unique_date_format_count} unique date formats across columns",
            severity=Severity.LOW,
        )
    )

    # 8. Create DB Report Record
    report = Report(
        id=report_id,
        token=generate_token_from_company_name(company_name),
        company_name=company_name,
        total_records=len(hubspot_df),
        total_fields=len(hubspot_df.columns),
        fields_with_issues=len(set(cols_with_issues)),
        config=column_config,
    )

    # 9. Save all objects to database
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("Warning: DATABASE_URL not set. Skipping database save.")
        return

    engine = create_engine(DATABASE_URL)

    # Create tables if they don't exist
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        # Save report
        session.add(report)
        session.flush()  # Flush to get the report ID for foreign keys

        # Save fields
        for field in fields:
            session.add(field)

        # Save warnings
        for warning in db_warnings:
            session.add(warning)

        # Save global issues
        for issue in global_issues:
            session.add(issue)

        # Commit all changes
        session.commit()
        print(f"Analysis saved to database with report ID: {report_id}")
        print(f"Report token: {report.token}")


if __name__ == "__main__":
    try:
        # Run main function
        main()
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        sys.exit(1)
