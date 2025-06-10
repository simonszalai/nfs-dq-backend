import sys
import uuid
from typing import List

import pandas as pd
from dotenv import load_dotenv

from app.anthropic.data_quality_analyzer import DataQualityAnalyzer

# Import all required modules
from app.database import save_report_to_database
from app.drive import get_drive_client
from app.inconsistency import analyze_inconsistency
from app.initial.global_date_formats import count_unique_date_formats
from app.initial.models import (
    FieldModel,
    GlobalIssue,
    Report,
    Severity,
    Warning,
    WarningType,
)
from app.initial.population import analyze_population
from app.initial.utils import generate_token_from_company_name
from app.load_data import load_hubspot_files, write_output_to_drive

SPARSE_THRESHOLD = 0.25

load_dotenv(override=True)


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
    print("✓ Connected to Google Drive")

    # Step 2: Load HubSpot files
    print("Loading HubSpot files...")
    all_companies = load_hubspot_files(drive)
    print(f"✓ Loaded {len(all_companies)} companies from HubSpot")

    # Check if company exists
    if company_name not in all_companies:
        print(f"Error: Company '{company_name}' not found in the loaded data.")
        print(f"Available companies: {list(all_companies.keys())}")
        return

    # Get the company's DataFrame
    hubspot_df: pd.DataFrame = all_companies[company_name]["hubspot"]
    print(
        f"✓ Retrieved data for {company_name}: {len(hubspot_df)} records, {len(hubspot_df.columns)} fields"
    )

    # Define column configuration
    column_config = all_companies[company_name]["config"]

    # Pre-generate report id
    report_id = str(uuid.uuid4())

    # 1. Analyze column population
    print("Analyzing column population...")
    population_results = analyze_population(hubspot_df)
    print(f"✓ Analyzed population for {len(population_results)} columns")

    # 2. Classify columns
    print("Classifying column types and formats...")
    classified_cols = analyze_inconsistency(hubspot_df)
    print(f"✓ Classified {len(classified_cols)} columns")

    # 3. Create DB Field Records from results of 1. and 2.
    print("Creating field records...")
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

        fields.append(field)

    print(f"✓ Created {len(fields)} field records")

    # Create a mapping from column names to field IDs
    column_to_field_id = {field.column_name: field.id for field in fields}

    # Create warnings for population and format issues
    print("Creating warnings for population and format issues...")
    db_warnings: List[Warning] = []

    # Create population warnings
    total_records = len(hubspot_df)
    for column_name, population_result in population_results.items():
        field_id = column_to_field_id.get(column_name)
        if field_id is None:
            continue

        population_rate = population_result.populated_count / total_records
        population_percentage = round(population_rate * 100, 1)

        # Determine severity based on population rate
        if population_result.populated_count == 0:
            # High severity for completely empty columns
            db_warnings.append(
                Warning(
                    field_id=field_id,
                    severity=Severity.HIGH,
                    message="Column is completely empty (0% populated)",
                    type=WarningType.EMPTY_FIELD,
                )
            )
        elif population_rate < 0.25:
            # Medium severity for < 25% populated
            db_warnings.append(
                Warning(
                    field_id=field_id,
                    severity=Severity.MEDIUM,
                    message=f"Column is sparsely populated ({population_percentage}% populated)",
                    type=WarningType.LOW_POPULATION,
                )
            )
        elif population_rate < 0.75:
            # Low severity for < 75% populated
            db_warnings.append(
                Warning(
                    field_id=field_id,
                    severity=Severity.LOW,
                    message=f"Column has moderate population ({population_percentage}% populated)",
                    type=WarningType.LOW_POPULATION,
                )
            )

    # Create format inconsistency warnings
    for column_name, classified_col in classified_cols.items():
        if classified_col.format_count > 1:
            field_id = column_to_field_id.get(column_name)
            if field_id is None:
                continue

            # Collect examples of different formats
            examples_meta = {}
            column_data = hubspot_df[column_name].dropna()

            if not column_data.empty:
                # Get up to 5 unique examples from the column
                unique_values = column_data.astype(str).str.strip().unique()
                examples = list(unique_values[: min(5, len(unique_values))])
                examples_meta = {"examples": examples}

            db_warnings.append(
                Warning(
                    field_id=field_id,
                    severity=Severity.MEDIUM,
                    message=f"Column has {classified_col.format_count} different formats detected",
                    type=WarningType.INCONSISTENT_FORMAT,
                    meta=examples_meta,
                )
            )

    print(f"✓ Created {len(db_warnings)} warnings for population and format issues")

    # 4. Analyze data quality with AI
    print("Analyzing data quality with AI...")
    analyzer = DataQualityAnalyzer()
    warnings = analyzer.analyze_dataframe(hubspot_df)
    print("✓ AI analysis complete")

    # 5. Create DB Warning Records from results of 4.
    print("Processing AI warnings...")
    for col in warnings.column_warnings.keys():
        col_warnings = warnings.column_warnings[col]

        # Get the field_id for this column
        field_id = column_to_field_id.get(col)
        if field_id is None:
            print(
                f"Warning: No field found for column '{col}', skipping warnings for this column"
            )
            continue

        for warning in col_warnings:
            # Create a new Warning with the field_id
            warning_data = warning.model_dump()
            warning_data["field_id"] = field_id
            # Remove 'id' if it exists to let SQLModel generate it
            warning_data.pop("id", None)

            # Handle examples - move them to meta field if they exist
            examples = warning_data.pop("examples", None)
            affected_count = warning_data.pop("affected_count", None)

            # Build meta field with examples and affected_count if available
            meta = {}
            if examples:
                meta["examples"] = examples
            if affected_count is not None:
                meta["affected_count"] = affected_count

            # Only add meta field if it has content
            if meta:
                warning_data["meta"] = meta

            db_warnings.append(Warning(**warning_data))

    print(f"✓ Processed {len(db_warnings)} total warnings")

    # 6. Analyze global issues
    print("Analyzing global issues...")
    unique_date_format_count = count_unique_date_formats(classified_cols, hubspot_df)
    print(f"✓ Found {unique_date_format_count} unique date formats")

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
    print(f"✓ Created {len(global_issues)} global issue records")

    # 8. Create DB Report Record
    print("Creating report record...")

    # Count unique fields with warnings
    fields_with_issues = len(set(warning.field_id for warning in db_warnings))

    report = Report(
        id=report_id,
        token=generate_token_from_company_name(company_name),
        company_name=company_name,
        total_records=len(hubspot_df),
        total_fields=len(hubspot_df.columns),
        fields_with_issues=fields_with_issues,
        config=column_config,
    )
    print(f"✓ Created report with {fields_with_issues} fields having issues")

    # 9. Save all objects to database
    print("Saving to database...")
    save_report_to_database(
        company_name=company_name,
        report=report,
        fields=fields,
        warnings=db_warnings,
        global_issues=global_issues,
    )
    print("✓ Analysis saved to database")

    # 10. Write output.json to Google Drive
    print("Writing output to Google Drive...")
    report_url = f"https://nfs-dq-frontend.onrender.com/reports/{report.token}"
    write_output_to_drive(drive, company_name, report_url)
    print("✓ Analysis complete!")


if __name__ == "__main__":
    try:
        # Run main function
        main()
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
