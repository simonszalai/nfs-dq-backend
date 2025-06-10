import sys
from io import StringIO
from typing import List

import pandas as pd
from dotenv import load_dotenv

from app.anthropic.column_matcher import ColumnMatcher
from app.drive import get_drive_client
from app.enrichment.enrichment_calculator import EnrichmentStatisticsCalculator

load_dotenv(override=True)


def load_clay_export_from_drive(drive, filename="clay_export.csv"):
    """
    Load the clay_export.csv file from Google Drive.

    Args:
        drive: Google Drive client
        filename: Name of the CSV file to load

    Returns:
        pd.DataFrame: The loaded clay export data
    """
    # Search for the clay_export.csv file
    file_list = drive.ListFile({"q": f"title='{filename}' and trashed=false"}).GetList()

    if not file_list:
        raise FileNotFoundError(
            f"Clay export file '{filename}' not found in Google Drive"
        )

    if len(file_list) > 1:
        print(f"Warning: Multiple files named '{filename}' found. Using the first one.")

    # Load the first matching file
    clay_file = file_list[0]
    csv_str = clay_file.GetContentString(mimetype="text/csv")
    return pd.read_csv(StringIO(csv_str))


def print_enrichment_report(enrichment_report):
    """
    Print detailed enrichment report with statistics.

    Args:
        enrichment_report: EnrichmentReportCalculation object with all the statistics
    """
    print("\n" + "=" * 60)
    print("ENRICHMENT REPORT")
    print("=" * 60)

    print(f"\nBasic Information:")
    print(f"  Total rows: {enrichment_report.total_rows}")
    print(f"  Total CRM columns: {enrichment_report.total_crm_columns}")
    print(f"  Total export columns: {enrichment_report.total_export_columns}")

    print(f"\nGlobal Statistics:")
    print(f"  New columns (export only): {enrichment_report.new_columns_count}")
    print(f"  Many-to-one relationships: {enrichment_report.many_to_one_count}")
    print(
        f"  Columns reduced by merging: {enrichment_report.columns_reduced_by_merging}"
    )
    print(f"  Records modified: {enrichment_report.records_modified_count}")
    print(f"  Export columns created: {enrichment_report.export_columns_created}")

    print(f"\nColumn-Level Statistics:")
    print("-" * 40)
    for mapping in enrichment_report.column_mappings:
        if mapping.comparison_stats:
            stats = mapping.comparison_stats
            print(f"\n✓ {mapping.crm_column} → {mapping.export_column}")
            print(f"  Confidence: {mapping.confidence:.2f}")
            print(f"  Discarded invalid data: {stats.discarded_invalid_data}")
            print(f"  Added new data: {stats.added_new_data}")
            print(f"  Fixed data: {stats.fixed_data}")
            print(f"  Good data: {stats.good_data}")
            print(f"  Correct % before: {stats.correct_percentage_before:.1f}%")
            print(f"  Correct % after: {stats.correct_percentage_after:.1f}%")
            print(
                f"  CRM type: {stats.crm_data_type} ({stats.crm_format_count} formats)"
            )
            print(
                f"  Export type: {stats.export_data_type} ({stats.export_format_count} formats)"
            )
        else:
            print(f"\n✗ {mapping.crm_column} → NO MATCH")

    print(f"\nSummary:")
    print(f"  Total mappings processed: {len(enrichment_report.column_mappings)}")
    print(
        f"  Mappings with statistics: {len([m for m in enrichment_report.column_mappings if m.comparison_stats])}"
    )
    print(
        f"  Data modification rate: {(enrichment_report.records_modified_count / enrichment_report.total_rows) * 100:.1f}%"
    )


def save_enrichment_to_database(enrichment_report, filename):
    """
    Save enrichment report to database.

    Args:
        enrichment_report: EnrichmentReportCalculation object with all the statistics
        filename: The filename to generate token from for idempotent saving
    """
    from app.enrichment.database import save_enrichment_report_to_database

    try:
        save_enrichment_report_to_database(enrichment_report, filename)
        print(
            f"✓ Enrichment analysis completed for {enrichment_report.total_rows} rows"
        )
        print(f"✓ Found {len(enrichment_report.column_mappings)} column mappings")
        print(f"✓ Records modified: {enrichment_report.records_modified_count}")
    except Exception as e:
        print(f"Error saving enrichment report to database: {e}")
        import traceback

        traceback.print_exc()
        raise


def enrich(clay_export_filename="clay_export.csv"):
    """
    Main function to analyze enrichment statistics for Clay export data.

    Args:
        clay_export_filename: Name of the Clay export CSV file in Google Drive
    """
    print("Starting enrichment analysis...")

    # Step 1: Connect to Google Drive
    print("Connecting to Google Drive...")
    drive = get_drive_client()
    print("✓ Connected to Google Drive")

    # Step 2: Load Clay export CSV
    print(f"Loading {clay_export_filename} from Google Drive...")
    try:
        df = load_clay_export_from_drive(drive, clay_export_filename)
        print(f"✓ Loaded Clay export data: {len(df)} rows, {len(df.columns)} columns")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    # Step 3: Extract CRM and Export columns
    print("Extracting CRM and export columns...")
    all_columns = df.columns.tolist()

    # Get columns with (crm) and (export) tags - preserve original case
    crm_columns = [col for col in all_columns if "(crm)" in col.lower()]
    export_columns = [col for col in all_columns if "(export)" in col.lower()]

    if not crm_columns:
        print("Error: No CRM columns found (columns ending with '(crm)')")
        return

    if not export_columns:
        print("Error: No export columns found (columns ending with '(export)')")
        return

    print(
        f"✓ Found {len(crm_columns)} CRM columns and {len(export_columns)} export columns"
    )

    # Step 4: Match columns using ColumnMatcher
    print("Matching CRM and export columns...")
    try:
        matcher = ColumnMatcher()
        column_matching_result = matcher.match_columns(df, crm_columns, export_columns)

        successful_mappings = len(
            [m for m in column_matching_result.mappings if m.export_column]
        )
        print(f"✓ Successfully matched {successful_mappings} column pairs")

    except Exception as e:
        print(f"Error during column matching: {e}")
        import traceback

        traceback.print_exc()
        return

    # Step 5: Calculate enrichment statistics
    print("Calculating enrichment statistics...")
    try:
        calculator = EnrichmentStatisticsCalculator()
        enrichment_report = calculator.calculate_statistics(
            df=df,
            column_matching_result=column_matching_result,
            crm_columns=crm_columns,
            export_columns=export_columns,
        )
        print("✓ Enrichment statistics calculated successfully")

    except Exception as e:
        print(f"Error during enrichment calculation: {e}")
        import traceback

        traceback.print_exc()
        return

    # Step 6: Save results to database
    print("Saving enrichment results...")
    try:
        save_enrichment_to_database(enrichment_report, clay_export_filename)
    except Exception as e:
        print(f"Error saving to database: {e}")
        import traceback

        traceback.print_exc()
        return

    # Step 7: Print detailed enrichment report
    print_enrichment_report(enrichment_report)

    print("✓ Enrichment analysis complete!")

    # Print summary without excessive detail
    print(f"\nQuick Summary:")
    print(f"  Total rows analyzed: {enrichment_report.total_rows}")
    print(f"  CRM columns: {enrichment_report.total_crm_columns}")
    print(f"  Export columns: {enrichment_report.total_export_columns}")
    print(f"  Column mappings: {len(enrichment_report.column_mappings)}")
    print(f"  Records modified: {enrichment_report.records_modified_count}")
    print(f"  New columns: {enrichment_report.new_columns_count}")
    print(f"  Many-to-one mappings: {enrichment_report.many_to_one_count}")


if __name__ == "__main__":
    try:
        # Run main function
        enrich()
    except KeyboardInterrupt:
        print("\nEnrichment analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
