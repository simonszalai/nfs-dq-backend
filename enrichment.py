import sys
from io import StringIO

import pandas as pd
from dotenv import load_dotenv

from app.anthropic.column_matcher import ColumnMatcher
from app.drive import find_files_in_folder, get_drive_client
from app.enrichment.enrichment_calculator import EnrichmentStatisticsCalculator
from app.load_data import write_enrichment_output_to_drive

MASTER_FOLDER_ID = "1ew2-2rkPnYn29KMyTdWYVlE6o40AOMz9"

load_dotenv(override=True)


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

    Returns:
        str: The token generated for the enrichment report
    """
    from app.enrichment.database import save_enrichment_report_to_database
    from app.initial.utils import generate_token_from_company_name

    try:
        save_enrichment_report_to_database(enrichment_report, filename)
        token = generate_token_from_company_name(filename)
        print(
            f"✓ Enrichment analysis completed for {enrichment_report.total_rows} rows"
        )
        print(f"✓ Found {len(enrichment_report.column_mappings)} column mappings")
        print(f"✓ Records modified: {enrichment_report.records_modified_count}")
        return token
    except Exception as e:
        print(f"Error saving enrichment report to database: {e}")
        import traceback

        traceback.print_exc()
        raise


def process_enrichment_report(folder_name: str):
    """
    Process enrichment report for Clay export data in a specific folder.

    Args:
        folder_name: Name of the folder containing Clay export data
    """
    print(f"Starting enrichment analysis for folder: {folder_name}")

    # Step 1: Connect to Google Drive
    print("Connecting to Google Drive...")
    drive = get_drive_client()
    print("✓ Connected to Google Drive")

    # Step 2: Find Clay export CSV in the folder
    print(f"Finding Clay export file in {folder_name} folder...")
    files = find_files_in_folder(drive, folder_name, MASTER_FOLDER_ID)

    clay_file = files.get("clay_file")

    if clay_file is None:
        raise FileNotFoundError(f"No Clay CSV file found in {folder_name} folder")

    print(f"✓ Found Clay file: {clay_file['title']}")

    # Step 3: Load Clay export CSV
    print("Loading Clay export file...")
    csv_str = clay_file.GetContentString(mimetype="text/csv")
    df = pd.read_csv(StringIO(csv_str))
    print(f"✓ Loaded Clay export data: {len(df)} rows, {len(df.columns)} columns")

    # Step 4: Extract CRM and Export columns
    print("Extracting CRM and export columns...")
    all_columns = df.columns.tolist()

    # Get columns with (crm) and (export) tags - preserve original case
    crm_columns = [col for col in all_columns if "(crm)" in col.lower()]
    export_columns = [col for col in all_columns if "(export)" in col.lower()]

    if not crm_columns:
        raise ValueError("No CRM columns found (columns containing '(crm)')")

    if not export_columns:
        raise ValueError("No export columns found (columns containing '(export)')")

    print(
        f"✓ Found {len(crm_columns)} CRM columns and {len(export_columns)} export columns"
    )

    # Step 5: Match columns using ColumnMatcher
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
        raise

    # Step 6: Calculate enrichment statistics
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
        raise

    # Step 7: Save results to database
    print("Saving enrichment results...")
    try:
        token = save_enrichment_to_database(enrichment_report, folder_name)
    except Exception as e:
        print(f"Error saving to database: {e}")
        import traceback

        traceback.print_exc()
        raise

    # Step 8: Write output.json to Google Drive
    print("Writing output to Google Drive...")
    try:
        report_url = f"https://nfs-dq-frontend.onrender.com/enrichments/{token}"
        write_enrichment_output_to_drive(drive, clay_file["title"], report_url)
    except Exception as e:
        print(f"Error writing output to Google Drive: {e}")
        import traceback

        traceback.print_exc()
        # Don't raise here - continue with the report printing

    # Step 9: Print detailed enrichment report
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
        # Run function with default folder name
        process_enrichment_report("nofluffselling")
    except KeyboardInterrupt:
        print("\nEnrichment analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
