import argparse
import sys

from app.anthropic import DataQualityAnalyzer
from app.col_population import analyze_column_population
from app.date_formats_across_cols import count_unique_date_formats
from app.detect_inconsistent_cols import classify_cols, detect_inconsistent_cols

# Import all required modules
from app.drive import get_drive_client
from app.load_data import load_hubspot_files
from app.save_to_db import save_analysis_to_database


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
    hubspot_df = all_companies[company_name]

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

    # Calculate basic metrics
    company_records = len(hubspot_df)
    total_properties = len(hubspot_df.columns)

    print(f"Company Records: {company_records}")
    print(f"Total Properties: {total_properties}")

    # Flatten critical columns from the config
    critical_cols = []
    for category in column_config["critical_columns"].values():
        critical_cols.extend([col for col in category.values() if col])

    print(f"Critical columns to analyze: {critical_cols}")

    # Step 3: Analyze column population
    print("Analyzing column population...")
    population_results = analyze_column_population(hubspot_df)

    # Step 4: Detect inconsistent columns
    print("Detecting inconsistent columns...")
    try:
        inconsistent_cols = detect_inconsistent_cols(hubspot_df, threshold=0.67)
        print(f"Found {len(inconsistent_cols)} inconsistent columns")
    except Exception as e:
        print(f"Error detecting inconsistent columns: {str(e)}")
        inconsistent_cols = {}

    # Step 5: Analyze data quality with AI
    print("Analyzing data quality with AI...")
    try:
        analyzer = DataQualityAnalyzer()
        warnings = analyzer.analyze_dataframe(hubspot_df)
        print(f"Generated {len(warnings)} data quality warnings")
    except Exception as e:
        print(f"Error in AI analysis: {str(e)}")
        warnings = {}

    # Step 6: Analyze date formats
    print("Analyzing date formats across columns...")
    try:
        classified_cols = classify_cols(hubspot_df)
        unique_date_format_count = count_unique_date_formats(
            classified_cols, hubspot_df
        )
        print(f"Date formats across columns: {unique_date_format_count}")
    except Exception as e:
        print(f"Error analyzing date formats: {str(e)}")
        classified_cols = {}
        unique_date_format_count = 0

    # Step 7: Save to database
    print("Saving analysis results to database...")
    try:
        token = save_analysis_to_database(
            company_name,
            company_records,
            total_properties,
            population_results,
            inconsistent_cols,
            classified_cols,
            warnings,
            unique_date_format_count,
            column_config,
        )
        print(f"Analysis saved successfully! Access token: {token}")
    except Exception as e:
        print(f"Error saving to database: {str(e)}")
        return

    print("\nAnalysis complete!")

    # Print summary
    print("\n=== ANALYSIS SUMMARY ===")
    print(f"Company: {company_name}")
    print(f"Total Records: {company_records}")
    print(f"Total Properties: {total_properties}")
    print(f"Inconsistent Columns: {len(inconsistent_cols)}")
    print(f"Data Quality Warnings: {len(warnings)}")
    print(f"Unique Date Formats: {unique_date_format_count}")
    print(f"Access Token: {token}")
    print("========================")


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Analyze HubSpot data quality for a company"
    )
    parser.add_argument(
        "--company",
        type=str,
        default="nofluffselling",
        help="Company name to analyze (default: nofluffselling)",
    )

    # Parse arguments
    args = parser.parse_args()

    try:
        # Run main function
        main(company_name=args.company)
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        sys.exit(1)
