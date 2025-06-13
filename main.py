import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from dotenv import load_dotenv

from app.database import check_reports_exist
from app.drive import find_files_in_folder, get_drive_client, list_folders_in_root
from enrichment import process_enrichment_report
from initial import process_initial_report

load_dotenv(override=True)

# Hardcoded root folder ID
ROOT_FOLDER_ID = "1ew2-2rkPnYn29KMyTdWYVlE6o40AOMz9"


def run_initial_report(folder_name: str):
    """
    Process initial report for a folder.

    Args:
        folder_name: Name of the folder (report name)
    """
    try:
        print(f"🔄 Processing initial report for: {folder_name}")
        from app.initial.utils import generate_token_from_company_name

        process_initial_report(folder_name)
        token = generate_token_from_company_name(folder_name)
        print(f"✅ Initial report completed for: {folder_name}")
        print(f"🔑 Token: {token}")
        return {"folder": folder_name, "type": "initial", "status": "success"}
    except Exception as e:
        print(f"❌ Initial report failed for {folder_name}: {str(e)}")
        import traceback

        traceback.print_exc()
        return {
            "folder": folder_name,
            "type": "initial",
            "status": "error",
            "error": str(e),
        }


def run_enrichment_report(folder_name: str):
    """
    Process enrichment report for a folder.

    Args:
        folder_name: Name of the folder (report name)
    """
    try:
        print(f"🔄 Processing enrichment report for: {folder_name}")
        from app.initial.utils import generate_token_from_company_name

        process_enrichment_report(folder_name)
        token = generate_token_from_company_name(folder_name)
        print(f"✅ Enrichment report completed for: {folder_name}")
        print(f"🔑 Token: {token}")
        return {"folder": folder_name, "type": "enrichment", "status": "success"}
    except Exception as e:
        print(f"❌ Enrichment report failed for {folder_name}: {str(e)}")
        import traceback

        traceback.print_exc()
        return {
            "folder": folder_name,
            "type": "enrichment",
            "status": "error",
            "error": str(e),
        }


def main(
    initial_override: Optional[List[str]] = None,
    enrichment_override: Optional[List[str]] = None,
    process_initial: bool = False,
):
    """
    Main orchestrator function to process all reports.

    Args:
        initial_override: List of folder names to force recalculate initial reports
        enrichment_override: List of folder names to force recalculate enrichment reports
        processInitial: Whether to process initial reports (defaults to False)
    """
    if initial_override is None:
        initial_override = []
    if enrichment_override is None:
        enrichment_override = []

    print("🚀 Starting batch report processing...")
    print(f"Root folder ID: {ROOT_FOLDER_ID}")
    print(f"Process initial reports: {process_initial}")
    print(f"Initial overrides: {initial_override}")
    print(f"Enrichment overrides: {enrichment_override}")

    # Step 1: Connect to Google Drive
    print("\n📁 Connecting to Google Drive...")
    try:
        drive = get_drive_client()
        print("✅ Connected to Google Drive")
    except Exception as e:
        print(f"❌ Failed to connect to Google Drive: {e}")
        return

    # Step 2: List all folders in root folder
    print(f"\n📋 Listing folders in root folder...")
    try:
        folder_names = list_folders_in_root(drive, ROOT_FOLDER_ID)
        print(f"✅ Found {len(folder_names)} folders: {folder_names}")
    except Exception as e:
        print(f"❌ Failed to list folders: {e}")
        return

    if not folder_names:
        print("⚠️  No folders found in root folder")
        return

    # Step 3: Process each folder
    tasks_to_process = []

    for folder_name in folder_names:
        print(f"\n🔍 Processing folder: {folder_name}")

        # Find files in the folder
        try:
            files = find_files_in_folder(drive, folder_name, ROOT_FOLDER_ID)
            hubspot_file = files.get("hubspot_file")
            clay_file = files.get("clay_file")
            config_file = files.get("config_file")

            print(f"  HubSpot file: {'✅' if hubspot_file else '❌'}")
            print(f"  Clay file: {'✅' if clay_file else '❌'}")
            print(f"  Config file: {'✅' if config_file else '❌'}")

        except Exception as e:
            print(f"  ❌ Error finding files in {folder_name}: {e}")
            continue

        # Check existing reports in database
        try:
            existing_reports = check_reports_exist(folder_name)
            initial_exists = existing_reports["initial_exists"]
            enrichment_exists = existing_reports["enrichment_exists"]

            print(f"  Initial report exists: {'✅' if initial_exists else '❌'}")
            print(f"  Enrichment report exists: {'✅' if enrichment_exists else '❌'}")

        except Exception as e:
            print(f"  ⚠️  Error checking existing reports for {folder_name}: {e}")
            # Assume reports don't exist if we can't check
            initial_exists = False
            enrichment_exists = False

        # Determine what needs to be processed
        process_initial_report_needed = (
            process_initial
            and hubspot_file is not None
            and (not initial_exists or folder_name in initial_override)
        )

        process_enrichment_needed = clay_file is not None and (
            not enrichment_exists or folder_name in enrichment_override
        )

        # Add to processing queue
        if process_initial_report_needed:
            tasks_to_process.append(
                {
                    "type": "initial",
                    "folder_name": folder_name,
                }
            )
            print(f"  ➕ Added initial report to processing queue")

        if process_enrichment_needed:
            tasks_to_process.append(
                {
                    "type": "enrichment",
                    "folder_name": folder_name,
                }
            )
            print(f"  ➕ Added enrichment report to processing queue")

    if not tasks_to_process:
        print("\n🎉 No reports need processing. All reports are up to date!")
        return

    print(f"\n🚀 Processing {len(tasks_to_process)} reports in parallel...")

    # Step 4: Process all tasks in parallel
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit all tasks
        future_to_task = {}
        for task in tasks_to_process:
            if task["type"] == "initial":
                future = executor.submit(
                    run_initial_report,
                    task["folder_name"],
                )
            else:  # enrichment
                future = executor.submit(run_enrichment_report, task["folder_name"])
            future_to_task[future] = task

        # Collect results as they complete
        for future in as_completed(future_to_task):
            result = future.result()
            results.append(result)

    # Step 5: Print summary
    print(f"\n📊 Processing Summary:")
    print("=" * 50)

    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "error"]

    print(f"Total tasks: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if successful:
        print(f"\n✅ Successful reports:")
        for result in successful:
            print(f"  - {result['folder']} ({result['type']})")

    if failed:
        print(f"\n❌ Failed reports:")
        for result in failed:
            print(f"  - {result['folder']} ({result['type']}): {result['error']}")

    print("\n🎉 Batch processing complete!")


if __name__ == "__main__":
    try:
        # Example usage - you can modify these lists or make them command line arguments
        initial_overrides = []  # Add folder names here to force recalculate initial reports
        enrichment_overrides = [
            "Databricks Summit 2025 – Post‑Event"
        ]  # Add folder names here to force recalculate enrichment reports

        main(
            initial_override=initial_overrides,
            enrichment_override=enrichment_overrides,
            process_initial=False,  # Set to True to enable initial report processing
        )
    except KeyboardInterrupt:
        print("\n❌ Batch processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
