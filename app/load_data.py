import json
from io import StringIO

import pandas as pd

MASTER_FOLDER_ID = "1ew2-2rkPnYn29KMyTdWYVlE6o40AOMz9"


def load_hubspot_files(drive, master_folder_id=MASTER_FOLDER_ID):
    """
    Returns {company_name: dict} for every sub-folder that
    contains files. Dict can contain 'hubspot' (DataFrame) and 'config' (dict).
    """
    company_data = {}

    # 1️⃣  list all *sub-folders* in the master folder
    subfolder_q = (
        f"'{master_folder_id}' in parents "
        "and mimeType='application/vnd.google-apps.folder' "
        "and trashed=false"
    )
    for sub in drive.ListFile({"q": subfolder_q}).GetList():
        company_id = sub["id"]
        company_name = sub["title"]
        print(f"Found company: {company_name}")

        company_files = {}

        # 2️⃣  list all files inside this sub-folder
        files_q = f"'{company_id}' in parents and trashed=false"
        for f in drive.ListFile({"q": files_q}).GetList():
            # Check for HubSpot CSV files
            if (
                "hubspot" in f["title"].lower()
                and f["mimeType"] != "application/vnd.google-apps.folder"
            ):
                # 3️⃣  download & parse the CSV
                csv_str = f.GetContentString(mimetype="text/csv")
                company_files["hubspot"] = pd.read_csv(StringIO(csv_str))

            # Check for config JSON files
            elif (
                "config" in f["title"].lower()
                and f["title"].lower().endswith(".json")
                and f["mimeType"] != "application/vnd.google-apps.folder"
            ):
                # Download & parse the JSON
                json_str = f.GetContentString()
                company_files["config"] = json.loads(json_str)

        # Only add company if we found at least one file
        if company_files:
            company_data[company_name] = company_files

    return company_data


def write_output_to_drive(
    drive, company_name, report_url, master_folder_id=MASTER_FOLDER_ID
):
    """
    Write output.json file to the company's Google Drive folder with the report URL.

    Args:
        drive: Google Drive client
        company_name: Name of the company folder
        report_url: URL of the generated report
        master_folder_id: ID of the master folder containing company subfolders
    """
    # Find the company folder
    subfolder_q = (
        f"'{master_folder_id}' in parents "
        "and mimeType='application/vnd.google-apps.folder' "
        "and trashed=false"
    )

    company_folder_id = None
    for sub in drive.ListFile({"q": subfolder_q}).GetList():
        if sub["title"] == company_name:
            company_folder_id = sub["id"]
            break

    if not company_folder_id:
        raise ValueError(f"Company folder '{company_name}' not found in Google Drive")

    # Create output data
    output_data = {"report_url": report_url}

    # Check if output.json already exists in the folder
    files_q = f"'{company_folder_id}' in parents and trashed=false"
    existing_output_file = None
    for f in drive.ListFile({"q": files_q}).GetList():
        if f["title"].lower() == "output.json":
            existing_output_file = f
            break

    # Create or update the output.json file
    if existing_output_file:
        # Update existing file
        existing_output_file.SetContentString(json.dumps(output_data, indent=2))
        existing_output_file.Upload()
        print(f"✓ Updated existing output.json in {company_name} folder")
    else:
        # Create new file
        output_file = drive.CreateFile(
            {
                "title": "output.json",
                "parents": [{"id": company_folder_id}],
                "mimeType": "application/json",
            }
        )
        output_file.SetContentString(json.dumps(output_data, indent=2))
        output_file.Upload()
        print(f"✓ Created output.json in {company_name} folder")


def write_enrichment_output_to_drive(drive, enrichment_filename, report_url):
    """
    Write enrichment_output.json file to the enrichment file's Google Drive folder with the report URL.

    Args:
        drive: Google Drive client
        enrichment_filename: Name of the enrichment file (e.g., clay_export.csv)
        report_url: URL of the generated enrichment report
        master_folder_id: ID of the master folder containing files
    """
    # Search for the enrichment file to determine its parent folder
    search_query = f"title='{enrichment_filename}' and trashed=false"
    files = drive.ListFile({"q": search_query}).GetList()

    if not files:
        raise ValueError(
            f"Enrichment file '{enrichment_filename}' not found in Google Drive"
        )

    # Get the first file's parent folder
    enrichment_file = files[0]
    parent_folders = enrichment_file["parents"]

    if not parent_folders:
        raise ValueError(
            f"Enrichment file '{enrichment_filename}' has no parent folder"
        )

    folder_id = parent_folders[0]["id"]

    # Create output data
    output_data = {"report_url": report_url}

    # Check if enrichment_output.json already exists in the folder
    files_q = f"'{folder_id}' in parents and trashed=false"
    existing_output_file = None
    for f in drive.ListFile({"q": files_q}).GetList():
        if f["title"].lower() == "enrichment_output.json":
            existing_output_file = f
            break

    # Create or update the enrichment_output.json file
    if existing_output_file:
        # Update existing file
        existing_output_file.SetContentString(json.dumps(output_data, indent=2))
        existing_output_file.Upload()
        print(f"✓ Updated existing enrichment_output.json for {enrichment_filename}")
    else:
        # Create new file
        output_file = drive.CreateFile(
            {
                "title": "enrichment_output.json",
                "parents": [{"id": folder_id}],
                "mimeType": "application/json",
            }
        )
        output_file.SetContentString(json.dumps(output_data, indent=2))
        output_file.Upload()
        print(f"✓ Created enrichment_output.json for {enrichment_filename}")
