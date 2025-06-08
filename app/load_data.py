from io import StringIO

import pandas as pd

MASTER_FOLDER_ID = "1ew2-2rkPnYn29KMyTdWYVlE6o40AOMz9"


def load_hubspot_files(drive, master_folder_id=MASTER_FOLDER_ID):
    """
    Returns {company_name: DataFrame} for every sub-folder that
    contains a file whose title includes 'hubspot'.
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

        # 2️⃣  list all files inside this sub-folder
        files_q = f"'{company_id}' in parents and trashed=false"
        for f in drive.ListFile({"q": files_q}).GetList():
            if (
                "hubspot" in f["title"].lower()
                and f["mimeType"] != "application/vnd.google-apps.folder"
            ):
                # 3️⃣  download & parse the CSV
                csv_str = f.GetContentString(mimetype="text/csv")
                company_data[company_name] = pd.read_csv(StringIO(csv_str))
                break  # stop after the first matching HubSpot file

    return company_data
