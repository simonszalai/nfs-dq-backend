from pathlib import Path

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


def get_drive_client():
    # ---------- BASIC CONFIG -------------------------------------------------
    gauth = GoogleAuth()
    gauth.settings.update(
        {
            # OAuth client you created in Google Cloud Console
            "client_config_backend": "file",
            "client_config_file": "client_secrets.json",
            # Where to cache the credentials (access- & refresh-token)
            "save_credentials": True,
            "save_credentials_backend": "file",
            "save_credentials_file": "credentials.json",
            # Ask Google for a refresh token the first time
            "get_refresh_token": True,
            "oauth_scope": ["https://www.googleapis.com/auth/drive"],
        }
    )

    # Force Google to give us that refresh-token on the FIRST run
    gauth.auth_params = {  # type: ignore
        "access_type": "offline",  # <- tells Google "I want a refresh token"
        "prompt": "consent",  # <- make Google show the consent screen even if already granted
    }

    cred_file = Path("credentials.json")

    # ---------- AUTH FLOW ----------------------------------------------------
    try:
        if cred_file.exists():
            # Credentials cached – load and refresh if needed
            gauth.LoadCredentialsFile(str(cred_file))
            if gauth.access_token_expired:
                gauth.Refresh()  # silent refresh using the stored refresh_token
                gauth.SaveCredentialsFile(str(cred_file))
            else:
                gauth.Authorize()
            print("✓ Re-authenticated silently with stored refresh token.")
        else:
            # First run – manual copy-paste flow
            auth_url = gauth.GetAuthUrl()  # ➊ get URL
            print("\n*** FIRST-TIME AUTHENTICATION ***")
            print("1. Open the following URL in your browser and grant access:")
            print(auth_url)
            code = input(
                "\n2. Paste the verification code here: "
            ).strip()  # ➋ user pastes code
            gauth.Auth(code)  # ➌ exchange code → access + refresh tokens
            gauth.SaveCredentialsFile(str(cred_file))
            print("✓ Credentials stored; future runs will be headless.")
    except Exception as e:
        raise RuntimeError(f"Authentication failed: {e}")

    # ---------- READY TO USE -------------------------------------------------
    drive = GoogleDrive(gauth)
    print("Google Drive client ready.")

    return drive


def list_folders_in_root(drive, root_folder_id):
    """
    List all folder names in the specified root folder.

    Args:
        drive: Google Drive client
        root_folder_id: ID of the root folder to search in

    Returns:
        List of folder names (report names)
    """
    subfolder_q = (
        f"'{root_folder_id}' in parents "
        "and mimeType='application/vnd.google-apps.folder' "
        "and trashed=false"
    )

    folders = []
    for folder in drive.ListFile({"q": subfolder_q}).GetList():
        folders.append(folder["title"])

    return folders


def find_files_in_folder(drive, folder_name, root_folder_id):
    """
    Find specific files (hubspot CSV, clay CSV, config.json) in a folder.

    Args:
        drive: Google Drive client
        folder_name: Name of the folder to search in
        root_folder_id: ID of the root folder containing the target folder

    Returns:
        Dict with 'hubspot_file', 'clay_file', 'config_file' keys containing file objects or None
    """
    # First find the folder ID
    subfolder_q = (
        f"'{root_folder_id}' in parents "
        "and mimeType='application/vnd.google-apps.folder' "
        "and trashed=false"
    )

    folder_id = None
    for folder in drive.ListFile({"q": subfolder_q}).GetList():
        if folder["title"] == folder_name:
            folder_id = folder["id"]
            break

    if not folder_id:
        return {"hubspot_file": None, "clay_file": None, "config_file": None}

    # Search for files in the folder
    files_q = f"'{folder_id}' in parents and trashed=false"

    result = {"hubspot_file": None, "clay_file": None, "config_file": None}

    for file in drive.ListFile({"q": files_q}).GetList():
        filename_lower = file["title"].lower()

        # Look for hubspot CSV
        if "hubspot" in filename_lower and filename_lower.endswith(".csv"):
            result["hubspot_file"] = file

        # Look for clay CSV
        elif "clay" in filename_lower and filename_lower.endswith(".csv"):
            result["clay_file"] = file

        # Look for config.json
        elif filename_lower == "config.json":
            result["config_file"] = file

    return result
