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
