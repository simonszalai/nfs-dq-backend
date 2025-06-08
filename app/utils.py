import hashlib


def generate_token_from_company_name(company_name: str) -> str:
    """Generate a deterministic 48-character token from company name"""
    return hashlib.sha256(company_name.encode()).hexdigest()[:48]
