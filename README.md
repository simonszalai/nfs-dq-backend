# DQ Backend - HubSpot Data Quality Analyzer

This tool analyzes the data quality of HubSpot company records, identifying issues such as empty fields, inconsistent formats, and data quality problems.

## Features

- Connect to Google Drive to load HubSpot data files
- Analyze column population rates
- Detect inconsistent data formats across columns
- AI-powered data quality analysis using Anthropic
- Identify date format inconsistencies
- Save analysis results to a database
- Generate access tokens for viewing reports

## Prerequisites

1. **Python 3.9+** installed
2. **Google Drive API credentials** configured
3. **Database connection** (PostgreSQL recommended)
4. **Anthropic API key** for AI analysis

## Setup

1. Install dependencies using Poetry:

   ```bash
   poetry install
   ```

2. Set required environment variables:

   ```bash
   # Database connection
   export DATABASE_URL="postgresql://username:password@host:port/dbname"

   # Anthropic API key (if using AI analysis)
   export ANTHROPIC_API_KEY="your-api-key-here"
   ```

3. Ensure Google Drive authentication is configured (the app will prompt for authentication on first run)

## Usage

### Basic Usage

Run the analysis for the default company (nofluffselling):

```bash
python main.py
```

### Analyze a Specific Company

```bash
python main.py --company "company_name"
```

### Example Output

```
Starting analysis for company: nofluffselling
Connecting to Google Drive...
✓ Re-authenticated silently with stored refresh token.
Google Drive client ready.
Loading HubSpot files...
Found company: nofluffselling
Company Records: 313
Total Properties: 195
Critical columns to analyze: ['Website URL', 'LinkedIn Company Page', ...]
Analyzing column population...
Detecting inconsistent columns...
Found 10 inconsistent columns
Analyzing data quality with AI...
Generated 13 data quality warnings
Analyzing date formats across columns...
Date formats across columns: 1
Saving analysis results to database...
Analysis saved successfully! Access token: 15f19394fd7fdfc87a6f612125c14d423f31ef50a3dc8286

Analysis complete!

=== ANALYSIS SUMMARY ===
Company: nofluffselling
Total Records: 313
Total Properties: 195
Inconsistent Columns: 10
Data Quality Warnings: 13
Unique Date Formats: 1
Access Token: 15f19394fd7fdfc87a6f612125c14d423f31ef50a3dc8286
========================
```

## What the Analysis Includes

1. **Column Population Analysis**: Identifies empty or sparsely populated columns
2. **Format Consistency**: Detects columns with multiple data formats (e.g., phone numbers, dates)
3. **Data Quality Warnings**: AI-powered analysis to identify:
   - Inconsistent formatting
   - Data quality issues
   - Duplicate data
   - Invalid entries
4. **Date Format Analysis**: Counts unique date formats across all date columns
5. **Critical Column Analysis**: Focuses on key business fields like:
   - Company information (Website, LinkedIn, Industry)
   - Financial data (Revenue, Funding)
   - Size and structure (Employee count, Locations)

## Access Token

The analysis generates a unique access token based on the company name. This token can be used to:

- View the analysis report in a web interface (if available)
- Share results with team members
- Track analysis history

## Troubleshooting

- **Google Drive authentication fails**: Delete stored credentials and re-authenticate
- **Database connection error**: Verify DATABASE_URL is correctly set
- **Company not found**: Check available companies in the Google Drive data
- **AI analysis fails**: Verify ANTHROPIC_API_KEY is set and valid

## Development

The main components are located in the `app/` directory:

- `drive.py`: Google Drive client
- `load_data.py`: Data loading utilities
- `col_population.py`: Column population analysis
- `detect_inconsistent_cols.py`: Format consistency detection
- `anthropic.py`: AI-powered data quality analysis
- `date_formats_across_cols.py`: Date format analysis
- `save_to_db.py`: Database persistence
- `models.py`: Database models
