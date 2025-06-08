"""
Database saving functionality for HubSpot data quality analysis

Environment Configuration:
Set DATABASE_URL environment variable before running:
  export DATABASE_URL="postgresql://username:password@host:port/dbname"

Example:
  export DATABASE_URL="postgresql://postgres:mypassword@localhost:5432/nfs_data_quality"
"""

import hashlib
import os
from datetime import datetime
from typing import Any, Dict, List

from sqlmodel import Session, SQLModel, create_engine, select

# Import the models we created earlier
from .models import FieldModel, GlobalIssue, Report, Severity, Warning, WarningType

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


def create_db_and_tables():
    """Create tables if they don't exist"""
    SQLModel.metadata.create_all(engine)


def generate_token_from_company_name(company_name: str) -> str:
    """Generate a deterministic 48-character token from company name"""
    return hashlib.sha256(company_name.encode()).hexdigest()[:48]


def _get_populated_count(
    column_name: str, population_results: Dict[str, Any], total_records: int
) -> int:
    """Calculate populated count for a column"""
    # Check if empty
    for result in population_results:
        if result["column_name"] == column_name:
            return result["populated_count"]

    # Default to total records if not found
    return total_records


def _create_field_warnings(
    column_name: str,
    populated_count: int,
    total_records: int,
    inconsistent_cols: Dict[str, Dict[str, Any]],
    ai_warnings: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Create warnings for a field"""
    warnings = []

    # Empty field warning
    if populated_count == 0:
        warnings.append(
            {
                "type": WarningType.EMPTY_FIELD,
                "message": "This field is completely empty across all records",
                "severity": Severity.HIGH,
            }
        )
    # Low population warning
    elif populated_count < total_records * 0.25:  # Less than 25%
        percentage = round((populated_count / total_records) * 100)
        warnings.append(
            {
                "type": WarningType.LOW_POPULATION,
                "message": f"Field is only {percentage}% populated",
                "severity": Severity.HIGH if percentage < 10 else Severity.MEDIUM,
            }
        )

    # Inconsistent format warning
    if column_name in inconsistent_cols:
        format_count = inconsistent_cols[column_name].get("format_count", 0)
        warnings.append(
            {
                "type": WarningType.INCONSISTENT_FORMAT,
                "message": f"Field has {format_count} different formats",
                "severity": Severity.MEDIUM,
                "meta": {"formats_detected": inconsistent_cols[column_name]},
            }
        )

    # AI-detected warnings (excluding duplicates)
    if column_name in ai_warnings:
        for warning in ai_warnings[column_name]:
            # Skip AI warnings for empty/low population (already handled)
            if warning["type"] in ["LOW_POPULATION", "EMPTY_FIELD"]:
                continue

            warning_type_map = {
                "INCONSISTENT_FORMAT": WarningType.INCONSISTENT_FORMAT,
                "DATA_QUALITY": WarningType.DATA_QUALITY,
                "DUPLICATE_DATA": WarningType.DUPLICATE_DATA,
            }

            warnings.append(
                {
                    "type": warning_type_map.get(warning["type"], WarningType.OTHER),
                    "message": warning["message"],
                    "severity": Severity[warning["severity"]],
                    "meta": {
                        "affected_count": warning.get("affected_count"),
                        "examples": warning.get("examples", []),
                    },
                }
            )

    return warnings


def _create_global_issues(
    fields_with_issues: int,
    total_fields: int,
    empty_count: int,
    sparse_count: int,
    unique_date_format_count: int,
) -> List[Dict[str, Any]]:
    """Create global issues based on analysis results"""
    issues = []

    if empty_count > 0:
        issues.append(
            {
                "type": "data_quality",
                "title": "Empty Columns Detected",
                "description": f"{empty_count} columns are completely empty across all records",
                "severity": Severity.HIGH,
            }
        )

    if sparse_count > 0:
        issues.append(
            {
                "type": "data_quality",
                "title": "Sparse Data Coverage",
                "description": f"{sparse_count} columns have less than 25% data coverage",
                "severity": Severity.MEDIUM,
            }
        )

    if unique_date_format_count > 1:
        issues.append(
            {
                "type": "data_quality",
                "title": "Inconsistent Date Formats",
                "description": f"Found {unique_date_format_count} different date formats across columns",
                "severity": Severity.MEDIUM,
            }
        )

    # Overall data quality
    if total_fields > 0:
        issue_percentage = round((fields_with_issues / total_fields) * 100)
        if issue_percentage > 50:
            issues.append(
                {
                    "type": "data_quality",
                    "title": "Poor Overall Data Quality",
                    "description": f"{issue_percentage}% of fields have data quality issues",
                    "severity": Severity.CRITICAL
                    if issue_percentage > 70
                    else Severity.HIGH,
                }
            )

    return issues


def save_analysis_to_database(
    company_name: str,
    company_records: int,
    total_properties: int,
    population_results: Dict[str, Any],
    inconsistent_cols: Dict[str, Dict[str, Any]],
    classified_cols: Dict[str, Dict[str, Any]],
    warnings: Dict[str, List[Dict[str, Any]]],
    unique_date_format_count: int,
    column_config: Dict,
) -> str:
    """Save the analysis results to the database (idempotent)"""

    token = generate_token_from_company_name(company_name)

    with Session(engine) as session:
        # Get or create report
        report = session.exec(select(Report).where(Report.token == token)).first()

        # Count issues
        empty_fields = [
            r["column_name"] for r in population_results if r["populated_count"] == 0
        ]
        sparse_fields = [
            r["column_name"]
            for r in population_results
            if 0 < r["populated_count"] < company_records * 0.25
        ]
        fields_with_warnings = (
            set(empty_fields)
            | set(sparse_fields)
            | set(inconsistent_cols.keys())
            | set(warnings.keys())
        )

        if report:
            # Update existing report
            report.total_records = company_records
            report.total_fields = total_properties
            report.fields_with_issues = len(fields_with_warnings)
            report.config = column_config
            report.generated_at = datetime.utcnow()

            # Clear existing data
            for field in report.fields:
                session.delete(field)
            for issue in report.global_issues:
                session.delete(issue)
            session.flush()
        else:
            # Create new report
            report = Report(
                token=token,
                company_name=company_name,
                total_records=company_records,
                total_fields=total_properties,
                fields_with_issues=len(fields_with_warnings),
                config=column_config,
            )
            session.add(report)
            session.flush()

        # Process all fields from population results
        for field_data in population_results:
            column_name = field_data["column_name"]
            populated_count = field_data["populated_count"]

            # Create field
            field = FieldModel(
                report_id=report.id,
                column_name=column_name,
                populated_count=populated_count,
                inferred_type=classified_cols.get(column_name, {}).get(
                    "type", "unknown"
                ),
                format_count=inconsistent_cols.get(column_name, {}).get("format_count"),
            )
            session.add(field)
            session.flush()

            # Create warnings
            field_warnings = _create_field_warnings(
                column_name,
                populated_count,
                company_records,
                inconsistent_cols,
                warnings,
            )

            for warning_data in field_warnings:
                warning = Warning(
                    field_id=field.id,
                    type=warning_data["type"],
                    message=warning_data["message"],
                    severity=warning_data["severity"],
                    meta=warning_data.get("meta"),
                )
                session.add(warning)

        # Create global issues
        global_issues = _create_global_issues(
            len(fields_with_warnings),
            total_properties,
            len(empty_fields),
            len(sparse_fields),
            unique_date_format_count,
        )

        for issue_data in global_issues:
            issue = GlobalIssue(
                report_id=report.id,
                type=issue_data["type"],
                title=issue_data["title"],
                description=issue_data["description"],
                severity=issue_data["severity"],
                meta=issue_data.get("meta"),
            )
            session.add(issue)

        session.commit()
        print(
            f"Report {'updated' if report else 'saved'} successfully with token: {token}"
        )
        return token
