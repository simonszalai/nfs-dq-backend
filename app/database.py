import os
from typing import List

from sqlmodel import Session, SQLModel, create_engine, select

from app.initial.models import FieldModel, GlobalIssue, Report, Warning
from app.initial.utils import generate_token_from_company_name


def save_report_to_database(
    company_name: str,
    report: Report,
    fields: List[FieldModel],
    warnings: List[Warning],
    global_issues: List[GlobalIssue],
) -> None:
    """
    Save a report and all its related data to the database.

    This function is idempotent - it will delete any existing report
    for the same company before saving the new one.

    Args:
        company_name: Name of the company
        report: The Report object to save
        fields: List of FieldModel objects
        warnings: List of Warning objects
        global_issues: List of GlobalIssue objects
    """
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("Warning: DATABASE_URL not set. Skipping database save.")
        return

    engine = create_engine(DATABASE_URL)

    # Create tables if they don't exist
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        # First, check if a report with this token already exists and delete it
        token = generate_token_from_company_name(company_name)
        existing_report = session.exec(
            select(Report).where(Report.token == token)
        ).first()

        if existing_report:
            print(f"Found existing report for {company_name}, removing old data...")

            # Delete all warnings for fields of this report
            for field in existing_report.fields:
                for warning in field.warnings:
                    session.delete(warning)
                session.delete(field)

            # Delete all global issues for this report
            for issue in existing_report.global_issues:
                session.delete(issue)

            # Delete the report itself
            session.delete(existing_report)
            session.commit()
            print("✓ Removed old report data")

        # Save report
        session.add(report)
        session.flush()  # Flush to get the report ID for foreign keys

        # Save fields
        for field in fields:
            session.add(field)

        # Save warnings
        for warning in warnings:
            session.add(warning)

        # Save global issues
        for issue in global_issues:
            session.add(issue)

        # Commit all changes
        session.commit()
        print(f"✓ Analysis saved to database with report ID: {report.id}")
        print(f"✓ Report token: {report.token}")
