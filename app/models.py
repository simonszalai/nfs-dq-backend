import uuid
from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from sqlmodel import JSON, Column, Field, Relationship, SQLModel


class WarningType(str, Enum):
    EMPTY_FIELD = "EMPTY_FIELD"
    LOW_POPULATION = "LOW_POPULATION"
    INCONSISTENT_FORMAT = "INCONSISTENT_FORMAT"
    DUPLICATE_DATA = "DUPLICATE_DATA"
    DEPRECATED_FIELD = "DEPRECATED_FIELD"
    DATA_QUALITY = "DATA_QUALITY"
    OTHER = "OTHER"


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Report(SQLModel, table=True):
    __tablename__ = "Report"

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    token: str = Field(default_factory=lambda: str(uuid4()), unique=True)
    company_name: str = Field(alias="companyName")
    generated_at: datetime = Field(default_factory=datetime.utcnow, alias="generatedAt")

    # Summary statistics
    total_records: int = Field(alias="totalRecords")
    total_fields: int = Field(alias="totalFields")
    fields_with_issues: int = Field(alias="fieldsWithIssues")

    # Critical columns configuration
    config: dict = Field(sa_column=Column(JSON))

    created_at: datetime = Field(default_factory=datetime.utcnow, alias="createdAt")

    # Relationships
    fields: List["FieldModel"] = Relationship(back_populates="report")
    global_issues: List["GlobalIssue"] = Relationship(back_populates="report")


class FieldModel(SQLModel, table=True):
    __tablename__ = "Field"

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    report_id: str = Field(
        foreign_key="Report.id", alias="reportId"
    )  # Changed from "report.id" to "Report.id"

    column_name: str = Field(alias="columnName")
    populated_count: int = Field(alias="populatedCount")
    inferred_type: str = Field(alias="inferredType")
    format_count: Optional[int] = Field(default=None, alias="formatCount")

    # Relationships
    report: Report = Relationship(back_populates="fields")
    warnings: List["Warning"] = Relationship(back_populates="field")


class Warning(SQLModel, table=True):
    __tablename__ = "Warning"

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    field_id: str = Field(
        foreign_key="Field.id", alias="fieldId"
    )  # Changed from "field.id" to "Field.id"

    type: WarningType
    message: str
    severity: Severity
    meta: Optional[dict] = Field(default=None, sa_column=Column(JSON))

    # Relationship
    field: FieldModel = Relationship(back_populates="warnings")


class GlobalIssue(SQLModel, table=True):
    __tablename__ = "GlobalIssue"

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    report_id: str = Field(
        foreign_key="Report.id", alias="reportId"
    )  # Changed from "report.id" to "Report.id"

    type: str
    title: str
    description: str
    severity: Severity
    meta: Optional[dict] = Field(default=None, sa_column=Column(JSON))

    # Relationship
    report: Report = Relationship(back_populates="global_issues")
