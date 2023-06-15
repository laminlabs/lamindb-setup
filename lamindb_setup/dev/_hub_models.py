from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy.sql import func
from sqlmodel import Field, MetaData, SQLModel, UniqueConstraint

CreatedAt = Field(index=True, sa_column_kwargs=dict(server_default=func.now()))
UpdatedAt = Field(default=None, index=True, sa_column_kwargs=dict(onupdate=func.now()))


class User(SQLModel, table=True):  # type: ignore
    __tablename__ = "users"
    metadata = MetaData(schema="auth")
    id: Optional[UUID] = Field(primary_key=True)


class Account(SQLModel, table=True):  # type: ignore
    """Accounts."""

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    user_id: Optional[UUID] = Field(foreign_key=User.id, index=True)
    """Maybe None because it may be an organizational account."""
    lnid: str = Field(index=True, unique=True)
    """User-facing base62 ID."""
    handle: str = Field(index=True, unique=True)
    name: Optional[str] = Field(default=None, index=True)
    bio: Optional[str] = None
    website: Optional[str] = None
    github_handle: Optional[str] = None
    twitter_handle: Optional[str] = None
    linkedin_handle: Optional[str] = None
    avatar_url: Optional[str] = None
    created_at: datetime = CreatedAt
    updated_at: Optional[datetime] = UpdatedAt


class Storage(SQLModel, table=True):  # type: ignore
    """Storage locations.

    A dobject or run-associated file can be stored in any desired S3,
    GCP, Azure or local storage location. This table tracks these locations
    along with metadata.
    """

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    lnid: str = Field(index=True)
    """User-facing base62 ID."""
    created_by: UUID = Field(foreign_key=Account.id, index=True)
    """ID of owning account."""
    root: str = Field(index=True, unique=True)
    """An s3 path, a local path, etc."""  # noqa
    type: Optional[str] = None
    """Local vs. s3 vs. gcp etc."""
    region: Optional[str] = None
    """Cloud storage region if applicable."""
    created_at: datetime = CreatedAt
    updated_at: Optional[datetime] = UpdatedAt


class Instance(SQLModel, table=True):  # type: ignore
    """Instances."""

    __table_args__ = (UniqueConstraint("account_id", "name"),)
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    account_id: UUID = Field(foreign_key=Account.id, index=True)
    """ID of owning account."""
    name: str
    """Instance name."""
    storage_id: UUID = Field(foreign_key=Storage.id, index=True)
    """Default storage for loading an instance."""
    db: Optional[str] = Field(default=None, unique=True)
    """Database connection string. None for SQLite."""
    schema_str: Optional[str] = None
    """Comma-separated string of schema modules."""
    description: Optional[str] = None
    """Short text describing the instance."""
    public: Optional[bool] = False
    """Flag indicating if the instance is publicly visible."""
    created_at: datetime = CreatedAt
    updated_at: Optional[datetime] = UpdatedAt
