from datetime import datetime

from sqlmodel import Field, SQLModel

from src.repositories.dependencies import GENERIC_TENANT_UUID


class User(SQLModel, table=True):
    # Unique ID for each user
    id: int = Field(primary_key=True)

    tenant_id: str = Field(default=GENERIC_TENANT_UUID)

    # Username for the user (should be unique)
    username: str = Field(index=True, unique=True)

    # Hashed password (never store plain-text passwords)
    password_hash: str

    # Role
    role: str

    # Timestamp for the last sign-in of the user
    last_sign_in: datetime = Field(default=None)

    # Account creation timestamp
    created_at: datetime = Field(default_factory=datetime.utcnow)
