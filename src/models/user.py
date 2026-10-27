from typing import List, Optional, Set

from pydantic import BaseModel, Extra


class Group(BaseModel, extra=Extra.ignore):
    id: str
    name: str
    # Full Keycloak group path (e.g. "/keep-org-a-admin"). This is what a token
    # carries and what tenant_role_grant.subject must match (VENA-5596).
    path: Optional[str] = None
    roles: list[str] = []
    members: list[str] = []
    memberCount: int = 0


class User(BaseModel, extra=Extra.ignore):
    email: str
    # The Keycloak username -- this is what the token carries as preferred_username
    # and what a user grant should be keyed by (VENA-5596).
    username: Optional[str] = None
    name: str
    role: Optional[str] = None
    picture: Optional[str]
    created_at: str
    last_login: Optional[str]
    ldap: Optional[bool] = False
    groups: Optional[list[Group]] = []


class Role(BaseModel):
    id: str
    name: str
    description: str
    scopes: Set[str]
    predefined: bool = True


class CreateOrUpdateRole(BaseModel):
    name: Optional[str]
    description: Optional[str]
    scopes: Optional[Set[str]]


class PermissionEntity(BaseModel):
    id: str  # permission id
    type: str  # 'user' or 'group'
    name: Optional[str]  # permission name


class ResourcePermission(BaseModel):
    resource_id: str
    resource_name: str
    resource_type: str
    permissions: List[PermissionEntity]
