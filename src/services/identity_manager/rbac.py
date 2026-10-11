# Most simple and naive RBAC implementation
# Got the inspiration from Auth0 -
# - https://github.com/auth0-developer-hub/api_fastapi_python_hello-world
# - https://developer.auth0.com/resources/code-samples/api/fastapi/basic-role-based-access-control#set-up-role-based-access-control-rbac

# The scope convention {verb}:{resource} is inspired by Auth0's RBAC

# Note that since we don't use Auth0's RBAC, I just took the concepts but left the implementation more simple

# TODO: move resources (alert, rule, etc.) to class constants
# TODO: move verbs (read, write, delete, update) to class constants
# TODO: custom roles
# TODO: implement a solid RBAC mechanism (probably OPA over Keycloak)


import enum

from fastapi import HTTPException


class Roles(enum.Enum):
    SUPERADMIN = "superadmin"
    ADMIN = "admin"
    NOC = "noc"
    EDITOR = "editor"
    VIEWER = "viewer"
    WEBHOOK = "webhook"


class Role:
    @classmethod
    def get_name(cls):
        return cls.__name__.lower()

    @classmethod
    def has_scopes(cls, scopes: list[str]) -> bool:
        required_scopes = set(scopes)
        available_scopes = set(cls.SCOPES)

        for scope in required_scopes:
            # First, check if the scope is available
            if scope in available_scopes:
                # Exact match, on to the next scope
                continue

            # If not, check if there's a wildcard permission for this action
            scope_parts = scope.split(":")
            if len(scope_parts) != 2:
                return False  # Invalid scope format
            action, resource = scope_parts
            if f"{action}:*" not in available_scopes:
                return False  # No wildcard permission for this action
        # All scopes are available
        return True


# Noc has read permissions and it can assign itself to alert
class Noc(Role):
    SCOPES = ["read:*"]
    DESCRIPTION = "read permissions and assign itself to alert"


# Viewer is read-only within the tenants it has access to (VENA-5596 §3.5).
class Viewer(Role):
    SCOPES = ["read:*"]
    DESCRIPTION = "read-only within a tenant"


# Editor operates fully within a tenant, minus tenant permission management
# (VENA-5596 §3.5 -- formerly `noc`).
class Editor(Role):
    SCOPES = ["read:*", "write:*", "delete:*", "update:*", "execute:*"]
    DESCRIPTION = "full operate within a tenant, minus permission management"


# Admin has all permissions
class Admin(Role):
    SCOPES = ["read:*", "write:*", "delete:*", "update:*", "execute:*"]
    DESCRIPTION = "do everything"


# Superadmin is a global admin across all tenants and is the only role that may
# create a tenant (VENA-5596 §3.5 / §3.7).
class SuperAdmin(Role):
    SCOPES = [
        "read:*",
        "write:*",
        "delete:*",
        "update:*",
        "execute:*",
        "create:*",
    ]
    DESCRIPTION = "global admin across all tenants; can create tenants"


# Webhook has write:alert permission to write alerts
# this is internal role used by API keys
class Webhook(Role):
    SCOPES = ["write:alert", "write:incident"]
    DESCRIPTION = "write alerts using API keys"



def get_role_by_role_name(role_name: str) -> list[str]:
    if role_name == Roles.SUPERADMIN.value:
        return SuperAdmin
    elif role_name == Roles.ADMIN.value:
        return Admin
    elif role_name == Roles.NOC.value:
        return Noc
    elif role_name == Roles.EDITOR.value:
        return Editor
    elif role_name == Roles.VIEWER.value:
        return Viewer
    elif role_name == Roles.WEBHOOK.value:
        return Webhook

    else:
        raise HTTPException(
            status_code=403,
            detail=f"Role {role_name} not found",
        )
