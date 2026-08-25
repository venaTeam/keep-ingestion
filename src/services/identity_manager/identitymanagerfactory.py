"""Auth verifier selection for `keep-ingestion`.

**This service accepts exactly one credential type: an API key.** Senders present
it as `X-API-KEY`, as an `?api_key=` query parameter, or via HTTP Basic/Digest —
all four are handled by `AuthVerifierBase` itself, which resolves the tenant and
role and then authorizes the route's scopes. So this factory hands back the base
verifier and nothing else.

The gateway's version dispatches on `AUTH_TYPE` and dynamically imports one of
six `identity_managers/*` packages. Those exist to verify **bearer tokens** — the
JWTs `keep-ui` sends — which no sender ever presents to an intake route. Carrying
them here would have cost more than dead code:

* `keycloak_authverifier` imports `create_tenant`, and the `oauth2proxy`,
  `onelogin` and `db` verifiers import `create_user` / `update_user_role` /
  `update_user_last_sign_in`. Every one is a **write**. A service whose database
  role is `SELECT`-only cannot run them, so shipping them would mean shipping
  code paths that can only fail — and would invite widening the grant to make
  them work, which is the boundary this split exists to draw.
* They pull in `python-keycloak`, `context_manager` and the user/tenant model
  tree, none of which the intake path touches.

`AUTH_TYPE` is therefore read but not honoured: it is logged once at startup if
it names something other than the API-key path, so a deploy that assumes UI-style
auth here finds out from a log line rather than from a 401.
"""

import enum
import logging
import os

from src.services.identity_manager.authverifierbase import AuthVerifierBase

logger = logging.getLogger(__name__)


class IdentityManagerTypes(enum.Enum):
    """The `AUTH_TYPE` values the platform recognises.

    Retained verbatim from the gateway even though this service honours none of
    them, because `config.py` and `main.py` use `NOAUTH.value` as the default for
    `AUTH_TYPE` and `get_app(auth_type=...)`. Keeping the enum identical means a
    shared chart can set the same `AUTH_TYPE` for every service without this one
    rejecting a value its siblings accept.
    """

    AUTH0 = "auth0"
    KEYCLOAK = "keycloak"
    OKTA = "okta"
    ONELOGIN = "onelogin"
    DB = "db"
    NOAUTH = "noauth"
    OAUTH2PROXY = "oauth2proxy"


_warned_auth_type = False


class IdentityManagerFactory:
    @staticmethod
    def get_auth_verifier(scopes: list[str] = []) -> AuthVerifierBase:
        """Return the API-key verifier, scoped to `scopes`.

        Signature-compatible with the gateway's, so the route definitions in
        `alerts.py` are unchanged from the ones they were copied from.
        """
        global _warned_auth_type
        auth_type = os.environ.get("AUTH_TYPE", "").lower()
        if auth_type and auth_type not in ("noauth", "no_auth", "apikey") and not _warned_auth_type:
            _warned_auth_type = True
            logger.warning(
                "AUTH_TYPE=%s is ignored by keep-ingestion: this service "
                "authenticates senders by API key only. Bearer-token identity "
                "managers live in keep-api-gateway.",
                auth_type,
            )
        return AuthVerifierBase(scopes)
