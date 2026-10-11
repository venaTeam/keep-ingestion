import hmac
import logging
import os
from typing import Optional

from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import (
    APIKeyHeader,
    HTTPAuthorizationCredentials,
    HTTPBasic,
    OAuth2PasswordBearer,
)
from starlette.datastructures import FormData

from src.config.core import config
from src.repositories.dependencies import extract_generic_body
from src.services.identity_manager.authenticatedentity import AuthenticatedEntity
from src.services.identity_manager.rbac import get_role_by_role_name

auth_header = APIKeyHeader(name="X-API-KEY", scheme_name="API Key", auto_error=False)
http_basic = HTTPBasic(auto_error=False)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

ALL_RESOURCES = set()


def get_all_scopes() -> list[str]:
    """
    Get all scopes

    Returns:
        list: The list of scopes.
    """
    # read, write, delete and update for every resource:
    scopes = []
    for resource in ALL_RESOURCES:
        for action in ["read", "write", "delete", "update"]:
            scopes.append(f"{action}:{resource}")
    return scopes


class AuthVerifierBase:
    """
    API-key authentication for `keep-ingestion`.

    The gateway's version supports bearer tokens and database-backed API keys.
    This service is the public alert intake: it accepts exactly one credential
    type, a single API key supplied through an OpenShift Secret, and resolves it
    to a configured tenant. There is no database access, no user provisioning,
    and no interactive login.

    The key is read from `KEEP_INGESTION_API_KEY` (required). The tenant it
    resolves to is read from `KEEP_INGESTION_TENANT_ID` and defaults to the
    GENERAL tenant constant used by the rest of the ingestion path.
    """

    def __init__(self, scopes: list[str] = [], tenant_id: Optional[str] = None) -> None:
        ALL_RESOURCES.update([scope.split(":")[1] for scope in scopes])
        self.scopes = scopes
        self.tenant_id = tenant_id
        self.logger = logging.getLogger(__name__)
        self._api_key = config("KEEP_INGESTION_API_KEY", default=None)
        if not self._api_key:
            raise ValueError(
                "KEEP_INGESTION_API_KEY is required. Mount it from the OpenShift Secret."
            )
        self._tenant_id = (
            config("KEEP_INGESTION_TENANT_ID", default=None)
            or tenant_id
            or self._default_tenant_id()
        )

    @staticmethod
    def _default_tenant_id() -> str:
        # Imported lazily to avoid a circular import at module load time.
        from src.repositories.dependencies import GENERIC_TENANT_UUID

        return GENERIC_TENANT_UUID

    def __call__(
        self,
        request: Request,
        api_key: Optional[str] = Security(auth_header),
        authorization: Optional[HTTPAuthorizationCredentials] = Security(http_basic),
        token: Optional[str] = Depends(oauth2_scheme),
        body: dict | bytes | FormData = Depends(extract_generic_body),
    ) -> AuthenticatedEntity:
        """
        Main entry point for authentication and authorization.

        Args:
            request (Request): The incoming request.
            api_key (Optional[str]): The API key from the header.
            authorization (Optional[HTTPAuthorizationCredentials]): The HTTP basic auth credentials.
            token (Optional[str]): The OAuth2 token (not supported here).

        Returns:
            AuthenticatedEntity: The authenticated entity.

        Raises:
            HTTPException: If authentication or authorization fails.
        """
        self.logger.debug("Starting authentication process")

        authenticated_entity = self.authenticate(
            request,
            api_key,
            authorization,
            token,
            body=body,
        )
        self.logger.debug(
            f"Authentication successful for entity: {authenticated_entity}"
        )

        self.logger.debug("Starting authorization process")
        self.authorize(authenticated_entity)
        self.logger.debug("Authorization successful")

        return authenticated_entity

    def authenticate(
        self,
        request: Request,
        api_key: Optional[str],
        authorization: Optional[HTTPAuthorizationCredentials],
        token: Optional[str],
        body: Optional[dict | bytes | FormData] = None,
    ) -> AuthenticatedEntity:
        """
        Authenticate the request using the configured ingestion API key.

        Bearer tokens and OAuth2 are not supported in this service.
        """
        self.logger.debug("Attempting authentication")
        if token:
            self.logger.error("Bearer-token authentication is not supported")
            raise HTTPException(
                status_code=401, detail="Bearer-token authentication is not supported"
            )

        api_key = self._extract_api_key(request, api_key, authorization)
        # HACK for cloudwatch without api key for self hosted deployments
        if isinstance(api_key, AuthenticatedEntity):
            return api_key

        if api_key:
            self.logger.debug("Attempting to authenticate with API key")
            try:
                return self._verify_api_key(request, api_key)
            except HTTPException:
                raise
            except Exception:
                self.logger.exception("Failed to validate API Key")
                raise HTTPException(
                    status_code=401, detail="Invalid authentication credentials"
                )

        self.logger.error(
            "No valid authentication method found.",
            extra={
                "headers": request.headers,
                "body": body,
            },
        )
        raise HTTPException(
            status_code=401, detail="Missing authentication credentials"
        )

    def authorize(self, authenticated_entity: AuthenticatedEntity) -> None:
        """
        Authorize the authenticated entity.

        Args:
            authenticated_entity (AuthenticatedEntity): The authenticated entity to authorize.

        Raises:
            HTTPException: If authorization fails.
        """
        self.logger.debug(f"Authorizing entity: {authenticated_entity}")
        self._authorize(authenticated_entity)

    def _authorize(self, authenticated_entity: AuthenticatedEntity) -> None:
        """
        Internal method to perform authorization.

        Args:
            authenticated_entity (AuthenticatedEntity): The authenticated entity to authorize.

        Raises:
            HTTPException: If the entity doesn't have the required scopes.
        """
        role = get_role_by_role_name(authenticated_entity.role)
        self.logger.debug(f"Checking scopes for role: {role}")
        if not role.has_scopes(self.scopes):
            self.logger.warning(
                f"Authorization failed. Required scopes: {self.scopes}"
            )
            raise HTTPException(
                status_code=403,
                detail=f"You don't have the required scopes to access this resource [required scopes: {self.scopes}]",
            )

    def _extract_api_key(
        self,
        request: Request,
        api_key: str,
        authorization: HTTPAuthorizationCredentials,
    ) -> str:
        """
        Extract the API key from various sources in the request.

        Args:
            request (Request): The incoming request.
            api_key (str): The API key from the header.
            authorization (HTTPAuthorizationCredentials): The HTTP basic auth credentials.

        Returns:
            str: The extracted API key.

        Raises:
            HTTPException: If no valid API key is found.
        """
        self.logger.debug("Extracting API key")
        api_key = api_key or request.query_params.get("api_key", None)
        if not api_key:
            # A special treatment for CloudWatch SNS Confirmation requests
            if (
                not authorization
                and "Amazon Simple Notification Service Agent"
                in request.headers.get("user-agent", "")
            ):
                self.logger.warning("Got an SNS request without any auth")
                allow_unauth = config(
                    "KEEP_CLOUDWATCH_DISABLE_API_KEY", default=False
                )
                if allow_unauth and request.url.path.endswith(
                    "/alerts/event/cloudwatch"
                ):
                    tenant_id = request.query_params.get("tenant_id", "keep")
                    self.logger.info(
                        f"Allowing unauthenticated access for tenant: {tenant_id} for CloudWatch"
                    )
                    return AuthenticatedEntity(
                        tenant_id=tenant_id,
                        email="system",
                        api_key_name="webhook",
                        role="webhook",
                    )
                raise HTTPException(
                    status_code=401,
                    headers={"WWW-Authenticate": "Basic"},
                    detail="Missing API Key",
                )

            auth_header = request.headers.get("Authorization")
            try:
                scheme, _, credentials = auth_header.partition(" ")
            except Exception:
                self.logger.error(
                    "Failed to parse Authorization header",
                    extra={
                        "url": str(request.url),
                        "user-agent": request.headers.get("user-agent"),
                    },
                )
                raise HTTPException(
                    status_code=401, detail="Missing API Key"
                )
            if scheme.lower() == "basic":
                api_key = authorization.password
            elif scheme.lower() == "digest":
                if not credentials:
                    self.logger.error("Invalid Digest credentials")
                    raise HTTPException(
                        status_code=403, detail="Invalid Digest credentials"
                    )
                else:
                    api_key = credentials
            else:
                self.logger.error(f"Unsupported authentication scheme: {scheme}")
                raise HTTPException(
                    status_code=401, detail="Missing API Key"
                )
        self.logger.debug("API key extracted successfully")
        return api_key

    def _verify_api_key(
        self,
        request: Request,
        api_key: str = Security(auth_header),
        authorization: HTTPAuthorizationCredentials = Security(http_basic),
    ) -> AuthenticatedEntity:
        """
        Verify the API key against the configured ingestion secret.

        Args:
            request (Request): The incoming request.
            api_key (str): The API key to verify.

        Returns:
            AuthenticatedEntity: The authenticated entity.

        Raises:
            HTTPException: If the API key is invalid.
        """
        self.logger.debug("Verifying API key")
        # Constant-time comparison to avoid timing side-channels.
        if not hmac.compare_digest(api_key, self._api_key):
            self.logger.warning("Invalid API Key")
            raise HTTPException(
                status_code=401, detail="Invalid API Key"
            )

        tenant_id = self._tenant_id
        request.state.tenant_id = tenant_id
        self.logger.debug(f"API key verified for tenant: {tenant_id}")
        return AuthenticatedEntity(
            tenant_id=tenant_id,
            email="ingestion",
            api_key_name="ingestion-secret",
            role="webhook",
        )

    def _verify_bearer_token(self, token: str) -> AuthenticatedEntity:
        """
        Verify the bearer token and return an authenticated entity.

        Raises:
            NotImplementedError: Bearer tokens are not supported by this service.
        """
        self.logger.error("_verify_bearer_token() method not implemented")
        raise NotImplementedError(
            "_verify_bearer_token() method not implemented for {}".format(
                self.__class__.__name__
            )
        )