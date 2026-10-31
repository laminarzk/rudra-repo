"""Token verification and role checks over the configured identity provider."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

from dataoutbound.common.logging.logger import get_logger
from dataoutbound.config.config_loader import ConfigLoader
from dataoutbound.util.exceptions import AuthError

logger = get_logger(__name__)

ROLE_CLAIM_CANDIDATES = ("roles", "role", "cognito:groups", "groups", "scope")
SUBJECT_CLAIM = "sub"
ALGORITHMS = ("RS256",)


def _decode_segment(segment: str) -> Dict[str, Any]:
    import base64
    import json

    padding = "=" * (-len(segment) % 4)
    try:
        raw = base64.urlsafe_b64decode(segment + padding)
        parsed = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise AuthError(f"Malformed token segment: {exc}") from exc
    if not isinstance(parsed, dict):
        raise AuthError("Malformed token: claims payload is not an object")
    return parsed


class TokenVerifier:
    """Verify JWTs against a JWKS endpoint and expose their claims.

    Signature verification uses the provider's public keys. When the runtime
    cannot reach the JWKS endpoint (for example during local development) it
    can be pointed at a static key set through configuration; verification is
    never silently skipped.
    """

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        *,
        jwks: Optional[Mapping[str, Any]] = None,
        issuer: Optional[str] = None,
        audience: Optional[str] = None,
    ) -> None:
        self._config = config or ConfigLoader.load_config("common")
        self._jwks = jwks
        self._issuer = issuer or self._config.get_str("auth.issuer")
        self._audience = audience or self._config.get_str("auth.audience")

    def _resolve_jwks(self) -> Mapping[str, Any]:
        if self._jwks is not None:
            return self._jwks
        jwks_uri = self._config.get_str("auth.jwks_uri")
        if not jwks_uri:
            raise AuthError("Authentication is not configured: missing 'auth.jwks_uri'")
        try:
            import requests

            response = requests.get(jwks_uri, timeout=10)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise AuthError(f"Failed to fetch JWKS document: {exc}") from exc
        if not isinstance(payload, dict):
            raise AuthError("JWKS document must be a JSON object")
        self._jwks = payload
        return payload

    def verify(self, token: Optional[str]) -> Dict[str, Any]:
        """Return the verified claims for ``token`` or raise ``AuthError``."""
        if not token:
            raise AuthError("Missing bearer token")

        parts = token.split(".")
        if len(parts) != 3:
            raise AuthError("Malformed bearer token")

        header = _decode_segment(parts[0])
        claims = _decode_segment(parts[1])

        algorithm = str(header.get("alg", ""))
        if algorithm not in ALGORITHMS:
            raise AuthError(f"Unsupported token algorithm: {algorithm or 'none'}")

        self._verify_signature(parts[0], parts[1], parts[2], header)
        self._verify_registered_claims(claims)
        return claims

    def _verify_signature(
        self,
        signing_input: str,
        first_segment: str,
        signature: str,
        header: Mapping[str, Any],
    ) -> None:
        jwks = self._resolve_jwks()
        keys = jwks.get("keys", []) if isinstance(jwks, Mapping) else []
        kid = str(header.get("kid", ""))

        matching = None
        for key in keys:
            if not isinstance(key, Mapping):
                continue
            if not kid or str(key.get("kid", "")) == kid:
                matching = key
                break
        if matching is None:
            raise AuthError("No matching signing key for the presented token")

        try:
            from jwt.algorithms import RSAAlgorithm

            public_key = RSAAlgorithm.from_jwk(matching)
        except Exception as exc:
            raise AuthError(f"Failed to load signing key: {exc}") from exc

        try:
            import jwt

            jwt.decode(
                f"{first_segment}.{signature}",
                key=public_key,
                algorithms=list(ALGORITHMS),
                options={"verify_aud": False, "verify_exp": False},
            )
        except Exception as exc:
            raise AuthError(f"Token signature verification failed: {exc}") from exc

    def _verify_registered_claims(self, claims: Mapping[str, Any]) -> None:
        if self._issuer and claims.get("iss") != self._issuer:
            raise AuthError("Token issuer does not match the configured issuer")
        if self._audience:
            audience = claims.get("aud")
            if isinstance(audience, str):
                audiences = [audience]
            elif isinstance(audience, (list, tuple)):
                audiences = [str(item) for item in audience]
            else:
                audiences = []
            if self._audience not in audiences:
                raise AuthError("Token audience does not match the configured audience")


_VERIFIER_LOCKED_SINGLETON: Optional[TokenVerifier] = None


def _default_verifier() -> TokenVerifier:
    global _VERIFIER_LOCKED_SINGLETON
    if _VERIFIER_LOCKED_SINGLETON is None:
        _VERIFIER_LOCKED_SINGLETON = TokenVerifier()
    return _VERIFIER_LOCKED_SINGLETON


def verify_token(
    token: Optional[str], *, verifier: Optional[TokenVerifier] = None
) -> Dict[str, Any]:
    """Verify a bearer token and return its claims."""
    active = verifier or _default_verifier()
    claims = active.verify(token)
    logger.debug("Token verified", extra={"sub": claims.get(SUBJECT_CLAIM)})
    return claims


def extract_roles(claims: Mapping[str, Any]) -> List[str]:
    """Return every role the claims grant, normalised to lowercase strings."""
    collected: List[str] = []
    for claim in ROLE_CLAIM_CANDIDATES:
        value = claims.get(claim)
        if isinstance(value, str):
            collected.extend(
                part.strip() for part in value.replace(",", " ").split() if part.strip()
            )
        elif isinstance(value, (list, tuple, set)):
            collected.extend(str(item).strip() for item in value if str(item).strip())
    return [role.lower() for role in collected]


def current_subject(claims: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the authenticated subject and its roles.

    Raises ``AuthError`` when the token carries no subject claim.
    """
    subject = claims.get(SUBJECT_CLAIM)
    if not subject:
        raise AuthError("Token does not contain a subject claim")
    return {
        "sub": str(subject),
        "roles": extract_roles(claims),
        "claims": dict(claims),
    }


def require_role(*required_roles: str) -> Callable[[Mapping[str, Any]], Dict[str, Any]]:
    """Return a dependency that guarantees at least one of ``required_roles``.

    The returned callable accepts verified claims and returns the subject
    dictionary, raising ``AuthError`` when no required role is present.
    """
    if not required_roles:
        raise AuthError("require_role requires at least one role name")
    expected = {role.lower() for role in required_roles}

    def _checker(claims: Mapping[str, Any]) -> Dict[str, Any]:
        subject = current_subject(claims)
        granted: Iterable[str] = subject["roles"]
        if expected.isdisjoint({role.lower() for role in granted}):
            raise AuthError("Subject is not authorized for this operation")
        return subject

    return _checker
