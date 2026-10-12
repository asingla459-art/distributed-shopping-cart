"""JWT authentication + role-based access control (RBAC).

Roles
  customer  - may only read/modify their own cart and orders
  admin     - may manage products, inspect cluster state, events and run chaos tests
  service   - machine identity (e.g. the order worker / monitoring), read-only admin views
"""
from __future__ import annotations

import time
import uuid

import jwt
from starlette.requests import Request

from . import config

ROLES = {"customer", "admin", "service"}


class AuthError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def issue_token(user_id: str, username: str, role: str) -> str:
    now = int(time.time())
    return jwt.encode({"sub": user_id, "name": username, "role": role, "iat": now,
                       "exp": now + config.JWT_TTL_SECONDS, "jti": uuid.uuid4().hex,
                       "iss": "dscart"}, config.JWT_SECRET, algorithm="HS256")


def principal(request: Request) -> dict:
    h = request.headers.get("authorization", "")
    if not h.lower().startswith("bearer "):
        raise AuthError(401, "UNAUTHENTICATED", "missing Bearer token")
    try:
        claims = jwt.decode(h[7:], config.JWT_SECRET, algorithms=["HS256"], issuer="dscart")
    except jwt.ExpiredSignatureError:
        raise AuthError(401, "TOKEN_EXPIRED", "token expired")
    except jwt.PyJWTError:
        raise AuthError(401, "INVALID_TOKEN", "invalid token")
    if claims.get("role") not in ROLES:
        raise AuthError(403, "FORBIDDEN", "unknown role")
    return claims


def require(request: Request, *roles: str) -> dict:
    p = principal(request)
    if roles and p["role"] not in roles:
        raise AuthError(403, "FORBIDDEN", f"requires role {' or '.join(roles)}")
    return p
