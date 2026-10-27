import hashlib
import hmac
import os
import time

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import config

_bearer = HTTPBearer(auto_error=False)


def _hash(password, salt):
    return hashlib.scrypt(password.encode(), salt=salt, n=2**14, r=8, p=1)


# Usuarios sembrados desde variables de entorno; en memoria solo se guarda el hash.
_USERS = {}
for _u, _p, _role in config.USERS_SEED:
    _salt = os.urandom(16)
    _USERS[_u] = (_salt, _hash(_p, _salt), _role)


def authenticate(username, password):
    rec = _USERS.get(username)
    salt = rec[0] if rec else b"0" * 16            # mismo costo exista o no el usuario
    candidate = _hash(password, salt)
    if rec and hmac.compare_digest(candidate, rec[1]):
        return rec[2]
    return None


def create_token(username, role):
    now = int(time.time())
    return jwt.encode({"sub": username, "role": role, "iat": now, "exp": now + config.JWT_MINUTES * 60},
                      config.JWT_SECRET, algorithm=config.JWT_ALG)


def current_user(cred: HTTPAuthorizationCredentials = Depends(_bearer)):
    if cred is None:
        raise HTTPException(401, "Falta el token de acceso", headers={"WWW-Authenticate": "Bearer"})
    try:
        data = jwt.decode(cred.credentials, config.JWT_SECRET, algorithms=[config.JWT_ALG])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "La sesión expiró. Inicia sesión de nuevo.") from None
    except jwt.PyJWTError:
        raise HTTPException(401, "Token inválido") from None
    if data.get("role") not in ("viewer", "admin"):
        raise HTTPException(403, "Rol no reconocido")
    return {"username": data["sub"], "role": data["role"]}


def require_admin(user=Depends(current_user)):
    if user["role"] != "admin":
        raise HTTPException(403, "Acción solo para administradores")
    return user
