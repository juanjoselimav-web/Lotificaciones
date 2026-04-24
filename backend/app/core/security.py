from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
import warnings
warnings.filterwarnings("ignore", ".*bcrypt.*")
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db
from app.core.config import get_settings

settings = get_settings()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=settings.access_token_expire_minutes))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No autenticado",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = db.execute(
        text("""
            SELECT u.id, u.email, u.nombre, u.activo, r.nombre as rol, r.nivel
            FROM usuarios u JOIN roles r ON r.id = u.rol_id
            WHERE u.id = :uid
        """),
        {"uid": user_id}
    ).fetchone()

    if user is None or not user.activo:
        raise credentials_exception
    return user


def require_role(min_nivel: int):
    """Decorador para requerir nivel mínimo de rol."""
    def checker(current_user=Depends(get_current_user)):
        if current_user.nivel < min_nivel:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos suficientes"
            )
        return current_user
    return checker
