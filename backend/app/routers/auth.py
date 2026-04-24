from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, EmailStr
from typing import Optional
import uuid

from app.database import get_db
from app.core.security import verify_password, hash_password, create_access_token, get_current_user

router = APIRouter(prefix="/api/auth", tags=["auth"])


class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    usuario: dict


class CrearUsuarioRequest(BaseModel):
    email: EmailStr
    nombre: str
    password: str
    rol_id: int
    proyecto_ids: Optional[list[int]] = []


@router.post("/login", response_model=LoginResponse)
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.execute(
        text("""
            SELECT u.id, u.email, u.nombre, u.hashed_password, u.activo,
                   r.nombre as rol, r.nivel
            FROM usuarios u JOIN roles r ON r.id = u.rol_id
            WHERE u.email = :email
        """),
        {"email": form_data.username}
    ).fetchone()

    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email o contraseña incorrectos"
        )

    if not user.activo:
        raise HTTPException(status_code=400, detail="Usuario inactivo")

    # Actualizar último acceso
    db.execute(
        text("UPDATE usuarios SET ultimo_acceso=NOW() WHERE id=:uid"),
        {"uid": str(user.id)}
    )
    db.commit()

    token = create_access_token({"sub": str(user.id), "rol": user.rol, "nivel": user.nivel})

    return {
        "access_token": token,
        "token_type": "bearer",
        "usuario": {
            "id": str(user.id),
            "email": user.email,
            "nombre": user.nombre,
            "rol": user.rol,
            "nivel": user.nivel
        }
    }


@router.get("/me")
async def get_me(current_user=Depends(get_current_user)):
    return {
        "id": str(current_user.id),
        "email": current_user.email,
        "nombre": current_user.nombre,
        "rol": current_user.rol,
        "nivel": current_user.nivel
    }


@router.post("/usuarios", status_code=201)
async def crear_usuario(
    data: CrearUsuarioRequest,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    if current_user.nivel < 4:
        raise HTTPException(status_code=403, detail="Solo administradores pueden crear usuarios")

    existing = db.execute(
        text("SELECT id FROM usuarios WHERE email=:email"),
        {"email": data.email}
    ).fetchone()
    if existing:
        raise HTTPException(status_code=400, detail="Email ya registrado")

    user_id = uuid.uuid4()
    db.execute(
        text("""
            INSERT INTO usuarios (id, email, nombre, hashed_password, rol_id)
            VALUES (:id, :email, :nombre, :password, :rol_id)
        """),
        {
            "id": str(user_id),
            "email": data.email,
            "nombre": data.nombre,
            "password": hash_password(data.password),
            "rol_id": data.rol_id
        }
    )

    # Asignar proyectos
    for pid in data.proyecto_ids:
        db.execute(
            text("INSERT INTO usuario_proyectos (usuario_id, proyecto_id) VALUES (:uid, :pid) ON CONFLICT DO NOTHING"),
            {"uid": str(user_id), "pid": pid}
        )

    db.commit()
    return {"id": str(user_id), "mensaje": "Usuario creado exitosamente"}


@router.get("/usuarios")
async def listar_usuarios(db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    if current_user.nivel < 4:
        raise HTTPException(status_code=403, detail="Solo administradores")

    usuarios = db.execute(text("""
        SELECT u.id, u.email, u.nombre, u.activo, u.ultimo_acceso,
               r.nombre as rol, u.created_at
        FROM usuarios u JOIN roles r ON r.id = u.rol_id
        ORDER BY u.created_at DESC
    """)).fetchall()

    return [dict(u._mapping) for u in usuarios]
