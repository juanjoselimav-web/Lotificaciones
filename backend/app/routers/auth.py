from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
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
                   u.ultimo_acceso, r.nombre as rol, r.nivel
            FROM usuarios u JOIN roles r ON r.id = u.rol_id
            WHERE u.email = :email
        """),
        {"email": form_data.username}
    ).fetchone()

    if not user or not verify_password(form_data.password, user.hashed_password):
        # Registrar intento fallido
        try:
            email_attempt = form_data.username
            nombre_found = user.nombre if user else None
            uid_found = str(user.id) if user else None
            db.execute(text("""
                INSERT INTO auditoria_accesos (usuario_id, email, nombre, tipo, detalle)
                VALUES (:uid, :email, :nombre, 'LOGIN_FAIL', 'Contraseña incorrecta')
            """), {"uid": uid_found, "email": email_attempt, "nombre": nombre_found})
            db.commit()
        except Exception:
            pass
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

    # Registrar auditoría de ingreso exitoso
    try:
        db.execute(text("""
            INSERT INTO auditoria_accesos (usuario_id, email, nombre, tipo, detalle)
            VALUES (:uid, :email, :nombre, 'LOGIN_OK', 'Ingreso exitoso')
        """), {"uid": str(user.id), "email": user.email, "nombre": user.nombre})
        db.commit()
    except Exception:
        pass

    # Detectar primer ingreso (ultimo_acceso era NULL antes del UPDATE)
    must_change_password = user.ultimo_acceso is None

    return {
        "access_token": token,
        "token_type": "bearer",
        "must_change_password": must_change_password,
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


@router.get("/usuarios/{user_id}")
async def get_usuario(user_id: str, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    if current_user.nivel < 4:
        raise HTTPException(status_code=403, detail="Solo administradores")
    u = db.execute(text("""
        SELECT u.id, u.email, u.nombre, u.activo, u.ultimo_acceso, u.created_at,
               r.id as rol_id, r.nombre as rol, r.nivel,
               COALESCE(array_agg(up.proyecto_id) FILTER (WHERE up.proyecto_id IS NOT NULL), '{}') AS proyecto_ids
        FROM usuarios u JOIN roles r ON r.id = u.rol_id
        LEFT JOIN usuario_proyectos up ON up.usuario_id = u.id
        WHERE u.id = :uid GROUP BY u.id, r.id
    """), {"uid": user_id}).fetchone()
    if not u:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return dict(u._mapping)


@router.put("/usuarios/{user_id}")
async def actualizar_usuario(
    user_id: str,
    data: dict,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    if current_user.nivel < 4:
        raise HTTPException(status_code=403, detail="Solo administradores")

    fields = []
    params = {"uid": user_id}
    if "nombre" in data:
        fields.append("nombre = :nombre"); params["nombre"] = data["nombre"]
    if "email" in data:
        fields.append("email = :email"); params["email"] = data["email"]
    if "rol_id" in data:
        fields.append("rol_id = :rol_id"); params["rol_id"] = data["rol_id"]
    if "activo" in data:
        fields.append("activo = :activo"); params["activo"] = data["activo"]
    if "password" in data and data["password"]:
        fields.append("hashed_password = :hp"); params["hp"] = hash_password(data["password"])

    if fields:
        db.execute(text(f"UPDATE usuarios SET {', '.join(fields)}, updated_at=NOW() WHERE id=:uid"), params)

    # Actualizar proyectos si se envían
    if "proyecto_ids" in data:
        db.execute(text("DELETE FROM usuario_proyectos WHERE usuario_id=:uid"), {"uid": user_id})
        for pid in data["proyecto_ids"]:
            db.execute(text("INSERT INTO usuario_proyectos (usuario_id, proyecto_id) VALUES (:uid,:pid) ON CONFLICT DO NOTHING"), {"uid": user_id, "pid": pid})

    db.commit()
    return {"mensaje": "Usuario actualizado"}


@router.delete("/usuarios/{user_id}")
async def desactivar_usuario(user_id: str, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    if current_user.nivel < 4:
        raise HTTPException(status_code=403, detail="Solo administradores")
    if user_id == str(current_user.id):
        raise HTTPException(status_code=400, detail="No puedes desactivar tu propia cuenta")
    db.execute(text("UPDATE usuarios SET activo=FALSE, updated_at=NOW() WHERE id=:uid"), {"uid": user_id})
    db.commit()
    return {"mensaje": "Usuario desactivado"}


@router.get("/roles")
async def listar_roles(db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    rows = db.execute(text("SELECT id, nombre, descripcion, nivel FROM roles ORDER BY nivel DESC")).fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/usuarios/{user_id}/reset-password")
async def reset_password(user_id: str, data: dict, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    if current_user.nivel < 4:
        raise HTTPException(status_code=403, detail="Solo administradores")
    nueva = data.get("password", "Rv4-2026!")
    db.execute(text("UPDATE usuarios SET hashed_password=:hp, updated_at=NOW() WHERE id=:uid"),
               {"hp": hash_password(nueva), "uid": user_id})
    db.commit()
    return {"mensaje": "Contraseña restablecida"}


@router.get("/auditoria")
async def get_auditoria(
    dias: int = Query(7),
    tipo: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    if current_user.nivel < 4:
        raise HTTPException(status_code=403, detail="Solo administradores")
    tipo_filter = "AND tipo = :tipo" if tipo else ""
    params = {"dias": dias}
    if tipo: params["tipo"] = tipo
    rows = db.execute(text(f"""
        SELECT a.id, a.email, a.nombre, a.tipo, a.ip, a.detalle, a.created_at
        FROM auditoria_accesos a
        WHERE a.created_at >= NOW() - (:dias || ' days')::INTERVAL
        {tipo_filter}
        ORDER BY a.created_at DESC
        LIMIT 500
    """), params).fetchall()
    return [dict(r._mapping) for r in rows]
