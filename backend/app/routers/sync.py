from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database import get_db
from app.core.security import get_current_user, require_role
from app.sync.sync_inventario import sync_inventario

router = APIRouter(prefix="/api/sync", tags=["sync"])


@router.post("/inventario")
async def trigger_sync_inventario(
    background_tasks: BackgroundTasks,
    current_user=Depends(require_role(3))  # Solo GERENTE y ADMIN
):
    """Dispara sincronización manual del inventario."""
    background_tasks.add_task(sync_inventario)
    return {"mensaje": "Sincronización iniciada en segundo plano"}


@router.get("/logs")
async def get_sync_logs(
    db: Session = Depends(get_db),
    current_user=Depends(require_role(3))
):
    """Historial de sincronizaciones."""
    logs = db.execute(text("""
        SELECT id, archivo, inicio, fin, estado,
               registros_leidos, registros_insertados,
               registros_actualizados, registros_error,
               mensaje_error
        FROM sync_log
        ORDER BY inicio DESC
        LIMIT 50
    """)).fetchall()
    return [dict(r._mapping) for r in logs]


@router.get("/estado")
async def get_sync_estado(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Estado de la última sincronización por archivo."""
    result = db.execute(text("""
        SELECT DISTINCT ON (archivo)
            archivo, inicio, fin, estado,
            registros_insertados + registros_actualizados AS total_procesados
        FROM sync_log
        ORDER BY archivo, inicio DESC
    """)).fetchall()
    return [dict(r._mapping) for r in result]
