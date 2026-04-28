from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

from app.database import get_db, engine
from app.core.security import get_current_user, require_role
from app.sync.sync_inventario import sync_inventario
from app.sync.sync_cartera import sync_cartera
from app.sync.sync_flujos import sincronizar_flujos
from app.sync.sync_reclasificaciones import sincronizar_reclasificaciones

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sync", tags=["sync"])


def _sync_todo():
    """Sincroniza todos los módulos en orden correcto."""
    logger.info("[SYNC TOTAL] Iniciando sincronización completa...")
    try:
        logger.info("[SYNC TOTAL] 1/4 Inventario...")
        sync_inventario()
        logger.info("[SYNC TOTAL] 2/4 Cartera...")
        sync_cartera()
        logger.info("[SYNC TOTAL] 3/4 Flujos de Efectivo...")
        sincronizar_flujos()
        logger.info("[SYNC TOTAL] 4/4 Reclasificaciones...")
        sincronizar_reclasificaciones()
        logger.info("[SYNC TOTAL] ✅ Sincronización completa finalizada")
    except Exception as e:
        logger.error(f"[SYNC TOTAL] ❌ Error: {e}")


@router.post("/todo")
async def trigger_sync_todo(
    background_tasks: BackgroundTasks,
    current_user=Depends(require_role(3))
):
    """Sincroniza TODOS los módulos: inventario, cartera, flujos y reclasificaciones."""
    background_tasks.add_task(_sync_todo)
    return {
        "mensaje": "Sincronización completa iniciada en segundo plano",
        "modulos": ["inventario", "cartera", "flujos", "reclasificaciones"],
        "nota": "Puede tardar 3-5 minutos. Revisa los logs para ver el progreso."
    }


@router.post("/inventario")
async def trigger_sync_inventario(
    background_tasks: BackgroundTasks,
    current_user=Depends(require_role(3))
):
    """Sincroniza solo el inventario de lotes."""
    background_tasks.add_task(sync_inventario)
    return {"mensaje": "Sincronización de inventario iniciada"}


@router.post("/cartera")
async def trigger_sync_cartera(
    background_tasks: BackgroundTasks,
    current_user=Depends(require_role(3))
):
    """Sincroniza solo cartera y desistimientos."""
    background_tasks.add_task(sync_cartera)
    return {"mensaje": "Sincronización de cartera iniciada"}


@router.post("/flujos")
async def trigger_sync_flujos(
    background_tasks: BackgroundTasks,
    current_user=Depends(require_role(3))
):
    """Sincroniza flujos de efectivo y reclasificaciones."""
    def _sync_flujos():
        sincronizar_flujos()
        sincronizar_reclasificaciones()
    background_tasks.add_task(_sync_flujos)
    return {"mensaje": "Sincronización de flujos iniciada"}


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
