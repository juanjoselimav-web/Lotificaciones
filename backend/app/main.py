from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
import logging
import os

from app.database import check_db_connection
from app.core.config import get_settings
from app.routers import auth, inventario, sync, cartera, ventas, api_publica, flujos
from app.sync.sync_inventario import sync_inventario
from app.sync.sync_cartera import sync_cartera
from app.sync.sync_flujos import sincronizar_flujos
from app.core.security import hash_password

settings = get_settings()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


def run_sync_inventario():
    try:
        logger.info("[SCHEDULER] Sincronizando inventario...")
        result = sync_inventario()
        logger.info(f"[SCHEDULER] Inventario: {result}")
    except Exception as e:
        logger.error(f"[SCHEDULER] Error inventario: {e}")


def run_sync_cartera():
    try:
        logger.info("[SCHEDULER] Sincronizando cartera...")
        result = sync_cartera()
        logger.info(f"[SCHEDULER] Cartera: {result}")
    except Exception as e:
        logger.error(f"[SCHEDULER] Error cartera: {e}")


def run_sync_flujos():
    try:
        logger.info("[SCHEDULER] Sincronizando flujos de efectivo...")
        result = sincronizar_flujos()
        logger.info(f"[SCHEDULER] Flujos: {result}")
    except Exception as e:
        logger.error(f"[SCHEDULER] Error flujos: {e}")


def create_admin_if_not_exists():
    """Crea usuario admin por defecto si no existe ningún usuario."""
    from app.database import SessionLocal
    from sqlalchemy import text
    db = SessionLocal()
    try:
        count = db.execute(text("SELECT COUNT(*) FROM usuarios")).scalar()
        if count == 0:
            rol_admin = db.execute(text("SELECT id FROM roles WHERE nombre='ADMIN'")).fetchone()
            if rol_admin:
                import uuid
                db.execute(text("""
                    INSERT INTO usuarios (id, email, nombre, hashed_password, rol_id)
                    VALUES (:id, :email, :nombre, :password, :rol_id)
                """), {
                    "id": str(uuid.uuid4()),
                    "email": "admin@rvcuatro.com",
                    "nombre": "Administrador",
                    "password": hash_password("Admin2024!"),
                    "rol_id": rol_admin[0]
                })
                db.commit()
                logger.info("[INIT] Usuario admin creado: admin@rvcuatro.com / Admin2024!")
                logger.warning("[INIT] ⚠️  CAMBIAR PASSWORD DEL ADMIN EN PRODUCCIÓN")
    except Exception as e:
        logger.error(f"[INIT] Error creando admin: {e}")
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"[INIT] Iniciando sistema Lotificaciones — Ambiente: {settings.environment}")

    if check_db_connection():
        logger.info("[INIT] ✅ Conexión a PostgreSQL exitosa")
    else:
        logger.error("[INIT] ❌ No se pudo conectar a PostgreSQL")

    # Crear admin por defecto
    create_admin_if_not_exists()

    # Sincronización inicial al arrancar
    try:
        logger.info("[INIT] Sincronizando inventario...")
        sync_inventario()
    except Exception as e:
        logger.warning(f"[INIT] Sync inventario falló: {e}")

    try:
        logger.info("[INIT] Sincronizando cartera...")
        sync_cartera()
    except Exception as e:
        logger.warning(f"[INIT] Sync cartera falló (puede ser normal si archivo no disponible): {e}")

    try:
        logger.info("[INIT] Sincronizando flujos de efectivo...")
        sincronizar_flujos()
    except Exception as e:
        logger.warning(f"[INIT] Sync flujos falló (puede ser normal si archivo no disponible): {e}")

    # Jobs automáticos cada hora
    scheduler.add_job(run_sync_inventario, trigger=IntervalTrigger(minutes=settings.sync_interval_minutes),
                      id="sync_inventario", replace_existing=True, max_instances=1)
    scheduler.add_job(run_sync_cartera, trigger=IntervalTrigger(minutes=settings.sync_interval_minutes),
                      id="sync_cartera", replace_existing=True, max_instances=1)
    scheduler.add_job(run_sync_flujos, trigger=IntervalTrigger(minutes=settings.sync_interval_minutes),
                      id="sync_flujos", replace_existing=True, max_instances=1)
    scheduler.start()
    logger.info(f"[INIT] ⏰ Sync automático cada {settings.sync_interval_minutes} minutos")

    yield

    # Shutdown
    scheduler.shutdown()
    logger.info("[SHUTDOWN] Sistema apagado correctamente")


app = FastAPI(
    title="Sistema Lotificaciones RV4",
    version="1.0.0",
    description="Plataforma de gestión y proyección financiera de proyectos inmobiliarios",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers API
app.include_router(auth.router)
app.include_router(inventario.router)
app.include_router(sync.router)
app.include_router(cartera.router)
app.include_router(ventas.router)
app.include_router(api_publica.router)
app.include_router(flujos.router)


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "ambiente": settings.environment,
        "db": check_db_connection()
    }


# Servir frontend estático
static_path = "/app/static"
if os.path.exists(static_path):
    app.mount("/static", StaticFiles(directory=static_path), name="static")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        index = os.path.join(static_path, "index.html")
        return FileResponse(index)
