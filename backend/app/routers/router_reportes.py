"""
==============================================================
ROUTER — Reportes / Presentaciones Ejecutivas
GET /api/reportes/presentacion-proyecto   → PPTX individual
GET /api/reportes/presentacion-consolidada → PPTX consolidado
==============================================================
"""
import os
import logging
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_db
from app.core.security import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/reportes", tags=["Reportes"])


def _get_openai_key() -> str:
    """Lee la API Key de OpenAI desde variable de entorno."""
    return os.getenv("OPENAI_API_KEY", "")


MESES_ES = {
    1:"Enero", 2:"Febrero", 3:"Marzo", 4:"Abril",
    5:"Mayo",  6:"Junio",   7:"Julio", 8:"Agosto",
    9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre"
}


@router.get(
    "/presentacion-proyecto",
    summary="Genera presentación ejecutiva por proyecto",
    description="""
    Genera y descarga un PPTX ejecutivo para un proyecto específico.
    Incluye: portada, flujo del mes, avance financiero, análisis de cartera.
    Si OPENAI_API_KEY está configurada, incluye análisis de texto automático.
    """,
    response_class=Response,
)
async def generar_pptx_proyecto(
    empresa_sap: str = Query(..., description="Ej: SBO_EFICIENCIA_URBANA"),
    mes:         int  = Query(..., ge=1, le=12, description="Mes (1-12)"),
    anio:        int  = Query(..., ge=2020, description="Año (ej. 2026)"),
    db:          Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    try:
        from app.services.reportes import generar_presentacion_proyecto
        openai_key = _get_openai_key()
        logger.info(f"[REPORTES] Generando PPTX proyecto={empresa_sap} mes={mes}/{anio} "
                    f"openai={'sí' if openai_key else 'no'}")

        pptx_bytes = generar_presentacion_proyecto(db, empresa_sap, mes, anio, openai_key)

        mes_str = MESES_ES.get(mes, str(mes))
        filename = f"Presentacion_{empresa_sap}_{mes_str}_{anio}.pptx"
        return Response(
            content=pptx_bytes,
            media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"[REPORTES] Error generando PPTX: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error generando presentación: {str(e)}")


@router.get(
    "/presentacion-consolidada",
    summary="Genera presentación ejecutiva consolidada de todos los proyectos",
    description="""
    Genera y descarga un PPTX ejecutivo consolidado.
    Incluye: flujo, cartera y ventas de todos los proyectos + sección individual por proyecto.
    Si OPENAI_API_KEY está configurada, incluye análisis de texto automático.
    """,
    response_class=Response,
)
async def generar_pptx_consolidada(
    mes:  int = Query(..., ge=1, le=12, description="Mes de cierre (1-12)"),
    anio: int = Query(..., ge=2020, description="Año (ej. 2026)"),
    db:   Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    try:
        from app.services.reportes import generar_presentacion_consolidada
        openai_key = _get_openai_key()
        logger.info(f"[REPORTES] Generando PPTX consolidado mes={mes}/{anio} "
                    f"openai={'sí' if openai_key else 'no'}")

        pptx_bytes = generar_presentacion_consolidada(db, mes, anio, openai_key)

        mes_str = MESES_ES.get(mes, str(mes))
        filename = f"Junta_Directiva_Lotificadoras_{mes_str}_{anio}.pptx"
        return Response(
            content=pptx_bytes,
            media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        logger.error(f"[REPORTES] Error generando PPTX consolidado: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error generando presentación: {str(e)}")


@router.get("/estado-openai",
    summary="Verifica si OpenAI está configurada")
async def estado_openai(current_user=Depends(get_current_user)):
    key = _get_openai_key()
    return {
        "openai_configurado": bool(key),
        "mensaje": "✅ Análisis AI activo" if key else "⚠️ Sin OpenAI — presentación sin análisis de texto"
    }
