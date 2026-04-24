"""
api_publica.py — API Pública RV4 Lotificaciones
Autenticación por API Key (header X-API-Key)
Prefijo: /api/v1/
"""
import secrets
import hashlib
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import datetime

from app.database import get_db

router = APIRouter(prefix="/api/v1", tags=["API Pública"])

# ══════════════════════════════════════════════════════════════
# AUTENTICACIÓN POR API KEY
# ══════════════════════════════════════════════════════════════

async def verify_api_key(
    x_api_key: str = Header(..., description="API Key de acceso"),
    db: Session = Depends(get_db)
):
    """Valida el API Key contra la base de datos."""
    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    key = db.execute(text("""
        SELECT ak.id, ak.nombre, ak.permisos, ak.activo, ak.ultimo_uso
        FROM api_keys ak
        WHERE ak.key_hash = :kh AND ak.activo = TRUE
    """), {"kh": key_hash}).fetchone()

    if not key:
        raise HTTPException(status_code=401, detail="API Key inválida o inactiva")

    # Registrar uso
    db.execute(text("""
        UPDATE api_keys SET ultimo_uso = NOW(), usos = usos + 1 WHERE key_hash = :kh
    """), {"kh": key_hash})
    db.commit()

    return dict(key._mapping)


# ══════════════════════════════════════════════════════════════
# GESTIÓN DE API KEYS (solo vía psql por seguridad)
# ══════════════════════════════════════════════════════════════

@router.get("/ping",
    summary="Verificar conexión",
    description="Endpoint de prueba. Verifica que el API Key es válido y la conexión funciona.")
async def ping(api_key=Depends(verify_api_key)):
    return {
        "status": "ok",
        "mensaje": "Conexión exitosa",
        "cliente": api_key["nombre"],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": "1.0"
    }


# ══════════════════════════════════════════════════════════════
# INVENTARIO
# ══════════════════════════════════════════════════════════════

@router.get("/inventario/resumen",
    summary="Resumen global de inventario",
    description="KPIs consolidados: total lotes, disponibles, vendidos, bloqueados y valor total.")
async def api_inventario_resumen(
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    r = db.execute(text("""
        SELECT
            COUNT(*) AS total_lotes,
            COUNT(*) FILTER (WHERE estatus='DISPONIBLE') AS disponibles,
            COUNT(*) FILTER (WHERE estatus IN ('VENTA','RESERVADO')) AS vendidos,
            COUNT(*) FILTER (WHERE estatus='BLOQUEADO') AS bloqueados,
            COUNT(*) FILTER (WHERE estatus='CANJE') AS canjes,
            COUNT(DISTINCT proyecto_id) AS total_proyectos,
            COALESCE(SUM(precio_final) FILTER (WHERE estatus='DISPONIBLE'), 0) AS valor_disponible,
            COALESCE(SUM(precio_final) FILTER (WHERE estatus IN ('VENTA','RESERVADO')), 0) AS valor_vendido,
            COALESCE(SUM(precio_final), 0) AS valor_total
        FROM lotes
    """)).fetchone()
    return dict(r._mapping)


@router.get("/inventario/proyectos",
    summary="Lista de proyectos",
    description="Todos los proyectos activos con sus estadísticas de inventario.")
async def api_inventario_proyectos(
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    rows = db.execute(text("""
        SELECT
            p.id,
            p.empresa_sap,
            p.nombre_sociedad,
            p.nombre_proyecto,
            COUNT(l.id) AS total_lotes,
            COUNT(l.id) FILTER (WHERE l.estatus='DISPONIBLE') AS disponibles,
            COUNT(l.id) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO')) AS vendidos,
            COUNT(l.id) FILTER (WHERE l.estatus='BLOQUEADO') AS bloqueados,
            COALESCE(SUM(l.precio_final) FILTER (WHERE l.estatus='DISPONIBLE'), 0) AS valor_disponible,
            ROUND(
                COUNT(l.id) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO'))::numeric
                / NULLIF(COUNT(l.id), 0) * 100, 1
            ) AS absorcion_pct
        FROM proyectos p
        LEFT JOIN lotes l ON l.proyecto_id = p.id
        WHERE p.activo = TRUE
        GROUP BY p.id, p.empresa_sap, p.nombre_sociedad, p.nombre_proyecto
        ORDER BY p.nombre_proyecto
    """)).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/inventario/lotes",
    summary="Lista de lotes",
    description="""Lista paginada de lotes con filtros opcionales.
    
**Filtros disponibles:**
- `proyecto_id`: ID del proyecto (ver /inventario/proyectos)
- `estatus`: DISPONIBLE | VENTA | RESERVADO | BLOQUEADO | CANJE
- `forma_pago`: CONTADO | CREDITOSININTERES | CREDITOCONINTERES
- `page` / `page_size`: Paginación (max 200 por página)
""")
async def api_inventario_lotes(
    proyecto_id: Optional[int] = Query(None, description="ID del proyecto"),
    estatus: Optional[str] = Query(None, description="Estatus del lote"),
    forma_pago: Optional[str] = Query(None, description="Forma de pago"),
    page: int = Query(1, ge=1, description="Número de página"),
    page_size: int = Query(50, ge=1, le=200, description="Registros por página"),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    conditions = ["1=1"]
    params = {}
    if proyecto_id:
        conditions.append("l.proyecto_id = :proyecto_id")
        params["proyecto_id"] = proyecto_id
    if estatus:
        conditions.append("l.estatus = :estatus")
        params["estatus"] = estatus.upper()
    if forma_pago:
        conditions.append("l.forma_pago = :forma_pago")
        params["forma_pago"] = forma_pago

    where = " AND ".join(conditions)
    total = db.execute(text(f"SELECT COUNT(*) FROM lotes l WHERE {where}"), params).scalar()

    rows = db.execute(text(f"""
        SELECT
            l.id,
            l.unidad_key,
            l.manzana,
            l.metraje_inventario,
            l.metraje_orden,
            l.precio_sin_descuento,
            l.descuento,
            l.precio_final,
            l.total_intereses,
            l.estatus,
            l.forma_pago,
            l.plazo,
            l.card_code,
            l.card_name,
            l.vendedor,
            l.fecha_venta,
            l.fecha_solicitud_pcv,
            l.status_promesa_compraventa,
            l.pagado_capital,
            l.pendiente_capital,
            l.saldo_cliente,
            p.nombre_proyecto,
            p.nombre_sociedad,
            p.empresa_sap
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE {where}
        ORDER BY p.nombre_proyecto, l.manzana, l.unidad_key
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": (page-1)*page_size}).fetchall()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": -(-total // page_size),
        "lotes": [dict(r._mapping) for r in rows]
    }


@router.get("/inventario/lotes/{unidad_key}",
    summary="Detalle de un lote",
    description="Información completa de un lote específico por su clave única (ej: GA-060, A1, J5).")
async def api_inventario_lote(
    unidad_key: str,
    proyecto_id: Optional[int] = Query(None, description="ID del proyecto (requerido si la clave no es única global)"),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    params = {"uk": unidad_key}
    pf = "AND l.proyecto_id = :pid" if proyecto_id else ""
    if proyecto_id: params["pid"] = proyecto_id

    row = db.execute(text(f"""
        SELECT l.*, p.nombre_proyecto, p.nombre_sociedad, p.empresa_sap
        FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.unidad_key = :uk {pf}
        LIMIT 1
    """), params).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Lote '{unidad_key}' no encontrado")
    return dict(row._mapping)


# ══════════════════════════════════════════════════════════════
# CARTERA
# ══════════════════════════════════════════════════════════════

@router.get("/cartera/resumen",
    summary="Resumen de cartera",
    description="KPIs de cartera: capital total, intereses, mora, clientes activos y proyección de cobro.")
async def api_cartera_resumen(
    empresa: Optional[str] = Query(None, description="Nombre de la empresa SAP (ej: Eficiencia Urbana)"),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    pf = "AND empresa = :empresa" if empresa else ""
    params = {"pcv": "BB"}
    if empresa: params["empresa"] = empresa

    r = db.execute(text(f"""
        SELECT
            SUM(CASE WHEN tipo_linea='BB' AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS capital_pendiente,
            SUM(CASE WHEN tipo_linea='S'  AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS intereses_pendientes,
            SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN saldo_pendiente ELSE 0 END) AS cartera_total,
            SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
                     AND fecha_programada_cobro < CURRENT_DATE THEN saldo_pendiente ELSE 0 END) AS mora_total,
            COUNT(DISTINCT CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN card_code END) AS clientes_activos,
            COUNT(DISTINCT CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
                                AND fecha_programada_cobro < CURRENT_DATE THEN card_code END) AS clientes_vencidos,
            SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+30
                     THEN saldo_pendiente ELSE 0 END) AS cobro_proyectado_30d,
            SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+90
                     THEN saldo_pendiente ELSE 0 END) AS cobro_proyectado_90d
        FROM ov_cartera
        WHERE tipo_linea IN ('BB','S') {pf}
    """), params).fetchone()

    d = dict(r._mapping)
    cartera = float(d.get("cartera_total") or 0)
    mora = float(d.get("mora_total") or 0)
    d["tasa_mora_pct"] = round(mora / cartera * 100, 2) if cartera > 0 else 0
    return {k: float(v) if isinstance(v, (int, float)) and v is not None else v for k, v in d.items()}


@router.get("/cartera/clientes/{card_code}",
    summary="Estado de cuenta de un cliente",
    description="""Estado de cuenta completo de un cliente por su código SAP.
    
Incluye resumen y detalle de todas las cuotas (capital e intereses) con su estado:
PAGADO | VENCIDO | PROXIMO | PENDIENTE
""")
async def api_cartera_cliente(
    card_code: str,
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    resumen = db.execute(text("""
        SELECT
            card_code, card_name,
            empresa,
            COUNT(DISTINCT doc_entry) AS num_lotes,
            SUM(CASE WHEN tipo_linea='BB' AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS capital_pendiente,
            SUM(CASE WHEN tipo_linea='S'  AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS intereses_pendientes,
            SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN saldo_pendiente ELSE 0 END) AS saldo_total,
            SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
                     AND fecha_programada_cobro < CURRENT_DATE THEN saldo_pendiente ELSE 0 END) AS monto_vencido
        FROM ov_cartera
        WHERE card_code = :cc AND tipo_linea IN ('BB','S')
        GROUP BY card_code, card_name, empresa
    """), {"cc": card_code}).fetchone()

    if not resumen:
        raise HTTPException(status_code=404, detail=f"Cliente '{card_code}' no encontrado en cartera")

    cuotas = db.execute(text("""
        SELECT
            doc_entry, doc_num, referencia_manzana_lote,
            tipo_linea, line_total, saldo_pendiente,
            fecha_programada_cobro, line_status,
            CASE
                WHEN line_status='C' THEN 'PAGADO'
                WHEN fecha_programada_cobro < CURRENT_DATE AND line_status='O' THEN 'VENCIDO'
                WHEN fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+30 THEN 'PROXIMO'
                ELSE 'PENDIENTE'
            END AS estado_cuota,
            CASE WHEN fecha_programada_cobro < CURRENT_DATE AND line_status='O'
                 THEN CURRENT_DATE - fecha_programada_cobro ELSE 0 END AS dias_vencido
        FROM ov_cartera
        WHERE card_code = :cc AND tipo_linea IN ('BB','S')
        ORDER BY referencia_manzana_lote, tipo_linea, fecha_programada_cobro
    """), {"cc": card_code}).fetchall()

    return {
        "resumen": dict(resumen._mapping),
        "cuotas": [dict(r._mapping) for r in cuotas]
    }


# ══════════════════════════════════════════════════════════════
# VENTAS
# ══════════════════════════════════════════════════════════════

@router.get("/ventas/resumen",
    summary="Resumen de ventas",
    description="KPIs de ventas: brutas, netas, desistimientos y mezcla de forma de pago para un período.")
async def api_ventas_resumen(
    año: int = Query(2026, description="Año de análisis"),
    mes: Optional[int] = Query(None, description="Mes (1-12). Si se omite, devuelve todo el año."),
    proyecto: Optional[str] = Query(None, description="Nombre del proyecto"),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    if mes:
        date_filter = "DATE_TRUNC('month', l.fecha_venta) = make_date(:año, :mes, 1)"
        params = {"año": año, "mes": mes}
    else:
        date_filter = "EXTRACT(YEAR FROM l.fecha_venta) = :año"
        params = {"año": año}
    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    if proyecto: params["proyecto"] = proyecto

    r = db.execute(text(f"""
        SELECT
            COUNT(*) AS ventas_brutas,
            COALESCE(SUM(l.precio_final), 0) AS valor_bruto,
            COALESCE(SUM(l.total_intereses), 0) AS intereses_pactados,
            COALESCE(AVG(l.precio_final), 0) AS ticket_promedio,
            COUNT(*) FILTER (WHERE l.forma_pago='CONTADO') AS contado,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOSININTERES') AS credito_sin_interes,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOCONINTERES') AS credito_con_interes,
            COALESCE(AVG(l.plazo) FILTER (WHERE l.plazo > 0), 0) AS plazo_promedio_meses
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO')
          AND {date_filter} AND l.fecha_venta IS NOT NULL {pf}
    """), params).fetchone()

    desist = db.execute(text(f"""
        SELECT COUNT(*) AS total, COALESCE(SUM(precio_con_descuento), 0) AS valor
        FROM desistimientos
        WHERE EXTRACT(YEAR FROM fecha_desistimiento) = :año
        {'AND EXTRACT(MONTH FROM fecha_desistimiento) = :mes' if mes else ''}
    """), params).fetchone()

    d = dict(r._mapping)
    brutas = int(d.get("ventas_brutas") or 0)
    desist_total = int(desist.total or 0)
    d["desistimientos"] = desist_total
    d["valor_desistido"] = float(desist.valor or 0)
    d["ventas_netas"] = brutas - desist_total
    d["tasa_desistimiento_pct"] = round(desist_total / brutas * 100, 1) if brutas > 0 else 0
    return {k: float(v) if isinstance(v, (int, float)) and v is not None else v for k, v in d.items()}


@router.get("/ventas/pcv",
    summary="Control de PCV pendientes",
    description="Lista de lotes vendidos sin Promesa de Compraventa firmada. Ordenados por antigüedad.")
async def api_ventas_pcv(
    proyecto: Optional[str] = Query(None, description="Nombre del proyecto"),
    antiguedad_min_dias: int = Query(0, description="Solo mostrar pendientes con más de N días sin PCV"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    conditions = [
        "l.estatus IN ('VENTA','RESERVADO')",
        "l.fecha_venta IS NOT NULL",
        "l.status_promesa_compraventa != 'Contrato Promesa de Compra venta'",
        "l.vendedor NOT IN ('-Ningún empleado del departamento de ventas-','Canje A','Bloqueado','Bloqueo Municipal','Apartado Proyecto Aptos')",
        f"CURRENT_DATE - l.fecha_venta::date >= {antiguedad_min_dias}"
    ]
    params = {}
    if proyecto:
        conditions.append("p.nombre_proyecto = :proyecto")
        params["proyecto"] = proyecto

    where = " AND ".join(conditions)
    total = db.execute(text(f"SELECT COUNT(*) FROM lotes l JOIN proyectos p ON p.id=l.proyecto_id WHERE {where}"), params).scalar()

    rows = db.execute(text(f"""
        SELECT l.unidad_key, l.manzana, l.card_code, l.card_name, l.vendedor,
               p.nombre_proyecto, l.fecha_venta, l.status_promesa_compraventa,
               l.precio_final, l.forma_pago, l.plazo,
               CURRENT_DATE - l.fecha_venta::date AS dias_sin_pcv
        FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id
        WHERE {where}
        ORDER BY dias_sin_pcv DESC
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": (page-1)*page_size}).fetchall()

    return {
        "total": total,
        "page": page,
        "pages": -(-total // page_size),
        "pendientes": [dict(r._mapping) for r in rows]
    }


# ══════════════════════════════════════════════════════════════
# DESISTIMIENTOS
# ══════════════════════════════════════════════════════════════

@router.get("/desistimientos",
    summary="Lista de desistimientos",
    description="Todos los desistimientos registrados con detalle de pagos y reintegros.")
async def api_desistimientos(
    empresa: Optional[str] = Query(None, description="Nombre de la empresa"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    where = "1=1"
    params = {}
    if empresa:
        where = "empresa = :empresa"
        params["empresa"] = empresa

    total = db.execute(text(f"SELECT COUNT(*) FROM desistimientos WHERE {where}"), params).scalar()
    rows = db.execute(text(f"""
        SELECT empresa, no_orden_venta, codigo_cliente, nombre_cliente,
               lote, asesor_venta, fecha_venta, fecha_desistimiento, plazo,
               precio_venta, precio_con_descuento, pagado_capital,
               reintegrado_cliente, total_desistimiento, motivo_desistimiento
        FROM desistimientos WHERE {where}
        ORDER BY fecha_desistimiento DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": (page-1)*page_size}).fetchall()

    return {
        "total": total, "page": page, "pages": -(-total // page_size),
        "desistimientos": [dict(r._mapping) for r in rows]
    }
