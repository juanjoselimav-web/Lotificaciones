"""
==============================================================
API EXTERNA — Integración I360
Prefijo: /api/ext/v1
Autenticación: X-API-Key (mismo sistema que api_publica)
Generado: Mayo 2026
==============================================================
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_db
from app.routers.api_publica import verify_api_key, verify_admin_key
from datetime import datetime
from typing import Optional

router = APIRouter(prefix="/api/ext/v1", tags=["I360 Integration"])

# ── Mapeo I360 ID → datos internos RV4 ────────────────────────────────────────
MAPEO_I360 = {
    360006: {"empresa_sap": "SBO_EFICIENCIA_URBANA", "sociedad": "EFICIENCIA URBANA", "nombre": "Hacienda Jumay"},
    360007: {"empresa_sap": "SBO_TEZZOLI",            "sociedad": "TEZZOLI",           "nombre": "Club Campestre Jumay"},
    360008: {"empresa_sap": "SBO_OTTAVIA",            "sociedad": "OTTAVIA",            "nombre": "Cañadas de Jalapa"},
    360009: {"empresa_sap": "SBO_UTILICA",            "sociedad": "UTILICA",            "nombre": "Condado Jutiapa"},
    360010: {"empresa_sap": "SBO_ROSSIO",             "sociedad": "ROSSIO",             "nombre": "Hacienda el Sol"},
    360011: {"empresa_sap": "SBO_GARBATELLA",         "sociedad": "GARBATELLA",         "nombre": "Club Residencial El Progreso"},
    360012: {"empresa_sap": "SBO_URBIVA_2",           "sociedad": "URBIVA",             "nombre": "Club del Bosque"},
    360013: {"empresa_sap": "SBO_SER_GEN_CCC",        "sociedad": "SER GEN CCC",        "nombre": "La Ceiba"},
    360014: {"empresa_sap": "SBO_OVEST",              "sociedad": "OVEST",              "nombre": "Hacienda Santa Lucia"},
    360015: {"empresa_sap": "SBO_FRUGALEX",           "sociedad": "FRUGALEX",           "nombre": "Oasis Zacapa"},
    360016: {"empresa_sap": "SBO_VILET",              "sociedad": "VILET",              "nombre": "Celajes De Tecpan"},
    360017: {"empresa_sap": "SBO_CAPIPOS",            "sociedad": "CAPIPOS",            "nombre": "Arboleada Santa Elena"},
    360018: {"empresa_sap": "SBO_CORCOLLE",           "sociedad": "CORCOLLE",           "nombre": "Hacienda El Cafetal Fase I"},
    360019: {"empresa_sap": "SBO_LEOFRENI",           "sociedad": "LEOFRENI",           "nombre": "Hacienda El Cafetal Fase II"},
    360020: {"empresa_sap": "SBO_GIBRALEON",          "sociedad": "GIBRALEON",          "nombre": "Hacienda El Cafetal Fase III"},
    360021: {"empresa_sap": "SBO_TALOCCI",            "sociedad": "TALOCCI",            "nombre": "Hacienda El Cafetal Fase IV"},
}


def _get_proyecto(proyecto_id: Optional[int]):
    """Retorna datos del proyecto o lanza 404 si no existe."""
    if proyecto_id is None:
        return None
    info = MAPEO_I360.get(proyecto_id)
    if not info:
        raise HTTPException(status_code=404,
            detail=f"Proyecto I360 {proyecto_id} no encontrado. IDs válidos: {sorted(MAPEO_I360.keys())}")
    return info


# ── 1. EMPRESAS ───────────────────────────────────────────────────────────────
@router.get("/empresas",
    summary="Lista de proyectos disponibles",
    description="Retorna todos los proyectos activos en RV4 con su ID I360 y métricas básicas.")
async def ext_empresas(
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    rows = db.execute(text("""
        SELECT
            p.empresa_sap,
            p.nombre_proyecto,
            p.nombre_sociedad,
            COUNT(l.id)                                                             AS total_lotes,
            COUNT(l.id) FILTER (WHERE l.estatus = 'DISPONIBLE')                    AS disponibles,
            COUNT(l.id) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO'))           AS vendidos,
            COUNT(l.id) FILTER (WHERE l.estatus = 'BLOQUEADO')                     AS bloqueados,
            COALESCE(SUM(l.precio_final) FILTER (
                WHERE l.estatus IN ('VENTA','RESERVADO')), 0)                       AS valor_vendido
        FROM proyectos p
        LEFT JOIN lotes l ON l.proyecto_id = p.id
        WHERE p.activo = TRUE
        GROUP BY p.empresa_sap, p.nombre_proyecto, p.nombre_sociedad
        ORDER BY p.nombre_proyecto
    """)).fetchall()

    # Construir respuesta enriquecida con ID I360
    empresa_sap_to_id = {v["empresa_sap"]: k for k, v in MAPEO_I360.items()}
    resultado = []
    for r in rows:
        d = dict(r._mapping)
        proyecto_id_i360 = empresa_sap_to_id.get(d["empresa_sap"])
        if proyecto_id_i360:
            absorcion = round(
                d["vendidos"] / d["total_lotes"] * 100, 1
            ) if d["total_lotes"] > 0 else 0
            resultado.append({
                "proyecto_id":      proyecto_id_i360,
                "empresa_sap":      d["empresa_sap"],
                "nombre_proyecto":  d["nombre_proyecto"],
                "nombre_sociedad":  d["nombre_sociedad"],
                "total_lotes":      d["total_lotes"],
                "disponibles":      d["disponibles"],
                "vendidos":         d["vendidos"],
                "bloqueados":       d["bloqueados"],
                "absorcion_pct":    absorcion,
                "valor_vendido":    float(d["valor_vendido"] or 0),
            })

    return {
        "generadoEn": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "total_proyectos": len(resultado),
        "proyectos": resultado
    }


# ── 2. INVENTARIO ─────────────────────────────────────────────────────────────
@router.get("/inventario",
    summary="Estado detallado de inventario por proyecto",
    description="Retorna lotes con estado, cliente y precios. Filtrable por proyecto_id I360.")
async def ext_inventario(
    proyecto_id: Optional[int] = Query(None, description="ID I360 del proyecto (ej. 360006)"),
    estatus: Optional[str] = Query(None, description="Filtrar por: DISPONIBLE, VENTA, RESERVADO, BLOQUEADO"),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    info = _get_proyecto(proyecto_id)
    conditions = ["1=1"]
    params = {}

    if info:
        conditions.append("p.empresa_sap = :empresa_sap")
        params["empresa_sap"] = info["empresa_sap"]
    if estatus:
        conditions.append("l.estatus = :estatus")
        params["estatus"] = estatus.upper()

    where = " AND ".join(conditions)

    lotes = db.execute(text(f"""
        SELECT
            p.empresa_sap,
            p.nombre_proyecto,
            l.manzana,
            l.unidad_key                    AS lote_referencia,
            l.unidad_actual,
            l.metraje_inventario            AS metros_cuadrados,
            l.estatus,
            l.estatus_raw,
            l.precio_sin_descuento,
            l.descuento,
            l.precio_final,
            l.forma_pago,
            l.card_code                     AS cliente_codigo,
            l.card_name                     AS cliente_nombre,
            l.vendedor,
            l.fecha_venta,
            l.fecha_inicial_cobro,
            l.fecha_final_cobro,
            l.plazo,
            l.pagado_capital,
            l.pendiente_capital,
            l.cuotas_pagadas,
            l.cuotas_pendientes,
            l.updated_at                    AS ultima_actualizacion
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE {where}
        ORDER BY p.nombre_proyecto, l.manzana, l.unidad_key
    """), params).fetchall()

    # Totales rápidos
    total   = len(lotes)
    disp    = sum(1 for r in lotes if r.estatus == 'DISPONIBLE')
    vend    = sum(1 for r in lotes if r.estatus in ('VENTA', 'RESERVADO'))
    bloq    = sum(1 for r in lotes if r.estatus == 'BLOQUEADO')

    return {
        "generadoEn":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "proyecto_id":  proyecto_id,
        "resumen": {
            "total":        total,
            "disponibles":  disp,
            "vendidos":     vend,
            "bloqueados":   bloq,
            "absorcion_pct": round(vend / total * 100, 1) if total > 0 else 0
        },
        "lotes": [
            {k: (float(v) if hasattr(v, '__float__') and not isinstance(v, bool) else
                 v.isoformat() if hasattr(v, 'isoformat') else v)
             for k, v in dict(r._mapping).items()}
            for r in lotes
        ]
    }


# ── 3. ESTADO GENERAL ─────────────────────────────────────────────────────────
@router.get("/estado-general",
    summary="Resumen ejecutivo — todos los proyectos o uno específico",
    description="KPIs de alto nivel: inventario, cartera, flujos del mes y desistimientos.")
async def ext_estado_general(
    proyecto_id: Optional[int] = Query(None),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    info = _get_proyecto(proyecto_id)
    now  = datetime.utcnow()

    # ── Inventario ──
    inv_where  = "p.empresa_sap = :empresa_sap" if info else "1=1"
    inv_params = {"empresa_sap": info["empresa_sap"]} if info else {}

    inv = db.execute(text(f"""
        SELECT
            COUNT(l.id)                                                       AS total,
            COUNT(l.id) FILTER (WHERE l.estatus = 'DISPONIBLE')              AS disponibles,
            COUNT(l.id) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO'))    AS vendidos,
            COALESCE(SUM(l.precio_final) FILTER (
                WHERE l.estatus IN ('VENTA','RESERVADO')), 0)                 AS valor_ventas
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE {inv_where}
    """), inv_params).fetchone()

    # ── Cartera del mes ──
    cart_where  = "empresa = :sociedad" if info else "1=1"
    cart_params = {"sociedad": info["sociedad"]} if info else {}

    cartera = db.execute(text(f"""
        SELECT
            COALESCE(SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END), 0) AS saldo_total,
            COALESCE(SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                              THEN saldo_pendiente ELSE 0 END), 0)                       AS mora_total,
            COUNT(DISTINCT CASE WHEN line_status='O' THEN card_code END)                AS clientes_activos
        FROM ov_cartera
        WHERE tipo_linea IN ('BB','S') AND {cart_where}
    """), cart_params).fetchone()

    # ── Flujos del mes actual ──
    fl_where  = "AND sociedad = :sociedad" if info else ""
    fl_params = {"anio": now.year, "mes": now.month}
    if info:
        fl_params["sociedad"] = info["sociedad"]

    flujos = db.execute(text(f"""
        SELECT
            COALESCE(SUM(monto_ingreso), 0) AS ingresos,
            COALESCE(SUM(monto_egreso),  0) AS egresos
        FROM flujos_efectivo
        WHERE EXTRACT(YEAR  FROM fecha_contable) = :anio
          AND EXTRACT(MONTH FROM fecha_contable) = :mes
          {fl_where}
    """), fl_params).fetchone()

    # ── Desistimientos ──
    des_where  = "WHERE empresa = :sociedad" if info else ""
    des_params = {"sociedad": info["sociedad"]} if info else {}

    desist = db.execute(text(f"""
        SELECT COUNT(*) AS total,
               COALESCE(SUM(pagado_capital), 0) AS monto_pagado
        FROM desistimientos {des_where}
    """), des_params).fetchone()

    saldo_cart = float(cartera.saldo_total or 0)
    mora_cart  = float(cartera.mora_total  or 0)

    return {
        "generadoEn":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "proyecto_id":   proyecto_id,
        "proyecto":      info["nombre"] if info else "Todos los proyectos",
        "periodo":       f"{now.year}-{now.month:02d}",
        "inventario": {
            "total_lotes":   int(inv.total   or 0),
            "disponibles":   int(inv.disponibles or 0),
            "vendidos":      int(inv.vendidos or 0),
            "absorcion_pct": round(int(inv.vendidos or 0) / int(inv.total or 1) * 100, 1),
            "valor_vendido": float(inv.valor_ventas or 0),
        },
        "cartera": {
            "saldo_total":      saldo_cart,
            "mora_total":       mora_cart,
            "tasa_mora_pct":    round(mora_cart / saldo_cart * 100, 2) if saldo_cart > 0 else 0,
            "clientes_activos": int(cartera.clientes_activos or 0),
        },
        "flujos_mes": {
            "ingresos": float(flujos.ingresos or 0),
            "egresos":  float(flujos.egresos  or 0),
            "neto":     float((flujos.ingresos or 0) - (flujos.egresos or 0)),
        },
        "desistimientos": {
            "total":       int(desist.total or 0),
            "monto_pagado": float(desist.monto_pagado or 0),
        }
    }


# ── 4. CARTERA ────────────────────────────────────────────────────────────────
@router.get("/cartera",
    summary="KPIs de cartera por proyecto",
    description="Saldos pendientes, mora, cobros próximos a 30/60/90 días.")
async def ext_cartera(
    proyecto_id: Optional[int] = Query(None),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    info = _get_proyecto(proyecto_id)
    where  = "empresa = :sociedad AND" if info else ""
    params = {"sociedad": info["sociedad"]} if info else {}

    kpis = db.execute(text(f"""
        SELECT
            SUM(CASE WHEN tipo_linea='BB' AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS capital_pendiente,
            SUM(CASE WHEN tipo_linea='S'  AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS intereses_pendientes,
            SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END)                     AS cartera_total,
            SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                     THEN saldo_pendiente ELSE 0 END)                                          AS mora_total,
            COUNT(DISTINCT CASE WHEN line_status='O' THEN card_code END)                       AS clientes_activos,
            COUNT(DISTINCT CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                                THEN card_code END)                                            AS clientes_en_mora,
            SUM(CASE WHEN line_status='O'
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+30
                     THEN saldo_pendiente ELSE 0 END)                                          AS cobro_30d,
            SUM(CASE WHEN line_status='O'
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+60
                     THEN saldo_pendiente ELSE 0 END)                                          AS cobro_60d,
            SUM(CASE WHEN line_status='O'
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+90
                     THEN saldo_pendiente ELSE 0 END)                                          AS cobro_90d
        FROM ov_cartera
        WHERE {where} tipo_linea IN ('BB','S')
    """), params).fetchone()

    d = {k: float(v) if v is not None else 0.0
         for k, v in dict(kpis._mapping).items()}
    d["tasa_mora_pct"] = round(d["mora_total"] / d["cartera_total"] * 100, 2) if d["cartera_total"] > 0 else 0
    d["generadoEn"]    = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
    d["proyecto_id"]   = proyecto_id
    d["proyecto"]      = info["nombre"] if info else "Todos los proyectos"
    return d


# ── 5. PROYECCION INGRESOS ───────────────────────────────────────────────────
@router.get("/proyeccion-ingresos",
    summary="Proyección de ingresos: histórico + cobros programados",
    description="Ingresos reales de flujos + cobros proyectados de cartera por mes.")
async def ext_proyeccion_ingresos(
    proyecto_id: Optional[int] = Query(None),
    meses: int = Query(6, ge=1, le=24, description="Meses hacia adelante a proyectar"),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    info   = _get_proyecto(proyecto_id)
    now    = datetime.utcnow()

    # Flujos históricos últimos 12 meses
    fl_where  = "AND sociedad = :sociedad" if info else ""
    fl_params = {"sociedad": info["sociedad"]} if info else {}

    historico = db.execute(text(f"""
        SELECT
            EXTRACT(YEAR  FROM fecha_contable)::INT AS anio,
            EXTRACT(MONTH FROM fecha_contable)::INT AS mes,
            COALESCE(SUM(monto_ingreso), 0) AS ingresos,
            COALESCE(SUM(monto_egreso),  0) AS egresos
        FROM flujos_efectivo
        WHERE fecha_contable >= (CURRENT_DATE - INTERVAL '12 months')
          {fl_where}
        GROUP BY anio, mes
        ORDER BY anio, mes
    """), fl_params).fetchall()

    # Cobros proyectados por mes (cartera pendiente)
    cart_where  = "AND empresa = :sociedad" if info else ""
    cart_params = {"sociedad": info["sociedad"]} if info else {}

    proyectado = db.execute(text(f"""
        SELECT
            EXTRACT(YEAR  FROM fecha_programada_cobro)::INT AS anio,
            EXTRACT(MONTH FROM fecha_programada_cobro)::INT AS mes,
            COALESCE(SUM(saldo_pendiente), 0)               AS cobro_proyectado
        FROM ov_cartera
        WHERE line_status = 'O'
          AND tipo_linea = 'BB'
          AND fecha_programada_cobro BETWEEN CURRENT_DATE
              AND (CURRENT_DATE + (:meses || ' months')::INTERVAL)
          {cart_where}
        GROUP BY anio, mes
        ORDER BY anio, mes
    """), {"meses": meses, **cart_params}).fetchall()

    return {
        "generadoEn":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "proyecto_id":  proyecto_id,
        "proyecto":     info["nombre"] if info else "Todos los proyectos",
        "historico_12m": [
            {"anio": r.anio, "mes": r.mes,
             "ingresos": float(r.ingresos), "egresos": float(r.egresos)}
            for r in historico
        ],
        "proyeccion_cartera": [
            {"anio": r.anio, "mes": r.mes, "cobro_proyectado": float(r.cobro_proyectado)}
            for r in proyectado
        ]
    }


# ── 6. DETALLE CARTERA (admin) ────────────────────────────────────────────────
@router.get("/detalle-cartera",
    summary="Detalle cliente a cliente — solo API Key admin",
    description="Requiere X-API-Key con nivel admin. Retorna cartera paginada por cliente.")
async def ext_detalle_cartera(
    proyecto_id: Optional[int] = Query(None),
    estado: Optional[str] = Query(None, description="AL_DIA o VENCIDO"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    api_key=Depends(verify_admin_key),
    db: Session = Depends(get_db)
):
    info = _get_proyecto(proyecto_id)
    conditions = ["tipo_linea IN ('BB','S')"]
    params: dict = {}

    if info:
        conditions.append("empresa = :sociedad")
        params["sociedad"] = info["sociedad"]

    where = " AND ".join(conditions)

    clientes = db.execute(text(f"""
        SELECT
            empresa,
            card_code,
            card_name,
            slp_name                                                                AS asesor,
            COUNT(DISTINCT doc_entry)                                               AS num_lotes,
            SUM(CASE WHEN tipo_linea='BB' AND line_status='O'
                     THEN saldo_pendiente ELSE 0 END)                              AS capital_pendiente,
            SUM(CASE WHEN tipo_linea='S'  AND line_status='O'
                     THEN saldo_pendiente ELSE 0 END)                              AS intereses_pendientes,
            SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END)        AS saldo_total,
            SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                     THEN saldo_pendiente ELSE 0 END)                              AS monto_vencido,
            CASE WHEN SUM(CASE WHEN line_status='O'
                               AND fecha_programada_cobro < CURRENT_DATE
                               THEN saldo_pendiente ELSE 0 END) > 0
                 THEN 'VENCIDO' ELSE 'AL_DIA' END                                 AS estado_cartera
        FROM ov_cartera
        WHERE {where}
        GROUP BY empresa, card_code, card_name, slp_name
        HAVING SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END) > 0
        {("HAVING" if "HAVING" not in "".join(conditions) else "AND") + " CASE WHEN SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE THEN saldo_pendiente ELSE 0 END) > 0 THEN 'VENCIDO' ELSE 'AL_DIA' END = :estado" if estado else ""}
        ORDER BY monto_vencido DESC, saldo_total DESC
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": (page-1)*page_size,
           **( {"estado": estado.upper()} if estado else {})}).fetchall()

    return {
        "generadoEn":  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "proyecto_id": proyecto_id,
        "page":        page,
        "page_size":   page_size,
        "clientes": [
            {k: (float(v) if hasattr(v, '__float__') and not isinstance(v, bool) else
                 v.isoformat() if hasattr(v, 'isoformat') else v)
             for k, v in dict(r._mapping).items()}
            for r in clientes
        ]
    }


# ── 7. FLUJO EFECTIVO ─────────────────────────────────────────────────────────
@router.get("/flujo",
    summary="Histórico de flujos de efectivo",
    description="Ingresos y egresos reales por período. Filtrable por proyecto y año.")
async def ext_flujo(
    proyecto_id: Optional[int] = Query(None),
    anio: Optional[int] = Query(None, description="Año a consultar, ej. 2025"),
    granularidad: str = Query("mes", regex="^(mes|anio)$"),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    info = _get_proyecto(proyecto_id)
    conditions = ["1=1"]
    params: dict = {}

    if info:
        conditions.append("sociedad = :sociedad")
        params["sociedad"] = info["sociedad"]
    if anio:
        conditions.append("EXTRACT(YEAR FROM fecha_contable) = :anio")
        params["anio"] = anio

    where = " AND ".join(conditions)
    group_by = "EXTRACT(YEAR FROM fecha_contable)::INT, EXTRACT(MONTH FROM fecha_contable)::INT" \
               if granularidad == "mes" \
               else "EXTRACT(YEAR FROM fecha_contable)::INT"

    select_period = """
        EXTRACT(YEAR  FROM fecha_contable)::INT AS anio,
        EXTRACT(MONTH FROM fecha_contable)::INT AS mes,
    """ if granularidad == "mes" else """
        EXTRACT(YEAR FROM fecha_contable)::INT AS anio,
        NULL::INT AS mes,
    """

    rows = db.execute(text(f"""
        SELECT
            {select_period}
            COALESCE(SUM(monto_ingreso), 0) AS ingresos,
            COALESCE(SUM(monto_egreso),  0) AS egresos,
            COALESCE(SUM(monto_ingreso) - SUM(monto_egreso), 0) AS neto
        FROM flujos_efectivo
        WHERE {where}
        GROUP BY {group_by}
        ORDER BY anio, mes NULLS LAST
    """), params).fetchall()

    # Saldo inicial
    si_params = {"sociedad": info["sociedad"]} if info else {}
    si_where  = "WHERE sociedad = :sociedad" if info else ""
    saldo_inicial = db.execute(text(f"""
        SELECT COALESCE(SUM(monto), 0) AS total
        FROM flujos_saldo_inicial {si_where}
    """), si_params).scalar() or 0

    return {
        "generadoEn":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "proyecto_id":   proyecto_id,
        "proyecto":      info["nombre"] if info else "Todos los proyectos",
        "saldo_inicial": float(saldo_inicial),
        "granularidad":  granularidad,
        "periodos": [
            {"anio": r.anio, "mes": r.mes,
             "ingresos": float(r.ingresos),
             "egresos":  float(r.egresos),
             "neto":     float(r.neto)}
            for r in rows
        ]
    }


# ── 8. METAS ─────────────────────────────────────────────────────────────────
@router.get("/metas",
    summary="Metas de venta vs. resultados reales",
    description="Compara metas registradas en el sistema contra ventas reales por período.")
async def ext_metas(
    proyecto_id: Optional[int] = Query(None),
    anio: Optional[int] = Query(None),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db)
):
    info = _get_proyecto(proyecto_id)
    now  = datetime.utcnow()
    anio = anio or now.year

    # Intentar obtener columnas de metas_ventas (pueden variar)
    meta_where  = "AND empresa = :sociedad" if info else ""
    meta_params = {"anio": anio}
    if info:
        meta_params["sociedad"] = info["sociedad"]

    metas = db.execute(text(f"""
        SELECT *
        FROM metas_ventas
        WHERE anio = :anio {meta_where}
        ORDER BY mes
    """), meta_params).fetchall()

    # Ventas reales del mismo período (lotes vendidos por mes)
    inv_where  = "AND p.empresa_sap = :empresa_sap" if info else ""
    inv_params = {"anio": anio}
    if info:
        inv_params["empresa_sap"] = info["empresa_sap"]

    reales = db.execute(text(f"""
        SELECT
            EXTRACT(MONTH FROM l.fecha_venta)::INT AS mes,
            COUNT(*)                                AS unidades_vendidas,
            COALESCE(SUM(l.precio_final), 0)        AS monto_vendido
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO')
          AND EXTRACT(YEAR FROM l.fecha_venta) = :anio
          AND l.fecha_venta IS NOT NULL
          {inv_where}
        GROUP BY mes
        ORDER BY mes
    """), inv_params).fetchall()

    reales_map = {r.mes: {"unidades": int(r.unidades_vendidas),
                           "monto": float(r.monto_vendido)} for r in reales}

    return {
        "generadoEn":  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "proyecto_id": proyecto_id,
        "proyecto":    info["nombre"] if info else "Todos los proyectos",
        "anio":        anio,
        "metas": [dict(r._mapping) for r in metas],
        "reales_por_mes": [
            {"mes": mes, **data} for mes, data in sorted(reales_map.items())
        ]
    }
