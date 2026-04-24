from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from app.database import get_db
from app.core.security import get_current_user

router = APIRouter(prefix="/api/cartera", tags=["cartera"])


def get_empresas_permitidas(current_user, db: Session) -> list[str]:
    """Mapea proyectos permitidos a nombres de empresa del archivo OV."""
    if current_user.nivel >= 3:
        result = db.execute(text("SELECT DISTINCT empresa FROM ov_cartera ORDER BY empresa")).fetchall()
        return [r[0] for r in result]
    # Para roles menores, filtrar por proyectos asignados via slp_code
    return []


@router.get("/kpis")
async def get_kpis(
    empresa: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """KPIs globales de cartera."""
    where = "tipo_linea IN ('BB','S')"
    params = {}
    if empresa:
        where += " AND empresa = :empresa"
        params["empresa"] = empresa

    kpis = db.execute(text(f"""
        SELECT
            SUM(CASE WHEN tipo_linea='BB' AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS capital_total,
            SUM(CASE WHEN tipo_linea='S'  AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS intereses_total,
            SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END)                     AS cartera_total,
            SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                     THEN saldo_pendiente ELSE 0 END)                                          AS mora_total,
            COUNT(DISTINCT CASE WHEN line_status='O' THEN card_code END)                       AS clientes_activos,
            COUNT(DISTINCT CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                                THEN card_code END)                                            AS clientes_vencidos,
            SUM(CASE WHEN line_status='O'
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+30
                     THEN saldo_pendiente ELSE 0 END)                                          AS cobro_30d,
            SUM(CASE WHEN line_status='O'
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+60
                     THEN saldo_pendiente ELSE 0 END)                                          AS cobro_60d,
            SUM(CASE WHEN line_status='O'
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+90
                     THEN saldo_pendiente ELSE 0 END)                                          AS cobro_90d,
            SUM(CASE WHEN line_status='O'
                     AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+365
                     THEN saldo_pendiente ELSE 0 END)                                          AS cobro_365d
        FROM ov_cartera
        WHERE {where}
    """), params).fetchone()

    # Desistimientos stats
    desist = db.execute(text(f"""
        SELECT COUNT(*) as total,
               COALESCE(SUM(pagado_capital),0) as total_pagado,
               COALESCE(SUM(reintegrado_cliente),0) as total_reintegrado
        FROM desistimientos
        {'WHERE empresa = :empresa' if empresa else ''}
    """), params).fetchone()

    kpis_dict = dict(kpis._mapping)
    kpis_dict["desistimientos_total"]     = desist.total
    kpis_dict["desistimientos_pagado"]    = float(desist.total_pagado)
    kpis_dict["desistimientos_reintegrado"] = float(desist.total_reintegrado)

    # Tasa de mora
    cartera = float(kpis_dict.get("cartera_total") or 0)
    mora    = float(kpis_dict.get("mora_total") or 0)
    kpis_dict["tasa_mora"] = round(mora / cartera * 100, 2) if cartera > 0 else 0

    return {k: float(v) if v is not None and isinstance(v, (int, float)) else v
            for k, v in kpis_dict.items()}


@router.get("/empresas")
async def get_empresas(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    result = db.execute(text("""
        SELECT empresa,
               SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN saldo_pendiente ELSE 0 END) as saldo_total,
               COUNT(DISTINCT CASE WHEN line_status='O' THEN card_code END) as clientes
        FROM ov_cartera
        GROUP BY empresa ORDER BY saldo_total DESC
    """)).fetchall()
    return [dict(r._mapping) for r in result]


@router.get("/clientes")
async def get_clientes(
    empresa: Optional[str] = Query(None),
    asesor: Optional[str] = Query(None),
    estado: Optional[str] = Query(None),  # AL_DIA, VENCIDO
    buscar: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Lista de clientes con resumen de cartera."""
    conditions = ["tipo_linea IN ('BB','S')"]
    params = {}

    if empresa:
        conditions.append("empresa = :empresa")
        params["empresa"] = empresa
    if asesor:
        conditions.append("slp_name ILIKE :asesor")
        params["asesor"] = f"%{asesor}%"
    if buscar:
        conditions.append("(card_name ILIKE :buscar OR card_code ILIKE :buscar)")
        params["buscar"] = f"%{buscar}%"

    where = " AND ".join(conditions)

    clientes = db.execute(text(f"""
        SELECT
            empresa,
            card_code,
            card_name,
            slp_name AS asesor,
            COUNT(DISTINCT doc_entry) AS num_lotes,
            SUM(CASE WHEN tipo_linea='BB' AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS capital_pendiente,
            SUM(CASE WHEN tipo_linea='S'  AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS intereses_pendientes,
            SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END) AS saldo_total,
            SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                     THEN saldo_pendiente ELSE 0 END) AS monto_vencido,
            MIN(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                     THEN fecha_programada_cobro END) AS primera_fecha_vencida,
            CASE WHEN SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                               THEN saldo_pendiente ELSE 0 END) > 0 THEN 'VENCIDO' ELSE 'AL_DIA'
            END AS estado_cartera
        FROM ov_cartera
        WHERE {where}
        GROUP BY empresa, card_code, card_name, slp_name
        HAVING SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END) > 0
        ORDER BY monto_vencido DESC, saldo_total DESC
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": (page-1)*page_size}).fetchall()

    total = db.execute(text(f"""
        SELECT COUNT(*) FROM (
            SELECT card_code FROM ov_cartera WHERE {where}
            GROUP BY empresa, card_code, card_name, slp_name
            HAVING SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END) > 0
        ) t
    """), params).scalar()

    return {
        "clientes": [dict(r._mapping) for r in clientes],
        "total": total,
        "page": page,
        "pages": -(-total // page_size)
    }


@router.get("/estado-cuenta/{card_code}")
async def get_estado_cuenta(
    card_code: str,
    empresa: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Estado de cuenta completo de un cliente."""
    params = {"card_code": card_code}
    empresa_filter = "AND empresa = :empresa" if empresa else ""
    if empresa:
        params["empresa"] = empresa

    # Resumen del cliente
    resumen = db.execute(text(f"""
        SELECT
            card_code, card_name, empresa,
            slp_name AS asesor,
            MIN(doc_date) AS fecha_primera_venta,
            COUNT(DISTINCT doc_entry) AS num_lotes,
            SUM(CASE WHEN tipo_linea='BB' THEN g_total ELSE 0 END) AS precio_total_capital,
            SUM(CASE WHEN tipo_linea='S'  THEN g_total ELSE 0 END) AS total_intereses_pactados,
            SUM(CASE WHEN line_status='C' AND tipo_linea='BB' THEN line_total ELSE 0 END) AS capital_pagado,
            SUM(CASE WHEN line_status='C' AND tipo_linea='S'  THEN line_total ELSE 0 END) AS intereses_pagados,
            SUM(CASE WHEN line_status='O' AND tipo_linea='BB' THEN saldo_pendiente ELSE 0 END) AS capital_pendiente,
            SUM(CASE WHEN line_status='O' AND tipo_linea='S'  THEN saldo_pendiente ELSE 0 END) AS intereses_pendientes,
            SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                     THEN saldo_pendiente ELSE 0 END) AS monto_vencido
        FROM ov_cartera
        WHERE card_code = :card_code AND tipo_linea IN ('BB','S') {empresa_filter}
        GROUP BY card_code, card_name, empresa, slp_name
    """), params).fetchone()

    if not resumen:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    # Cuotas detalladas
    cuotas = db.execute(text(f"""
        SELECT
            doc_entry, doc_num, referencia_manzana_lote, codigo_lote,
            tipo_linea, line_num, line_total, saldo_pendiente,
            fecha_programada_cobro, line_status, forma_pago, plazo,
            CASE
                WHEN line_status = 'C' THEN 'PAGADO'
                WHEN fecha_programada_cobro < CURRENT_DATE AND line_status='O' THEN 'VENCIDO'
                WHEN fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+30 THEN 'PROXIMO'
                ELSE 'PENDIENTE'
            END AS estado_cuota,
            CASE
                WHEN fecha_programada_cobro < CURRENT_DATE AND line_status='O'
                THEN CURRENT_DATE - fecha_programada_cobro
                ELSE 0
            END AS dias_vencido
        FROM ov_cartera
        WHERE card_code = :card_code AND tipo_linea IN ('BB','S') {empresa_filter}
        ORDER BY referencia_manzana_lote, tipo_linea, fecha_programada_cobro
    """), params).fetchall()

    return {
        "resumen": dict(resumen._mapping),
        "cuotas": [dict(r._mapping) for r in cuotas]
    }


@router.get("/proyeccion-mensual")
async def get_proyeccion_mensual(
    empresa: Optional[str] = Query(None),
    meses: int = Query(12, ge=1, le=24),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Proyección mensual de cobros próximos N meses."""
    params = {"meses": meses}
    empresa_filter = "AND empresa = :empresa" if empresa else ""
    if empresa:
        params["empresa"] = empresa

    rows = db.execute(text(f"""
        SELECT
            DATE_TRUNC('month', fecha_programada_cobro)::DATE AS mes,
            SUM(CASE WHEN tipo_linea='BB' THEN saldo_pendiente ELSE 0 END) AS capital,
            SUM(CASE WHEN tipo_linea='S'  THEN saldo_pendiente ELSE 0 END) AS intereses,
            SUM(saldo_pendiente) AS total,
            COUNT(*) AS num_cuotas
        FROM ov_cartera
        WHERE line_status='O'
          AND tipo_linea IN ('BB','S')
          AND fecha_programada_cobro BETWEEN CURRENT_DATE
              AND CURRENT_DATE + (:meses || ' months')::INTERVAL
          {empresa_filter}
        GROUP BY DATE_TRUNC('month', fecha_programada_cobro)
        ORDER BY mes
    """), params).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/aging")
async def get_aging(
    empresa: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Antigüedad de cartera vencida."""
    params = {}
    empresa_filter = "AND empresa = :empresa" if empresa else ""
    if empresa:
        params["empresa"] = empresa

    rows = db.execute(text(f"""
        SELECT
            CASE
                WHEN CURRENT_DATE - fecha_programada_cobro BETWEEN 1  AND 30  THEN '1-30 días'
                WHEN CURRENT_DATE - fecha_programada_cobro BETWEEN 31 AND 60  THEN '31-60 días'
                WHEN CURRENT_DATE - fecha_programada_cobro BETWEEN 61 AND 90  THEN '61-90 días'
                WHEN CURRENT_DATE - fecha_programada_cobro BETWEEN 91 AND 180 THEN '91-180 días'
                ELSE '+180 días'
            END AS rango,
            COUNT(DISTINCT card_code) AS clientes,
            COUNT(*) AS cuotas,
            SUM(saldo_pendiente) AS monto
        FROM ov_cartera
        WHERE line_status='O' AND tipo_linea IN ('BB','S')
          AND fecha_programada_cobro < CURRENT_DATE
          AND saldo_pendiente > 0
          {empresa_filter}
        GROUP BY 1 ORDER BY MIN(CURRENT_DATE - fecha_programada_cobro)
    """), params).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/alertas")
async def get_alertas(
    empresa: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Alertas automáticas de cartera."""
    params = {}
    ef = "AND empresa = :empresa" if empresa else ""
    if empresa: params["empresa"] = empresa

    alertas = []

    # 1. Sobrepagos (saldo negativo)
    sobrepagos = db.execute(text(f"""
        SELECT card_code, card_name, empresa, referencia_manzana_lote,
               SUM(saldo_pendiente) AS saldo_total
        FROM ov_cartera WHERE line_status='O' AND tipo_linea IN ('BB','S') {ef}
        GROUP BY card_code, card_name, empresa, referencia_manzana_lote
        HAVING SUM(saldo_pendiente) < 0
    """), params).fetchall()
    for r in sobrepagos:
        alertas.append({
            "tipo": "SOBREPAGO", "nivel": "ROJO",
            "mensaje": f"Saldo negativo: {r.card_name} — {r.empresa}",
            "detalle": f"Lote: {r.referencia_manzana_lote} | Saldo: Q {r.saldo_total:,.2f}",
            "card_code": r.card_code, "empresa": r.empresa
        })

    # 2. Clientes con desistimiento + cartera abierta
    desist_con_cartera = db.execute(text(f"""
        SELECT d.nombre_cliente, d.empresa, d.lote, d.fecha_desistimiento,
               SUM(o.saldo_pendiente) AS saldo_abierto
        FROM desistimientos d
        JOIN ov_cartera o ON o.card_code = d.codigo_cliente
                         AND o.empresa ILIKE '%' || split_part(d.empresa,' ',1) || '%'
                         AND o.line_status = 'O'
        WHERE o.tipo_linea IN ('BB','S')
        {'AND d.empresa = :empresa' if empresa else ''}
        GROUP BY d.nombre_cliente, d.empresa, d.lote, d.fecha_desistimiento
        HAVING SUM(o.saldo_pendiente) > 0
        LIMIT 20
    """), params).fetchall()
    for r in desist_con_cartera:
        alertas.append({
            "tipo": "DESISTIMIENTO_CON_CARTERA", "nivel": "ROJO",
            "mensaje": f"Desistimiento con cartera abierta: {r.nombre_cliente}",
            "detalle": f"Lote: {r.lote} | Desistió: {r.fecha_desistimiento} | Saldo abierto: Q {r.saldo_abierto:,.2f}",
            "card_code": None, "empresa": r.empresa
        })

    # 3. Alta concentración (cliente con >10% de cartera de su empresa)
    concentracion = db.execute(text(f"""
        WITH empresa_total AS (
            SELECT empresa, SUM(saldo_pendiente) AS total_empresa
            FROM ov_cartera WHERE line_status='O' AND tipo_linea IN ('BB','S') {ef}
            GROUP BY empresa
        ),
        cliente_total AS (
            SELECT o.empresa, o.card_code, o.card_name,
                   SUM(o.saldo_pendiente) AS total_cliente
            FROM ov_cartera o
            WHERE o.line_status='O' AND o.tipo_linea IN ('BB','S') {ef}
            GROUP BY o.empresa, o.card_code, o.card_name
        )
        SELECT c.empresa, c.card_code, c.card_name, c.total_cliente,
               e.total_empresa,
               ROUND(c.total_cliente / e.total_empresa * 100, 1) AS pct
        FROM cliente_total c JOIN empresa_total e ON e.empresa = c.empresa
        WHERE c.total_cliente / e.total_empresa > 0.10
        ORDER BY pct DESC LIMIT 10
    """), params).fetchall()
    for r in concentracion:
        alertas.append({
            "tipo": "ALTA_CONCENTRACION", "nivel": "AMARILLO",
            "mensaje": f"Alta concentración: {r.card_name} — {r.pct}% de {r.empresa}",
            "detalle": f"Saldo: Q {r.total_cliente:,.0f} de Q {r.total_empresa:,.0f} totales",
            "card_code": r.card_code, "empresa": r.empresa
        })

    # 4. Vencidas >90 días con saldo significativo
    venc_90 = db.execute(text(f"""
        SELECT card_code, card_name, empresa,
               COUNT(*) AS cuotas_vencidas,
               SUM(saldo_pendiente) AS monto_vencido,
               MIN(fecha_programada_cobro) AS primera_vencida
        FROM ov_cartera
        WHERE line_status='O' AND tipo_linea IN ('BB','S')
          AND fecha_programada_cobro < CURRENT_DATE - 90
          AND saldo_pendiente > 0 {ef}
        GROUP BY card_code, card_name, empresa
        ORDER BY monto_vencido DESC LIMIT 10
    """), params).fetchall()
    for r in venc_90:
        alertas.append({
            "tipo": "VENCIDO_90_DIAS", "nivel": "ROJO",
            "mensaje": f"Vencido +90 días: {r.card_name}",
            "detalle": f"{r.cuotas_vencidas} cuotas | Q {r.monto_vencido:,.2f} | Desde: {r.primera_vencida}",
            "card_code": r.card_code, "empresa": r.empresa
        })

    return {
        "total": len(alertas),
        "rojas": len([a for a in alertas if a["nivel"] == "ROJO"]),
        "amarillas": len([a for a in alertas if a["nivel"] == "AMARILLO"]),
        "alertas": alertas
    }


@router.get("/desistimientos")
async def get_desistimientos(
    empresa: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    params = {}
    where = "1=1"
    if empresa:
        where += " AND empresa = :empresa"
        params["empresa"] = empresa

    total = db.execute(text(f"SELECT COUNT(*) FROM desistimientos WHERE {where}"), params).scalar()
    rows = db.execute(text(f"""
        SELECT * FROM desistimientos WHERE {where}
        ORDER BY fecha_desistimiento DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": (page-1)*page_size}).fetchall()

    return {
        "desistimientos": [dict(r._mapping) for r in rows],
        "total": total, "page": page, "pages": -(-total // page_size)
    }
