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
    año: Optional[int] = Query(None),
    mes: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """
    KPIs de cartera. Lógica confirmada con RV Cuatro:
    - Cartera total = TODAS las líneas open BB+S con saldo>0 (sin filtro DocDate)
    - Aging por MES CALENDARIO: 0-30=mes actual, 31-60=mes anterior, 61-90=mes-2, etc.
    - Mora (31+ días) = saldo vencido en meses ANTERIORES al mes filtrado
    - Cobro 30d = cuotas del mes siguiente, 60d = mes+2, 90d = mes+3
    """
    import calendar
    from datetime import date, timedelta

    ef = "AND empresa = :empresa" if empresa else ""
    params = {}
    if empresa:
        params["empresa"] = empresa

    # Determinar mes/año de referencia
    if año and mes:
        ref_year, ref_month = año, mes
    else:
        today = date.today()
        ref_year, ref_month = today.year, today.month

    last_day = calendar.monthrange(ref_year, ref_month)[1]
    ref_date = f"{ref_year}-{ref_month:02d}-{last_day}"   # último día del mes

    # Mes anterior (para mora = vencidas antes del mes actual)
    if ref_month == 1:
        prev_year, prev_month = ref_year - 1, 12
    else:
        prev_year, prev_month = ref_year, ref_month - 1
    prev_last = calendar.monthrange(prev_year, prev_month)[1]
    mora_cutoff = f"{prev_year}-{prev_month:02d}-{prev_last}"  # último día del mes anterior

    # Mes de inicio del mes actual (para aging 0-30)
    current_month_start = f"{ref_year}-{ref_month:02d}-01"

    # Meses futuros para cobros
    def next_month(y, m, n):
        m2 = m + n
        y2 = y + (m2 - 1) // 12
        m2 = (m2 - 1) % 12 + 1
        last = calendar.monthrange(y2, m2)[1]
        return f"{y2}-{m2:02d}-01", f"{y2}-{m2:02d}-{last}"

    cobro30_start, cobro30_end = next_month(ref_year, ref_month, 1)
    cobro60_start, cobro60_end = next_month(ref_year, ref_month, 2)
    cobro90_start, cobro90_end = next_month(ref_year, ref_month, 3)

    # Aging: meses hacia atrás
    def month_range(y, m, n_back):
        """Retorna (start, end) del mes N meses atrás desde (y,m)."""
        m2 = m - n_back
        y2 = y
        while m2 <= 0:
            m2 += 12
            y2 -= 1
        last = calendar.monthrange(y2, m2)[1]
        return f"{y2}-{m2:02d}-01", f"{y2}-{m2:02d}-{last}"

    # 0-30d = mes actual
    aging00_start = current_month_start
    aging00_end   = ref_date
    # 31-60d = mes anterior
    aging31_start, aging31_end = month_range(ref_year, ref_month, 1)
    # 61-90d = mes-2
    aging61_start, aging61_end = month_range(ref_year, ref_month, 2)
    # 91-180d = meses -3 a -6
    aging91_start, _ = month_range(ref_year, ref_month, 6)
    _, aging91_end   = month_range(ref_year, ref_month, 3)
    # +180d = antes de mes-6
    aging180_end = aging91_start  # exclusive

    params.update({
        "aging00_start": aging00_start, "aging00_end": aging00_end,
        "aging31_start": aging31_start, "aging31_end": aging31_end,
        "aging61_start": aging61_start, "aging61_end": aging61_end,
        "aging91_start": aging91_start, "aging91_end": aging91_end,
        "aging180_end": aging180_end,
        "mora_cutoff": mora_cutoff,
        "cobro30_start": cobro30_start, "cobro30_end": cobro30_end,
        "cobro60_start": cobro60_start, "cobro60_end": cobro60_end,
        "cobro90_start": cobro90_start, "cobro90_end": cobro90_end,
    })

    kpis = db.execute(text(f"""
        SELECT
            -- Cartera: TODAS las líneas open con saldo>0 (sin filtro DocDate)
            SUM(CASE WHEN tipo_linea='BB' AND saldo_pendiente > 0
                     THEN saldo_pendiente ELSE 0 END) AS capital_total,
            SUM(CASE WHEN tipo_linea='S'  AND saldo_pendiente > 0
                     THEN saldo_pendiente ELSE 0 END) AS intereses_total,
            SUM(CASE WHEN saldo_pendiente > 0
                     THEN saldo_pendiente ELSE 0 END) AS cartera_total,

            -- Mora = saldo vencido en meses ANTERIORES al mes filtrado (31+ días)
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro < :mora_cutoff::date
                     THEN saldo_pendiente ELSE 0 END) AS mora_total,

            -- Aging por mes calendario
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro BETWEEN :aging00_start::date AND :aging00_end::date
                     THEN saldo_pendiente ELSE 0 END) AS aging_0_30,
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro BETWEEN :aging31_start::date AND :aging31_end::date
                     THEN saldo_pendiente ELSE 0 END) AS aging_31_60,
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro BETWEEN :aging61_start::date AND :aging61_end::date
                     THEN saldo_pendiente ELSE 0 END) AS aging_61_90,
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro BETWEEN :aging91_start::date AND :aging91_end::date
                     THEN saldo_pendiente ELSE 0 END) AS aging_91_180,
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro < :aging180_end::date
                     THEN saldo_pendiente ELSE 0 END) AS aging_180_mas,

            COUNT(DISTINCT CASE WHEN saldo_pendiente > 0 THEN card_code END) AS clientes_activos,
            COUNT(DISTINCT CASE WHEN saldo_pendiente > 0
                                AND fecha_programada_cobro < :mora_cutoff::date
                                THEN card_code END) AS clientes_vencidos,

            -- Cobros = cuotas de meses futuros individuales
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro BETWEEN :cobro30_start::date AND :cobro30_end::date
                     THEN saldo_pendiente ELSE 0 END) AS cobro_30d,
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro BETWEEN :cobro60_start::date AND :cobro60_end::date
                     THEN saldo_pendiente ELSE 0 END) AS cobro_60d,
            SUM(CASE WHEN saldo_pendiente > 0
                     AND fecha_programada_cobro BETWEEN :cobro90_start::date AND :cobro90_end::date
                     THEN saldo_pendiente ELSE 0 END) AS cobro_90d
        FROM ov_cartera
        WHERE line_status='O'
          AND tipo_linea IN ('BB', 'S')
          {ef}
    """), params).fetchone()

    # Desistimientos del período
    des_where = "1=1"
    des_p = {}
    if empresa:
        des_where += " AND empresa = :empresa"
        des_p["empresa"] = empresa
    if año and mes:
        des_where += " AND EXTRACT(YEAR FROM fecha_desistimiento)=:año AND EXTRACT(MONTH FROM fecha_desistimiento)=:mes"
        des_p.update({"año": año, "mes": mes})

    desist = db.execute(text(f"""
        SELECT COUNT(*) as total,
               COALESCE(SUM(pagado_capital),0) as total_pagado,
               COALESCE(SUM(reintegrado_cliente),0) as total_reintegrado
        FROM desistimientos WHERE {des_where}
    """), des_p).fetchone()

    d = dict(kpis._mapping)
    d["desistimientos_total"]       = desist.total
    d["desistimientos_pagado"]      = float(desist.total_pagado)
    d["desistimientos_reintegrado"] = float(desist.total_reintegrado)
    d["mora_31_60"]   = float(d.get("aging_31_60") or 0)
    d["mora_61_90"]   = float(d.get("aging_61_90") or 0)
    d["mora_91_180"]  = float(d.get("aging_91_180") or 0)
    d["mora_180_mas"] = float(d.get("aging_180_mas") or 0)
    d["mora_0_30"]    = float(d.get("aging_0_30") or 0)

    cartera = float(d.get("cartera_total") or 0)
    mora    = float(d.get("mora_total") or 0)
    d["tasa_mora"] = round(mora / cartera * 100, 2) if cartera > 0 else 0

    return {k: float(v) if v is not None and isinstance(v, (int, float)) else v
            for k, v in d.items()}


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
    año: Optional[int] = Query(None),
    mes: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Proyección de cobros: cuotas de los N meses siguientes al mes filtrado."""
    import calendar
    from datetime import date

    ef = "AND empresa = :empresa" if empresa else ""
    params = {"meses": meses}
    if empresa: params["empresa"] = empresa

    if año and mes:
        ref_year, ref_month = año, mes
    else:
        today = date.today()
        ref_year, ref_month = today.year, today.month

    # Inicio = primer día del mes siguiente
    if ref_month == 12:
        start_year, start_mes = ref_year + 1, 1
    else:
        start_year, start_mes = ref_year, ref_month + 1
    start_date = f"{start_year}-{start_mes:02d}-01"
    params["start_date"] = start_date

    rows = db.execute(text(f"""
        SELECT
            DATE_TRUNC('month', fecha_programada_cobro)::DATE AS mes,
            SUM(CASE WHEN tipo_linea='BB' THEN saldo_pendiente ELSE 0 END) AS capital,
            SUM(CASE WHEN tipo_linea='S'  THEN saldo_pendiente ELSE 0 END) AS intereses,
            SUM(saldo_pendiente) AS total,
            COUNT(*) AS num_cuotas
        FROM ov_cartera
        WHERE line_status='O'
          AND tipo_linea IN ('BB', 'S')
          AND saldo_pendiente > 0
          AND fecha_programada_cobro >= :start_date::date
          AND fecha_programada_cobro < :start_date::date + (:meses || ' months')::INTERVAL
          {ef}
        GROUP BY DATE_TRUNC('month', fecha_programada_cobro)
        ORDER BY mes
    """), params).fetchall()

    return [dict(r._mapping) for r in rows]


@router.get("/aging")
async def get_aging(
    empresa: Optional[str] = Query(None),
    año: Optional[int] = Query(None),
    mes: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Antigüedad de cartera por mes calendario."""
    import calendar
    from datetime import date

    ef = "AND empresa = :empresa" if empresa else ""
    params = {}
    if empresa: params["empresa"] = empresa

    if año and mes:
        ref_year, ref_month = año, mes
    else:
        today = date.today()
        ref_year, ref_month = today.year, today.month

    last_day = calendar.monthrange(ref_year, ref_month)[1]
    ref_date = f"{ref_year}-{ref_month:02d}-{last_day}"

    # Build month ranges for aging buckets
    def get_month_range(y, m, n_back):
        m2 = m - n_back
        y2 = y
        while m2 <= 0:
            m2 += 12
            y2 -= 1
        last = calendar.monthrange(y2, m2)[1]
        return f"{y2}-{m2:02d}-01", f"{y2}-{m2:02d}-{last}", f"{calendar.month_abbr[m2]} {y2}"

    buckets = [
        (f"{ref_year}-{ref_month:02d}-01", ref_date,
         f"0-30 días ({calendar.month_name[ref_month]} {ref_year})",
         "0-30 días"),
        (*get_month_range(ref_year, ref_month, 1)[:2],
         f"31-60 días ({get_month_range(ref_year, ref_month, 1)[2]})",
         "31-60 días"),
        (*get_month_range(ref_year, ref_month, 2)[:2],
         f"61-90 días ({get_month_range(ref_year, ref_month, 2)[2]})",
         "61-90 días"),
        (*get_month_range(ref_year, ref_month, 3)[:2],
         f"91-120 días ({get_month_range(ref_year, ref_month, 3)[2]})",
         "91-120 días"),
    ]

    result = []
    for start_d, end_d, label, rango_base in buckets:
        row = db.execute(text(f"""
            SELECT
                COUNT(DISTINCT card_code) AS clientes,
                COUNT(*) AS cuotas,
                COALESCE(SUM(saldo_pendiente), 0) AS monto
            FROM ov_cartera
            WHERE line_status='O'
              AND tipo_linea IN ('BB','S')
              AND saldo_pendiente > 0
              AND fecha_programada_cobro BETWEEN :s::date AND :e::date
              {ef}
        """), {**params, "s": start_d, "e": end_d}).fetchone()
        result.append({
            "rango": rango_base,
            "label": label,
            "clientes": row.clientes,
            "cuotas": row.cuotas,
            "monto": float(row.monto)
        })

    return result


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
