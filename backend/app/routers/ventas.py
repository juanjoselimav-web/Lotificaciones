from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from app.database import get_db
from app.core.config import get_settings
from app.core.security import get_current_user

router = APIRouter(prefix="/api/ventas", tags=["ventas"])

# ── REGLAS DE NEGOCIO — VENTAS ───────────────────────────────
_PLAZO_NO_VENTA = "('Canje A','canje a','CANJE A','Casa Modelo','casa modelo','Ansak, S.A.','Ansak S.A.','Ansak','Apartado Proyecto Aptos','Bloqueo Municipal','Bloqueado')"
_VEND_NO_VENTA  = "('Canje A','canje a','CANJE A','Bloqueado','Bloqueo Municipal','Apartado Proyecto Aptos','Ansak, S.A.','Ansak S.A.')"
_PLAZO_ALERTA   = "('Final Proyecto','Final de Proyecto','Final proyecto','Venta Interna')"

def no_venta_where():
    return f"(COALESCE(l.plazo_raw,'') IN {_PLAZO_NO_VENTA} OR COALESCE(l.vendedor,'') IN {_VEND_NO_VENTA})"

def es_venta_where():
    return f"NOT (COALESCE(l.plazo_raw,'') IN {_PLAZO_NO_VENTA}) AND NOT (COALESCE(l.vendedor,'') IN {_VEND_NO_VENTA})"

def alerta_where():
    return f"(COALESCE(l.vendedor,'') = '-Ningún empleado del departamento de ventas-' OR COALESCE(l.plazo_raw,'') IN {_PLAZO_ALERTA} OR l.unidad_key IS NULL OR l.unidad_key = '' OR (l.forma_pago = 'CREDITOCONINTERES' AND (l.total_intereses IS NULL OR l.total_intereses = 0)))"



# Constante para status de PCV firmado
PCV_FIRMADO = "PCV FIRMADO"  # valor exacto en BD (ajustar si difiere)


# ── VENDEDORES ────────────────────────────────────────────────

@router.get("/vendedores")
async def get_vendedores(
    equipo: Optional[str] = Query(None),
    sin_asignar: bool = Query(False),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    where = "1=1"
    params = {}
    if sin_asignar:
        where = "equipo = 'SIN_ASIGNAR' AND es_sistema = FALSE AND activo = TRUE"
    elif equipo:
        where = "equipo = :equipo"
        params["equipo"] = equipo

    rows = db.execute(text(f"""
        SELECT v.*, 
               COUNT(DISTINCT l.id) AS ventas_total
        FROM vendedores v
        LEFT JOIN lotes l ON l.vendedor = v.nombre 
                         AND l.estatus IN ('VENTA','RESERVADO','CANJE')
        WHERE {where}
        GROUP BY v.id
        ORDER BY v.equipo, v.nombre
    """), params).fetchall()
    return [dict(r._mapping) for r in rows]


@router.put("/vendedores/{vendedor_id}")
async def update_vendedor(
    vendedor_id: int,
    equipo: str = Query(...),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    if current_user.nivel < 3:
        raise HTTPException(status_code=403, detail="Sin permisos")
    if equipo not in ('CONSERSA', 'RV4', 'SIN_ASIGNAR'):
        raise HTTPException(status_code=400, detail="Equipo inválido: usa CONSERSA, RV4 o SIN_ASIGNAR")
    db.execute(text(
        "UPDATE vendedores SET equipo=:e, updated_at=NOW() WHERE id=:id"
    ), {"e": equipo, "id": vendedor_id})
    db.commit()
    return {"ok": True}


# ── KPIs RESUMEN ──────────────────────────────────────────────

@router.get("/historico-anios")
async def get_historico_anios(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """
    Resumen de ventas brutas, netas y valor por año — SIN filtro de período.
    Devuelve TODA la historia para el slide 'Ventas por año (histórico)'.
    Una sola query, sin paralelismo de 8 llamadas como el código viejo.
    """
    rows = db.execute(text(f"""
        WITH ventas_anio AS (
            SELECT
                EXTRACT(YEAR FROM l.fecha_venta)::INT AS anio,
                COUNT(*) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                    AND {es_venta_where()}) AS brutas,
                COALESCE(SUM(l.precio_final) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                    AND {es_venta_where()}), 0) AS valor
            FROM lotes l
            WHERE l.fecha_venta IS NOT NULL
              AND EXTRACT(YEAR FROM l.fecha_venta) BETWEEN 2020 AND EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY EXTRACT(YEAR FROM l.fecha_venta)
        ),
        desist_anio AS (
            SELECT
                EXTRACT(YEAR FROM fecha_desistimiento)::INT AS anio,
                COUNT(*) AS total_desist
            FROM desistimientos
            WHERE fecha_desistimiento IS NOT NULL
              AND EXTRACT(YEAR FROM fecha_desistimiento) BETWEEN 2020 AND EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY EXTRACT(YEAR FROM fecha_desistimiento)
        )
        SELECT
            v.anio,
            v.brutas,
            COALESCE(d.total_desist, 0) AS desistimientos,
            (v.brutas - COALESCE(d.total_desist, 0)) AS netas,
            v.valor
        FROM ventas_anio v
        LEFT JOIN desist_anio d ON d.anio = v.anio
        WHERE v.brutas > 0
        ORDER BY v.anio
    """)).fetchall()
    return {
        "anios": [
            {
                "anio": int(r.anio),
                "brutas": int(r.brutas or 0),
                "netas": int(r.netas or 0),
                "desistimientos": int(r.desistimientos or 0),
                "valor": float(r.valor or 0),
            }
            for r in rows
        ]
    }


@router.get("/kpis")
async def get_kpis(
    año: int = Query(2026),
    mes: Optional[int] = Query(None),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    # Conteo de ventas desde lotes.fecha_venta (inventario deduplicado, 1 fila por lote)
    if mes:
        date_filter = "DATE_TRUNC('month', l.fecha_venta) = make_date(:año, :mes, 1)"
        params = {"año": año, "mes": mes}
    else:
        date_filter = "EXTRACT(YEAR FROM l.fecha_venta) = :año"
        params = {"año": año}

    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    if proyecto: params["proyecto"] = proyecto

    ventas = db.execute(text(f"""
        SELECT
            -- Ventas reales (excluye Canje A, Casa Modelo, Ansak, Bloqueo, Apartado)
            COUNT(*) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}) AS ventas_brutas,
            COALESCE(SUM(l.precio_final) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}), 0) AS valor_bruto,
            COALESCE(SUM(l.total_intereses) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}), 0) AS intereses_pactados,
            COALESCE(AVG(l.precio_final) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}), 0) AS ticket_promedio,
            COUNT(*) FILTER (WHERE l.forma_pago='CONTADO' AND l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}) AS contado,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOSININTERES' AND l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}) AS sin_interes,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOCONINTERES' AND l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}) AS con_interes,
            COALESCE(AVG(l.plazo) FILTER (WHERE l.plazo > 0 AND l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}), 0) AS plazo_promedio,
            -- Sin asesor (cuentan como venta pero van a alertas)
            COUNT(*) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()}
                AND (l.vendedor IS NULL OR l.vendedor='-Ningún empleado del departamento de ventas-')
                ) AS sin_vendedor,
            -- Casos especiales excluidos del conteo
            COUNT(*) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND ({no_venta_where()})
                ) AS canjes_bloqueados,
            -- Alertas (ventas reales con problemas de datos)
            COUNT(*) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
                AND {es_venta_where()} AND {alerta_where()}
                ) AS total_alertas
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE {date_filter} AND l.fecha_venta IS NOT NULL {pf}
    """), params).fetchone()

        # Desistimientos del período — filtro por empresa usando mapeo proyecto→empresa
    PROY_EMPRESA_MAP = {
        'Hacienda Jumay': 'Eficiencia Urbana',
        'La Ceiba': 'Servicios Generales',
        'Hacienda el Sol': 'Rossio',
        'Oasis Zacapa': 'Frugalex',
        'Cañadas de Jalapa': 'Ottavia',
        'Condado Jutiapa': 'Utilica',
        'Club Campestre Jumay': 'Tezzoli',
        'Club del Bosque': 'Urbiva 2',
        'Club Residencial El Progreso': 'Garbatella',
        'Club Residencial Progreso': 'Garbatella',
        'Arboleda Santa Elena': 'Capipos',
        'Arboleada Santa Elena': 'Capipos',
        'Hacienda Santa Lucia': 'Ovest',
        'Hacienda El Cafetal Fase I': 'Corcolle',
        'Hacienda El Cafetal Fase III': 'Gibraleon',
    }
    des_where = "1=1"
    des_params = {}
    if mes:
        des_where += " AND EXTRACT(YEAR FROM fecha_desistimiento) = :año AND EXTRACT(MONTH FROM fecha_desistimiento) = :mes"
        des_params.update({"año": año, "mes": mes})
    else:
        des_where += " AND EXTRACT(YEAR FROM fecha_desistimiento) = :año"
        des_params["año"] = año
    if proyecto:
        empresa_des = PROY_EMPRESA_MAP.get(proyecto)
        if empresa_des:
            des_where += " AND empresa = :empresa_des"
            des_params["empresa_des"] = empresa_des
    desist = db.execute(text(f"""
        SELECT COUNT(*) AS total,
               COALESCE(SUM(precio_con_descuento), 0) AS valor
        FROM desistimientos WHERE {des_where}
    """), des_params).fetchone()

    v = dict(ventas._mapping)
    d = dict(desist._mapping)
    netas = int(v.get("ventas_brutas") or 0) - int(d.get("total") or 0)
    brutas = int(v.get("ventas_brutas") or 0)
    tasa = round(int(d.get("total") or 0) / brutas * 100, 1) if brutas > 0 else 0

    return {
        **{k: float(val) if isinstance(val, (int, float)) and val is not None else val
           for k, val in v.items()},
        "desistimientos": int(d.get("total") or 0),
        "valor_desistido": float(d.get("valor") or 0),
        "ventas_netas": netas,
        "tasa_desistimiento": tasa,
        "sin_vendedor": int(v.get("sin_vendedor") or 0),
        "canjes_bloqueados": int(v.get("canjes_bloqueados") or 0),
        "total_alertas": int(v.get("total_alertas") or 0),
    }


# ── REGISTROS A REVISAR ───────────────────────────────────────

@router.get("/registros-revision")
async def get_registros_revision(
    año: int = Query(2026),
    mes: Optional[int] = Query(None),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    if mes:
        date_filter = "DATE_TRUNC('month', l.fecha_venta) = make_date(:año, :mes, 1)"
        params = {"año": año, "mes": mes}
    else:
        date_filter = "EXTRACT(YEAR FROM l.fecha_venta) = :año"
        params = {"año": año}

    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    if proyecto: params["proyecto"] = proyecto

    rows = db.execute(text(f"""
        SELECT
            l.unidad_key,
            l.manzana,
            p.nombre_proyecto,
            l.estatus,
            l.vendedor,
            l.plazo_raw,
            l.fecha_venta,
            l.precio_final,
            l.forma_pago,
            l.total_intereses,
            l.card_name AS cliente,
            CASE
                WHEN {no_venta_where()}
                    THEN 'Caso especial (no es venta): ' || COALESCE(l.plazo_raw, l.vendedor, '—')
                WHEN l.unidad_key IS NULL OR l.unidad_key = ''
                    THEN 'Sin código de lote'
                WHEN l.vendedor IS NULL OR l.vendedor = '-Ningún empleado del departamento de ventas-'
                    THEN 'Venta sin asesor asignado'
                WHEN l.plazo_raw IN ('Final Proyecto','Final de Proyecto','Final proyecto')
                    THEN 'Venta especial: Final Proyecto'
                WHEN l.plazo_raw = 'Venta Interna'
                    THEN 'Venta especial: Venta Interna'
                WHEN l.forma_pago = 'CREDITOCONINTERES' AND (l.total_intereses IS NULL OR l.total_intereses = 0)
                    THEN 'Crédito con intereses pero monto intereses = 0'
                ELSE 'Otro'
            END AS issue
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE {date_filter}
          AND l.fecha_venta IS NOT NULL
          AND l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND (
              {no_venta_where()}
              OR l.unidad_key IS NULL OR l.unidad_key = ''
              OR l.vendedor IS NULL
              OR l.vendedor = '-Ningún empleado del departamento de ventas-'
              OR l.plazo_raw IN ('Final Proyecto','Final de Proyecto','Final proyecto','Venta Interna')
              OR (l.forma_pago = 'CREDITOCONINTERES' AND (l.total_intereses IS NULL OR l.total_intereses = 0))
          )
          {pf}
        ORDER BY
            CASE WHEN {no_venta_where()} THEN 2 ELSE 1 END,
            p.nombre_proyecto, l.fecha_venta
    """), params).fetchall()

    return [dict(r._mapping) for r in rows]


# ── TENDENCIA MENSUAL ─────────────────────────────────────────

@router.get("/tendencia-mensual")
async def get_tendencia(
    año: int = Query(2026),
    mes: Optional[int] = Query(None),
    meses_atras: int = Query(12),
    todo_el_tiempo: bool = Query(False),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """
    Últimos N meses de tendencia. Con año+mes, muestra los 12 meses terminando
    en ese mes. Sin mes, termina en el mes actual.
    """
    # Mapping proyecto → empresa en desistimientos
    PROYECTO_EMPRESA = {
        'Hacienda Jumay': 'Eficiencia Urbana',
        'La Ceiba': 'Servicios Generales',
        'Hacienda el Sol': 'Rossio',
        'Oasis Zacapa': 'Frugalex',
        'Cañadas de Jalapa': 'Ottavia',
        'Condado Jutiapa': 'Utilica',
        'Club Campestre Jumay': 'Tezzoli',
        'Club del Bosque': 'Urbiva 2',
        'Club Residencial Progreso': 'Garbatella',
        'Arboleda Santa Elena': 'Capipos',
        'Hacienda Santa Lucia': 'Ovest',
        'Hacienda El Cafetal Fase I': 'Corcolle',
        'Hacienda El Cafetal Fase III': 'Gibraleon',
    }

    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    import calendar
    if todo_el_tiempo:
        date_filter = "l.fecha_venta IS NOT NULL"
        params = {}
        desist_date = "1=1"
    elif mes and año:
        last_day = calendar.monthrange(año, mes)[1]
        end_date = f"{año}-{mes:02d}-{last_day}"
        params = {"end_date": end_date, "meses": meses_atras}
        date_filter = "l.fecha_venta >= CAST(:end_date AS DATE) - (:meses || ' months')::INTERVAL AND l.fecha_venta <= CAST(:end_date AS DATE) AND l.fecha_venta IS NOT NULL"
        desist_date = "fecha_desistimiento >= CAST(:end_date AS DATE) - (:meses || ' months')::INTERVAL AND fecha_desistimiento <= CAST(:end_date AS DATE)"
    else:
        date_filter = "l.fecha_venta >= CURRENT_DATE - (:meses || ' months')::INTERVAL AND l.fecha_venta IS NOT NULL"
        params = {"meses": meses_atras}
        desist_date = "fecha_desistimiento >= CURRENT_DATE - (:meses || ' months')::INTERVAL"
    if proyecto: params["proyecto"] = proyecto

    ventas = db.execute(text(f"""
        SELECT
            DATE_TRUNC('month', l.fecha_venta)::DATE AS mes,
            COUNT(*) AS ventas_brutas,
            COALESCE(SUM(l.precio_final), 0) AS valor_bruto,
            COUNT(*) FILTER (WHERE l.forma_pago='CONTADO') AS contado,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOSININTERES') AS sin_interes,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOCONINTERES') AS con_interes,
            COALESCE(AVG(l.plazo) FILTER (WHERE l.plazo > 0), 0) AS plazo_promedio,
            COUNT(*) FILTER (WHERE l.vendedor IS NULL OR l.vendedor IN (
                '-Ningún empleado del departamento de ventas-',
                'Canje A','Bloqueado','Bloqueo Municipal','Apartado Proyecto Aptos'
            )) AS sin_vendedor
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND {date_filter}
          {pf}
        GROUP BY DATE_TRUNC('month', l.fecha_venta)
        ORDER BY mes
    """), params).fetchall()

        # Filter desistimientos by empresa if proyecto given
    empresa_filter = PROYECTO_EMPRESA.get(proyecto, '') if proyecto else ''
    desist_params = dict(params)


    if empresa_filter:
        desist_params["empresa"] = empresa_filter
        empresa_where = "AND empresa = :empresa"
    else:
        empresa_where = ""

    desist = db.execute(text(f"""
        SELECT
            DATE_TRUNC('month', fecha_desistimiento)::DATE AS mes,
            COUNT(*) AS desistimientos,
            COALESCE(SUM(precio_con_descuento), 0) AS valor_desistido
        FROM desistimientos
        WHERE {desist_date} {empresa_where}
        GROUP BY DATE_TRUNC('month', fecha_desistimiento)
        ORDER BY mes
    """), desist_params).fetchall()

    # Merge by mes
    desist_map = {str(r.mes): dict(r._mapping) for r in desist}
    result = []
    for r in ventas:
        mes_str = str(r.mes)
        d = desist_map.get(mes_str, {"desistimientos": 0, "valor_desistido": 0})
        row = dict(r._mapping)
        row["desistimientos"] = d.get("desistimientos", 0)
        row["valor_desistido"] = float(d.get("valor_desistido", 0))
        row["ventas_netas"] = int(row["ventas_brutas"]) - int(d.get("desistimientos", 0))
        result.append(row)

    return result


# ── MEZCLA FINANCIERA ─────────────────────────────────────────

@router.get("/mezcla-plazos")
async def get_mezcla_plazos(
    año: int = Query(2026),
    mes: Optional[int] = Query(None),
    todo_el_tiempo: bool = Query(False),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    if todo_el_tiempo:
        date_filter = "l.fecha_venta IS NOT NULL"
        params = {}
    elif mes:
        date_filter = "DATE_TRUNC('month', fecha_venta) = make_date(:año, :mes, 1)"
        params = {"año": año, "mes": mes}
    else:
        date_filter = "EXTRACT(YEAR FROM fecha_venta) = :año"
        params = {"año": año}
    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    if proyecto: params["proyecto"] = proyecto

    rows = db.execute(text(f"""
        SELECT
            l.plazo,
            l.forma_pago,
            COUNT(*) AS lotes,
            COALESCE(SUM(l.precio_final), 0) AS valor,
            COALESCE(SUM(l.total_intereses), 0) AS intereses
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND {date_filter}
          AND l.fecha_venta IS NOT NULL
          AND l.plazo IS NOT NULL AND l.plazo > 0
          {pf}
        GROUP BY l.plazo, l.forma_pago
        ORDER BY l.plazo, l.forma_pago
    """), params).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/proyectos")
async def get_proyectos_ventas(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    rows = db.execute(text("""
        SELECT DISTINCT p.nombre_proyecto
        FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE') AND l.fecha_venta IS NOT NULL
        ORDER BY p.nombre_proyecto
    """)).fetchall()
    return [r[0] for r in rows]



@router.get("/analisis-financiero")
async def get_analisis_financiero(
    año: int = Query(2026),
    mes: Optional[int] = Query(None),
    todo_el_tiempo: bool = Query(False),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """KPIs financieros para la gráfica de impacto: tasas, intereses cobrados/no cobrados."""
    if todo_el_tiempo:
        date_filter = "l.fecha_venta IS NOT NULL"
        params = {}
    elif mes:
        date_filter = "DATE_TRUNC('month', l.fecha_venta) = make_date(:año, :mes, 1)"
        params = {"año": año, "mes": mes}
    else:
        date_filter = "EXTRACT(YEAR FROM l.fecha_venta) = :año"
        params = {"año": año}
    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    if proyecto: params["proyecto"] = proyecto

    r = db.execute(text(f"""
        SELECT
            COUNT(*) FILTER (WHERE l.forma_pago='CONTADO') AS lotes_contado,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOSININTERES') AS lotes_sin_int,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOCONINTERES') AS lotes_con_int,
            COALESCE(SUM(l.precio_final) FILTER (WHERE l.forma_pago='CONTADO'), 0) AS capital_contado,
            COALESCE(SUM(l.precio_final) FILTER (WHERE l.forma_pago='CREDITOSININTERES'), 0) AS capital_sin_int,
            COALESCE(SUM(l.precio_final) FILTER (WHERE l.forma_pago='CREDITOCONINTERES'), 0) AS capital_con_int,
            COALESCE(SUM(l.total_intereses) FILTER (WHERE l.forma_pago='CREDITOCONINTERES'), 0) AS intereses_cobrados,
            COALESCE(SUM(l.total_intereses) FILTER (WHERE l.forma_pago='CREDITOSININTERES'), 0) AS intereses_sin_int_pagados,
            COALESCE(AVG(l.plazo) FILTER (WHERE l.forma_pago='CREDITOCONINTERES' AND l.plazo > 0), 0) AS plazo_prom_con_int,
            COALESCE(AVG(l.plazo) FILTER (WHERE l.forma_pago='CREDITOSININTERES' AND l.plazo > 0), 0) AS plazo_prom_sin_int
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND {date_filter}
          AND l.fecha_venta IS NOT NULL
          {pf}
    """), params).fetchone()

    d = dict(r._mapping)

    # Tasa anual implícita de contratos con interés
    cap_con = float(d.get('capital_con_int') or 0)
    int_cob = float(d.get('intereses_cobrados') or 0)
    plazo_con = float(d.get('plazo_prom_con_int') or 0)
    tasa_total = (int_cob / cap_con * 100) if cap_con > 0 else 0
    tasa_anual = (tasa_total / (plazo_con / 12)) if plazo_con > 0 else 0

    # Intereses no cobrados (aplicando tasa implícita a contratos sin interés)
    cap_sin = float(d.get('capital_sin_int') or 0)
    plazo_sin = float(d.get('plazo_prom_sin_int') or 0)
    tasa_anual_decimal = tasa_anual / 100
    intereses_no_cobrados = cap_sin * tasa_anual_decimal * (plazo_sin / 12) if plazo_sin > 0 else 0

    return {
        **{k: float(v) if v is not None else 0 for k, v in d.items()},
        "tasa_anual_implicita": round(tasa_anual, 2),
        "tasa_total_sobre_capital": round(tasa_total, 2),
        "intereses_no_cobrados": round(intereses_no_cobrados, 2),
        "ratio_cobrado_vs_oportunidad": round(int_cob / (int_cob + intereses_no_cobrados) * 100, 1) if (int_cob + intereses_no_cobrados) > 0 else 0,
    }



@router.get("/inconsistencias")
async def get_inconsistencias(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Detecta registros con datos inconsistentes en ventas."""
    issues = []

    # 1. ConInterés pero Total Intereses = 0
    rows1 = db.execute(text("""
        SELECT l.unidad_key, l.manzana, l.card_name, l.vendedor,
               p.nombre_proyecto, l.precio_final, l.plazo,
               l.total_intereses, l.forma_pago, l.fecha_venta
        FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND l.forma_pago = 'CREDITOCONINTERES'
          AND (l.total_intereses IS NULL OR l.total_intereses = 0)
        ORDER BY p.nombre_proyecto, l.manzana
    """)).fetchall()
    for r in rows1:
        issues.append({
            "tipo": "CON_INTERES_SIN_MONTO",
            "nivel": "ROJO",
            "mensaje": f"Crédito con interés sin monto de interés: {r.card_name}",
            "detalle": f"{r.nombre_proyecto} | {r.manzana} | Q {float(r.precio_final or 0):,.0f} | Plazo: {r.plazo}m | Intereses: Q 0",
            "unidad_key": r.unidad_key,
            "proyecto": r.nombre_proyecto,
            "accion": "Revisar contrato en SAP — el campo Total Intereses está en cero"
        })

    # 2. SinInterés pero con Total Intereses > 0
    rows2 = db.execute(text("""
        SELECT l.unidad_key, l.manzana, l.card_name, l.vendedor,
               p.nombre_proyecto, l.precio_final, l.plazo,
               l.total_intereses, l.forma_pago, l.fecha_venta
        FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND l.forma_pago = 'CREDITOSININTERES'
          AND l.total_intereses > 0
        ORDER BY l.total_intereses DESC
    """)).fetchall()
    for r in rows2:
        issues.append({
            "tipo": "SIN_INTERES_CON_MONTO",
            "nivel": "ROJO",
            "mensaje": f"Crédito sin interés con monto de interés registrado: {r.card_name}",
            "detalle": f"{r.nombre_proyecto} | {r.manzana} | Capital: Q {float(r.precio_final or 0):,.0f} | Intereses registrados: Q {float(r.total_intereses or 0):,.0f}",
            "unidad_key": r.unidad_key,
            "proyecto": r.nombre_proyecto,
            "accion": "Verificar tipo de crédito en SAP — puede ser error de clasificación"
        })

    # 3. Crédito sin plazo
    rows3 = db.execute(text("""
        SELECT l.unidad_key, l.manzana, l.card_name,
               p.nombre_proyecto, l.precio_final, l.forma_pago
        FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND l.forma_pago LIKE 'CREDITO%'
          AND (l.plazo IS NULL OR l.plazo = 0)
    """)).fetchall()
    for r in rows3:
        issues.append({
            "tipo": "CREDITO_SIN_PLAZO",
            "nivel": "AMARILLO",
            "mensaje": f"Crédito sin plazo definido: {r.card_name}",
            "detalle": f"{r.nombre_proyecto} | {r.manzana} | {r.forma_pago} | Q {float(r.precio_final or 0):,.0f}",
            "unidad_key": r.unidad_key,
            "proyecto": r.nombre_proyecto,
            "accion": "Registrar el plazo correcto en SAP"
        })

    return {
        "total": len(issues),
        "rojas": len([i for i in issues if i["nivel"] == "ROJO"]),
        "amarillas": len([i for i in issues if i["nivel"] == "AMARILLO"]),
        "issues": issues
    }


@router.get("/detalle-mes")
async def get_detalle_mes(
    mes: str = Query(...),  # formato: 2026-03-01
    forma_pago: Optional[str] = Query(None),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Detalle de lotes vendidos en un mes específico para drill-down."""
    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    fp = "AND l.forma_pago = :forma_pago" if forma_pago else ""
    params = {"mes": mes}
    if proyecto: params["proyecto"] = proyecto
    if forma_pago: params["forma_pago"] = forma_pago

    rows = db.execute(text(f"""
        SELECT l.unidad_key, l.manzana, l.card_name, l.vendedor,
               p.nombre_proyecto, l.precio_final, l.total_intereses,
               l.forma_pago, l.plazo, l.fecha_venta, l.estatus,
               COALESCE(v.equipo, 'SIN_ASIGNAR') AS equipo
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        LEFT JOIN vendedores v ON v.nombre = l.vendedor
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND DATE_TRUNC('month', l.fecha_venta) = CAST(:mes AS DATE)
          {pf} {fp}
        ORDER BY l.fecha_venta DESC, p.nombre_proyecto
    """), params).fetchall()

    return {
        "mes": mes,
        "total": len(rows),
        "valor_total": sum(float(r.precio_final or 0) for r in rows),
        "intereses_total": sum(float(r.total_intereses or 0) for r in rows),
        "lotes": [dict(r._mapping) for r in rows]
    }


@router.get("/detalle-plazo")
async def get_detalle_plazo(
    plazo: int = Query(...),
    forma_pago: str = Query(...),
    proyecto: Optional[str] = Query(None),
    todo_el_tiempo: bool = Query(False),
    año: int = Query(2026),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Detalle de lotes para un plazo específico — drill-down en mix de plazos."""
    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    if todo_el_tiempo:
        date_filter = "l.fecha_venta IS NOT NULL"
        params = {"plazo": plazo, "forma_pago": forma_pago}
    else:
        date_filter = "EXTRACT(YEAR FROM l.fecha_venta) = :año"
        params = {"plazo": plazo, "forma_pago": forma_pago, "año": año}
    if proyecto: params["proyecto"] = proyecto

    rows = db.execute(text(f"""
        SELECT l.unidad_key, l.manzana, l.card_name, l.vendedor,
               p.nombre_proyecto, l.precio_final, l.total_intereses,
               l.forma_pago, l.plazo, l.fecha_venta,
               COALESCE(v.equipo, 'SIN_ASIGNAR') AS equipo
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        LEFT JOIN vendedores v ON v.nombre = l.vendedor
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND l.plazo = :plazo
          AND l.forma_pago = :forma_pago
          AND {date_filter}
          {pf}
        ORDER BY l.fecha_venta DESC
    """), params).fetchall()

    return {
        "plazo": plazo,
        "forma_pago": forma_pago,
        "total": len(rows),
        "valor_total": sum(float(r.precio_final or 0) for r in rows),
        "intereses_total": sum(float(r.total_intereses or 0) for r in rows),
        "lotes": [dict(r._mapping) for r in rows]
    }



@router.get("/mezcla-financiera")
async def get_mezcla(
    meses_atras: int = Query(12),
    todo_el_tiempo: bool = Query(False),
    año: int = Query(2026),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    params = {}
    if proyecto: params["proyecto"] = proyecto
    if todo_el_tiempo:
        date_filter = "l.fecha_venta IS NOT NULL"
    else:
        date_filter = "l.fecha_venta >= CURRENT_DATE - (:meses || ' months')::INTERVAL AND l.fecha_venta IS NOT NULL"
        params["meses"] = meses_atras
    rows = db.execute(text(f"""
        SELECT
            DATE_TRUNC('month', l.fecha_venta)::DATE AS mes,
            l.forma_pago,
            COUNT(*) AS lotes,
            COALESCE(SUM(l.precio_final), 0) AS valor,
            COALESCE(SUM(l.total_intereses), 0) AS intereses,
            COALESCE(AVG(l.plazo) FILTER (WHERE l.plazo > 0), 0) AS plazo_prom
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND {date_filter}
          AND l.forma_pago IS NOT NULL
          {pf}
        GROUP BY DATE_TRUNC('month', l.fecha_venta), l.forma_pago
        ORDER BY mes, l.forma_pago
    """), params).fetchall()
    return [dict(r._mapping) for r in rows]


# ── POR VENDEDOR ──────────────────────────────────────────────

@router.get("/por-vendedor")
async def get_por_vendedor(
    año: int = Query(2026),
    mes: Optional[int] = Query(None),
    proyecto: Optional[str] = Query(None),
    equipo: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    if mes:
        date_filter = "DATE_TRUNC('month', l.fecha_venta) = make_date(:año, :mes, 1)"
        params = {"año": año, "mes": mes}
    else:
        date_filter = "EXTRACT(YEAR FROM l.fecha_venta) = :año"
        params = {"año": año}

    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    if proyecto: params["proyecto"] = proyecto

    ef = "AND v.equipo = :equipo" if equipo else ""
    if equipo: params["equipo"] = equipo

    rows = db.execute(text(f"""
        SELECT
            l.vendedor,
            COALESCE(v.equipo, 'SIN_ASIGNAR') AS equipo,
            p.nombre_proyecto AS proyecto,
            COUNT(*) AS ventas_brutas,
            COALESCE(SUM(l.precio_final), 0) AS valor_bruto,
            COALESCE(AVG(l.precio_final), 0) AS ticket_promedio,
            COALESCE(AVG(l.plazo) FILTER (WHERE l.plazo > 0), 0) AS plazo_promedio,
            COUNT(*) FILTER (WHERE l.forma_pago='CONTADO') AS contado,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOSININTERES') AS sin_interes,
            COUNT(*) FILTER (WHERE l.forma_pago='CREDITOCONINTERES') AS con_interes
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        LEFT JOIN vendedores v ON v.nombre = l.vendedor
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND {date_filter}
          AND l.fecha_venta IS NOT NULL
          AND l.vendedor IS NOT NULL
          AND l.vendedor NOT IN (
              '-Ningún empleado del departamento de ventas-',
              'Canje A','Bloqueado','Bloqueo Municipal','Apartado Proyecto Aptos'
          )
          {pf} {ef}
        GROUP BY l.vendedor, v.equipo, p.nombre_proyecto
        ORDER BY ventas_brutas DESC
    """), params).fetchall()

   # Get desistimientos by vendor+empresa - deduplicated by no_orden_venta+lote
    desist = db.execute(text(f"""
        SELECT asesor_venta AS vendedor, empresa, COUNT(*) AS desistimientos
        FROM (
            SELECT DISTINCT no_orden_venta, lote, asesor_venta, empresa
            FROM desistimientos
            WHERE EXTRACT(YEAR FROM fecha_desistimiento) = :año
            {'AND EXTRACT(MONTH FROM fecha_desistimiento) = :mes' if mes else ''}
        ) AS d
        GROUP BY asesor_venta, empresa
    """), params).fetchall()
    # Per-project map: (vendedor, empresa) → count
    _desist_by_proy = {}
    for r in desist:
        _desist_by_proy[(r.vendedor, r.empresa)] = r.desistimientos
    # Mapping proyecto→empresa para cruzar
    _PROY_EMP = {
        'Hacienda Jumay': 'Eficiencia Urbana', 'La Ceiba': 'Servicios Generales',
        'Hacienda el Sol': 'Rossio', 'Oasis Zacapa': 'Frugalex',
        'Cañadas de Jalapa': 'Ottavia', 'Condado Jutiapa': 'Utilica',
        'Club Campestre Jumay': 'Tezzoli', 'Club del Bosque': 'Urbiva 2',
        'Club Residencial Progreso': 'Garbatella', 'Arboleda Santa Elena': 'Capipos',
        'Hacienda Santa Lucia': 'Ovest', 'Hacienda El Cafetal Fase I': 'Corcolle',
        'Hacienda El Cafetal Fase III': 'Gibraleon', 'Hacienda El Cafetal Fase II': 'Leofreni',
        'Celajes De Tecpan': 'Vilet',
    }
    # Also keep total per vendor for team totals
    desist_map = {}
    for r in desist:
        desist_map[r.vendedor] = desist_map.get(r.vendedor, 0) + r.desistimientos

    # Get metas
    metas = db.execute(text("""
        SELECT responsable, proyecto, meta_consersa, meta_rv4,
               meta_consersa + meta_rv4 AS meta_total
        FROM metas_ventas
        WHERE año = :año AND mes = 0
    """), {"año": año}).fetchall()
    metas_map = {(r.responsable, r.proyecto): dict(r._mapping) for r in metas}

    result = []
    for r in rows:
        row = dict(r._mapping)
        # Normalizar vendedor para evitar errores con NULL
        vend = (row.get("vendedor") or "").strip() or "Sin asesor"
        row["vendedor"] = vend
        empresa_d = _PROY_EMP.get(row.get("proyecto"), "")
        d = _desist_by_proy.get((vend, empresa_d), 0)
        row["desistimientos"] = d
        row["ventas_netas"] = int(row["ventas_brutas"]) - d

        # Match meta by responsable name (fuzzy: last name match)
        meta_key = None
        vend_lower = vend.lower()
        proy_lower = (row.get("proyecto") or "").lower()
        for (resp, proy) in metas_map:
            resp_l = (resp or "").lower()
            proy_l = (proy or "").lower()
            if resp_l and (resp_l in vend_lower or vend_lower in resp_l):
                if proy_l and (proy_l in proy_lower or proy_lower in proy_l):
                    meta_key = (resp, proy)
                    break
        if meta_key:
            m = metas_map[meta_key]
            meta_equipo = m["meta_consersa"] if row["equipo"] == "CONSERSA" else m["meta_rv4"] if row["equipo"] == "RV4" else m["meta_total"]
            row["meta"] = meta_equipo
            row["cumplimiento_pct"] = round(int(row["ventas_brutas"]) / meta_equipo * 100, 1) if meta_equipo > 0 else 0
        else:
            row["meta"] = None
            row["cumplimiento_pct"] = None

        result.append(row)

    return result


# ── SIN ASESOR (caso especial: ventas sin vendedor asignado) ─────

@router.get("/sin-asesor")
async def get_sin_asesor(
    año: int = Query(2026),
    mes: Optional[int] = Query(None),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Lista detallada de ventas sin asesor asignado del período."""
    if mes:
        date_filter = "DATE_TRUNC('month', l.fecha_venta) = make_date(:año, :mes, 1)"
        params = {"año": año, "mes": mes}
    else:
        date_filter = "EXTRACT(YEAR FROM l.fecha_venta) = :año"
        params = {"año": año}

    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    if proyecto: params["proyecto"] = proyecto

    rows = db.execute(text(f"""
        SELECT
            l.unidad_key, l.manzana, p.nombre_proyecto,
            l.card_name AS cliente, l.vendedor,
            l.fecha_venta, l.precio_final, l.forma_pago,
            l.plazo, l.plazo_raw, l.status_promesa_compraventa
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND l.fecha_venta IS NOT NULL
          AND {date_filter}
          AND (l.vendedor IS NULL OR l.vendedor = '-Ningún empleado del departamento de ventas-')
          AND COALESCE(l.plazo_raw,'') NOT IN (
              'Canje A','Casa Modelo','Ansak, S.A.','Ansak S.A.','Ansak',
              'Apartado Proyecto Aptos','Bloqueo Municipal','Bloqueado'
          )
          AND COALESCE(l.vendedor,'') NOT IN (
              'Canje A','Bloqueado','Bloqueo Municipal','Apartado Proyecto Aptos'
          )
          {pf}
        ORDER BY p.nombre_proyecto, l.fecha_venta DESC
    """), params).fetchall()

    return [dict(r._mapping) for r in rows]


# ── METAS VS AVANCE ───────────────────────────────────────────

@router.get("/metas")
async def get_metas(
    año: int = Query(2026),
    mes: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Metas vs avance por PROYECTO (sin responsable), mensual. Lee metas del Excel."""
    import pandas as pd
    from pathlib import Path
    settings = get_settings()

    metas_excel = []
    try:
        df_m = pd.read_excel(Path(settings.path_ov_cartera), sheet_name="METAS VENTAS", header=0)
        df_m.columns = [str(c).strip() for c in df_m.columns]
        for _, row in df_m.iterrows():
            desc = str(row.get("Descripción", "") or "").strip()
            if not desc or desc.lower() == "nan": continue
            metas_excel.append({
                "proyecto": desc,
                "meta_consersa": int(float(row.get("Metas Consersa", 0) or 0)),
                "meta_rv4":      int(float(row.get("Metas RV4", 0) or 0)),
            })
    except Exception as e:
        import logging; logging.getLogger(__name__).warning(f"No se pudo leer metas Excel: {e}")

    date_filter = "DATE_TRUNC('month', l.fecha_venta) = make_date(:año, :mes, 1)" if mes else "EXTRACT(YEAR FROM l.fecha_venta) = :año"
    params = {"año": año, **({"mes": mes} if mes else {})}

    ventas_rows = db.execute(text(f"""
        SELECT p.nombre_proyecto AS proyecto,
               CASE
                   WHEN l.vendedor IS NULL OR l.vendedor = '-Ningún empleado del departamento de ventas-'
                       THEN 'SIN_ASIGNAR'
                   ELSE COALESCE(v.equipo, 'SIN_ASIGNAR')
               END AS equipo,
               COUNT(l.id) AS ventas
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        LEFT JOIN vendedores v ON v.nombre = l.vendedor
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND l.fecha_venta IS NOT NULL
          AND {date_filter}
          AND COALESCE(l.vendedor,'') NOT IN (
              'Canje A','Bloqueado','Bloqueo Municipal','Apartado Proyecto Aptos','Ansak, S.A.','Ansak S.A.'
          )
          AND COALESCE(l.plazo_raw,'') NOT IN (
              'Canje A','Casa Modelo','Ansak, S.A.','Ansak S.A.','Ansak','Apartado Proyecto Aptos','Bloqueo Municipal','Bloqueado'
          )
        GROUP BY p.nombre_proyecto, CASE
                   WHEN l.vendedor IS NULL OR l.vendedor = '-Ningún empleado del departamento de ventas-'
                       THEN 'SIN_ASIGNAR'
                   ELSE COALESCE(v.equipo, 'SIN_ASIGNAR')
               END
    """), params).fetchall()

    PROY_MAP = {
        "Ottavia":"Cañadas de Jalapa","Tezzoli":"Club Campestre Jumay",
        "Eficiencia Urbana":"Hacienda Jumay","Servicios Generales":"La Ceiba",
        "Capipos":"Arboleda Santa Elena","Urbiva":"Club del Bosque",
        "Corcolle":"Hacienda El Cafetal Fase I","Frugalex":"Oasis Zacapa",
        "Ovest":"Hacienda Santa Lucia","Vilet":"Celajes De Tecpan",
        "Rossio":"Hacienda el Sol","Utilica":"Condado Jutiapa",
        "Garbatella":"Club Residencial El Progreso",
    }

    ventas_map = {}
    for r in ventas_rows:
        p = r.proyecto
        if p not in ventas_map: ventas_map[p] = {"consersa":0,"rv4":0,"sin_asignar":0}
        eq = r.equipo.upper()
        if eq == "CONSERSA": ventas_map[p]["consersa"] += r.ventas
        elif eq == "RV4":    ventas_map[p]["rv4"]      += r.ventas
        else:                ventas_map[p]["sin_asignar"] += r.ventas

    result = []
    for m in metas_excel:
        desc = m["proyecto"]
        bd_nombre = PROY_MAP.get(desc, desc)
        v = ventas_map.get(bd_nombre, {"consersa":0,"rv4":0,"sin_asignar":0})
        vc, vr, vs = v["consersa"], v["rv4"], v["sin_asignar"]
        vt = vc + vr + vs
        mt = m["meta_consersa"] + m["meta_rv4"]
        if mt == 0: continue
        result.append({
            "proyecto": desc,
            "nombre_proyecto_bd": bd_nombre,
            "meta_consersa": m["meta_consersa"],
            "meta_rv4": m["meta_rv4"],
            "meta_total": mt,
            "ventas_consersa": vc,
            "ventas_rv4": vr,
            "ventas_sin_asignar": vs,
            "ventas_total": vt,
            "cumplimiento_pct": round(vt/mt*100, 1) if mt > 0 else 0.0,
            "cumplimiento_consersa_pct": round(vc/m["meta_consersa"]*100,1) if m["meta_consersa"] > 0 else 0.0,
            "cumplimiento_rv4_pct": round(vr/m["meta_rv4"]*100,1) if m["meta_rv4"] > 0 else 0.0,
        })

    return result


@router.get("/pcv/kpis")
async def get_pcv_kpis(
    año: Optional[int] = Query(None),
    mes: Optional[int] = Query(None),
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """KPIs de Promesas de Compraventa.
    Con PCV = 'Contrato Promesa de Compra venta'
    Sin PCV = 'Propuesta', 'Consulta Infornet', NULL o vacío
    Filtro por fecha de venta del lote (año/mes) y proyecto.
    """
    params = {}
    date_f = ""
    proj_f = ""

    # Filtro de fecha sobre fecha_venta del lote
    if año and mes:
        date_f = "AND DATE_TRUNC('month', l.fecha_venta) = make_date(:año, :mes, 1)"
        params.update({"año": año, "mes": mes})
    elif año:
        date_f = "AND EXTRACT(YEAR FROM l.fecha_venta) = :año"
        params["año"] = año

    if proyecto:
        proj_f = "AND p.nombre_proyecto = :proyecto"
        params["proyecto"] = proyecto

    # Solo ventas reales (excluye canjes/bloqueos)
    es_venta = es_venta_where()

    row = db.execute(text(f"""
        SELECT
            COUNT(l.id) FILTER (WHERE {es_venta}) AS total_ventas,
            -- Con PCV = tiene contrato firmado
            COUNT(l.id) FILTER (WHERE {es_venta}
                AND l.status_promesa_compraventa = 'Contrato Promesa de Compra venta') AS con_pcv,
            -- Sin PCV = Propuesta, Consulta Infornet, NULL o vacío
            COUNT(l.id) FILTER (WHERE {es_venta}
                AND COALESCE(l.status_promesa_compraventa,'') NOT IN (
                    'Contrato Promesa de Compra venta', 'Disponible'
                )) AS sin_pcv,
            -- Sin PCV del año actual
            COUNT(l.id) FILTER (WHERE {es_venta}
                AND l.fecha_venta >= DATE_TRUNC('year', CURRENT_DATE)
                AND COALESCE(l.status_promesa_compraventa,'') NOT IN (
                    'Contrato Promesa de Compra venta', 'Disponible'
                )) AS sin_pcv_año,
            -- Días promedio desde venta hasta PCV (solo los que tienen PCV)
            COALESCE(AVG(CASE
                WHEN l.status_promesa_compraventa = 'Contrato Promesa de Compra venta'
                     AND l.fecha_solicitud_pcv IS NOT NULL AND l.fecha_venta IS NOT NULL
                THEN (l.fecha_solicitud_pcv::date - l.fecha_venta::date)::int
            END), 0) AS dias_prom_gestion,
            -- Aging sin PCV por días desde fecha_venta hasta HOY
            COUNT(l.id) FILTER (WHERE {es_venta}
                AND COALESCE(l.status_promesa_compraventa,'') NOT IN (
                    'Contrato Promesa de Compra venta', 'Disponible'
                ) AND l.fecha_venta IS NOT NULL
                AND (CURRENT_DATE - l.fecha_venta::date) BETWEEN 0 AND 15) AS sin_pcv_0_15,
            COUNT(l.id) FILTER (WHERE {es_venta}
                AND COALESCE(l.status_promesa_compraventa,'') NOT IN (
                    'Contrato Promesa de Compra venta', 'Disponible'
                ) AND l.fecha_venta IS NOT NULL
                AND (CURRENT_DATE - l.fecha_venta::date) BETWEEN 16 AND 30) AS sin_pcv_16_30,
            COUNT(l.id) FILTER (WHERE {es_venta}
                AND COALESCE(l.status_promesa_compraventa,'') NOT IN (
                    'Contrato Promesa de Compra venta', 'Disponible'
                ) AND l.fecha_venta IS NOT NULL
                AND (CURRENT_DATE - l.fecha_venta::date) BETWEEN 31 AND 90) AS sin_pcv_31_90,
            COUNT(l.id) FILTER (WHERE {es_venta}
                AND COALESCE(l.status_promesa_compraventa,'') NOT IN (
                    'Contrato Promesa de Compra venta', 'Disponible'
                ) AND l.fecha_venta IS NOT NULL
                AND (CURRENT_DATE - l.fecha_venta::date) > 90) AS sin_pcv_mas90
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND l.fecha_venta IS NOT NULL
          {date_f} {proj_f}
    """), params).fetchone()

    d = dict(row._mapping) if row else {}
    total = d.get("total_ventas") or 0
    con   = d.get("con_pcv") or 0
    d["sin_pcv"] = d.get("sin_pcv") or 0
    d["pct_cumplimiento"] = round(con / total * 100, 1) if total else 0
    return {k: (float(v) if v is not None else 0) for k, v in d.items()}


@router.get("/registros-revision")
async def get_registros_revision(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Inconsistencias detectadas automáticamente entre SAP y tablero."""
    issues = []

    # Crédito con interés pero total_intereses = 0
    rows = db.execute(text("""
        SELECT l.id, l.unidad_key, l.forma_pago, l.precio_final, l.total_intereses,
               p.nombre_proyecto, l.vendedor, l.card_name
        FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO')
          AND l.forma_pago = 'CREDITOCONINTERES'
          AND (l.total_intereses IS NULL OR l.total_intereses = 0)
        LIMIT 20
    """)).fetchall()
    for r in rows:
        issues.append({
            "tipo": "CON_INTERES_SIN_MONTO", "nivel": "ROJO",
            "mensaje": f"Crédito con interés sin monto: {r.card_name or r.unidad_key}",
            "detalle": f"{r.nombre_proyecto} | {r.unidad_key} | Q {r.precio_final:,.0f}",
            "accion": "Revisar contrato en SAP — Total Intereses = 0"
        })

    # Ventas sin vendedor asignado (excluye canjes/bloqueados)
    rows2 = db.execute(text("""
        SELECT COUNT(*) as cnt, l.proyecto_id, p.nombre_proyecto
        FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id
        WHERE l.estatus IN ('VENTA','RESERVADO')
          AND (l.vendedor IS NULL OR l.vendedor = '')
        GROUP BY l.proyecto_id, p.nombre_proyecto
        HAVING COUNT(*) > 0
        LIMIT 10
    """)).fetchall()
    for r in rows2:
        issues.append({
            "tipo": "VENTA_SIN_VENDEDOR", "nivel": "GRIS",
            "mensaje": f"{r.cnt} ventas sin vendedor en {r.nombre_proyecto}",
            "detalle": f"Ventas sin asignación de vendedor",
            "accion": "Verificar en SAP el campo Vendedor"
        })

    rojas    = sum(1 for i in issues if i["nivel"] == "ROJO")
    amarillas = sum(1 for i in issues if i["nivel"] == "AMARILLO")
    grises   = sum(1 for i in issues if i["nivel"] == "GRIS")
    return {"total": len(issues), "rojas": rojas, "amarillas": amarillas,
            "grises": grises, "issues": issues}


@router.get("/pcv/pendientes")
async def get_pcv_pendientes(
    proyecto: Optional[str] = Query(None),
    vendedor: Optional[str] = Query(None),
    antiguedad: Optional[str] = Query(None),  # 0-15, 15-30, 30-90, 90+
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Lotes vendidos sin PCV firmado."""
    # Sin PCV = Propuesta, Consulta Infornet, NULL o vacío
    conditions = [
        "l.estatus IN ('VENTA','RESERVADO','CANJE')",
        "l.fecha_venta IS NOT NULL",
        "COALESCE(l.status_promesa_compraventa,'') NOT IN ('Contrato Promesa de Compra venta','Disponible')",
    ]
    params = {}

    if proyecto:
        conditions.append("p.nombre_proyecto = :proyecto")
        params["proyecto"] = proyecto
    if vendedor:
        conditions.append("l.vendedor ILIKE :vendedor")
        params["vendedor"] = f"%{vendedor}%"
    if antiguedad:
        if antiguedad == "0-15":
            conditions.append("CURRENT_DATE - l.fecha_venta::date <= 15")
        elif antiguedad == "15-30":
            conditions.append("CURRENT_DATE - l.fecha_venta::date BETWEEN 16 AND 30")
        elif antiguedad == "30-90":
            conditions.append("CURRENT_DATE - l.fecha_venta::date BETWEEN 31 AND 90")
        elif antiguedad == "90+":
            conditions.append("CURRENT_DATE - l.fecha_venta::date > 90")

    where = " AND ".join(conditions)
    offset = (page - 1) * page_size

    total = db.execute(text(f"SELECT COUNT(*) FROM lotes l JOIN proyectos p ON p.id=l.proyecto_id WHERE {where}"), params).scalar()

    rows = db.execute(text(f"""
        SELECT
            l.unidad_key, l.manzana, l.card_name, l.card_code,
            l.vendedor, p.nombre_proyecto,
            l.fecha_venta, l.fecha_solicitud_pcv,
            l.status_promesa_compraventa,
            l.precio_final, l.forma_pago, l.plazo,
            COALESCE(v.equipo, 'SIN_ASIGNAR') AS equipo,
            CURRENT_DATE - l.fecha_venta::date AS dias_sin_pcv,
            CASE
                WHEN CURRENT_DATE - l.fecha_venta::date <= 15 THEN 'VERDE'
                WHEN CURRENT_DATE - l.fecha_venta::date <= 30 THEN 'AMARILLO'
                WHEN CURRENT_DATE - l.fecha_venta::date <= 90 THEN 'ROJO'
                ELSE 'CRITICO'
            END AS semaforo
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        LEFT JOIN vendedores v ON v.nombre = l.vendedor
        WHERE {where}
        ORDER BY dias_sin_pcv DESC
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": offset}).fetchall()

    return {
        "pendientes": [dict(r._mapping) for r in rows],
        "total": total,
        "page": page,
        "pages": -(-total // page_size)
    }


@router.get("/pcv/por-vendedor")
async def get_pcv_por_vendedor(
    proyecto: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Cumplimiento de PCV por vendedor."""
    pf = "AND p.nombre_proyecto = :proyecto" if proyecto else ""
    params = {}
    if proyecto: params["proyecto"] = proyecto

    rows = db.execute(text(f"""
        SELECT
            l.vendedor,
            COALESCE(v.equipo, 'SIN_ASIGNAR') AS equipo,
            COUNT(*) AS total_ventas,
            COUNT(*) FILTER (WHERE l.status_promesa_compraventa = :pcv_val) AS con_pcv,
            COUNT(*) FILTER (WHERE l.status_promesa_compraventa != :pcv_val
                              OR l.status_promesa_compraventa IS NULL) AS sin_pcv,
            COUNT(*) FILTER (WHERE (l.status_promesa_compraventa != :pcv_val
                              OR l.status_promesa_compraventa IS NULL)
                              AND CURRENT_DATE - l.fecha_venta::date > 30) AS sin_pcv_critico,
            ROUND(
                COUNT(*) FILTER (WHERE l.status_promesa_compraventa = :pcv_val)::numeric
                / NULLIF(COUNT(*), 0) * 100, 1
            ) AS pct_cumplimiento,
            COALESCE(AVG(
                CASE WHEN l.status_promesa_compraventa = :pcv_val
                     AND l.fecha_solicitud_pcv IS NOT NULL AND l.fecha_venta IS NOT NULL
                THEN l.fecha_solicitud_pcv::date - l.fecha_venta::date END
            ), 0) AS dias_prom_gestion
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        LEFT JOIN vendedores v ON v.nombre = l.vendedor
        WHERE l.estatus IN ('VENTA','RESERVADO','CANJE')
          AND l.fecha_venta IS NOT NULL
          AND COALESCE(l.vendedor,'') NOT IN (
              'Canje A','Bloqueado','Bloqueo Municipal','Apartado Proyecto Aptos'
          )
          {pf}
        GROUP BY l.vendedor, v.equipo
        ORDER BY sin_pcv DESC
    """), {**params, "pcv_val": PCV_FIRMADO}).fetchall()

    return [dict(r._mapping) for r in rows]
