"""
flujos.py — Router FastAPI para Flujos de Efectivo (histórico)
Endpoints:
  GET /flujos/resumen     → tabla de flujo agregada (semana / mes / año)
  GET /flujos/detalle     → transacciones individuales con filtros
  GET /flujos/periodos    → períodos disponibles para la UI
  POST /flujos/sync       → dispara sincronización manual (ADMIN)
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from app.database import get_db
from app.core.security import get_current_user
from sqlalchemy.orm import Session

router = APIRouter(prefix="/api/flujos", tags=["flujos"])

# ── Orden de secciones para el estado de flujo ──────────────────────────────
ORDEN_SECCIONES = [
    "INGRESOS",
    "EGRESOS / URBANIZACION",
    "EGRESOS / ADMINISTRACION",
    "FINANCIAMIENTO",
    "TERRENO",
    "IMPUESTOS",
    "SIN CLASIFICAR",
]


def _orden_seccion(seccion: str) -> int:
    try:
        return ORDEN_SECCIONES.index(seccion)
    except ValueError:
        return len(ORDEN_SECCIONES)


# ── Helper: calcular saldo inicial de un período ─────────────────────────────
def _get_saldo_inicial(db: Session, sociedad: str, granularidad: str, periodos: list[str]) -> dict:
    """
    Para el PRIMER período retorna el saldo de flujos_saldo_inicial.
    Para los demás calcula: saldo_final del período anterior.
    Devuelve dict {periodo: saldo_inicial}.
    """
    if not periodos:
        return {}

    saldos = {}

    if granularidad == "mes":
        # Primer período: buscar en tabla saldo_inicial
        p0 = periodos[0]
        anio0, mes0 = int(p0[:4]), int(p0[5:7])
        row = db.execute(text("""
            SELECT monto FROM flujos_saldo_inicial
            WHERE sociedad = :soc AND anio = :anio AND mes = :mes
            ORDER BY id LIMIT 1
        """), {"soc": sociedad, "anio": anio0, "mes": mes0}).fetchone()

        # Fix: si no hay match exacto para ese mes, usar el saldo_inicial
        # más reciente anterior al primer período.
        # Cubre casos como FRUGALEX (PI en mes=11, primer dato en mes=12).
        if not row:
            row = db.execute(text("""
                SELECT monto FROM flujos_saldo_inicial
                WHERE sociedad = :soc
                  AND (anio < :anio OR (anio = :anio AND mes <= :mes))
                ORDER BY anio DESC, mes DESC
                LIMIT 1
            """), {"soc": sociedad, "anio": anio0, "mes": mes0}).fetchone()

        saldos[p0] = float(row[0]) if row else 0.0

        # Períodos siguientes: acumular
        for i in range(1, len(periodos)):
            prev = periodos[i - 1]
            pa, pm = int(prev[:4]), int(prev[5:7])
            movs = db.execute(text("""
                SELECT COALESCE(SUM(monto_ingreso),0) - COALESCE(SUM(monto_egreso),0)
                FROM flujos_efectivo
                WHERE sociedad=:soc AND anio=:anio AND mes=:mes
            """), {"soc": sociedad, "anio": pa, "mes": pm}).fetchone()
            neto_prev = float(movs[0]) if movs else 0.0
            saldos[periodos[i]] = saldos[prev] + neto_prev

    elif granularidad == "semana":
        p0 = periodos[0]
        a0, s0 = p0.split("-S")
        row = db.execute(text("""
            SELECT monto FROM flujos_saldo_inicial
            WHERE sociedad=:soc AND anio=:anio AND semana_iso=:sem
            ORDER BY id LIMIT 1
        """), {"soc": sociedad, "anio": int(a0), "sem": int(s0)}).fetchone()
        # Si no hay exactamente esa semana en la tabla, usar el primer registro disponible
        if not row:
            row = db.execute(text("""
                SELECT monto FROM flujos_saldo_inicial
                WHERE sociedad=:soc ORDER BY anio, mes LIMIT 1
            """), {"soc": sociedad}).fetchone()
        saldos[p0] = float(row[0]) if row else 0.0
        for i in range(1, len(periodos)):
            prev = periodos[i - 1]
            pa, ps = prev.split("-S")
            movs = db.execute(text("""
                SELECT COALESCE(SUM(monto_ingreso),0) - COALESCE(SUM(monto_egreso),0)
                FROM flujos_efectivo
                WHERE sociedad=:soc AND anio=:anio AND semana_iso=:sem
            """), {"soc": sociedad, "anio": int(pa), "sem": int(ps)}).fetchone()
            saldos[periodos[i]] = saldos[prev] + (float(movs[0]) if movs else 0.0)

    elif granularidad == "anio":
        a0 = periodos[0]
        row = db.execute(text("""
            SELECT monto FROM flujos_saldo_inicial
            WHERE sociedad=:soc AND anio=:anio ORDER BY id LIMIT 1
        """), {"soc": sociedad, "anio": int(a0)}).fetchone()
        saldos[a0] = float(row[0]) if row else 0.0
        for i in range(1, len(periodos)):
            prev = periodos[i - 1]
            movs = db.execute(text("""
                SELECT COALESCE(SUM(monto_ingreso),0) - COALESCE(SUM(monto_egreso),0)
                FROM flujos_efectivo
                WHERE sociedad=:soc AND anio=:anio
            """), {"soc": sociedad, "anio": int(prev)}).fetchone()
            saldos[periodos[i]] = saldos[prev] + (float(movs[0]) if movs else 0.0)

    return saldos


# ── GET /flujos/periodos ─────────────────────────────────────────────────────
@router.get("/periodos")
def get_periodos(
    sociedad: str = Query("EFICIENCIA URBANA"),
    granularidad: str = Query("mes", regex="^(semana|mes|anio)$"),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Devuelve los períodos disponibles para el selector de la UI."""
    if granularidad == "mes":
        rows = db.execute(text("""
            SELECT DISTINCT TO_CHAR(DATE_TRUNC('month', fecha_contable), 'YYYY-MM') AS periodo
            FROM flujos_efectivo WHERE sociedad=:soc
            ORDER BY periodo
        """), {"soc": sociedad}).fetchall()
    elif granularidad == "semana":
        rows = db.execute(text("""
            SELECT DISTINCT CONCAT(anio, '-S', LPAD(semana_iso::text, 2, '0')) AS periodo
            FROM flujos_efectivo WHERE sociedad=:soc
            ORDER BY periodo
        """), {"soc": sociedad}).fetchall()
    else:
        rows = db.execute(text("""
            SELECT DISTINCT anio::text AS periodo
            FROM flujos_efectivo WHERE sociedad=:soc
            ORDER BY periodo
        """), {"soc": sociedad}).fetchall()

    return {"periodos": [r[0] for r in rows]}


# ── GET /flujos/resumen ──────────────────────────────────────────────────────
@router.get("/resumen")
def get_resumen_flujos(
    sociedad: str = Query("EFICIENCIA URBANA"),
    granularidad: str = Query("mes", regex="^(semana|mes|anio)$"),
    desde: Optional[str] = Query(None, description="Período inicio ej: 2024-01 / 2024-S01 / 2024"),
    hasta: Optional[str] = Query(None, description="Período fin"),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """
    Devuelve la tabla del estado de flujo de efectivo:
    filas = sección/categoría, columnas = períodos seleccionados.
    Incluye SALDO INICIAL y SALDO FINAL calculados.
    """
    # 1. Obtener períodos disponibles
    all_periodos_resp = get_periodos(sociedad, granularidad, db, current_user)
    all_periodos = all_periodos_resp["periodos"]

    if not all_periodos:
        return {"sociedad": sociedad, "granularidad": granularidad, "periodos": [], "secciones": [], "saldos_iniciales": {}, "saldos_finales": {}}

    # Filtrar rango si se especificó
    periodos = all_periodos
    if desde:
        periodos = [p for p in periodos if p >= desde]
    if hasta:
        periodos = [p for p in periodos if p <= hasta]
    if not periodos:
        return {"sociedad": sociedad, "granularidad": granularidad, "periodos": [], "secciones": [], "saldos_iniciales": {}, "saldos_finales": {}}

    # 2. Calcular saldos iniciales
    saldos_ini = _get_saldo_inicial(db, sociedad, granularidad, periodos)

    # 3. Obtener movimientos agrupados
    if granularidad == "mes":
        query = text("""
            SELECT
                TO_CHAR(DATE_TRUNC('month', fecha_contable), 'YYYY-MM') AS periodo,
                seccion, nombre_categoria,
                SUM(monto_ingreso) AS ing,
                SUM(monto_egreso)  AS egr
            FROM flujos_efectivo
            WHERE sociedad=:soc
              AND TO_CHAR(DATE_TRUNC('month', fecha_contable), 'YYYY-MM') = ANY(:periodos)
            GROUP BY 1,2,3 ORDER BY 1,2,3
        """)
    elif granularidad == "semana":
        query = text("""
            SELECT
                CONCAT(anio, '-S', LPAD(semana_iso::text, 2, '0')) AS periodo,
                seccion, nombre_categoria,
                SUM(monto_ingreso) AS ing,
                SUM(monto_egreso)  AS egr
            FROM flujos_efectivo
            WHERE sociedad=:soc
              AND CONCAT(anio, '-S', LPAD(semana_iso::text, 2, '0')) = ANY(:periodos)
            GROUP BY 1,2,3 ORDER BY 1,2,3
        """)
    else:
        query = text("""
            SELECT
                anio::text AS periodo,
                seccion, nombre_categoria,
                SUM(monto_ingreso) AS ing,
                SUM(monto_egreso)  AS egr
            FROM flujos_efectivo
            WHERE sociedad=:soc
              AND anio::text = ANY(:periodos)
            GROUP BY 1,2,3 ORDER BY 1,2,3
        """)

    rows = db.execute(query, {"soc": sociedad, "periodos": periodos}).fetchall()

    # 4. Estructurar: secciones → categorías → períodos
    # data[seccion][categoria][periodo] = {ing, egr}
    data: dict = {}
    for r in rows:
        periodo, seccion, cat, ing, egr = r
        ing, egr = float(ing or 0), float(egr or 0)
        data.setdefault(seccion, {}).setdefault(cat, {})[periodo] = {"ing": ing, "egr": egr}

    # 5. Construir respuesta en orden correcto
    secciones_ordenadas = sorted(data.keys(), key=_orden_seccion)

    secciones_out = []
    for sec in secciones_ordenadas:
        cats = data[sec]
        cats_out = []
        for cat, movs in sorted(cats.items()):
            fila = {"categoria": cat, "montos": {}}
            for p in periodos:
                m = movs.get(p, {})
                fila["montos"][p] = {
                    "ingreso": m.get("ing", 0.0),
                    "egreso":  m.get("egr", 0.0),
                    "neto":    m.get("ing", 0.0) - m.get("egr", 0.0),
                }
            cats_out.append(fila)

        # Total de sección por período
        totales = {}
        for p in periodos:
            ing_t = sum(c["montos"][p]["ingreso"] for c in cats_out)
            egr_t = sum(c["montos"][p]["egreso"]  for c in cats_out)
            totales[p] = {"ingreso": ing_t, "egreso": egr_t, "neto": ing_t - egr_t}

        secciones_out.append({
            "seccion":    sec,
            "categorias": cats_out,
            "totales":    totales,
        })

    # 6. Aplicar reclasificaciones (mueven montos entre secciones, no afectan flujo neto)
    if granularidad == "mes":
        reclas = db.execute(text("""
            SELECT TO_CHAR(DATE_TRUNC('month', fecha_contable), 'YYYY-MM') AS periodo,
                   seccion_origen, seccion_destino, SUM(monto) AS monto
            FROM flujos_reclasificaciones
            WHERE sociedad=:soc
              AND TO_CHAR(DATE_TRUNC('month', fecha_contable), 'YYYY-MM') = ANY(:periodos)
            GROUP BY 1,2,3
        """), {"soc": sociedad, "periodos": periodos}).fetchall()
    elif granularidad == "semana":
        reclas = db.execute(text("""
            SELECT CONCAT(anio, '-S', LPAD(EXTRACT(WEEK FROM fecha_contable)::text, 2, '0')) AS periodo,
                   seccion_origen, seccion_destino, SUM(monto) AS monto
            FROM flujos_reclasificaciones
            WHERE sociedad=:soc
              AND CONCAT(anio, '-S', LPAD(EXTRACT(WEEK FROM fecha_contable)::text, 2, '0')) = ANY(:periodos)
            GROUP BY 1,2,3
        """), {"soc": sociedad, "periodos": periodos}).fetchall()
    else:
        reclas = db.execute(text("""
            SELECT anio::text AS periodo,
                   seccion_origen, seccion_destino, SUM(monto) AS monto
            FROM flujos_reclasificaciones
            WHERE sociedad=:soc AND anio::text = ANY(:periodos)
            GROUP BY 1,2,3
        """), {"soc": sociedad, "periodos": periodos}).fetchall()

    # Apply reclasificaciones: reduce origen section, increase destino section
    # Build lookup by seccion name for quick access
    sec_totales_map = {s["seccion"]: s["totales"] for s in secciones_out}

    for r in reclas:
        periodo, sec_ori, sec_dst, monto = r
        monto = float(monto or 0)
        if not monto or sec_ori == sec_dst:
            continue
        # Reduce egreso in origen section
        if sec_ori in sec_totales_map and periodo in sec_totales_map[sec_ori]:
            sec_totales_map[sec_ori][periodo]["egreso"]  -= monto
            sec_totales_map[sec_ori][periodo]["neto"]    += monto
        # Increase egreso in destino section (create if needed)
        if sec_dst not in sec_totales_map:
            sec_totales_map[sec_dst] = {}
            secciones_out.append({"seccion": sec_dst, "categorias": [], "totales": sec_totales_map[sec_dst]})
        if periodo not in sec_totales_map[sec_dst]:
            sec_totales_map[sec_dst][periodo] = {"ingreso": 0.0, "egreso": 0.0, "neto": 0.0}
        sec_totales_map[sec_dst][periodo]["egreso"] += monto
        sec_totales_map[sec_dst][periodo]["neto"]   -= monto

    # 7. Saldos finales = saldo_inicial + neto_total del período
    saldos_fin = {}
    for p in periodos:
        neto_p = sum(
            sec["totales"][p]["ingreso"] - sec["totales"][p]["egreso"]
            for sec in secciones_out
            if p in sec["totales"]
        )
        saldos_fin[p] = saldos_ini.get(p, 0.0) + neto_p

    return {
        "sociedad":       sociedad,
        "granularidad":   granularidad,
        "periodos":       periodos,
        "secciones":      secciones_out,
        "saldos_iniciales": saldos_ini,
        "saldos_finales":   saldos_fin,
    }


# ── GET /flujos/detalle ──────────────────────────────────────────────────────
@router.get("/detalle")
def get_detalle_flujos(
    sociedad: str = Query("EFICIENCIA URBANA"),
    granularidad: str = Query("mes", regex="^(semana|mes|anio)$"),
    periodo: str = Query(..., description="Período específico ej: 2024-03"),
    seccion: Optional[str] = Query(None),
    categoria: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=10, le=200),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Drill-down: transacciones individuales de un período/sección."""
    where = "WHERE sociedad=:soc"
    params: dict = {"soc": sociedad}

    if granularidad == "mes":
        where += " AND TO_CHAR(DATE_TRUNC('month', fecha_contable), 'YYYY-MM') = :periodo"
    elif granularidad == "semana":
        where += " AND CONCAT(anio, '-S', LPAD(semana_iso::text, 2, '0')) = :periodo"
    else:
        where += " AND anio::text = :periodo"
    params["periodo"] = periodo

    if seccion:
        where += " AND seccion = :seccion"
        params["seccion"] = seccion
    if categoria:
        where += " AND nombre_categoria = :categoria"
        params["categoria"] = categoria

    total = db.execute(text(f"SELECT COUNT(*) FROM flujos_efectivo {where}"), params).scalar()
    offset = (page - 1) * page_size

    rows = db.execute(text(f"""
        SELECT
            fecha_contable, seccion, nombre_categoria,
            tipo_transaccion, modulo,
            monto_ingreso, monto_egreso,
            cliente_nombre, sn_nombre,
            cobro_comentario, pago_comentario,
            banco_nombre, belnr
        FROM flujos_efectivo {where}
        ORDER BY fecha_contable, belnr
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": offset}).fetchall()

    cols = ["fecha_contable", "seccion", "nombre_categoria", "tipo_transaccion",
            "modulo", "monto_ingreso", "monto_egreso", "cliente_nombre", "sn_nombre",
            "cobro_comentario", "pago_comentario", "banco_nombre", "belnr"]

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
        "registros": [dict(zip(cols, r)) for r in rows],
    }


# ── POST /flujos/sync ────────────────────────────────────────────────────────
@router.post("/sync")
def sync_flujos_manual(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Dispara sincronización manual del Excel de flujos. Solo ADMIN (nivel 4)."""
    if current_user.nivel < 4:
        raise HTTPException(status_code=403, detail="Solo administradores pueden sincronizar flujos")
    try:
        from app.sync.sync_flujos import sincronizar_flujos
        from app.sync.sync_reclasificaciones import sincronizar_reclasificaciones
        resultado_flujos = sincronizar_flujos()
        resultado_reclas = sincronizar_reclasificaciones()
        return {"ok": True, "resultado": resultado_flujos, "reclasificaciones": resultado_reclas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
