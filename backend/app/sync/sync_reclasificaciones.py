"""
sync_reclasificaciones.py
Sincroniza la hoja RECLASIFICACIONES de PRESUPUESTO_PRESTAMOS_Y_TIERRA.xlsx
hacia la tabla flujos_reclasificaciones.
"""
import os
import math
import logging
from typing import Optional
import pandas as pd
from sqlalchemy import text
from app.database import engine

logger = logging.getLogger(__name__)

PRESUPUESTO_PATH = os.environ.get(
    "PATH_PRESUPUESTO",
    r"C:\Users\jlima\OneDrive - rvcuatro.com\Finanzas - Desarrollos - Planificación financiera\19. Lotes\Inventario\2. Tablero Lotificaciones Preliminar (5010)\PRESUPUESTO, PRESTAMOS Y TIERRA.xlsx"
)

SOCIEDADES_ACTIVAS = {"EFICIENCIA URBANA"}


def _sf(val) -> float:
    if val is None: return 0.0
    if isinstance(val, float) and math.isnan(val): return 0.0
    try: return float(val)
    except: return 0.0

def _clean(val) -> Optional[str]:
    if val is None: return None
    if isinstance(val, float):
        if math.isnan(val): return None
        return str(int(val)) if val == int(val) else str(val)
    s = str(val).strip()
    return None if s.lower() in ("nan","none","null","") else s

def _to_date(val):
    if val is None: return None
    try:
        if pd.isna(val): return None
    except: pass
    if isinstance(val, pd.Timestamp): return val.date() if not pd.isnull(val) else None
    import datetime
    if isinstance(val, datetime.datetime): return val.date()
    if isinstance(val, datetime.date): return val
    return None


def sincronizar_reclasificaciones() -> dict:
    resultado = {"insertados": 0, "omitidos": 0, "errores": []}

    if not os.path.exists(PRESUPUESTO_PATH):
        msg = f"Archivo no encontrado: {PRESUPUESTO_PATH}"
        logger.error(msg)
        resultado["errores"].append(msg)
        return resultado

    try:
        xf = pd.ExcelFile(PRESUPUESTO_PATH)
    except Exception as e:
        resultado["errores"].append(f"Error abriendo Excel: {e}")
        return resultado

    if "RECLASIFICACIONES" not in xf.sheet_names:
        resultado["errores"].append("Hoja RECLASIFICACIONES no encontrada")
        return resultado

    df = xf.parse("RECLASIFICACIONES")
    df.columns = [c.strip() for c in df.columns]
    df = df.dropna(how="all")
    df['FECHA_CONTABLE'] = pd.to_datetime(df['FECHA_CONTABLE'], errors='coerce')

    # Collect all valid rows first, then do TRUNCATE + bulk INSERT per sociedad
    # This correctly handles multiple identical transactions (same cuenta, fecha, monto)
    rows_by_sociedad: dict = {}
    omi = 0

    for _, row in df.iterrows():
        soc = _clean(row.get("SOCIEDAD"))
        if not soc or soc not in SOCIEDADES_ACTIVAS:
            omi += 1
            continue

        fecha = _to_date(row.get("FECHA_CONTABLE"))
        monto = _sf(row.get("MONTO_PRORRATEADO"))
        sec_ori = _clean(row.get("SECCION"))
        sec_dst = _clean(row.get("SECCION2") or row.get("SECCION4"))  # Excel usa SECCION2

        if not fecha or not sec_ori or not sec_dst or monto == 0:
            omi += 1
            continue

        if soc not in rows_by_sociedad:
            rows_by_sociedad[soc] = []

        rows_by_sociedad[soc].append({
            "sociedad":       soc,
            "cuenta":         _clean(row.get("CUENTA_CONTRAPARTIDA")),
            "cuenta_nombre":  _clean(row.get("CUENTA_CONTRAPARTIDA_NOMBRE")),
            "monto":          monto,
            "fecha_contable": fecha,
            "anio":           fecha.year,
            "mes":            fecha.month,
            "seccion_origen": sec_ori,
            "nombre_origen":  _clean(row.get("NOMBRE")),
            "seccion_destino":sec_dst,
            "subseccion":     _clean(row.get("SUBSECCION")),
            "nombre_destino": _clean(row.get("NOMBRE5")),
            "concepto":       _clean(row.get("CONCEPTO")),
        })

    ins = 0
    try:
        with engine.begin() as conn:
            for soc, filas in rows_by_sociedad.items():
                # TRUNCATE this sociedad's reclasificaciones and re-insert all
                # This preserves ALL rows including duplicates with same cuenta+fecha+monto
                conn.execute(text(
                    "DELETE FROM flujos_reclasificaciones WHERE sociedad = :soc"
                ), {"soc": soc})

                for params in filas:
                    conn.execute(text("""
                        INSERT INTO flujos_reclasificaciones
                            (sociedad, cuenta, cuenta_nombre, monto, fecha_contable,
                             anio, mes, seccion_origen, nombre_origen,
                             seccion_destino, subseccion, nombre_destino, concepto)
                        VALUES
                            (:sociedad, :cuenta, :cuenta_nombre, :monto, :fecha_contable,
                             :anio, :mes, :seccion_origen, :nombre_origen,
                             :seccion_destino, :subseccion, :nombre_destino, :concepto)
                    """), params)
                    ins += 1

                logger.info(f"[RECLASIF] {soc}: {len(filas)} registros insertados")
    except Exception as e:
        resultado["errores"].append(f"Error en bulk insert: {e}")
        logger.error(f"[RECLASIF] Error: {e}")

    resultado["insertados"] = ins
    resultado["omitidos"]   = omi
    logger.info(f"[RECLASIF] TOTAL insertados={ins} omitidos={omi}")
    return resultado
