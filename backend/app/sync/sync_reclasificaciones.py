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

    ins = 0
    omi = 0

    for _, row in df.iterrows():
        soc = _clean(row.get("SOCIEDAD"))
        if not soc or soc not in SOCIEDADES_ACTIVAS:
            omi += 1
            continue

        fecha = _to_date(row.get("FECHA_CONTABLE"))
        monto = _sf(row.get("MONTO_PRORRATEADO"))
        sec_ori = _clean(row.get("SECCION"))
        sec_dst = _clean(row.get("SECCION4"))

        if not fecha or not sec_ori or not sec_dst or monto == 0:
            omi += 1
            continue

        try:
            with engine.begin() as conn:
                res = conn.execute(text("""
                    INSERT INTO flujos_reclasificaciones
                        (sociedad, cuenta, cuenta_nombre, monto, fecha_contable,
                         anio, mes, seccion_origen, nombre_origen,
                         seccion_destino, nombre_destino, concepto)
                    VALUES
                        (:sociedad, :cuenta, :cuenta_nombre, :monto, :fecha_contable,
                         :anio, :mes, :seccion_origen, :nombre_origen,
                         :seccion_destino, :nombre_destino, :concepto)
                    ON CONFLICT (sociedad, cuenta, fecha_contable, seccion_origen, seccion_destino, monto)
                    DO NOTHING
                """), {
                    "sociedad":      soc,
                    "cuenta":        _clean(row.get("CUENTA_CONTRAPARTIDA")),
                    "cuenta_nombre": _clean(row.get("CUENTA_CONTRAPARTIDA_NOMBRE")),
                    "monto":         monto,
                    "fecha_contable": fecha,
                    "anio":          fecha.year,
                    "mes":           fecha.month,
                    "seccion_origen":  sec_ori,
                    "nombre_origen":   _clean(row.get("NOMBRE")),
                    "seccion_destino": sec_dst,
                    "nombre_destino":  _clean(row.get("NOMBRE5")),
                    "concepto":        _clean(row.get("CONCEPTO")),
                })
                if res.rowcount > 0:
                    ins += 1
                else:
                    omi += 1
        except Exception as e:
            omi += 1
            logger.warning(f"Reclasificación omitida: {e}")

    resultado["insertados"] = ins
    resultado["omitidos"]   = omi
    logger.info(f"[RECLASIF] insertados={ins} omitidos={omi}")
    return resultado
