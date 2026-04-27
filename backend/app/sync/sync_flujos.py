"""
sync_flujos.py
Sincroniza el archivo FLUJOS DE EFECTIVO.xlsx hacia la tabla flujos_efectivo.
Trabaja hoja por hoja (por sociedad). Actualmente activa: EFICIENCIA URBANA.
Ampliar SOCIEDADES_ACTIVAS para habilitar otras sociedades.
"""
import os
import re
import logging
from datetime import datetime, date
from typing import Optional

import pandas as pd
from sqlalchemy import text
from app.database import engine

logger = logging.getLogger(__name__)

# ── Configuración ────────────────────────────────────────────────────────────

FLUJOS_PATH = os.environ.get(
    "PATH_FLUJOS",
    r"C:\Users\jlima\OneDrive - rvcuatro.com\Finanzas - Desarrollos - Planificación financiera\19. Lotes\Inventario\2. Tablero Lotificaciones Preliminar (5010)\FLUJOS DE EFECTIVO.xlsx"
)

# Hoja Excel → nombre sociedad en BD
SOCIEDADES_ACTIVAS = {
    "EFICIENCIA URBANA": "EFICIENCIA URBANA",
    # Descomentar para activar más sociedades:
    # "SERVICIOS GENERALES": "SERVICIOS GENERALES",
    # "ROSSIO":              "ROSSIO",
    # "FRUGALEX":            "FRUGALEX",
    # "NOALLA":              "NOALLA",
    # "VILET":               "VILET",
}

# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe_float(val) -> Optional[float]:
    """Convierte valor de celda a float, resuelve fórmulas simples tipo =A+B.
    Retorna None para NaN, None, vacío o no-numérico."""
    import math
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null", ""):
        return None
    if s.startswith("="):
        nums = re.findall(r"[\d.]+", s)
        if nums:
            return sum(float(n) for n in nums)
    try:
        result = float(s)
        return None if math.isnan(result) else result
    except (ValueError, TypeError):
        return None



def _clean_str(val) -> Optional[str]:
    """Limpia valores string que pandas puede leer como float (ej: 1110200102.0 → '1110200102')."""
    if val is None:
        return None
    import math
    if isinstance(val, float):
        if math.isnan(val):
            return None
        # Si es un entero representado como float, quitar el .0
        if val == int(val):
            return str(int(val))
        return str(val)
    s = str(val).strip()
    return None if s.lower() in ("nan", "none", "null", "") else s


def _safe_int(val) -> Optional[int]:
    """Convierte a int de forma segura, retorna None para NaN/None."""
    import math
    if val is None:
        return None
    if isinstance(val, float):
        if math.isnan(val):
            return None
        return int(val)
    if isinstance(val, int):
        return val
    try:
        f = float(str(val).strip())
        return None if math.isnan(f) else int(f)
    except (ValueError, TypeError):
        return None

def _semana_label(semana_iso: int) -> str:
    return f"S{semana_iso}"


def _to_date(val) -> Optional[date]:
    if val is None:
        return None
    if pd.isna(val) if not isinstance(val, (list, dict)) else False:
        return None
    if isinstance(val, pd.Timestamp):
        return val.date() if not pd.isnull(val) else None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return None


# ── Lectura de ESTRUCTURA RDI ────────────────────────────────────────────────

def _cargar_estructura_rdi(xf: pd.ExcelFile) -> dict:
    """
    Devuelve dict con clave (sociedad, cuenta, ubicacion_codigo) → (seccion, nombre).
    También clave (sociedad, cuenta, None) como fallback.
    """
    df = xf.parse("ESTRUCTURA RDI", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    mapping = {}
    for _, row in df.iterrows():
        soc   = str(row.get("SOCIEDAD", "") or "").strip()
        cta   = str(row.get("CUENTA_CONTRAPARTIDA", "") or "").strip()
        ubic  = str(row.get("UBICACION_CODIGO", "") or "").strip() or None
        sec   = str(row.get("SECCION", "") or "").strip()
        nom   = str(row.get("NOMBRE", "") or "").strip()
        if soc and cta and sec:
            mapping[(soc, cta, ubic)] = (sec, nom)
    return mapping


def _resolver_categoria(mapping: dict, sociedad: str, cuenta: str, ubicacion: Optional[str]):
    """Busca primero con ubicacion, luego sin ella como fallback."""
    cuenta = str(cuenta or "").strip()
    ubicacion = str(ubicacion or "").strip() or None
    resultado = mapping.get((sociedad, cuenta, ubicacion))
    if not resultado:
        resultado = mapping.get((sociedad, cuenta, None))
    return resultado  # (seccion, nombre) o None


# ── Lectura de PARTIDA INICIAL ────────────────────────────────────────────────

def _cargar_partida_inicial(xf: pd.ExcelFile) -> list[dict]:
    """Lee la hoja PARTIDA INICIAL y devuelve lista de dicts para flujos_saldo_inicial."""
    df = xf.parse("PARTIDA INICIAL")
    df.columns = [c.strip() for c in df.columns]
    registros = []
    meses_map = {
        "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4,
        "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8,
        "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12,
    }
    for _, row in df.iterrows():
        if str(row.get("SECCION", "")).strip() != "SALDO INICIAL":
            continue
        soc    = str(row.get("SOCIEDAD", "") or "").strip()
        anio   = row.get("AÑO")
        mes_s  = str(row.get("MES", "") or "").strip().upper()
        sem_s  = str(row.get("SEMANA", "") or "").strip()
        monto  = _safe_float(row.get("MONTO"))
        mes    = meses_map.get(mes_s)
        # Extraer número de semana: "S6" → 6
        sem_match = re.search(r"\d+", sem_s)
        semana_iso = int(sem_match.group()) if sem_match else None
        if soc and anio and mes and monto is not None:
            registros.append({
                "sociedad":     soc,
                "anio":         int(anio),
                "mes":          mes,
                "semana_iso":   semana_iso,
                "semana_label": sem_s,
                "monto":        monto,
            })
    return registros


# ── Sync principal ────────────────────────────────────────────────────────────

def sincronizar_flujos() -> dict:
    resultado = {"insertados": 0, "omitidos": 0, "errores": [], "sociedades": []}

    if not os.path.exists(FLUJOS_PATH):
        msg = f"Archivo no encontrado: {FLUJOS_PATH}"
        logger.error(msg)
        resultado["errores"].append(msg)
        return resultado

    try:
        xf = pd.ExcelFile(FLUJOS_PATH)
    except Exception as e:
        resultado["errores"].append(f"Error abriendo Excel: {e}")
        return resultado

    estructura = _cargar_estructura_rdi(xf)

    # Insertar saldos iniciales (ON CONFLICT DO NOTHING)
    partidas = _cargar_partida_inicial(xf)
    if partidas:
        with engine.begin() as conn:
            for p in partidas:
                conn.execute(text("""
                    INSERT INTO flujos_saldo_inicial
                        (sociedad, anio, mes, semana_iso, semana_label, monto)
                    VALUES
                        (:sociedad, :anio, :mes, :semana_iso, :semana_label, :monto)
                    ON CONFLICT (sociedad, anio, mes, semana_iso) DO NOTHING
                """), p)

    for hoja, sociedad in SOCIEDADES_ACTIVAS.items():
        if hoja not in xf.sheet_names:
            resultado["errores"].append(f"Hoja '{hoja}' no encontrada en Excel")
            continue

        try:
            df = xf.parse(hoja)
            df.columns = [c.strip() for c in df.columns]
            df = df.dropna(how="all")

            ins = 0
            omi = 0

            for _, row in df.iterrows():
                    belnr  = row.get("BELNR")
                    gjahr  = row.get("GJAHR")
                    linea  = row.get("LINEA", 0)
                    fc     = _to_date(row.get("FECHA_CONTABLE"))

                    if not fc or pd.isna(belnr) or pd.isna(gjahr):
                        omi += 1
                        continue

                    cuenta  = _clean_str(row.get("CUENTA_CONTRAPARTIDA")) or ""
                    ubicod  = _clean_str(row.get("UBICACION_CODIGO"))
                    cat     = _resolver_categoria(estructura, sociedad, cuenta, ubicod)
                    seccion = cat[0] if cat else "SIN CLASIFICAR"
                    nombre  = cat[1] if cat else (_clean_str(row.get("CUENTA_CONTRAPARTIDA_NOMBRE")) or "SIN CLASIFICAR")

                    monto_ing = _safe_float(row.get("MONTO_INGRESO_BANCO")) or 0.0
                    monto_egr = _safe_float(row.get("MONTO_EGRESO_BANCO"))  or 0.0
                    monto_apl = _safe_float(row.get("MONTO_APLICADO_FACTURA"))  # puede ser None

                    semana_iso = fc.isocalendar()[1]

                    try:
                        with engine.begin() as conn:
                          res = conn.execute(text("""
                            INSERT INTO flujos_efectivo (
                                sociedad, vertical, belnr, gjahr, linea,
                                banco_codigo, banco_nombre,
                                fecha_contable, anio, mes, semana_iso, semana_label,
                                cuenta_contrapartida, cuenta_contrapartida_nombre,
                                ubicacion_codigo, ubicacion_nombre,
                                seccion, nombre_categoria,
                                monto_ingreso, monto_egreso, monto_aplicado,
                                tipo_transaccion, modulo,
                                cobro_num, cobro_fecha, cliente_codigo, cliente_nombre, cobro_comentario,
                                pago_num, pago_fecha, sn_codigo, sn_nombre, pago_comentario
                            ) VALUES (
                                :sociedad, :vertical, :belnr, :gjahr, :linea,
                                :banco_codigo, :banco_nombre,
                                :fecha_contable, :anio, :mes, :semana_iso, :semana_label,
                                :cuenta_contrapartida, :cuenta_contrapartida_nombre,
                                :ubicacion_codigo, :ubicacion_nombre,
                                :seccion, :nombre_categoria,
                                :monto_ingreso, :monto_egreso, :monto_aplicado,
                                :tipo_transaccion, :modulo,
                                :cobro_num, :cobro_fecha, :cliente_codigo, :cliente_nombre, :cobro_comentario,
                                :pago_num, :pago_fecha, :sn_codigo, :sn_nombre, :pago_comentario
                            )
                            ON CONFLICT (sociedad, belnr, gjahr, linea) DO NOTHING
                        """), {
                            "sociedad":                   sociedad,
                            "vertical":                   _clean_str(row.get("VERTICAL")),
                            "belnr":                      _safe_int(belnr),
                            "gjahr":                      _safe_int(gjahr),
                            "linea":                      _safe_int(linea) or 0,
                            "banco_codigo":               _clean_str(row.get("BANCO_CODIGO")),  # limpia float→str
                            "banco_nombre":               _clean_str(row.get("BANCO_NOMBRE")),
                            "fecha_contable":             fc,
                            "anio":                       fc.year,
                            "mes":                        fc.month,
                            "semana_iso":                 semana_iso,
                            "semana_label":               _semana_label(semana_iso),
                            "cuenta_contrapartida":       _clean_str(cuenta) or None,
                            "cuenta_contrapartida_nombre":_clean_str(row.get("CUENTA_CONTRAPARTIDA_NOMBRE")),
                            "ubicacion_codigo":           _clean_str(ubicod) or None,
                            "ubicacion_nombre":           _clean_str(row.get("UBICACION_NOMBRE")),
                            "seccion":                    seccion,
                            "nombre_categoria":           nombre,
                            "monto_ingreso":              monto_ing,
                            "monto_egreso":               monto_egr,
                            "monto_aplicado":             monto_apl,
                            "tipo_transaccion":           _clean_str(row.get("TIPO_TRANSACCION")),
                            "modulo":                     _clean_str(row.get("MODULO")),
                            "cobro_num":                  _safe_int(row.get("COBRO_NUM")),
                            "cobro_fecha":                _to_date(row.get("COBRO_FECHA")),
                            "cliente_codigo":             _clean_str(row.get("CLIENTE_CODIGO")),
                            "cliente_nombre":             _clean_str(row.get("CLIENTE_NOMBRE")),
                            "cobro_comentario":           _clean_str(row.get("COBRO_COMENTARIO")),
                            "pago_num":                   _safe_int(row.get("PAGO_NUM")),
                            "pago_fecha":                 _to_date(row.get("PAGO_FECHA")),
                            "sn_codigo":                  _clean_str(row.get("SN_CODIGO")),
                            "sn_nombre":                  _clean_str(row.get("SN_NOMBRE")),
                            "pago_comentario":            _clean_str(row.get("PAGO_COMENTARIO")),
                        })
                          ins += res.rowcount
                          if res.rowcount == 0:
                              omi += 1
                    except Exception as row_err:
                        omi += 1
                        logger.warning(f"Fila omitida ({sociedad} belnr={belnr}): {row_err}")

            resultado["insertados"] += ins
            resultado["omitidos"]   += omi
            resultado["sociedades"].append({"sociedad": sociedad, "insertados": ins, "omitidos": omi})
            logger.info(f"[{sociedad}] insertados={ins} omitidos={omi}")

        except Exception as e:
            msg = f"Error procesando hoja '{hoja}': {e}"
            logger.error(msg)
            resultado["errores"].append(msg)

    return resultado
