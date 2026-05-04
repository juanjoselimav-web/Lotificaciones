"""
sync_flujos.py — v3
Sincroniza FLUJOS DE EFECTIVO.xlsx → tabla flujos_efectivo.

Lógica de neteo (Opción B):
  1. Excluir filas anteriores al mes siguiente de PARTIDA INICIAL (vienen de esa tabla)
  2. Excluir MOVIMIENTO_MANUAL
  3. Eliminar 'Traslado Entre Cuentas' en ambos módulos
  4. Eliminar TRANSFERENCIAS cruzadas (mismo belnr+gjahr en INGRESOS y EGRESOS → neto=0)
  5. Netear 'Ingresos Transitoria' vs 'Ingresos por aplicar Terreno' → Otros Ingresos
  6. Para INGRESOS: solo COBRO_DIRECTO y COBRO_FACTURA (excepto cuentas de neteo)
  7. El signo final de cada fila = RDI.seccion + MODULO:
     - Si MODULO=EGRESOS pero RDI→INGRESOS → reduce ingresos (devolución)
     - Si MODULO=INGRESOS pero RDI→EGRESOS → reduce egresos (recupero)
     La columna monto_ingreso/monto_egreso refleja el neto real según RDI
"""
import os
import re
import datetime
import logging
import math
from typing import Optional

import pandas as pd
from sqlalchemy import text
from app.database import engine

logger = logging.getLogger(__name__)

# ── Rutas ─────────────────────────────────────────────────────────────────────
FLUJOS_PATH = os.environ.get(
    "PATH_FLUJOS",
    r"C:\Users\jlima\OneDrive - rvcuatro.com\Finanzas - Desarrollos - Planificación financiera\19. Lotes\Inventario\2. Tablero Lotificaciones Preliminar (5010)\FLUJOS DE EFECTIVO.xlsx"
)

SOCIEDADES_ACTIVAS = {
    # ── Sociedades existentes ──────────────────────────────
    "EFICIENCIA URBANA":   "EFICIENCIA URBANA",
    "TEZZOLI":             "TEZZOLI",
    "ROSSIO":              "ROSSIO",
    "GARBATELLA":          "GARBATELLA",
    "FRUGALEX":            "FRUGALEX",
    "TALOCCI":             "TALOCCI",
    "GIBRALEON":           "GIBRALEON",
    "CAPIPOS":             "CAPIPOS",
    "OVEST":               "OVEST",
    "CORCOLLE":            "CORCOLLE",
    "LEOFRENI":            "LEOFRENI",
    # ── Nuevas sociedades ─────────────────────────────────
    "SERVICIOS GENERALES": "SER GEN CCC",   # La Ceiba
    "URBIVA":              "URBIVA",         # Club del Bosque
    "UTILICA":             "UTILICA",        # Condado Jutiapa
    "VILET":               "VILET",          # Celajes De Tecpan
    "OTTAVIA":             "OTTAVIA",        # Cañadas de Jalapa
}

# ── Constantes de neteo ───────────────────────────────────────────────────────
CTAS_ELIMINAR          = {"Traslado Entre Cuentas"}
TIPOS_INGRESO_VALIDOS  = {"COBRO_DIRECTO", "COBRO_FACTURA"}
SECCIONES_INGRESO      = {"INGRESOS"}
SECCIONES_EGRESO       = {
    "EGRESOS / URBANIZACION", "EGRESOS / ADMINISTRACION",
    "FINANCIAMIENTO", "TERRENO", "IMPUESTOS"
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe_float(val) -> Optional[float]:
    if val is None: return None
    if isinstance(val, float) and math.isnan(val): return None
    if isinstance(val, (int, float)): return float(val)
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"): return None
    if s.startswith("="):
        nums = re.findall(r"[\d.]+", s)
        if nums: return sum(float(n) for n in nums)
    try:
        r = float(s)
        return None if math.isnan(r) else r
    except (ValueError, TypeError):
        return None

def _safe_int(val) -> Optional[int]:
    if val is None: return None
    if isinstance(val, float) and math.isnan(val): return None
    if isinstance(val, int): return val
    if isinstance(val, float): return int(val)
    try:
        f = float(str(val).strip())
        return None if math.isnan(f) else int(f)
    except (ValueError, TypeError):
        return None

def _clean_str(val) -> Optional[str]:
    if val is None: return None
    if isinstance(val, float):
        if math.isnan(val): return None
        return str(int(val)) if val == int(val) else str(val)
    s = str(val).strip()
    return None if s.lower() in ("nan", "none", "null", "") else s

def _to_date(val) -> Optional[datetime.date]:
    if val is None: return None
    try:
        if pd.isna(val): return None
    except (TypeError, ValueError): pass
    if isinstance(val, pd.Timestamp):
        return val.date() if not pd.isnull(val) else None
    if isinstance(val, datetime.datetime): return val.date()
    if isinstance(val, datetime.date): return val
    return None

def _semana_label(s: int) -> str: return f"S{s}"

# ── Cargar ESTRUCTURA RDI ─────────────────────────────────────────────────────
def _cargar_rdi(xf: pd.ExcelFile) -> dict:
    """Retorna {(sociedad, cuenta, ubicacion): (seccion, nombre)}"""
    df = xf.parse("ESTRUCTURA RDI", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    mapping = {}
    for _, row in df.iterrows():
        soc  = str(row.get("SOCIEDAD", "") or "").strip()
        cta  = str(row.get("CUENTA_CONTRAPARTIDA", "") or "").strip()
        ubr  = str(row.get("UBICACION_CODIGO", "") or "").strip()
        ubic = None if ubr.lower() in ("nan", "none", "null", "") else ubr
        sec  = str(row.get("SECCION", "") or "").strip()
        nom  = str(row.get("NOMBRE", "") or "").strip()
        if soc and cta and sec:
            mapping[(soc, cta, ubic)] = (sec, nom)
    return mapping

def _resolver(mapping, sociedad, cuenta, ubicacion):
    cuenta    = str(cuenta or "").strip()
    ubicacion = _clean_str(ubicacion)
    return (mapping.get((sociedad, cuenta, ubicacion)) or
            mapping.get((sociedad, cuenta, None)))

# ── Cargar PARTIDA INICIAL ────────────────────────────────────────────────────
def _cargar_partida_inicial(xf: pd.ExcelFile) -> tuple:
    """
    Retorna (saldos, movimientos, fecha_inicio_real):
      saldos          → flujos_saldo_inicial
      movimientos     → flujos_efectivo (ingresos/egresos del mes inicial)
      fecha_inicio_real → primer día del mes SIGUIENTE al mes de PARTIDA INICIAL
    """
    df = xf.parse("PARTIDA INICIAL")
    df.columns = [c.strip() for c in df.columns]
    MESES = {"ENERO":1,"FEBRERO":2,"MARZO":3,"ABRIL":4,"MAYO":5,"JUNIO":6,
              "JULIO":7,"AGOSTO":8,"SEPTIEMBRE":9,"OCTUBRE":10,"NOVIEMBRE":11,"DICIEMBRE":12}

    saldos = []
    movimientos = []
    max_anio, max_mes = 0, 0
    max_por_sociedad = {}   # {sociedad: (max_anio, max_mes)}

    for idx, row in df.iterrows():
        soc   = str(row.get("SOCIEDAD","") or "").strip()
        sec   = str(row.get("SECCION","")  or "").strip()
        nom   = str(row.get("NOMBRE","")   or "").strip()
        anio  = _safe_int(row.get("AÑO"))
        mes_s = str(row.get("MES","") or "").strip().upper()
        sem_s = str(row.get("SEMANA","") or "").strip()
        monto = _safe_float(row.get("MONTO"))
        mes   = MESES.get(mes_s)

        sem_m = re.search(r"\d+", sem_s)
        semana_iso = int(sem_m.group()) if sem_m else 1

        if not (soc and anio and mes and monto is not None):
            continue

        # Track latest period global y por sociedad
        if (anio, mes) > (max_anio, max_mes):
            max_anio, max_mes = anio, mes
        prev = max_por_sociedad.get(soc, (0, 0))
        if (anio, mes) > prev:
            max_por_sociedad[soc] = (anio, mes)

        if sec == "SALDO INICIAL":
            saldos.append({
                "sociedad": soc, "anio": anio, "mes": mes,
                "semana_iso": semana_iso, "semana_label": sem_s, "monto": monto,
            })
        else:
            # Movement from initial period → insert into flujos_efectivo
            es_ing = sec == "INGRESOS"
            fecha  = datetime.date(anio, mes, 1)
            movimientos.append({
                "sociedad": soc, "vertical": None,
                "belnr": -(idx + 1), "gjahr": anio, "linea": 0,
                "banco_codigo": None, "banco_nombre": None,
                "fecha_contable": fecha, "anio": anio, "mes": mes,
                "semana_iso": semana_iso, "semana_label": sem_s,
                "cuenta_contrapartida": None, "cuenta_contrapartida_nombre": nom,
                "ubicacion_codigo": None, "ubicacion_nombre": None,
                "seccion": sec, "nombre_categoria": nom,
                "monto_ingreso": monto if es_ing else 0.0,
                "monto_egreso":  0.0   if es_ing else monto,
                "monto_aplicado": None,
                "tipo_transaccion": "PARTIDA_INICIAL",
                "modulo": "INGRESOS" if es_ing else "EGRESOS",
                "cobro_num": None, "cobro_fecha": None,
                "cliente_codigo": None, "cliente_nombre": None, "cobro_comentario": None,
                "pago_num": None, "pago_fecha": None,
                "sn_codigo": None, "sn_nombre": None, "pago_comentario": None,
                "row_num": -(idx + 1),
            })

    # Compute fecha_inicio POR SOCIEDAD (evita que el corte de una sociedad afecte a otra)
    fechas_inicio = {}
    for soc_pi, (a, m) in max_por_sociedad.items():
        if m == 12:
            fechas_inicio[soc_pi] = datetime.date(a + 1, 1, 1)
        else:
            fechas_inicio[soc_pi] = datetime.date(a, m + 1, 1)

    return saldos, movimientos, fechas_inicio

# ── Preprocesar DataFrame ─────────────────────────────────────────────────────
def _preprocesar_df(df: pd.DataFrame, mapping: dict, sociedad: str,
                    fecha_inicio: Optional[datetime.date]) -> list[dict]:
    """
    Aplica todas las reglas y devuelve lista de dicts listos para INSERT.
    Cada dict ya tiene monto_ingreso y monto_egreso calculados según RDI + MODULO.
    """
    df = df.copy()
    df["FECHA_CONTABLE"] = pd.to_datetime(df["FECHA_CONTABLE"], errors="coerce")
    df["LINEA"] = df["LINEA"].fillna(0)

    # 1. Filtrar fechas válidas y rango
    df = df[df["FECHA_CONTABLE"].notna()].copy()
    df = df[df["FECHA_CONTABLE"].dt.year >= 2000].copy()
    if fecha_inicio:
        df = df[df["FECHA_CONTABLE"].dt.date >= fecha_inicio].copy()

    # 2. Eliminar Traslado Entre Cuentas en ambos módulos
    df = df[~df["CUENTA_CONTRAPARTIDA_NOMBRE"].isin(CTAS_ELIMINAR)].copy()

    # 3b. Eliminar TRANSFERENCIAS cruzadas (belnr+gjahr en ambos módulos → neto=0)
    grupos = df.groupby(["BELNR","GJAHR"])["MODULO"].apply(lambda x: set(x.dropna()))
    cross_set = set(map(tuple, grupos[grupos.apply(
        lambda x: "INGRESOS" in x and "EGRESOS" in x)].index.tolist()))
    if cross_set:
        df = df[~df.apply(lambda r: (r["BELNR"], r["GJAHR"]) in cross_set, axis=1)].copy()

    # 7. Build output rows: apply RDI + MODULO sign to determine monto_ingreso/egreso
    rows_out = []
    for row_idx, row in df.iterrows():
        belnr = row.get("BELNR")
        gjahr = row.get("GJAHR")
        linea = row.get("LINEA", 0)
        fc    = _to_date(row.get("FECHA_CONTABLE"))

        if not fc or pd.isna(belnr) or pd.isna(gjahr):
            continue

        cuenta = _clean_str(row.get("CUENTA_CONTRAPARTIDA")) or ""
        ubicod = _clean_str(row.get("UBICACION_CODIGO"))
        cat    = _resolver(mapping, sociedad, cuenta, ubicod)
        seccion = cat[0] if cat else "SIN CLASIFICAR"
        nombre  = cat[1] if cat else (_clean_str(row.get("CUENTA_CONTRAPARTIDA_NOMBRE")) or "SIN CLASIFICAR")

        monto_prorr = _safe_float(row.get("MONTO_PRORRATEADO")) or 0.0
        modulo_val  = _clean_str(row.get("MODULO")) or ""

        # MODULO determines sign: INGRESOS → monto_ingreso, EGRESOS → monto_egreso
        # RDI section determines WHERE it shows in the report (already set above)
        # The router calculates neto = ingreso - egreso per section
        if modulo_val == "INGRESOS":
            monto_ing = monto_prorr
            monto_egr = 0.0
        else:
            monto_ing = 0.0
            monto_egr = monto_prorr

        semana_iso = fc.isocalendar()[1]

        rows_out.append({
            "sociedad":                    sociedad,
            "vertical":                    _clean_str(row.get("VERTICAL")),
            "belnr":                       _safe_int(belnr),
            "gjahr":                       _safe_int(gjahr),
            "linea":                       _safe_int(linea) or 0,
            "banco_codigo":                _clean_str(row.get("BANCO_CODIGO")),
            "banco_nombre":                _clean_str(row.get("BANCO_NOMBRE")),
            "fecha_contable":              fc,
            "anio":                        fc.year,
            "mes":                         fc.month,
            "semana_iso":                  semana_iso,
            "semana_label":                _semana_label(semana_iso),
            "cuenta_contrapartida":        cuenta or None,
            "cuenta_contrapartida_nombre": _clean_str(row.get("CUENTA_CONTRAPARTIDA_NOMBRE")),
            "ubicacion_codigo":            ubicod,
            "ubicacion_nombre":            _clean_str(row.get("UBICACION_NOMBRE")),
            "seccion":                     seccion,
            "nombre_categoria":            nombre,
            "monto_ingreso":               monto_ing,
            "monto_egreso":                monto_egr,
            "monto_aplicado":              _safe_float(row.get("MONTO_APLICADO_FACTURA")),
            "tipo_transaccion":            _clean_str(row.get("TIPO_TRANSACCION")),
            "modulo":                      modulo_val or None,
            "cobro_num":                   _safe_int(row.get("COBRO_NUM")),
            "cobro_fecha":                 _to_date(row.get("COBRO_FECHA")),
            "cliente_codigo":              _clean_str(row.get("CLIENTE_CODIGO")),
            "cliente_nombre":              _clean_str(row.get("CLIENTE_NOMBRE")),
            "cobro_comentario":            _clean_str(row.get("COBRO_COMENTARIO")),
            "pago_num":                    _safe_int(row.get("PAGO_NUM")),
            "pago_fecha":                  _to_date(row.get("PAGO_FECHA")),
            "sn_codigo":                   _clean_str(row.get("SN_CODIGO")),
            "sn_nombre":                   _clean_str(row.get("SN_NOMBRE")),
            "pago_comentario":             _clean_str(row.get("PAGO_COMENTARIO")),
            "row_num":                      int(row_idx) if not isinstance(row_idx, float) else 0,
        })

    return rows_out

# ── Sync principal ────────────────────────────────────────────────────────────
INSERT_SQL = text("""
    INSERT INTO flujos_efectivo (
        sociedad, vertical, belnr, gjahr, linea, row_num,
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
        :sociedad, :vertical, :belnr, :gjahr, :linea, :row_num,
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
    ON CONFLICT (sociedad, belnr, gjahr, linea, cuenta_contrapartida, ubicacion_codigo, row_num) DO NOTHING
""")


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

    # FIX PERMANENTE: Truncar antes de sincronizar para evitar duplicados
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE flujos_efectivo RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE TABLE flujos_saldo_inicial RESTART IDENTITY CASCADE"))
        logger.info("[SYNC FLUJOS] Tablas limpiadas — sync fresco")
    except Exception as e:
        logger.warning(f"[SYNC FLUJOS] No se pudo limpiar tablas: {e}")

    mapping = _cargar_rdi(xf)
    saldos, movs_ini, fechas_inicio = _cargar_partida_inicial(xf)

    # Insertar saldo inicial
    if saldos:
        with engine.begin() as conn:
            for p in saldos:
                conn.execute(text("""
                    INSERT INTO flujos_saldo_inicial
                        (sociedad, anio, mes, semana_iso, semana_label, monto)
                    VALUES (:sociedad, :anio, :mes, :semana_iso, :semana_label, :monto)
                    ON CONFLICT (sociedad, anio, mes, semana_iso) DO NOTHING
                """), p)

    # Insertar movimientos del período inicial (Oct 2023, etc.)
    if movs_ini:
        with engine.begin() as conn:
            for m in movs_ini:
                try:
                    conn.execute(INSERT_SQL, m)
                except Exception as e:
                    logger.warning(f"Movimiento PI omitido: {e}")

    # Procesar cada sociedad
    for hoja, sociedad in SOCIEDADES_ACTIVAS.items():
        if hoja not in xf.sheet_names:
            resultado["errores"].append(f"Hoja '{hoja}' no encontrada")
            continue

        try:
            df_raw = xf.parse(hoja)
            df_raw.columns = [c.strip() for c in df_raw.columns]
            df_raw = df_raw.dropna(how="all")

            fecha_inicio_soc = fechas_inicio.get(sociedad)
            logger.info(f"[{sociedad}] Filas brutas: {len(df_raw)}, fecha_inicio: {fecha_inicio_soc}")

            rows = _preprocesar_df(df_raw, mapping, sociedad, fecha_inicio_soc)
            logger.info(f"[{sociedad}] Filas a insertar tras preprocesado: {len(rows)}")

            ins = 0
            omi = 0

            for row_dict in rows:
                try:
                    with engine.begin() as conn:
                        res = conn.execute(INSERT_SQL, row_dict)
                        if res.rowcount > 0:
                            ins += 1
                        else:
                            omi += 1
                except Exception as row_err:
                    omi += 1
                    logger.warning(f"Fila omitida ({sociedad} belnr={row_dict.get('belnr')}): {row_err}")

            resultado["insertados"]  += ins
            resultado["omitidos"]    += omi
            resultado["sociedades"].append({"sociedad": sociedad, "insertados": ins, "omitidos": omi})
            logger.info(f"[{sociedad}] insertados={ins} omitidos={omi}")

        except Exception as e:
            msg = f"Error procesando hoja '{hoja}': {e}"
            logger.error(msg)
            resultado["errores"].append(msg)

    return resultado
