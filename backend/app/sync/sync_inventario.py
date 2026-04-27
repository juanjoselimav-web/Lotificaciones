"""
sync_inventario.py — Job de sincronización Excel → PostgreSQL

Lógica de fuentes (basada en hoja DETALLE EMPRESAS del archivo):
  - ID 1  SBO_EFICIENCIA_URBANA (Hacienda Jumay):  hoja SBO completa, 667 filas
  - ID 2  SBO_SER_GEN_CCC       (La Ceiba):         hoja SBO completa, 360 filas
  - IDs 3-16: INVENTARIO CONSBA = universo de lotes
              Hojas SBO_xxx existentes = marcan lotes vendidos (cruce)
              Hojas sin hoja propia (LEOFRENI, TALOCCI, VILET) = todo desde CONSBA

  IDs 13, 15, 16 (SBO_LEOFRENI, SBO_TALOCCI, SBO_VILET) no tienen hoja
  en el archivo actualmente — están en migración a SAP.

Fix clave: db.rollback() después de cada error para evitar InFailedSqlTransaction
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

from app.database import SessionLocal
from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# ID 1 y 2 tienen inventario COMPLETO en su hoja SBO (disponibles + vendidos)
PROYECTOS_SBO_COMPLETO = {
    "SBO_EFICIENCIA_URBANA": "SBO_EFICIENCIA_URBANA",  # ID 1 - Hacienda Jumay
    "SBO_SER_GEN_CCC":       "SBO_SER_GEN_CCC",        # ID 2 - La Ceiba
}

# IDs 3-16 con hoja SBO existente en el archivo (solo marcan vendidos, base en CONSBA)
# LEOFRENI (13), TALOCCI (15) y VILET (16) NO tienen hoja aún — se omiten del cruce
PROYECTOS_SBO_PARCIAL = {
    "SBO_ROSSIO":     "SBO_ROSSIO",     # ID 3  - Hacienda el Sol
    "SBO_FRUGALEX":   "SBO_FRUGALEX",   # ID 4  - Oasis Zacapa
    "SBO_OTTAVIA":    "SBO_OTTAVIA",     # ID 5  - Cañadas de Jalapa
    "SBO_UTILICA":    "SBO_UTILICA",     # ID 6  - Condado Jutiapa
    "SBO_TEZZOLI":    "SBO_TEZZOLI",     # ID 7  - Club Campestre Jumay
    "SBO_URBIVA_2":   "SBO_URBIVA_2",    # ID 8  - Club del Bosque
    "SBO_GARBATELLA": "SBO_GARBATELLA",  # ID 9  - Club Residencial Progreso
    "SBO_CAPIPOS":    "SBO_CAPIPOS",     # ID 10 - Arboleda Santa Elena
    "SBO_OVEST":      "SBO_OVEST",       # ID 11 - Hacienda Santa Lucia (hoja vacía aún)
    "SBO_CORCOLLE":   "SBO_CORCOLLE",    # ID 12 - Hacienda El Cafetal Fase I
    "SBO_GIBRALEON":  "SBO_GIBRALEON",   # ID 14 - Hacienda El Cafetal Fase III
    # ID 13 SBO_LEOFRENI  — sin hoja en el archivo, en migración SAP
    # ID 15 SBO_TALOCCI   — sin hoja en el archivo, en migración SAP
    # ID 16 SBO_VILET     — sin hoja en el archivo, en migración SAP
}


def clean_val(val):
    if val is None: return None
    if isinstance(val, float) and np.isnan(val): return None
    try:
        if pd.isna(val): return None
    except Exception:
        pass
    return val

def clean_date(val):
    val = clean_val(val)
    if val is None: return None
    if isinstance(val, (datetime, pd.Timestamp)):
        d = val.date()
        # Fechas inválidas (año 1900) → None
        if d.year < 1950: return None
        return d
    if isinstance(val, date):
        if val.year < 1950: return None
        return val
    return None

def clean_decimal(val, default=0):
    val = clean_val(val)
    if val is None: return default
    try:
        f = float(val)
        return f if not np.isnan(f) else default
    except Exception:
        return default

def clean_int(val, default=None):
    val = clean_val(val)
    if val is None: return default
    try: return int(float(val))
    except Exception: return default

def clean_str(val):
    val = clean_val(val)
    if val is None: return None
    s = str(val).strip()
    return s if s else None

def normalizar_estatus(raw: str) -> str:
    if not raw: return "DISPONIBLE"
    upper = raw.strip().upper()
    mapping = {
        "DISPONIBLE": "DISPONIBLE", "DISPONIBLE ": "DISPONIBLE",
        "VENTA": "VENTA", " VENTA": "VENTA", "VENDIDO": "VENTA",
        "RESERVADO": "RESERVADO",
        "BLOQUEADO": "BLOQUEADO", "BLOQUEADA": "BLOQUEADO",
        "CANJE A": "CANJE", "CANJEA": "CANJE", "CANJE ": "CANJE", "CANJE": "CANJE",
        "VENTA ADMON": "VENTA_ADMINISTRATIVA", "VENTA ADMI": "VENTA_ADMINISTRATIVA",
        "VENTA ADMINISTRATIVA": "VENTA_ADMINISTRATIVA",
    }
    return mapping.get(upper, "DISPONIBLE")

def get_precio_consba(row) -> float:
    """Usa siempre Precio Final. Si está en 0 es un problema de datos a corregir en la fuente."""
    return clean_decimal(row.get("Precio Final"), 0)

def get_proyecto_id(db: Session, empresa_sap: str):
    result = db.execute(
        text("SELECT id FROM proyectos WHERE empresa_sap = :emp"),
        {"emp": empresa_sap}
    ).fetchone()
    return result[0] if result else None

def safe_upsert(db: Session, proyecto_id: int, unidad_key: str, data: dict) -> str:
    """
    Hace INSERT o UPDATE con rollback automático en caso de error.
    Retorna 'insert', 'update' o 'error'.
    """
    try:
        existing = db.execute(
            text("SELECT id FROM lotes WHERE proyecto_id=:pid AND unidad_key=:uk"),
            {"pid": proyecto_id, "uk": unidad_key}
        ).fetchone()

        if existing:
            db.execute(text("""
                UPDATE lotes SET
                    unidad_actual=:unidad_actual, manzana=:manzana,
                    metraje_inventario=:metraje_inventario, metraje_orden=:metraje_orden,
                    precio_sin_descuento=:precio_sin_descuento, descuento=:descuento,
                    precio_con_descuento=:precio_con_descuento, precio_final=:precio_final,
                    precio_base_m2=:precio_base_m2, precio_esquina=:precio_esquina,
                    es_esquina=:es_esquina, estatus=:estatus, estatus_raw=:estatus_raw,
                    status_promesa_compraventa=:status_promesa_compraventa,
                    card_code=:card_code, card_name=:card_name,
                    telefono_cliente=:telefono_cliente, vendedor=:vendedor,
                    forma_pago=:forma_pago, plazo=:plazo,
                    fecha_venta=:fecha_venta, fecha_inicial_cobro=:fecha_inicial_cobro,
                    fecha_final_cobro=:fecha_final_cobro, fecha_solicitud_pcv=:fecha_solicitud_pcv,
                    pagado_capital=:pagado_capital, pagado_interes=:pagado_interes,
                    pendiente_capital=:pendiente_capital, pendiente_interes=:pendiente_interes,
                    cuotas_pagadas=:cuotas_pagadas, cuotas_pendientes=:cuotas_pendientes,
                    saldo_cliente=:saldo_cliente, total_intereses=:total_intereses, fuente=:fuente, updated_at=NOW()
                WHERE proyecto_id=:proyecto_id AND unidad_key=:unidad_key
            """), {**data, "proyecto_id": proyecto_id, "unidad_key": unidad_key})
            return "update"
        else:
            db.execute(text("""
                INSERT INTO lotes (
                    proyecto_id, unidad_key, unidad_actual, manzana,
                    metraje_inventario, metraje_orden,
                    precio_sin_descuento, descuento, precio_con_descuento, precio_final,
                    precio_base_m2, precio_esquina, es_esquina,
                    estatus, estatus_raw, status_promesa_compraventa,
                    card_code, card_name, telefono_cliente, vendedor,
                    forma_pago, plazo, fecha_venta, fecha_inicial_cobro, fecha_final_cobro,
                    fecha_solicitud_pcv, pagado_capital, pagado_interes,
                    pendiente_capital, pendiente_interes, cuotas_pagadas, cuotas_pendientes,
                    saldo_cliente, total_intereses, fuente
                ) VALUES (
                    :proyecto_id, :unidad_key, :unidad_actual, :manzana,
                    :metraje_inventario, :metraje_orden,
                    :precio_sin_descuento, :descuento, :precio_con_descuento, :precio_final,
                    :precio_base_m2, :precio_esquina, :es_esquina,
                    :estatus, :estatus_raw, :status_promesa_compraventa,
                    :card_code, :card_name, :telefono_cliente, :vendedor,
                    :forma_pago, :plazo, :fecha_venta, :fecha_inicial_cobro, :fecha_final_cobro,
                    :fecha_solicitud_pcv, :pagado_capital, :pagado_interes,
                    :pendiente_capital, :pendiente_interes, :cuotas_pagadas, :cuotas_pendientes,
                    :saldo_cliente, :total_intereses, :fuente
                )
            """), {**data, "proyecto_id": proyecto_id, "unidad_key": unidad_key})
            return "insert"

    except Exception as e:
        db.rollback()  # ← CRÍTICO: resetea la transacción fallida
        # Log detallado para diagnóstico — muestra los valores que causaron el error
        numeric_vals = {k: v for k, v in data.items() if isinstance(v, (int, float)) and v is not None}
        logger.error(f"[SYNC] Error upsert {unidad_key}: {str(e)[:200]}")
        logger.error(f"[SYNC] Valores numéricos de {unidad_key}: {numeric_vals}")
        return "error"


def build_from_sbo(row, empresa_sap: str) -> dict:
    """Construye dict de lote desde fila de hoja SBO completa."""
    doc_num = clean_int(row.get("DocNum"))
    if doc_num == 0: doc_num = None

    estatus_raw = clean_str(row.get("Status de venta")) or "DISPONIBLE"
    estatus = normalizar_estatus(estatus_raw)

    precio_sd = clean_decimal(row.get("Precio Sin Descuento"), 0)
    precio_cd = clean_decimal(row.get("Precio con Descuento"), 0)
    precio_final = precio_cd if precio_cd > 0 else precio_sd

    return {
        "unidad_actual":              clean_str(row.get("Lote")),
        "manzana":                    clean_str(row.get("Manzana")),
        "metraje_inventario":         clean_decimal(row.get("Metraje Inventario"), None),
        "metraje_orden":              clean_decimal(row.get("Metraje Orden"), None),
        "precio_sin_descuento":       precio_sd,
        "descuento":                  clean_decimal(row.get("Descuento"), 0),
        "precio_con_descuento":       precio_cd,
        "precio_final":               precio_final,
        "precio_base_m2":             None,
        "precio_esquina":             0,
        "es_esquina":                 False,
        "estatus":                    estatus,
        "estatus_raw":                estatus_raw,
        "status_promesa_compraventa": clean_str(row.get("Status Promesa Compraventa")),
        "card_code":                  clean_str(row.get("CardCode")),
        "card_name":                  clean_str(row.get("CardName")),
        "telefono_cliente":           clean_str(row.get("Telefono Cliente")),
        "vendedor":                   clean_str(row.get("Vendedor")),
        "forma_pago":                 clean_str(row.get("U_Formapago")),
        "plazo":                      clean_int(row.get("Plazo"), None),
        "fecha_venta":                clean_date(row.get("Fecha de Venta")),
        "fecha_inicial_cobro":        clean_date(row.get("Fecha Inicial de Cobro")),
        "fecha_final_cobro":          clean_date(row.get("Fecha Final de Cobro")),
        "fecha_solicitud_pcv":        clean_date(row.get("Fecha de Solicitud PCV")),
        "pagado_capital":             clean_decimal(row.get("PagadoCapital"), 0),
        "pagado_interes":             clean_decimal(row.get("PagadoInteres"), 0),
        "pendiente_capital":          clean_decimal(row.get("PendienteCapital"), 0),
        "pendiente_interes":          clean_decimal(row.get("PendienteInteres"), 0),
        "cuotas_pagadas":             clean_int(row.get("No. Cuotas Pagadas"), 0),
        "cuotas_pendientes":          clean_int(row.get("No. Cuotas Pendientes"), 0),
        "saldo_cliente":              clean_decimal(row.get("Saldo Cliente"), 0),
        "total_intereses":            clean_decimal(row.get("Total Intereses"), 0),
        "fuente":                     empresa_sap,
    }


def sync_inventario():
    archivo = "INVENTARIO"
    path = Path(settings.path_inventario)
    db: Session = SessionLocal()
    log_id = None

    try:
        result = db.execute(
            text("INSERT INTO sync_log (archivo, inicio, estado) VALUES (:a, NOW(), 'EJECUTANDO') RETURNING id"),
            {"a": archivo}
        )
        db.commit()
        log_id = result.fetchone()[0]

        if not path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {path}")

        logger.info(f"[SYNC] Iniciando sincronización: {path.name}")
        xl = pd.ExcelFile(path)
        insertados = actualizados = errores = total_leidos = 0

        # ══════════════════════════════════════════════════════════════
        # PASO 1: Proyectos con SBO COMPLETO (Hacienda Jumay + La Ceiba)
        # ══════════════════════════════════════════════════════════════
        for sheet_name, empresa_sap in PROYECTOS_SBO_COMPLETO.items():
            if sheet_name not in xl.sheet_names:
                logger.warning(f"[SYNC] Hoja no encontrada: {sheet_name}")
                continue

            df = pd.read_excel(path, sheet_name=sheet_name)
            if df.empty:
                logger.warning(f"[SYNC] Hoja vacía: {sheet_name}")
                continue

            proyecto_id = get_proyecto_id(db, empresa_sap)
            if not proyecto_id:
                logger.warning(f"[SYNC] Proyecto no en BD: {empresa_sap}")
                continue

            logger.info(f"[SYNC] {sheet_name}: {len(df)} lotes (fuente completa)")
            total_leidos += len(df)

            for _, row in df.iterrows():
                lote_key = clean_str(row.get("Lote"))
                if not lote_key:
                    continue
                data = build_from_sbo(row, empresa_sap)
                result = safe_upsert(db, proyecto_id, lote_key, data)
                if result == "insert": insertados += 1
                elif result == "update": actualizados += 1
                else: errores += 1

            db.commit()
            logger.info(f"[SYNC] {sheet_name} completado")

        # ══════════════════════════════════════════════════════════════
        # PASO 2: Demás proyectos — CONSBA + cruce SBO
        # ══════════════════════════════════════════════════════════════
        if "INVENTARIO CONSBA" not in xl.sheet_names:
            logger.warning("[SYNC] Hoja INVENTARIO CONSBA no encontrada")
        else:
            df_consba = pd.read_excel(path, sheet_name="INVENTARIO CONSBA", dtype=str)
            df_empresas = pd.read_excel(path, sheet_name="DETALLE EMPRESAS")
            total_leidos += len(df_consba)

            # Mapa nombre_proyecto → {id, empresa} — solo proyectos NO completos
            proyecto_map = {}
            for _, row in df_empresas.iterrows():
                nombre = str(row.get("Nombre del proyecto", "")).strip()
                emp = str(row.get("Empresa SAP", "")).strip()
                if emp not in PROYECTOS_SBO_COMPLETO:
                    pid = get_proyecto_id(db, emp)
                    if pid:
                        proyecto_map[nombre] = {"id": pid, "empresa": emp}

            # Pre-cargar ventas desde hojas SBO parciales
            # clave: (empresa_sap, unidad_key_consba) → row completa
            # El campo 'Manzana' en SBO tiene formato 'Lote 5 Manzana J' o 'Manzana J Lote 5'
            # El campo 'Unidad_Key' en CONSBA tiene formato 'J5'
            # Necesitamos convertir el formato SBO al formato CONSBA para el cruce
            ventas_sbo = {}
            for sheet_name, empresa_sap in PROYECTOS_SBO_PARCIAL.items():
                if sheet_name not in xl.sheet_names:
                    continue
                df_sbo = pd.read_excel(path, sheet_name=sheet_name)
                if df_sbo.empty:
                    continue
                for _, row in df_sbo.iterrows():
                    manzana_raw = clean_str(row.get("Manzana"))
                    if not manzana_raw:
                        continue
                    estatus_sbo = normalizar_estatus(clean_str(row.get("Status de venta")) or "")
                    if estatus_sbo == "DISPONIBLE":
                        continue
                    # Convertir formato SBO a formato CONSBA
                    # 'Lote 5 Manzana J' → 'J5'
                    # 'Manzana J Lote 5' → 'J5'
                    import re
                    m1 = re.search(r'[Ll]ote\s+(\d+)\s+[Mm]anzana\s+([A-Z])', manzana_raw)
                    m2 = re.search(r'[Mm]anzana\s+([A-Z])\s+[Ll]ote\s+(\d+)', manzana_raw)
                    if m1:
                        consba_key = f"{m1.group(2)}{m1.group(1)}"
                    elif m2:
                        consba_key = f"{m2.group(1)}{m2.group(2)}"
                    else:
                        continue
                    ventas_sbo[(empresa_sap, consba_key)] = row

            logger.info(f"[SYNC] {len(ventas_sbo)} lotes vendidos cargados de hojas SBO parciales")

            batch_count = 0
            for _, row in df_consba.iterrows():
                nombre_proyecto = clean_str(row.get("Nombre del proyecto"))
                unidad_key = clean_str(row.get("Unidad_Key"))
                if not unidad_key or not nombre_proyecto:
                    continue

                pinfo = proyecto_map.get(nombre_proyecto)
                if not pinfo:
                    continue

                proyecto_id = pinfo["id"]
                empresa_sap = pinfo["empresa"]

                venta_row = ventas_sbo.get((empresa_sap, unidad_key))

                if venta_row is not None:
                    # Tiene venta en SBO — usar datos de SBO
                    data = build_from_sbo(venta_row, empresa_sap)
                    # Si precio viene en 0 de SBO, complementar con CONSBA
                    if data["precio_final"] == 0:
                        data["precio_final"] = get_precio_consba(row)
                        data["precio_sin_descuento"] = data["precio_final"]
                        data["precio_con_descuento"] = data["precio_final"]
                else:
                    # No está en SBO → disponible, usar CONSBA
                    estatus_raw = clean_str(row.get("Estatus")) or "DISPONIBLE"
                    estatus = normalizar_estatus(estatus_raw)
                    # Durante migración: si CONSBA dice vendido pero no está en SBO → disponible
                    if estatus == "VENTA":
                        estatus = "DISPONIBLE"

                    precio = get_precio_consba(row)

                    data = {
                        "unidad_actual":              clean_str(row.get("Unidad Actual ")),
                        "manzana":                    clean_str(row.get("Proyecto_TAB")),
                        "metraje_inventario":         clean_decimal(row.get("Medidas"), None),
                        "metraje_orden":              None,
                        "precio_sin_descuento":       precio,
                        "descuento":                  0,
                        "precio_con_descuento":       precio,
                        "precio_final":               precio,
                        "precio_base_m2":             clean_decimal(row.get("Precio base m2"), None),
                        "precio_esquina":             clean_decimal(row.get("Precio de Esquina"), 0),
                        "es_esquina":                 (clean_str(row.get("Esquina")) or "").upper() == "SI",
                        "estatus":                    estatus,
                        "estatus_raw":                estatus_raw,
                        "status_promesa_compraventa": clean_str(row.get("Estatus de PCV¨S")),
                        "card_code":                  None,
                        "card_name":                  clean_str(row.get("Cliente")),
                        "telefono_cliente":           None,
                        "vendedor":                   clean_str(row.get("Nombre del vendedor")),
                        "forma_pago":                 clean_str(row.get("Plan De Financiamiento ")),
                        "plazo":                      None,
                        "fecha_venta":                clean_date(row.get("Fecha de venta")),
                        "fecha_inicial_cobro":        None,
                        "fecha_final_cobro":          None,
                        "fecha_solicitud_pcv":        clean_date(row.get("Fecha de PCV")),
                        "pagado_capital":             0,
                        "pagado_interes":             0,
                        "pendiente_capital":          0,
                        "pendiente_interes":          0,
                        "cuotas_pagadas":             0,
                        "cuotas_pendientes":          0,
                        "saldo_cliente":              0,
                        "total_intereses":            0,
                        "fuente":                     "CONSBA",
                    }

                result = safe_upsert(db, proyecto_id, unidad_key, data)
                if result == "insert": insertados += 1
                elif result == "update": actualizados += 1
                else: errores += 1

                batch_count += 1
                if batch_count % 200 == 0:
                    db.commit()
                    logger.info(f"[SYNC] Procesados {batch_count} lotes CONSBA...")

            db.commit()

        logger.info(f"[SYNC] ✅ Fin: {insertados} insertados, {actualizados} actualizados, {errores} errores")

        db.execute(text("""
            UPDATE sync_log SET fin=NOW(), estado='EXITOSO',
            registros_leidos=:l, registros_insertados=:i,
            registros_actualizados=:a, registros_error=:e
            WHERE id=:id
        """), {"l": total_leidos, "i": insertados, "a": actualizados, "e": errores, "id": log_id})
        db.commit()
        return {"status": "ok", "insertados": insertados, "actualizados": actualizados, "errores": errores}

    except Exception as e:
        logger.error(f"[SYNC] ❌ Error crítico: {e}")
        db.rollback()
        if log_id:
            try:
                db.execute(text("UPDATE sync_log SET fin=NOW(), estado='ERROR', mensaje_error=:m WHERE id=:id"),
                           {"m": str(e)[:500], "id": log_id})
                db.commit()
            except Exception:
                pass
        raise
    finally:
        db.close()
