"""
sync_cartera.py — Sincronización OV (CARTERA) y DESISTIMIENTOS → PostgreSQL
89,931 líneas — usa UPSERT por (empresa, doc_entry, line_num)
Tipo N = se carga pero excluido de KPIs y vistas (filtrado en SQL)
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
        return d if d.year >= 1950 else None
    if isinstance(val, date):
        return val if val.year >= 1950 else None
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

def clean_str(val, max_len=None):
    val = clean_val(val)
    if val is None: return None
    s = str(val).strip()
    if not s: return None
    if max_len: s = s[:max_len]
    return s


def sync_cartera():
    """Sincroniza OV_CARTERA y DESISTIMIENTOS desde el archivo Excel."""
    archivo = "OV_CARTERA"
    path = Path(settings.path_ov_cartera)
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

        # FIX PERMANENTE: Truncar antes de re-sincronizar para garantizar datos frescos
        db.execute(text("TRUNCATE TABLE ov_cartera RESTART IDENTITY CASCADE"))
        db.execute(text("TRUNCATE TABLE desistimientos RESTART IDENTITY CASCADE"))
        db.commit()
        import logging; logging.getLogger(__name__).info("[SYNC CARTERA] Tablas limpiadas — sync fresco")

        logger.info(f"[SYNC CARTERA] Leyendo: {path.name}")

        # ── OV CARTERA ──────────────────────────────────────────────────
        df = pd.read_excel(path, sheet_name="CONSOLIDADO OV (CARTERA)")
        df['FechaProgramadaCobro'] = pd.to_datetime(df['FechaProgramadaCobro'], errors='coerce')
        df['DocDate']              = pd.to_datetime(df['DocDate'], errors='coerce')
        df['TaxDate']              = pd.to_datetime(df['TaxDate'], errors='coerce')
        df['FechaVentaLote']       = pd.to_datetime(df['FechaVentaLote'], errors='coerce')

        total_leidos = len(df)
        insertados = actualizados = errores = 0

        logger.info(f"[SYNC CARTERA] {total_leidos} líneas de cartera a procesar")

        batch = []
        BATCH_SIZE = 500

        for _, row in df.iterrows():
            empresa   = clean_str(row.get("Empresa"), 100)
            doc_entry = clean_int(row.get("DocEntry"))
            line_num  = clean_int(row.get("LineNum"))

            if not empresa or doc_entry is None or line_num is None:
                errores += 1
                continue

            batch.append({
                "empresa":                empresa,
                "doc_entry":              doc_entry,
                "doc_num":                clean_int(row.get("DocNum")),
                "doc_date":               clean_date(row.get("DocDate")),
                "tax_date":               clean_date(row.get("TaxDate")),
                "card_code":              clean_str(row.get("CardCode"), 50),
                "card_name":              clean_str(row.get("CardName"), 200),
                "slp_code":               clean_int(row.get("SlpCode")),
                "slp_name":               clean_str(row.get("SlpName"), 150),
                "referencia_manzana_lote": clean_str(row.get("Referencia_ManzanaLote"), 200),
                "codigo_lote":            clean_str(row.get("CodigoLote"), 50),
                "fecha_venta_lote":       clean_date(row.get("FechaVentaLote")),
                "plazo":                  clean_str(row.get("Plazo"), 20),
                "forma_pago":             clean_str(row.get("FormaPago"), 50),
                "status_ov":              clean_str(row.get("StatusOV"), 50),
                "line_num":               line_num,
                "item_code":              clean_str(row.get("ItemCode"), 50),
                "descripcion":            clean_str(row.get("Dscription"), 100),
                "quantity":               clean_int(row.get("Quantity"), 1),
                "price":                  clean_decimal(row.get("Price"), 0),
                "disc_prcnt":             clean_decimal(row.get("DiscPrcnt"), 0),
                "line_total":             clean_decimal(row.get("LineTotal"), 0),
                "g_total":                clean_decimal(row.get("GTotal"), 0),
                "fecha_programada_cobro": clean_date(row.get("FechaProgramadaCobro")),
                "line_status":            clean_str(row.get("LineStatus"), 5),
                "tipo_linea":             clean_str(row.get("TipoLinea"), 5),
                "saldo_pendiente":        clean_decimal(row.get("SaldoPendiente"), 0),
            })

            if len(batch) >= BATCH_SIZE:
                i, a, e = _upsert_batch(db, batch)
                insertados += i; actualizados += a; errores += e
                batch = []

        if batch:
            i, a, e = _upsert_batch(db, batch)
            insertados += i; actualizados += a; errores += e

        db.commit()
        logger.info(f"[SYNC CARTERA] OV: {insertados} ins, {actualizados} upd, {errores} err")

        # ── DESISTIMIENTOS ───────────────────────────────────────────────
        df_d = pd.read_excel(path, sheet_name="DESISTIMIENTOS")
        desist_ins = desist_upd = 0

        for _, row in df_d.iterrows():
            try:
                data = {
                    "empresa":                    clean_str(row.get("Empresa"), 100),
                    "no_orden_venta":             clean_int(row.get("No. OrdenVenta")),
                    "codigo_cliente":             clean_str(row.get("CodigoCliente"), 50),
                    "nombre_cliente":             clean_str(row.get("NombreCliente"), 200),
                    "lote":                       clean_str(row.get("Lote"), 200),
                    "media_orden":                clean_str(row.get("Media Orden"), 50),
                    "metraje_orden":              clean_decimal(row.get("Metraje Orden"), None),
                    "asesor_venta":               clean_str(row.get("Asesor de Venta"), 150),
                    "status_informe_ponf":        clean_str(row.get("Status Informe PONF"), 100),
                    "status_promesa_compraventa": clean_str(row.get("Status Promesa Compraventa"), 100),
                    "fecha_solicitud_pcv":        clean_date(row.get("Fecha de Solicitud PCV")),
                    "fecha_venta":                clean_date(row.get("Fechaventa")),
                    "fecha_inicio_cobro":         clean_date(row.get("Fechainiciocobro")),
                    "fecha_desistimiento":        clean_date(row.get("FechaDesistimiento")),
                    "plazo":                      clean_str(row.get("Plazo"), 20),
                    "precio_venta":               clean_decimal(row.get("Precioventa"), 0),
                    "descuento":                  clean_decimal(row.get("Descuento"), 0),
                    "precio_con_descuento":       clean_decimal(row.get("Precio con Descuento"), 0),
                    "valor_cuota_anticipo":       clean_decimal(row.get("ValorCuotaAnticipo"), 0),
                    "valor_cuota_gastos_admin":   clean_decimal(row.get("ValorCuotaGastosAdmin"), 0),
                    "pendiente_tramite_anticipo": clean_decimal(row.get("PendienteTramiteAnticipo"), 0),
                    "pendiente_tramite_gastos":   clean_decimal(row.get("PendienteTramiteGastosAdmin"), 0),
                    "cuotas_pagadas":             clean_int(row.get("CuotasPagadas"), 0),
                    "motivo_desistimiento":       clean_str(row.get("Motivo Desistimiento")),
                    "pagado_capital":             clean_decimal(row.get("Pagado Anticipo/ Capital"), 0),
                    "pagado_gastos_admin":        clean_decimal(row.get("Pagado GastosAdmin"), 0),
                    "retenido_facturado":         clean_decimal(row.get("RetenidoFacturado"), 0),
                    "reintegrado_cliente":        clean_decimal(row.get("ReintegradoCliente"), 0),
                    "total_desistimiento":        clean_decimal(row.get("TotalDesistimiento"), 0),
                    "no_cheque":                  clean_str(row.get("No. Cheque"), 50),
                }
                if not data["empresa"] or data["no_orden_venta"] is None:
                    continue

                existing = db.execute(
                    text("SELECT id FROM desistimientos WHERE empresa=:e AND no_orden_venta=:n AND lote=:l"),
                    {"e": data["empresa"], "n": data["no_orden_venta"], "l": data["lote"] or ""}
                ).fetchone()

                if existing:
                    db.execute(text("""
                        UPDATE desistimientos SET
                            nombre_cliente=:nombre_cliente, codigo_cliente=:codigo_cliente,
                            fecha_desistimiento=:fecha_desistimiento, precio_venta=:precio_venta,
                            pagado_capital=:pagado_capital, reintegrado_cliente=:reintegrado_cliente,
                            total_desistimiento=:total_desistimiento, updated_at=NOW()
                        WHERE empresa=:empresa AND no_orden_venta=:no_orden_venta AND lote=:lote
                    """), data)
                    desist_upd += 1
                else:
                    db.execute(text("""
                        INSERT INTO desistimientos (
                            empresa, no_orden_venta, codigo_cliente, nombre_cliente, lote,
                            media_orden, metraje_orden, asesor_venta, status_informe_ponf,
                            status_promesa_compraventa, fecha_solicitud_pcv, fecha_venta,
                            fecha_inicio_cobro, fecha_desistimiento, plazo, precio_venta,
                            descuento, precio_con_descuento, valor_cuota_anticipo,
                            valor_cuota_gastos_admin, pendiente_tramite_anticipo,
                            pendiente_tramite_gastos, cuotas_pagadas, motivo_desistimiento,
                            pagado_capital, pagado_gastos_admin, retenido_facturado,
                            reintegrado_cliente, total_desistimiento, no_cheque
                        ) VALUES (
                            :empresa, :no_orden_venta, :codigo_cliente, :nombre_cliente, :lote,
                            :media_orden, :metraje_orden, :asesor_venta, :status_informe_ponf,
                            :status_promesa_compraventa, :fecha_solicitud_pcv, :fecha_venta,
                            :fecha_inicio_cobro, :fecha_desistimiento, :plazo, :precio_venta,
                            :descuento, :precio_con_descuento, :valor_cuota_anticipo,
                            :valor_cuota_gastos_admin, :pendiente_tramite_anticipo,
                            :pendiente_tramite_gastos, :cuotas_pagadas, :motivo_desistimiento,
                            :pagado_capital, :pagado_gastos_admin, :retenido_facturado,
                            :reintegrado_cliente, :total_desistimiento, :no_cheque
                        )
                    """), data)
                    desist_ins += 1
            except Exception as ex:
                db.rollback()
                logger.error(f"[SYNC DESIST] Error: {ex}")

        db.commit()
        logger.info(f"[SYNC CARTERA] Desistimientos: {desist_ins} ins, {desist_upd} upd")

        # Update sync log
        db.execute(text("""
            UPDATE sync_log SET fin=NOW(), estado='EXITOSO',
            registros_leidos=:l, registros_insertados=:i,
            registros_actualizados=:a, registros_error=:e
            WHERE id=:id
        """), {"l": total_leidos, "i": insertados+desist_ins,
               "a": actualizados+desist_upd, "e": errores, "id": log_id})
        db.commit()
        return {"status": "ok", "insertados": insertados, "actualizados": actualizados}

    except Exception as e:
        logger.error(f"[SYNC CARTERA] ❌ Error crítico: {e}")
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


def _upsert_batch(db, batch):
    """Inserta/actualiza un batch de registros de cartera."""
    ins = upd = err = 0
    for data in batch:
        try:
            existing = db.execute(
                text("SELECT id FROM ov_cartera WHERE empresa=:empresa AND doc_entry=:doc_entry AND line_num=:line_num"),
                {"empresa": data["empresa"], "doc_entry": data["doc_entry"], "line_num": data["line_num"]}
            ).fetchone()

            if existing:
                db.execute(text("""
                    UPDATE ov_cartera SET
                        line_status=:line_status, saldo_pendiente=:saldo_pendiente,
                        tipo_linea=:tipo_linea, card_name=:card_name,
                        slp_name=:slp_name, updated_at=NOW()
                    WHERE empresa=:empresa AND doc_entry=:doc_entry AND line_num=:line_num
                """), data)
                upd += 1
            else:
                db.execute(text("""
                    INSERT INTO ov_cartera (
                        empresa, doc_entry, doc_num, doc_date, tax_date,
                        card_code, card_name, slp_code, slp_name,
                        referencia_manzana_lote, codigo_lote, fecha_venta_lote,
                        plazo, forma_pago, status_ov, line_num, item_code,
                        descripcion, quantity, price, disc_prcnt,
                        line_total, g_total, fecha_programada_cobro,
                        line_status, tipo_linea, saldo_pendiente
                    ) VALUES (
                        :empresa, :doc_entry, :doc_num, :doc_date, :tax_date,
                        :card_code, :card_name, :slp_code, :slp_name,
                        :referencia_manzana_lote, :codigo_lote, :fecha_venta_lote,
                        :plazo, :forma_pago, :status_ov, :line_num, :item_code,
                        :descripcion, :quantity, :price, :disc_prcnt,
                        :line_total, :g_total, :fecha_programada_cobro,
                        :line_status, :tipo_linea, :saldo_pendiente
                    )
                """), data)
                ins += 1
        except Exception as ex:
            db.rollback()
            logger.error(f"[SYNC CARTERA] Error upsert {data.get('empresa')} doc:{data.get('doc_entry')} line:{data.get('line_num')}: {str(ex)[:100]}")
            err += 1
    db.commit()
    return ins, upd, err
