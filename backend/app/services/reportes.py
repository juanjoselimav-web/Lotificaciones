"""
==============================================================
GENERADOR DE PRESENTACIONES EJECUTIVAS — RV4 Lotificaciones
Genera PPTX con datos reales desde PostgreSQL + análisis OpenAI
Colores RV4: Navy #053D57 | Amarillo #FFCA08 | Naranja #F8931D
==============================================================
"""
import io
import os
import logging
from datetime import datetime, date
from typing import Optional

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

logger = logging.getLogger(__name__)

# ── Paleta RV4 ────────────────────────────────────────────────────────────────
NAVY   = RGBColor(0x05, 0x3D, 0x57)
YELLOW = RGBColor(0xFF, 0xCA, 0x08)
ORANGE = RGBColor(0xF8, 0x93, 0x1D)
WHITE  = RGBColor(0xFF, 0xFF, 0xFF)
GRAY   = RGBColor(0x33, 0x33, 0x33)
LGRAY  = RGBColor(0xF2, 0xF2, 0xF2)
GREEN  = RGBColor(0x1A, 0x85, 0x38)
RED    = RGBColor(0xC0, 0x39, 0x2B)

# Dimensiones slide 16:9
SW = Inches(13.33)
SH = Inches(7.5)

MESES_ES = {
    1:"Enero", 2:"Febrero", 3:"Marzo", 4:"Abril",
    5:"Mayo", 6:"Junio", 7:"Julio", 8:"Agosto",
    9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre"
}

# ── Helpers de dibujo ─────────────────────────────────────────────────────────

def _bg(slide, color: RGBColor):
    """Fondo sólido de color para todo el slide."""
    from pptx.oxml.ns import qn
    from lxml import etree
    fill = slide.background.fill
    fill.solid()
    fill.fore_color.rgb = color


def _rect(slide, x, y, w, h, fill: RGBColor, alpha=None):
    shape = slide.shapes.add_shape(1, x, y, w, h)  # MSO_SHAPE_TYPE.RECTANGLE = 1
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill
    shape.line.fill.background()
    return shape


def _txt(slide, text, x, y, w, h, size=14, bold=False, color=WHITE,
         align=PP_ALIGN.LEFT, italic=False, wrap=True):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = wrap
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = str(text)
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.italic = italic
    run.font.color.rgb = color
    run.font.name = "Calibri"
    return tb


def _kpi_card(slide, x, y, w, h, label, value, bg=NAVY, val_color=YELLOW):
    """Tarjeta KPI con fondo de color, valor grande y label pequeño."""
    _rect(slide, x, y, w, h, bg)
    _txt(slide, value, x, y + Inches(0.1), w, Inches(0.65),
         size=26, bold=True, color=val_color, align=PP_ALIGN.CENTER)
    _txt(slide, label, x, y + Inches(0.72), w, Inches(0.35),
         size=10, bold=False, color=WHITE, align=PP_ALIGN.CENTER)


def _header_bar(slide, title, subtitle=""):
    """Barra superior navy con título y subtítulo."""
    _rect(slide, 0, 0, SW, Inches(1.1), NAVY)
    _txt(slide, title, Inches(0.4), Inches(0.1), SW - Inches(0.8), Inches(0.6),
         size=22, bold=True, color=WHITE)
    if subtitle:
        _txt(slide, subtitle, Inches(0.4), Inches(0.65), SW - Inches(0.8), Inches(0.4),
             size=12, bold=False, color=YELLOW)


def _footer(slide, text="RV4 Lotificaciones — Confidencial"):
    _rect(slide, 0, SH - Inches(0.3), SW, Inches(0.3), NAVY)
    _txt(slide, text, Inches(0.3), SH - Inches(0.28), SW - Inches(0.6), Inches(0.26),
         size=8, color=LGRAY, align=PP_ALIGN.LEFT)


def _table_simple(slide, headers, rows, x, y, w, h):
    """Tabla simple con encabezado navy y filas alternadas."""
    if not rows:
        return
    cols = len(headers)
    col_w = w // cols
    row_h = min(Inches(0.38), h // (len(rows) + 1))

    # Header
    for ci, hdr in enumerate(headers):
        _rect(slide, x + ci * col_w, y, col_w, row_h, NAVY)
        _txt(slide, hdr, x + ci * col_w + Inches(0.05), y + Inches(0.05),
             col_w - Inches(0.1), row_h - Inches(0.05),
             size=9, bold=True, color=WHITE, align=PP_ALIGN.CENTER)

    # Rows
    for ri, row in enumerate(rows):
        bg = LGRAY if ri % 2 == 0 else WHITE
        for ci, cell in enumerate(row):
            _rect(slide, x + ci * col_w, y + (ri + 1) * row_h,
                  col_w, row_h, bg)
            _txt(slide, str(cell) if cell is not None else "",
                 x + ci * col_w + Inches(0.05),
                 y + (ri + 1) * row_h + Inches(0.02),
                 col_w - Inches(0.1), row_h - Inches(0.04),
                 size=9, bold=False, color=GRAY, align=PP_ALIGN.CENTER)


def _fmt_q(val, miles=False):
    """Formatea valor monetario en Q."""
    try:
        v = float(val or 0)
        if miles:
            v = v / 1000
            return f"Q {v:,.1f}K"
        if abs(v) >= 1_000_000:
            return f"Q {v/1_000_000:,.2f}M"
        return f"Q {v:,.2f}"
    except:
        return "Q 0.00"


# ── OpenAI — análisis de texto ────────────────────────────────────────────────

def _openai_analisis(prompt: str, api_key: str, modelo="gpt-4o-mini") -> str:
    """Llama a OpenAI para generar análisis ejecutivo. Fallback si falla."""
    if not api_key:
        return ""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=modelo,
            messages=[
                {"role": "system", "content":
                 "Eres un analista financiero ejecutivo de una empresa inmobiliaria guatemalteca. "
                 "Redacta análisis concisos, directos y profesionales en español. "
                 "Máximo 4 oraciones por análisis. Sin bullets. Solo párrafo."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=250,
            temperature=0.4
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"[OpenAI] Error generando análisis: {e}")
        return ""


# ── Slides individuales ───────────────────────────────────────────────────────

def slide_portada(prs, titulo, subtitulo, mes_str):
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
    _bg(slide, NAVY)
    # Franja amarilla inferior
    _rect(slide, 0, SH - Inches(0.7), SW, Inches(0.7), YELLOW)
    # Logo placeholder (rectángulo blanco arriba izquierda)
    _rect(slide, Inches(0.5), Inches(0.4), Inches(2.5), Inches(0.7), WHITE)
    _txt(slide, "IRV4", Inches(0.5), Inches(0.4), Inches(2.5), Inches(0.7),
         size=28, bold=True, color=NAVY, align=PP_ALIGN.CENTER)
    # Título principal
    _txt(slide, titulo, Inches(1.5), Inches(2.2), Inches(10.3), Inches(1.0),
         size=40, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    # Subtítulo
    _txt(slide, subtitulo, Inches(1.5), Inches(3.3), Inches(10.3), Inches(0.7),
         size=26, bold=False, color=YELLOW, align=PP_ALIGN.CENTER)
    # Mes en franja amarilla
    _txt(slide, mes_str, Inches(0.5), SH - Inches(0.65), Inches(12), Inches(0.6),
         size=14, bold=True, color=NAVY, align=PP_ALIGN.CENTER)


def slide_separador_proyecto(prs, nombre_proyecto, nombre_sociedad=""):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, NAVY)
    _rect(slide, 0, Inches(3.1), SW, Inches(0.06), YELLOW)
    _txt(slide, nombre_proyecto.upper(),
         Inches(1), Inches(2.3), Inches(11.3), Inches(0.9),
         size=44, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    if nombre_sociedad:
        _txt(slide, nombre_sociedad, Inches(1), Inches(3.3),
             Inches(11.3), Inches(0.5),
             size=18, bold=False, color=YELLOW, align=PP_ALIGN.CENTER)
    _footer(slide)


def slide_flujo_proyecto(prs, proyecto, mes, anio, datos_flujo, analisis=""):
    """Slide de flujo de efectivo individual por proyecto."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _header_bar(slide,
                f"FLUJO DE EFECTIVO — {proyecto.upper()}",
                f"{MESES_ES[mes].upper()} {anio} | Cifras en quetzales")

    ingresos = float(datos_flujo.get("ingresos", 0))
    egresos  = float(datos_flujo.get("egresos",  0))
    neto     = ingresos - egresos
    saldo_i  = float(datos_flujo.get("saldo_inicial", 0))
    saldo_f  = saldo_i + neto

    # KPI cards (fila superior)
    card_w = Inches(2.9)
    card_h = Inches(1.15)
    cy = Inches(1.25)
    cards = [
        ("SALDO INICIAL",  _fmt_q(saldo_i),  NAVY),
        ("INGRESOS",       _fmt_q(ingresos), GREEN),
        ("EGRESOS",        _fmt_q(egresos),  RED),
        ("SALDO FINAL",    _fmt_q(saldo_f),  ORANGE),
    ]
    for i, (lbl, val, col) in enumerate(cards):
        _kpi_card(slide, Inches(0.3) + i * (card_w + Inches(0.15)),
                  cy, card_w, card_h, lbl, val, bg=col)

    # Tabla de movimientos
    ingresos_det = datos_flujo.get("ingresos_detalle", [])
    egresos_det  = datos_flujo.get("egresos_detalle",  [])

    # Columna ingresos
    _rect(slide, Inches(0.3), Inches(2.6), Inches(5.8), Inches(0.35), NAVY)
    _txt(slide, "INGRESOS", Inches(0.3), Inches(2.6),
         Inches(5.8), Inches(0.35), size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    rows_ing = [(r["concepto"], _fmt_q(r["monto"])) for r in ingresos_det[:8]]
    _table_simple(slide, ["Concepto", "Monto"], rows_ing,
                  Inches(0.3), Inches(2.95), Inches(5.8), Inches(2.5))

    # Columna egresos
    _rect(slide, Inches(7.2), Inches(2.6), Inches(5.8), Inches(0.35), NAVY)
    _txt(slide, "EGRESOS", Inches(7.2), Inches(2.6),
         Inches(5.8), Inches(0.35), size=11, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    rows_egr = [(r["concepto"], _fmt_q(r["monto"])) for r in egresos_det[:8]]
    _table_simple(slide, ["Concepto", "Monto"], rows_egr,
                  Inches(7.2), Inches(2.95), Inches(5.8), Inches(2.5))

    # Análisis OpenAI
    if analisis:
        _rect(slide, Inches(0.3), Inches(5.6), SW - Inches(0.6), Inches(1.55), LGRAY)
        _txt(slide, "📊 ANÁLISIS EJECUTIVO",
             Inches(0.4), Inches(5.65), Inches(5), Inches(0.3),
             size=9, bold=True, color=NAVY)
        _txt(slide, analisis,
             Inches(0.4), Inches(5.95), SW - Inches(0.9), Inches(1.1),
             size=10, color=GRAY, wrap=True)

    _footer(slide, f"RV4 Lotificaciones | {proyecto} | {MESES_ES[mes]} {anio}")


def slide_avance_financiero(prs, proyecto, mes, anio, datos_inv, analisis=""):
    """Slide de avance financiero / inventario por proyecto."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _header_bar(slide,
                f"AVANCE FINANCIERO — {proyecto.upper()}",
                f"Datos al {_ultimo_dia(mes, anio)} | Cifras en quetzales")

    total    = int(datos_inv.get("total", 0))
    disp     = int(datos_inv.get("disponibles", 0))
    vend     = int(datos_inv.get("vendidos", 0))
    bloq     = int(datos_inv.get("bloqueados", 0))
    absorcion= round(vend / total * 100, 1) if total > 0 else 0
    valor    = float(datos_inv.get("valor_vendido", 0))

    # KPI cards
    card_w = Inches(2.3)
    card_h = Inches(1.2)
    cy = Inches(1.25)
    cards = [
        ("TOTAL LOTES",   str(total),          NAVY),
        ("DISPONIBLES",   str(disp),           GREEN),
        ("VENDIDOS",      str(vend),           ORANGE),
        ("BLOQUEADOS",    str(bloq),           GRAY),
        ("ABSORCIÓN",     f"{absorcion}%",     RGBColor(0x1A, 0x75, 0x9F)),
    ]
    for i, (lbl, val, col) in enumerate(cards):
        _kpi_card(slide, Inches(0.3) + i * (card_w + Inches(0.15)),
                  cy, card_w, card_h, lbl, val, bg=col)

    # Valor total vendido
    _rect(slide, Inches(0.3), Inches(2.65), SW - Inches(0.6), Inches(0.45), YELLOW)
    _txt(slide, f"VALOR TOTAL VENDIDO: {_fmt_q(valor)}",
         Inches(0.5), Inches(2.65), SW - Inches(1.0), Inches(0.45),
         size=14, bold=True, color=NAVY, align=PP_ALIGN.CENTER)

    # Tabla por manzana
    manzanas = datos_inv.get("por_manzana", [])
    if manzanas:
        _table_simple(slide,
                      ["Manzana", "Total", "Disponibles", "Vendidos", "Absorción %"],
                      [(m["manzana"], m["total"], m["disponibles"],
                        m["vendidos"], f"{m['absorcion']}%") for m in manzanas[:10]],
                      Inches(0.3), Inches(3.2), SW - Inches(0.6), Inches(3.0))

    if analisis:
        _rect(slide, Inches(0.3), Inches(6.1), SW - Inches(0.6), Inches(1.1), LGRAY)
        _txt(slide, "📊 ANÁLISIS EJECUTIVO",
             Inches(0.4), Inches(6.15), Inches(5), Inches(0.3),
             size=9, bold=True, color=NAVY)
        _txt(slide, analisis,
             Inches(0.4), Inches(6.45), SW - Inches(0.9), Inches(0.65),
             size=10, color=GRAY, wrap=True)

    _footer(slide, f"RV4 Lotificaciones | {proyecto} | {MESES_ES[mes]} {anio}")


def slide_analisis_cartera(prs, proyecto, mes, anio, datos_cart, analisis=""):
    """Slide de análisis de cartera por proyecto."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _header_bar(slide,
                f"ANÁLISIS DE CARTERA — {proyecto.upper()}",
                f"Al cierre de {MESES_ES[mes]} {anio} | Cifras en quetzales")

    total_cart = float(datos_cart.get("cartera_total", 0))
    mora       = float(datos_cart.get("mora_total",    0))
    tasa_mora  = round(mora / total_cart * 100, 2) if total_cart > 0 else 0
    clientes   = int(datos_cart.get("clientes_activos", 0))
    cobro_30   = float(datos_cart.get("cobro_30d", 0))

    cards = [
        ("CARTERA TOTAL",  _fmt_q(total_cart), NAVY),
        ("EN MORA",        _fmt_q(mora),       RED),
        ("TASA DE MORA",   f"{tasa_mora}%",    ORANGE),
        ("CLIENTES",       str(clientes),      GREEN),
        ("COBRO 30D",      _fmt_q(cobro_30),   RGBColor(0x1A, 0x75, 0x9F)),
    ]
    card_w = Inches(2.3)
    card_h = Inches(1.2)
    for i, (lbl, val, col) in enumerate(cards):
        _kpi_card(slide, Inches(0.3) + i * (card_w + Inches(0.15)),
                  Inches(1.25), card_w, card_h, lbl, val, bg=col)

    # Clientes en mora
    clientes_mora = datos_cart.get("clientes_mora_detalle", [])
    if clientes_mora:
        _rect(slide, Inches(0.3), Inches(2.6), SW - Inches(0.6), Inches(0.35), NAVY)
        _txt(slide, "CLIENTES CON SALDO VENCIDO",
             Inches(0.3), Inches(2.6), SW - Inches(0.6), Inches(0.35),
             size=10, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
        _table_simple(slide,
                      ["Cliente", "Lote(s)", "Saldo Vencido", "Estado"],
                      [(c["card_name"][:30], c.get("lotes",""),
                        _fmt_q(c["monto_vencido"]), c["estado"])
                       for c in clientes_mora[:8]],
                      Inches(0.3), Inches(2.95), SW - Inches(0.6), Inches(2.8))

    if analisis:
        _rect(slide, Inches(0.3), Inches(5.9), SW - Inches(0.6), Inches(1.3), LGRAY)
        _txt(slide, "📊 ANÁLISIS EJECUTIVO",
             Inches(0.4), Inches(5.95), Inches(5), Inches(0.3),
             size=9, bold=True, color=NAVY)
        _txt(slide, analisis,
             Inches(0.4), Inches(6.25), SW - Inches(0.9), Inches(0.85),
             size=10, color=GRAY, wrap=True)

    _footer(slide, f"RV4 Lotificaciones | {proyecto} | {MESES_ES[mes]} {anio}")


def slide_flujo_consolidado(prs, mes, anio, proyectos_data):
    """Slide de flujo consolidado de todos los proyectos."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _header_bar(slide,
                f"FLUJO DE EFECTIVO CONSOLIDADO",
                f"AL CIERRE DE {MESES_ES[mes].upper()} {anio} | Cifras en quetzales")

    total_ing = sum(float(p.get("ingresos", 0)) for p in proyectos_data)
    total_egr = sum(float(p.get("egresos",  0)) for p in proyectos_data)
    neto      = total_ing - total_egr

    cards = [
        ("TOTAL INGRESOS", _fmt_q(total_ing), GREEN),
        ("TOTAL EGRESOS",  _fmt_q(total_egr), RED),
        ("RESULTADO NETO", _fmt_q(neto), NAVY if neto >= 0 else RED),
    ]
    card_w = Inches(3.8)
    for i, (lbl, val, col) in enumerate(cards):
        _kpi_card(slide, Inches(0.4) + i * (card_w + Inches(0.3)),
                  Inches(1.25), card_w, Inches(1.1), lbl, val, bg=col)

    rows = [(p["proyecto"], _fmt_q(p.get("ingresos", 0)),
             _fmt_q(p.get("egresos", 0)),
             _fmt_q(p.get("ingresos", 0) - p.get("egresos", 0)))
            for p in proyectos_data]
    _table_simple(slide,
                  ["Proyecto", "Ingresos", "Egresos", "Neto"],
                  rows,
                  Inches(0.3), Inches(2.55), SW - Inches(0.6), Inches(4.6))
    _footer(slide, f"RV4 Lotificaciones | Consolidado | {MESES_ES[mes]} {anio}")


def slide_ventas_consolidado(prs, mes, anio, proyectos_data):
    """Slide de ventas consolidadas."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _header_bar(slide,
                "CONSOLIDADO DE VENTAS",
                f"Al cierre de {MESES_ES[mes]} {anio}")

    total_vend = sum(int(p.get("vendidos_mes", 0)) for p in proyectos_data)
    total_val  = sum(float(p.get("valor_mes",  0)) for p in proyectos_data)

    _kpi_card(slide, Inches(0.4), Inches(1.25), Inches(5.8), Inches(1.0),
              "LOTES VENDIDOS EN EL MES", str(total_vend), ORANGE)
    _kpi_card(slide, Inches(7.0), Inches(1.25), Inches(5.8), Inches(1.0),
              "VALOR TOTAL VENTAS DEL MES", _fmt_q(total_val), NAVY)

    rows = [(p["proyecto"],
             p.get("vendidos_mes", 0),
             _fmt_q(p.get("valor_mes", 0)),
             p.get("total_lotes", 0),
             f"{p.get('absorcion', 0)}%")
            for p in proyectos_data]
    _table_simple(slide,
                  ["Proyecto", "Ventas Mes", "Valor Mes", "Total Lotes", "Absorción"],
                  rows,
                  Inches(0.3), Inches(2.55), SW - Inches(0.6), Inches(4.6))
    _footer(slide, f"RV4 Lotificaciones | Ventas Consolidadas | {MESES_ES[mes]} {anio}")


def slide_cartera_consolidado(prs, mes, anio, proyectos_data):
    """Slide de cartera consolidada."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(slide, WHITE)
    _header_bar(slide,
                "RECUPERACIÓN DE CARTERA CONSOLIDADA",
                f"Al cierre de {MESES_ES[mes]} {anio} | Cifras en quetzales")

    total_cart = sum(float(p.get("cartera_total", 0)) for p in proyectos_data)
    total_mora = sum(float(p.get("mora_total",    0)) for p in proyectos_data)
    tasa_mora  = round(total_mora / total_cart * 100, 2) if total_cart > 0 else 0

    cards = [
        ("CARTERA TOTAL",   _fmt_q(total_cart), NAVY),
        ("TOTAL EN MORA",   _fmt_q(total_mora), RED),
        ("TASA MORA PROM.", f"{tasa_mora}%",    ORANGE),
    ]
    card_w = Inches(3.8)
    for i, (lbl, val, col) in enumerate(cards):
        _kpi_card(slide, Inches(0.4) + i * (card_w + Inches(0.3)),
                  Inches(1.25), card_w, Inches(1.0), lbl, val, bg=col)

    rows = [(p["proyecto"],
             _fmt_q(p.get("cartera_total", 0)),
             _fmt_q(p.get("mora_total",    0)),
             f"{p.get('tasa_mora', 0)}%",
             p.get("clientes_activos", 0))
            for p in proyectos_data]
    _table_simple(slide,
                  ["Proyecto", "Cartera", "Mora", "Tasa Mora", "Clientes"],
                  rows,
                  Inches(0.3), Inches(2.55), SW - Inches(0.6), Inches(4.6))
    _footer(slide, f"RV4 Lotificaciones | Cartera Consolidada | {MESES_ES[mes]} {anio}")


# ── Consultas a la BD ─────────────────────────────────────────────────────────

def _ultimo_dia(mes, anio):
    import calendar
    d = calendar.monthrange(anio, mes)[1]
    return f"{d} de {MESES_ES[mes]} de {anio}"


def _q_flujo_proyecto(db, sociedad, mes, anio):
    from sqlalchemy import text
    row = db.execute(text("""
        SELECT
            COALESCE(SUM(monto_ingreso), 0) AS ingresos,
            COALESCE(SUM(monto_egreso),  0) AS egresos
        FROM flujos_efectivo
        WHERE sociedad = :soc
          AND EXTRACT(YEAR  FROM fecha_contable) = :a
          AND EXTRACT(MONTH FROM fecha_contable) = :m
    """), {"soc": sociedad, "a": anio, "m": mes}).fetchone()

    # Saldo inicial del mes
    si_row = db.execute(text("""
        SELECT COALESCE(SUM(monto), 0) AS si
        FROM flujos_saldo_inicial
        WHERE sociedad = :soc
    """), {"soc": sociedad}).fetchone()

    # Ingresos por RDI (detalle)
    ingresos_det = db.execute(text("""
        SELECT nombre_rdi AS concepto, COALESCE(SUM(monto_ingreso), 0) AS monto
        FROM flujos_efectivo
        WHERE sociedad = :soc
          AND EXTRACT(YEAR  FROM fecha_contable) = :a
          AND EXTRACT(MONTH FROM fecha_contable) = :m
          AND monto_ingreso > 0
        GROUP BY nombre_rdi
        ORDER BY monto DESC
        LIMIT 10
    """), {"soc": sociedad, "a": anio, "m": mes}).fetchall()

    egresos_det = db.execute(text("""
        SELECT nombre_rdi AS concepto, COALESCE(SUM(monto_egreso), 0) AS monto
        FROM flujos_efectivo
        WHERE sociedad = :soc
          AND EXTRACT(YEAR  FROM fecha_contable) = :a
          AND EXTRACT(MONTH FROM fecha_contable) = :m
          AND monto_egreso > 0
        GROUP BY nombre_rdi
        ORDER BY monto DESC
        LIMIT 10
    """), {"soc": sociedad, "a": anio, "m": mes}).fetchall()

    return {
        "ingresos":          float(row.ingresos or 0),
        "egresos":           float(row.egresos  or 0),
        "saldo_inicial":     float(si_row.si    or 0),
        "ingresos_detalle":  [{"concepto": r.concepto, "monto": float(r.monto)} for r in ingresos_det],
        "egresos_detalle":   [{"concepto": r.concepto, "monto": float(r.monto)} for r in egresos_det],
    }


def _q_inventario_proyecto(db, empresa_sap):
    from sqlalchemy import text
    row = db.execute(text("""
        SELECT
            COUNT(*)                                                          AS total,
            COUNT(*) FILTER (WHERE estatus='DISPONIBLE')                     AS disponibles,
            COUNT(*) FILTER (WHERE estatus IN ('VENTA','RESERVADO'))         AS vendidos,
            COUNT(*) FILTER (WHERE estatus='BLOQUEADO')                      AS bloqueados,
            COALESCE(SUM(precio_final) FILTER (WHERE estatus IN ('VENTA','RESERVADO')), 0) AS valor_vendido
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE p.empresa_sap = :esp
    """), {"esp": empresa_sap}).fetchone()

    por_manzana = db.execute(text("""
        SELECT
            l.manzana,
            COUNT(*)                                                         AS total,
            COUNT(*) FILTER (WHERE estatus='DISPONIBLE')                    AS disponibles,
            COUNT(*) FILTER (WHERE estatus IN ('VENTA','RESERVADO'))        AS vendidos
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE p.empresa_sap = :esp AND l.manzana IS NOT NULL
        GROUP BY l.manzana ORDER BY l.manzana
        LIMIT 12
    """), {"esp": empresa_sap}).fetchall()

    def absorcion(v, t): return round(v / t * 100, 1) if t > 0 else 0

    return {
        "total":       int(row.total or 0),
        "disponibles": int(row.disponibles or 0),
        "vendidos":    int(row.vendidos or 0),
        "bloqueados":  int(row.bloqueados or 0),
        "valor_vendido": float(row.valor_vendido or 0),
        "por_manzana": [
            {"manzana":    r.manzana,
             "total":      int(r.total),
             "disponibles":int(r.disponibles),
             "vendidos":   int(r.vendidos),
             "absorcion":  absorcion(int(r.vendidos), int(r.total))}
            for r in por_manzana
        ]
    }


def _q_cartera_proyecto(db, sociedad):
    from sqlalchemy import text
    row = db.execute(text("""
        SELECT
            COALESCE(SUM(CASE WHEN line_status='O' THEN saldo_pendiente ELSE 0 END), 0)       AS cartera_total,
            COALESCE(SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                              THEN saldo_pendiente ELSE 0 END), 0)                            AS mora_total,
            COUNT(DISTINCT CASE WHEN line_status='O' THEN card_code END)                      AS clientes_activos,
            COALESCE(SUM(CASE WHEN line_status='O'
                              AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+30
                              THEN saldo_pendiente ELSE 0 END), 0)                            AS cobro_30d
        FROM ov_cartera
        WHERE tipo_linea IN ('BB','S') AND empresa = :soc
    """), {"soc": sociedad}).fetchone()

    mora_det = db.execute(text("""
        SELECT card_name,
               STRING_AGG(DISTINCT unidad_key, ', ') AS lotes,
               SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                        THEN saldo_pendiente ELSE 0 END) AS monto_vencido,
               'VENCIDO' AS estado
        FROM ov_cartera oc
        LEFT JOIN lotes l ON l.card_code = oc.card_code
        WHERE oc.tipo_linea IN ('BB','S') AND oc.empresa = :soc
          AND oc.line_status='O' AND oc.fecha_programada_cobro < CURRENT_DATE
        GROUP BY oc.card_code, oc.card_name
        HAVING SUM(CASE WHEN line_status='O' AND fecha_programada_cobro < CURRENT_DATE
                        THEN saldo_pendiente ELSE 0 END) > 0
        ORDER BY monto_vencido DESC
        LIMIT 8
    """), {"soc": sociedad}).fetchall()

    cart = float(row.cartera_total or 0)
    mora = float(row.mora_total    or 0)
    return {
        "cartera_total":          cart,
        "mora_total":             mora,
        "tasa_mora":              round(mora / cart * 100, 2) if cart > 0 else 0,
        "clientes_activos":       int(row.clientes_activos or 0),
        "cobro_30d":              float(row.cobro_30d or 0),
        "clientes_mora_detalle":  [dict(r._mapping) for r in mora_det],
    }


def _q_ventas_mes(db, empresa_sap, mes, anio):
    from sqlalchemy import text
    row = db.execute(text("""
        SELECT COUNT(*) AS vend, COALESCE(SUM(l.precio_final), 0) AS valor,
               COUNT(*) AS total_g
        FROM lotes l
        JOIN proyectos p ON p.id = l.proyecto_id
        WHERE p.empresa_sap = :esp
          AND l.estatus IN ('VENTA','RESERVADO')
          AND EXTRACT(YEAR FROM l.fecha_venta) = :a
          AND EXTRACT(MONTH FROM l.fecha_venta) = :m
          AND l.fecha_venta IS NOT NULL
    """), {"esp": empresa_sap, "a": anio, "m": mes}).fetchone()
    return {"vendidos_mes": int(row.vend or 0), "valor_mes": float(row.valor or 0)}


# ── GENERADORES PRINCIPALES ───────────────────────────────────────────────────

PROYECTOS_RV4 = [
    {"nombre": "Hacienda Jumay",               "sociedad": "EFICIENCIA URBANA", "empresa_sap": "SBO_EFICIENCIA_URBANA"},
    {"nombre": "La Ceiba",                     "sociedad": "SER GEN CCC",       "empresa_sap": "SBO_SER_GEN_CCC"},
    {"nombre": "Hacienda el Sol",              "sociedad": "ROSSIO",            "empresa_sap": "SBO_ROSSIO"},
    {"nombre": "Oasis Zacapa",                 "sociedad": "FRUGALEX",          "empresa_sap": "SBO_FRUGALEX"},
    {"nombre": "Cañadas de Jalapa",            "sociedad": "OTTAVIA",           "empresa_sap": "SBO_OTTAVIA"},
    {"nombre": "Condado Jutiapa",              "sociedad": "UTILICA",           "empresa_sap": "SBO_UTILICA"},
    {"nombre": "Club Campestre Jumay",         "sociedad": "TEZZOLI",           "empresa_sap": "SBO_TEZZOLI"},
    {"nombre": "Club del Bosque",              "sociedad": "URBIVA",            "empresa_sap": "SBO_URBIVA_2"},
    {"nombre": "Club Residencial El Progreso", "sociedad": "GARBATELLA",        "empresa_sap": "SBO_GARBATELLA"},
    {"nombre": "Arboleada Santa Elena",        "sociedad": "CAPIPOS",           "empresa_sap": "SBO_CAPIPOS"},
    {"nombre": "Hacienda Santa Lucia",         "sociedad": "OVEST",             "empresa_sap": "SBO_OVEST"},
    {"nombre": "Hacienda El Cafetal Fase I",   "sociedad": "CORCOLLE",          "empresa_sap": "SBO_CORCOLLE"},
    {"nombre": "Hacienda El Cafetal Fase II",  "sociedad": "LEOFRENI",          "empresa_sap": "SBO_LEOFRENI"},
    {"nombre": "Hacienda El Cafetal Fase III", "sociedad": "GIBRALEON",         "empresa_sap": "SBO_GIBRALEON"},
    {"nombre": "Hacienda El Cafetal Fase IV",  "sociedad": "TALOCCI",           "empresa_sap": "SBO_TALOCCI"},
    {"nombre": "Celajes De Tecpan",            "sociedad": "VILET",             "empresa_sap": "SBO_VILET"},
]


def generar_presentacion_proyecto(db, empresa_sap: str, mes: int, anio: int,
                                   openai_key: str = "") -> bytes:
    """
    Genera una presentación ejecutiva individual para un proyecto.
    Retorna bytes del archivo PPTX.
    """
    info = next((p for p in PROYECTOS_RV4 if p["empresa_sap"] == empresa_sap), None)
    if not info:
        raise ValueError(f"Proyecto {empresa_sap} no encontrado")

    nombre   = info["nombre"]
    sociedad = info["sociedad"]

    # Recopilar datos
    flujo = _q_flujo_proyecto(db, sociedad, mes, anio)
    inv   = _q_inventario_proyecto(db, empresa_sap)
    cart  = _q_cartera_proyecto(db, sociedad)
    ventas= _q_ventas_mes(db, empresa_sap, mes, anio)

    # Análisis OpenAI
    analisis_flujo = analisis_inv = analisis_cart = ""
    if openai_key:
        analisis_flujo = _openai_analisis(
            f"Analiza el flujo de efectivo del proyecto {nombre} en {MESES_ES[mes]} {anio}: "
            f"Ingresos Q{flujo['ingresos']:,.0f}, Egresos Q{flujo['egresos']:,.0f}, "
            f"Neto Q{flujo['ingresos']-flujo['egresos']:,.0f}. "
            f"Da una evaluación ejecutiva concisa del desempeño financiero del mes.",
            openai_key)
        analisis_inv = _openai_analisis(
            f"Analiza el avance de ventas del proyecto {nombre}: "
            f"{inv['vendidos']} lotes vendidos de {inv['total']} totales ({inv['total']-inv['disponibles']-inv['bloqueados']} absorbidos), "
            f"absorción {round(inv['vendidos']/inv['total']*100,1) if inv['total']>0 else 0}%, "
            f"valor vendido Q{inv['valor_vendido']:,.0f}. Evalúa el ritmo de ventas.",
            openai_key)
        analisis_cart = _openai_analisis(
            f"Analiza la cartera del proyecto {nombre}: "
            f"Cartera total Q{cart['cartera_total']:,.0f}, mora Q{cart['mora_total']:,.0f} "
            f"({cart['tasa_mora']}%), {cart['clientes_activos']} clientes activos. "
            f"Evalúa la salud de la cartera y recomienda acciones.",
            openai_key)

    # Crear PPTX
    prs = Presentation()
    prs.slide_width  = SW
    prs.slide_height = SH
    # Asegurar layout en blanco
    while len(prs.slide_layouts) < 7:
        prs.slide_layouts[0]

    mes_str = f"Resultados a {MESES_ES[mes]} {anio}"
    slide_portada(prs, "Junta Directiva Lotificadoras", nombre.upper(), mes_str)
    slide_separador_proyecto(prs, nombre, info["empresa_sap"])
    slide_flujo_proyecto(prs, nombre, mes, anio, flujo, analisis_flujo)
    slide_avance_financiero(prs, nombre, mes, anio, {**inv, **ventas}, analisis_inv)
    slide_analisis_cartera(prs, nombre, mes, anio, cart, analisis_cart)

    buf = io.BytesIO()
    prs.save(buf)
    buf.seek(0)
    return buf.read()


def generar_presentacion_consolidada(db, mes: int, anio: int,
                                      openai_key: str = "") -> bytes:
    """
    Genera una presentación ejecutiva consolidada de todos los proyectos
    más secciones individuales por cada uno.
    Retorna bytes del archivo PPTX.
    """
    prs = Presentation()
    prs.slide_width  = SW
    prs.slide_height = SH

    mes_str = f"Resultados a {MESES_ES[mes]} {anio}"
    slide_portada(prs, "Junta Directiva Lotificadoras", "CONSOLIDADO", mes_str)

    # Recopilar datos de todos los proyectos
    todos = []
    for p in PROYECTOS_RV4:
        flujo  = _q_flujo_proyecto(db, p["sociedad"], mes, anio)
        inv    = _q_inventario_proyecto(db, p["empresa_sap"])
        cart   = _q_cartera_proyecto(db, p["sociedad"])
        ventas = _q_ventas_mes(db, p["empresa_sap"], mes, anio)
        absorcion = round(inv["vendidos"] / inv["total"] * 100, 1) if inv["total"] > 0 else 0
        todos.append({
            "proyecto":       p["nombre"],
            "sociedad":       p["sociedad"],
            "empresa_sap":    p["empresa_sap"],
            "ingresos":       flujo["ingresos"],
            "egresos":        flujo["egresos"],
            "cartera_total":  cart["cartera_total"],
            "mora_total":     cart["mora_total"],
            "tasa_mora":      cart["tasa_mora"],
            "clientes_activos": cart["clientes_activos"],
            "vendidos_mes":   ventas["vendidos_mes"],
            "valor_mes":      ventas["valor_mes"],
            "total_lotes":    inv["total"],
            "absorcion":      absorcion,
        })

    # ── Slides consolidados ──
    # Separador consolidado
    slide_separador_proyecto(prs, "CONSOLIDADO", "Todos los proyectos")
    slide_flujo_consolidado(prs, mes, anio, todos)
    slide_cartera_consolidado(prs, mes, anio, todos)
    slide_ventas_consolidado(prs, mes, anio, todos)

    # Análisis consolidado con OpenAI
    if openai_key:
        total_ing = sum(p["ingresos"] for p in todos)
        total_egr = sum(p["egresos"]  for p in todos)
        total_mora = sum(p["mora_total"] for p in todos)
        analisis_cons = _openai_analisis(
            f"Analiza el desempeño consolidado de {len(todos)} proyectos inmobiliarios en "
            f"{MESES_ES[mes]} {anio}: Ingresos totales Q{total_ing:,.0f}, "
            f"Egresos Q{total_egr:,.0f}, Mora total Q{total_mora:,.0f}. "
            f"Da un análisis ejecutivo del portafolio completo.",
            openai_key)
        if analisis_cons:
            slide_cons = prs.slides.add_slide(prs.slide_layouts[6])
            _bg(slide_cons, NAVY)
            _txt(slide_cons, "ANÁLISIS EJECUTIVO DEL PORTAFOLIO",
                 Inches(1), Inches(1.5), Inches(11.3), Inches(0.7),
                 size=28, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
            _rect(slide_cons, Inches(1), Inches(2.4), Inches(11.3), Inches(0.05), YELLOW)
            _txt(slide_cons, analisis_cons,
                 Inches(1.2), Inches(2.6), Inches(11.0), Inches(3.5),
                 size=16, color=LGRAY, align=PP_ALIGN.CENTER, wrap=True)
            _footer(slide_cons, f"RV4 Lotificaciones | {MESES_ES[mes]} {anio}")

    # ── Secciones individuales ──
    for p in PROYECTOS_RV4:
        flujo  = _q_flujo_proyecto(db, p["sociedad"], mes, anio)
        inv    = _q_inventario_proyecto(db, p["empresa_sap"])
        cart   = _q_cartera_proyecto(db, p["sociedad"])
        ventas = _q_ventas_mes(db, p["empresa_sap"], mes, anio)

        analisis_fl = analisis_in = analisis_ca = ""
        if openai_key:
            analisis_fl = _openai_analisis(
                f"Resumen ejecutivo de flujo de {p['nombre']} en {MESES_ES[mes]} {anio}: "
                f"Ingresos Q{flujo['ingresos']:,.0f}, Egresos Q{flujo['egresos']:,.0f}.",
                openai_key)

        slide_separador_proyecto(prs, p["nombre"], p["empresa_sap"])
        slide_flujo_proyecto(prs, p["nombre"], mes, anio, flujo, analisis_fl)
        slide_avance_financiero(prs, p["nombre"], mes, anio, {**inv, **ventas}, analisis_in)
        slide_analisis_cartera(prs, p["nombre"], mes, anio, cart, analisis_ca)

    buf = io.BytesIO()
    prs.save(buf)
    buf.seek(0)
    return buf.read()
