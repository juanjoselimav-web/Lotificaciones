#!/usr/bin/env python3
"""
Adds 2 new endpoints to cartera.py and ventas.py.
Run: docker exec loti_backend python3 /tmp/patch_endpoints.py
"""

cartera_path = '/app/app/routers/cartera.py'
ventas_path  = '/app/app/routers/ventas.py'

# ── cartera.py ──────────────────────────────────────────────────────────────
with open(cartera_path, 'r') as f:
    cartera = f.read()

if 'intereses-sin-cobrar' not in cartera:
    ep = '''

@router.get("/intereses-sin-cobrar")
async def get_intereses_sin_cobrar(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Intereses sin cobrar por año (tipo_linea=S, line_status=O). Sin filtro de fecha."""
    rows = db.execute(text("""
        SELECT EXTRACT(YEAR FROM fecha_programada_cobro)::int AS anio,
               SUM(saldo_pendiente) AS intereses
        FROM ov_cartera
        WHERE tipo_linea = 'S'
          AND line_status = 'O'
          AND saldo_pendiente > 0
          AND fecha_programada_cobro IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)).fetchall()
    por_anio = [{"anio": r.anio, "intereses": round(float(r.intereses or 0), 2)} for r in rows]
    return {"por_anio": por_anio, "total": round(sum(r["intereses"] for r in por_anio), 2)}
'''
    with open(cartera_path, 'a') as f:
        f.write(ep)
    print("[OK] cartera.py: /api/cartera/intereses-sin-cobrar ADDED")
else:
    print("[SKIP] cartera.py: already exists")

# ── ventas.py ───────────────────────────────────────────────────────────────
with open(ventas_path, 'r') as f:
    ventas = f.read()

if 'por-plazo-historico' not in ventas:
    ep2 = '''

@router.get("/por-plazo-historico")
async def get_ventas_por_plazo_historico(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """Ventas activas por grupo de plazo x año. Sin filtro de fecha."""
    from collections import defaultdict
    PLAZO_MAP = {
        "Contado":"Contado",
        "3":"3 a 18 meses","5":"3 a 18 meses","6":"3 a 18 meses",
        "10":"3 a 18 meses","12":"3 a 18 meses","18":"3 a 18 meses",
        "24":"2 a 4 años","36":"2 a 4 años","48":"2 a 4 años",
        "60":"5 años","72":"6 años","84":"7 años","96":"8 años","120":"10 años",
    }
    GROUP_ORDER = ["Contado","3 a 18 meses","2 a 4 años","5 años","6 años","7 años","8 años","10 años"]
    rows = db.execute(text("""
        SELECT plazo,
               EXTRACT(YEAR FROM fecha_venta_lote)::int AS anio_venta,
               COUNT(DISTINCT CONCAT(doc_entry::text, '-', COALESCE(manzana_lote, ''))) AS unidades
        FROM ov_cartera
        WHERE tipo_linea = 'BB'
          AND line_status = 'O'
          AND fecha_venta_lote IS NOT NULL
        GROUP BY 1, 2 ORDER BY 2
    """)).fetchall()
    data = defaultdict(lambda: defaultdict(int))
    years_set = set()
    for r in rows:
        grupo = PLAZO_MAP.get(str(r.plazo or "").strip())
        if not grupo: continue
        anio = int(r.anio_venta) if r.anio_venta else 0
        if anio < 2020: continue
        years_set.add(anio)
        data[grupo][anio] += int(r.unidades or 0)
    years = sorted(years_set)
    grupos_out, total_general, totales_anio = [], 0, {yr: 0 for yr in years}
    for grupo in GROUP_ORDER:
        if grupo not in data: continue
        por_anio = {yr: data[grupo].get(yr, 0) for yr in years}
        acum = sum(por_anio.values())
        if not acum: continue
        grupos_out.append({"grupo": grupo, "por_anio": por_anio})
        total_general += acum
        for yr in years: totales_anio[yr] += por_anio[yr]
    return {"grupos": grupos_out, "years": years, "totales_anio": totales_anio, "total_general": total_general}
'''
    with open(ventas_path, 'a') as f:
        f.write(ep2)
    print("[OK] ventas.py: /api/ventas/por-plazo-historico ADDED")
else:
    print("[SKIP] ventas.py: already exists")

print("Done — restart backend to activate endpoints.")
