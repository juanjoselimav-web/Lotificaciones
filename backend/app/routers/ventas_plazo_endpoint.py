

@router.get("/por-plazo-historico")
async def get_ventas_por_plazo_historico(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """
    Ventas activas por grupo de plazo x año — tabla fija sin filtro de fecha.
    Agrupa por plazo y año de venta, solo líneas activas (line_status=O o tipo_linea=BB).
    """
    PLAZO_MAP = {'Contado': 'Contado', '3': '3 a 18 meses', '5': '3 a 18 meses', '6': '3 a 18 meses', '10': '3 a 18 meses', '12': '3 a 18 meses', '18': '3 a 18 meses', '24': '2 a 4 años', '36': '2 a 4 años', '48': '2 a 4 años', '60': '5 años', '72': '6 años', '84': '7 años', '96': '8 años', '120': '10 años'}
    GROUP_ORDER = ['Contado', '3 a 18 meses', '2 a 4 años', '5 años', '6 años', '7 años', '8 años', '10 años']

    rows = db.execute(text("""
        SELECT
            plazo,
            EXTRACT(YEAR FROM fecha_venta_lote)::int AS anio_venta,
            COUNT(DISTINCT CONCAT(doc_entry, '-', manzana_lote)) AS unidades
        FROM ov_cartera
        WHERE tipo_linea = 'BB'
          AND line_status = 'O'
          AND fecha_venta_lote IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 2
    """)).fetchall()

    from collections import defaultdict
    # Accumulate by grupo and year
    data = defaultdict(lambda: defaultdict(int))
    years_set = set()
    for r in rows:
        plazo_val = str(r.plazo or '').strip()
        grupo = PLAZO_MAP.get(plazo_val, None)
        if not grupo:
            continue  # skip unknown plazos (Canje, Final Proyecto, etc.)
        anio = int(r.anio_venta) if r.anio_venta else 0
        if not anio or anio < 2020:
            continue
        years_set.add(anio)
        data[grupo][anio] += int(r.unidades or 0)

    years = sorted(years_set)
    grupos_out = []
    total_general = 0
    totales_anio = {yr: 0 for yr in years}

    for grupo in GROUP_ORDER:
        if grupo not in data:
            continue
        por_anio = {yr: data[grupo].get(yr, 0) for yr in years}
        acum = sum(por_anio.values())
        if acum == 0:
            continue
        grupos_out.append({"grupo": grupo, "por_anio": por_anio})
        total_general += acum
        for yr in years:
            totales_anio[yr] += por_anio[yr]

    return {
        "grupos": grupos_out,
        "years": years,
        "totales_anio": totales_anio,
        "total_general": total_general,
    }
