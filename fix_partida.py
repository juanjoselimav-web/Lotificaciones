import re

path = "/app/app/sync/sync_flujos.py"
with open(path, encoding="utf-8") as f:
    content = f.read()

# Fix 1: Filter bad years in _preprocesar_df - add after the fecha_inicio filter
OLD_FECHA = """    if fecha_inicio:
        df = df[df["FECHA_CONTABLE"].notna() & (df["FECHA_CONTABLE"].dt.date >= fecha_inicio)].copy()"""

NEW_FECHA = """    if fecha_inicio:
        df = df[df["FECHA_CONTABLE"].notna() & (df["FECHA_CONTABLE"].dt.date >= fecha_inicio)].copy()

    # Filtrar fechas con año inválido (epoch/corrupto)
    df = df[df["FECHA_CONTABLE"].dt.year >= 2000].copy()"""

content = content.replace(OLD_FECHA, NEW_FECHA)
print("Fix 1 applied" if OLD_FECHA not in content else "Fix 1 FAILED")

# Fix 2: Expand _cargar_partida_inicial to also return movement rows for flujos_efectivo
OLD_PARTIDA = """def _cargar_partida_inicial(xf: pd.ExcelFile) -> list[dict]:
    \"\"\"Lee la hoja PARTIDA INICIAL y devuelve lista de dicts para flujos_saldo_inicial.\"\"\"
    df = xf.parse(\"PARTIDA INICIAL\")
    df.columns = [c.strip() for c in df.columns]
    registros = []
    meses_map = {
        \"ENERO\": 1, \"FEBRERO\": 2, \"MARZO\": 3, \"ABRIL\": 4,
        \"MAYO\": 5, \"JUNIO\": 6, \"JULIO\": 7, \"AGOSTO\": 8,
        \"SEPTIEMBRE\": 9, \"OCTUBRE\": 10, \"NOVIEMBRE\": 11, \"DICIEMBRE\": 12,
    }
    for _, row in df.iterrows():
        if str(row.get(\"SECCION\", \"\")).strip() != \"SALDO INICIAL\":
            continue
        soc    = str(row.get(\"SOCIEDAD\", \"\") or \"\").strip()
        anio   = row.get(\"AÑO\")
        mes_s  = str(row.get(\"MES\", \"\") or \"\").strip().upper()
        sem_s  = str(row.get(\"SEMANA\", \"\") or \"\").strip()
        monto  = _safe_float(row.get(\"MONTO\"))
        mes    = meses_map.get(mes_s)
        # Extraer número de semana: \"S6\" → 6
        sem_match = re.search(r\"\\d+\", sem_s)
        semana_iso = int(sem_match.group()) if sem_match else None
        if soc and anio and mes and monto is not None:
            registros.append({
                \"sociedad\":     soc,
                \"anio\":         int(anio),
                \"mes\":          mes,
                \"semana_iso\":   semana_iso,
                \"semana_label\": sem_s,
                \"monto\":        monto,
            })
    return registros"""

NEW_PARTIDA = """def _cargar_partida_inicial(xf: pd.ExcelFile) -> tuple:
    \"\"\"
    Lee la hoja PARTIDA INICIAL.
    Retorna (saldos, movimientos):
      - saldos: lista de dicts para flujos_saldo_inicial (solo SALDO INICIAL)
      - movimientos: lista de dicts para flujos_efectivo (ingresos/egresos del período inicial)
    \"\"\"
    df = xf.parse(\"PARTIDA INICIAL\")
    df.columns = [c.strip() for c in df.columns]
    saldos = []
    movimientos = []
    meses_map = {
        \"ENERO\": 1, \"FEBRERO\": 2, \"MARZO\": 3, \"ABRIL\": 4,
        \"MAYO\": 5, \"JUNIO\": 6, \"JULIO\": 7, \"AGOSTO\": 8,
        \"SEPTIEMBRE\": 9, \"OCTUBRE\": 10, \"NOVIEMBRE\": 11, \"DICIEMBRE\": 12,
    }
    for _, row in df.iterrows():
        soc   = str(row.get(\"SOCIEDAD\", \"\") or \"\").strip()
        anio  = row.get(\"AÑO\")
        mes_s = str(row.get(\"MES\", \"\") or \"\").strip().upper()
        sem_s = str(row.get(\"SEMANA\", \"\") or \"\").strip()
        sec   = str(row.get(\"SECCION\", \"\") or \"\").strip()
        nom   = str(row.get(\"NOMBRE\", \"\") or \"\").strip()
        monto = _safe_float(row.get(\"MONTO\"))
        mes   = meses_map.get(mes_s)
        sem_match  = re.search(r\"\\d+\", sem_s)
        semana_iso = int(sem_match.group()) if sem_match else None
        if not (soc and anio and mes and monto is not None):
            continue

        if sec == \"SALDO INICIAL\":
            saldos.append({
                \"sociedad\":     soc,
                \"anio\":         int(anio),
                \"mes\":          mes,
                \"semana_iso\":   semana_iso,
                \"semana_label\": sem_s,
                \"monto\":        monto,
            })
        else:
            # Movimiento del período inicial → va a flujos_efectivo
            # Determinar si es ingreso o egreso según sección
            es_ingreso = sec == \"INGRESOS\"
            import datetime
            fecha = datetime.date(int(anio), mes, 1)
            movimientos.append({
                \"sociedad\":                    soc,
                \"vertical\":                   None,
                \"belnr\":                      -(len(movimientos)+1),  # negativo = partida inicial
                \"gjahr\":                      int(anio),
                \"linea\":                      0,
                \"banco_codigo\":               None,
                \"banco_nombre\":               None,
                \"fecha_contable\":             fecha,
                \"anio\":                       int(anio),
                \"mes\":                        mes,
                \"semana_iso\":                 semana_iso or 1,
                \"semana_label\":               sem_s,
                \"cuenta_contrapartida\":       None,
                \"cuenta_contrapartida_nombre\": nom,
                \"ubicacion_codigo\":           None,
                \"ubicacion_nombre\":           None,
                \"seccion\":                    sec,
                \"nombre_categoria\":           nom,
                \"monto_ingreso\":              monto if es_ingreso else 0.0,
                \"monto_egreso\":               0.0 if es_ingreso else monto,
                \"monto_aplicado\":             None,
                \"tipo_transaccion\":           \"PARTIDA_INICIAL\",
                \"modulo\":                     \"INGRESOS\" if es_ingreso else \"EGRESOS\",
                \"cobro_num\":                  None,
                \"cobro_fecha\":                None,
                \"cliente_codigo\":             None,
                \"cliente_nombre\":             None,
                \"cobro_comentario\":           None,
                \"pago_num\":                   None,
                \"pago_fecha\":                 None,
                \"sn_codigo\":                  None,
                \"sn_nombre\":                  None,
                \"pago_comentario\":            None,
            })
    return saldos, movimientos"""

content = content.replace(OLD_PARTIDA, NEW_PARTIDA)
print("Fix 2 applied" if OLD_PARTIDA not in content else "Fix 2 FAILED")

# Fix 3: Update sincronizar_flujos to handle the new tuple return and insert movimientos
OLD_USE_PARTIDA = """    # Insertar saldos iniciales (ON CONFLICT DO NOTHING)
    partidas = _cargar_partida_inicial(xf)
    if partidas:
        with engine.begin() as conn:
            for p in partidas:
                conn.execute(text(\"\"\"
                    INSERT INTO flujos_saldo_inicial
                        (sociedad, anio, mes, semana_iso, semana_label, monto)
                    VALUES
                        (:sociedad, :anio, :mes, :semana_iso, :semana_label, :monto)
                    ON CONFLICT (sociedad, anio, mes, semana_iso) DO NOTHING
                \"\"\"), p)"""

NEW_USE_PARTIDA = """    # Insertar saldos iniciales y movimientos de PARTIDA INICIAL
    saldos_ini, movs_ini = _cargar_partida_inicial(xf)
    if saldos_ini:
        with engine.begin() as conn:
            for p in saldos_ini:
                conn.execute(text(\"\"\"
                    INSERT INTO flujos_saldo_inicial
                        (sociedad, anio, mes, semana_iso, semana_label, monto)
                    VALUES
                        (:sociedad, :anio, :mes, :semana_iso, :semana_label, :monto)
                    ON CONFLICT (sociedad, anio, mes, semana_iso) DO NOTHING
                \"\"\"), p)

    # Insertar movimientos del período inicial (ingresos/egresos de Oct, etc.)
    if movs_ini:
        with engine.begin() as conn:
            for m in movs_ini:
                try:
                    conn.execute(text(\"\"\"
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
                    \"\"\"), m)
                except Exception as e:
                    logger.warning(f\"Movimiento partida inicial omitido: {e}\")"""

content = content.replace(OLD_USE_PARTIDA, NEW_USE_PARTIDA)
print("Fix 3 applied" if OLD_USE_PARTIDA not in content else "Fix 3 FAILED")

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

import ast
ast.parse(open(path).read())
print("Syntax OK")
