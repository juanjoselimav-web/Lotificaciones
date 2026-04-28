import re

path = "/app/app/sync/sync_flujos.py"
with open(path, encoding="utf-8") as f:
    content = f.read()

# Find and replace entire _preprocesar_df function
old_match = re.search(r'def _preprocesar_df\(df.*?return df\n\n', content, re.DOTALL)
if not old_match:
    print("ERROR: function not found")
    exit(1)

NEW_FUNC = '''def _preprocesar_df(df, sociedad, fecha_inicio):
    df = df.copy()
    df["FECHA_CONTABLE"] = __import__("pandas").to_datetime(df["FECHA_CONTABLE"], errors="coerce")
    df["LINEA"] = df["LINEA"].fillna(0)

    if fecha_inicio:
        df = df[df["FECHA_CONTABLE"].notna() & (df["FECHA_CONTABLE"].dt.date >= fecha_inicio)].copy()

    df = df[df["TIPO_TRANSACCION"] != "MOVIMIENTO_MANUAL"].copy()

    mask_ing   = df["MODULO"] == "INGRESOS"
    mask_neto  = df["CUENTA_CONTRAPARTIDA_NOMBRE"].isin({CTA_NETO_A, CTA_NETO_B})
    mask_valid = df["TIPO_TRANSACCION"].isin(TIPOS_INGRESO_VALIDOS)
    df = df[~mask_ing | mask_valid | mask_neto].copy()

    mask_elim = (df["MODULO"] == "INGRESOS") & df["CUENTA_CONTRAPARTIDA_NOMBRE"].isin(CTAS_ELIMINAR_INGRESOS)
    df = df[~mask_elim].copy()

    try:
        grupos = df.groupby(["BELNR", "GJAHR"])["MODULO"].apply(lambda x: set(x.dropna()))
        cross_idx = grupos[grupos.apply(lambda x: "INGRESOS" in x and "EGRESOS" in x)].index
        cross_set = set(map(tuple, cross_idx.tolist()))
        mask_cross = df.apply(lambda r: (r["BELNR"], r["GJAHR"]) in cross_set, axis=1)
        df = df[~mask_cross].copy()
    except Exception as e:
        pass

    mask_a = (df["MODULO"] == "INGRESOS") & (df["CUENTA_CONTRAPARTIDA_NOMBRE"] == CTA_NETO_A)
    mask_b = (df["MODULO"] == "INGRESOS") & (df["CUENTA_CONTRAPARTIDA_NOMBRE"] == CTA_NETO_B)

    if mask_a.any() or mask_b.any():
        rows_a = df[mask_a].copy()
        rows_b = df[mask_b].copy()
        df = df[~mask_a & ~mask_b].copy()

        import pandas as pd
        sum_a = rows_a.groupby(["BELNR","GJAHR"])["MONTO_PRORRATEADO"].sum()
        sum_b = rows_b.groupby(["BELNR","GJAHR"])["MONTO_PRORRATEADO"].sum()
        all_keys = sum_a.index.union(sum_b.index)

        template_src = rows_a if len(rows_a) > 0 else rows_b
        tmpl_dict = {}
        for _, r in template_src.iterrows():
            k = (r["BELNR"], r["GJAHR"])
            if k not in tmpl_dict:
                tmpl_dict[k] = r.copy()

        neto_list = []
        for key in all_keys:
            a_val = float(sum_a.get(key, 0))
            b_val = float(sum_b.get(key, 0))
            neto  = a_val - b_val
            if abs(neto) < 0.01:
                continue
            tmpl = tmpl_dict.get(key)
            if tmpl is None:
                continue
            new_row = tmpl.copy()
            new_row["MONTO_PRORRATEADO"]           = abs(neto)
            new_row["CUENTA_CONTRAPARTIDA_NOMBRE"]  = "Otros Ingresos"
            new_row["CUENTA_CONTRAPARTIDA"]         = float("nan")
            new_row["MODULO"]                       = "INGRESOS" if neto > 0 else "EGRESOS"
            new_row["LINEA"]                        = 9999
            neto_list.append(new_row)

        if neto_list:
            cols = [c for c in df.columns]
            neto_data = []
            for nr in neto_list:
                row_dict = {c: nr.get(c, None) for c in cols}
                neto_data.append(row_dict)
            df_neto = pd.DataFrame(neto_data, columns=cols)
            df = pd.concat([df, df_neto], ignore_index=True)

    return df

'''

content = content.replace(old_match.group(), NEW_FUNC)
with open(path, "w", encoding="utf-8") as f:
    f.write(content)

import ast
ast.parse(open(path).read())
print("OK: syntax valid, template_by_key replaced with tmpl_dict")
