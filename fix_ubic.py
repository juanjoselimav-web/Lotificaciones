path = '/app/app/sync/sync_flujos.py'
with open(path) as f:
    content = f.read()

old = '        ubic  = str(row.get("UBICACION_CODIGO", "") or "").strip() or None'
new = '        ubic_raw = str(row.get("UBICACION_CODIGO", "") or "").strip()\n        ubic  = None if ubic_raw.lower() in ("nan", "none", "null", "") else ubic_raw'

content = content.replace(old, new)
with open(path, 'w') as f:
    f.write(content)
print('OK' if 'ubic_raw' in content else 'FAIL')
