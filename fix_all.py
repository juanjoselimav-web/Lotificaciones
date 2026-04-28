path = "/app/app/sync/sync_flujos.py"
with open(path, encoding="utf-8") as f:
    content = f.read()

# Fix 1: Add year filter - find the exact string in container
import re
# Find the fecha_inicio block
m = re.search(r'if fecha_inicio:.*?\.copy\(\)', content, re.DOTALL)
if m:
    print("Found block:", repr(m.group()[:100]))
    OLD = m.group()
    NEW = OLD + "\n\n    # Filtrar fechas con año inválido (epoch/corrupto)\n    df = df[df[\"FECHA_CONTABLE\"].dt.year >= 2000].copy()"
    content = content.replace(OLD, NEW, 1)
    print("Fix 1: OK")
else:
    # Try adding it after fecha filter line
    OLD2 = "    df = df[df[\"FECHA_CONTABLE\"].notna() & (df[\"FECHA_CONTABLE\"].dt.date >= fecha_inicio)].copy()"
    if OLD2 in content:
        content = content.replace(OLD2, OLD2 + "\n\n    # Filtrar fechas con año inválido\n    df = df[df[\"FECHA_CONTABLE\"].dt.year >= 2000].copy()")
        print("Fix 1: OK (alt)")
    else:
        # Add year filter right after the MOVIMIENTO_MANUAL filter
        OLD3 = "    df = df[df[\"TIPO_TRANSACCION\"] != \"MOVIMIENTO_MANUAL\"].copy()"
        if OLD3 in content:
            content = content.replace(OLD3, "    # Filtrar fechas con año inválido (epoch/corrupto)\n    df = df[df[\"FECHA_CONTABLE\"].dt.year >= 2000].copy()\n\n" + OLD3)
            print("Fix 1: OK (alt2)")
        else:
            print("Fix 1: FAILED - manual needed")

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

import ast
ast.parse(open(path).read())
print("Syntax OK")
print("Year filter present:", "dt.year >= 2000" in content)
