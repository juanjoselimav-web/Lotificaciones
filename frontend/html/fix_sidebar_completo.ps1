# ================================================================
# fix_sidebar_completo.ps1
# Arregla DOS problemas en todos los HTML del tablero:
#
# 1. Agrega "Proyecciones" en la seccion Financiero,
#    justo despues de "Flujos de Efectivo"
#
# 2. Mueve "Junta Directiva" a la seccion Administracion
#    como nav-item normal (actualmente esta oculto por JS)
#
# Ejecutar desde: C:\lotificaciones-git\frontend\html
# Comando: .\fix_sidebar_completo.ps1
# ================================================================

$archivos = @(
    "dashboard.html",
    "inventario.html",
    "ventas.html",
    "cartera.html",
    "flujos.html",
    "proyecciones.html",
    "jd.html",
    "usuarios.html",
    "sync-logs.html"
)

$exito  = 0
$errores = 0

foreach ($archivo in $archivos) {
    if (-not (Test-Path $archivo)) {
        Write-Host "OMITIDO (no existe): $archivo" -ForegroundColor Yellow
        continue
    }

    $contenido = [System.IO.File]::ReadAllText((Resolve-Path $archivo), [System.Text.Encoding]::UTF8)
    $original  = $contenido
    $cambios   = @()

    # ────────────────────────────────────────────────────────────
    # FIX 1: Agregar link de Proyecciones despues de flujos.html
    # Solo si NO existe ya
    # ────────────────────────────────────────────────────────────
    if ($contenido -notmatch 'href="/proyecciones\.html"') {
        $linkFlujos = '<a class="nav-item" href="/flujos.html">'
        # Detectar si el archivo ES proyecciones.html (debe tener active)
        if ($archivo -eq "proyecciones.html") {
            $linkProyecciones = '      <a class="nav-item active" href="/proyecciones.html">' + "`n" +
                '        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>Proyecciones' + "`n" +
                '      </a>'
        } else {
            $linkProyecciones = '      <a class="nav-item" href="/proyecciones.html">' + "`n" +
                '        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>Proyecciones' + "`n" +
                '      </a>'
        }

        # Buscar el cierre del link de flujos (</a>) y agregar despues
        $patron = '(<a[^>]*href="/flujos\.html"[^>]*>[\s\S]*?</a>)'
        if ($contenido -match $patron) {
            $contenido = [regex]::Replace(
                $contenido,
                '(<a[^>]*href="/flujos\.html"[^>]*>[\s\S]*?</a>)',
                "`$1`n$linkProyecciones",
                [System.Text.RegularExpressions.RegexOptions]::None
            )
            $cambios += "Proyecciones agregado"
        }
    } else {
        $cambios += "Proyecciones ya existe"
    }

    # ────────────────────────────────────────────────────────────
    # FIX 2: Junta Directiva — sacar de adminSection y ponerla
    # como nav-item normal visible para todos
    # ────────────────────────────────────────────────────────────

    # Paso 2a: Eliminar el JS que oculta items de adminSection
    # La linea que hace el ocultamiento es:
    # document.querySelectorAll('#adminSection .nav-item:not(#btn-junta)').forEach(...)
    if ($contenido -match "adminSection .nav-item:not\(#btn-junta\)") {
        $contenido = [regex]::Replace(
            $contenido,
            "document\.querySelectorAll\('#adminSection \.nav-item:not\(#btn-junta\)'\)\.forEach[^;]+;",
            ""
        )
        $cambios += "JS ocultamiento adminSection eliminado"
    }

    # Paso 2b: Reemplazar el btn-junta con estilo correcto (nav-item normal)
    # Quitar el style inline que lo hace ver diferente
    if ($contenido -match 'id="btn-junta"') {
        # Si es jd.html debe tener active, el resto no
        if ($archivo -eq "jd.html") {
            $nuevoJunta = '<a class="nav-item active" id="btn-junta" href="/jd.html">'
        } else {
            $nuevoJunta = '<a class="nav-item" id="btn-junta" href="/jd.html">'
        }
        # Reemplazar cualquier variante del a con btn-junta (con o sin style, con o sin active)
        $contenido = [regex]::Replace(
            $contenido,
            '<a[^>]*id="btn-junta"[^>]*>',
            $nuevoJunta
        )
        $cambios += "btn-junta normalizado"
    }

    # ────────────────────────────────────────────────────────────
    # Guardar solo si hubo cambios reales
    # ────────────────────────────────────────────────────────────
    if ($contenido -ne $original) {
        # Guardar con LF (Linux/Docker)
        $contenido = $contenido -replace "`r`n", "`n"
        [System.IO.File]::WriteAllText(
            (Resolve-Path $archivo).Path,
            $contenido,
            [System.Text.Encoding]::UTF8
        )
        Write-Host ("OK: {0,-30} [{1}]" -f $archivo, ($cambios -join " | ")) -ForegroundColor Cyan
        $exito++
    } else {
        Write-Host ("SIN CAMBIOS: {0}" -f $archivo) -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor White
Write-Host "Archivos modificados: $exito" -ForegroundColor Green
if ($errores -gt 0) {
    Write-Host "Errores: $errores" -ForegroundColor Red
}
Write-Host ""
Write-Host "Siguiente paso:" -ForegroundColor Yellow
Write-Host "  cd C:\lotificaciones-git" -ForegroundColor Yellow
Write-Host "  docker compose up -d" -ForegroundColor Yellow
Write-Host "  (sin --build, solo reinicia nginx con los archivos nuevos)" -ForegroundColor Gray
