# Integración al sidebar del tablero RV4

## 1. Copiar archivos al servidor

Copia toda la carpeta `presentacion/` dentro del directorio público del tablero. Sugerencia de ubicación:

```
<raiz_tablero>/static/presentacion/
  ├── presentacion.html
  ├── presentacion.js
  └── logo_rv4.png
```

Si tu FastAPI ya sirve `/static/`, los archivos quedan accesibles en:
- `/static/presentacion/presentacion.html`

Si usas otra ruta, ajusta el `href` del paso siguiente.

## 2. Agregar el botón en el sidebar

Abre el HTML donde se renderiza el sidebar (`dashboard.html` o el componente que define la navegación) y agrega este bloque dentro de la lista de items:

```html
<a href="/static/presentacion/presentacion.html"
   target="_blank"
   class="sidebar-item"
   id="btn-presentacion-junta">
  <svg width="20" height="20" fill="none" viewBox="0 0 24 24"
       stroke="currentColor" stroke-width="2">
    <rect x="2" y="4" width="20" height="14" rx="2"/>
    <line x1="8" y1="20" x2="16" y2="20"/>
    <line x1="12" y1="18" x2="12" y2="20"/>
  </svg>
  <span>Presentación Junta</span>
</a>
```

Ajusta la clase (`sidebar-item`) al nombre real que use tu sidebar.

## 3. SSO automático (opcional, recomendado)

Para que la presentación abra **ya autenticada** sin que el usuario tenga que iniciar sesión otra vez, modifica el `href` del botón para inyectar el token:

```html
<a id="btn-presentacion-junta" target="_blank" class="sidebar-item">
  <svg>…</svg>
  <span>Presentación Junta</span>
</a>

<script>
  document.getElementById('btn-presentacion-junta').addEventListener('click', e => {
    e.preventDefault();
    const token = localStorage.getItem('token');
    const usuario = localStorage.getItem('usuario');
    const url = `/static/presentacion/presentacion.html?token=${encodeURIComponent(token||'')}&usuario=${btoa(usuario||'')}`;
    window.open(url, '_blank');
  });
</script>
```

La presentación ya está preparada: lee `?token=` y `?usuario=` de la URL, los guarda en su propio localStorage y limpia los parámetros.

## 4. Funcionalidad de la presentación

| Control | Ubicación | Acción |
|---|---|---|
| Selector de período | Esquina inferior izquierda | Mes + Año — recarga datos al cambiar |
| Toggle Claro/Oscuro | Esquina superior derecha | Persistente en localStorage |
| Imprimir / Guardar PDF | Esquina superior derecha | Abre diálogo de impresión |
| Navegación slides | Flechas laterales · ←/→ · espacio | 25 slides totales |
| Filtro de sociedad (Flujos) | Slide 21 | Cambia entre las 13 sociedades |

## 5. Endpoints que consume

La presentación reusa los mismos endpoints del dashboard:

- `/api/inventario/resumen`
- `/api/ventas/kpis`, `/api/ventas/mezcla-financiera`, `/api/ventas/analisis-financiero`
- `/api/ventas/por-vendedor`, `/api/ventas/metas`, `/api/ventas/tendencia-mensual`
- `/api/ventas/pcv/kpis`, `/api/ventas/registros-revision`
- `/api/cartera/kpis`, `/api/cartera/aging`, `/api/cartera/proyeccion-mensual`
- `/api/cartera/alertas`, `/api/cartera/desistimientos`
- `/api/flujos/resumen`

Si algún endpoint difiere en path o forma del payload, ajusta la sección correspondiente en `presentacion.js` (cada función `loadXxx()` está claramente nombrada).

## 6. Modo demo

Si abres `presentacion.html` sin sesión activa, carga automáticamente con datos de ejemplo (etiquetada como "Modo demo · sin sesión" en la esquina inferior derecha). Útil para revisar diseño sin tocar la API.
