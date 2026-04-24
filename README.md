# 🏗️ RV4 Lotificaciones — Sistema de Gestión Inmobiliaria

## Stack
- **Frontend:** HTML + CSS + Vanilla JS (sin frameworks, máxima velocidad)
- **Backend:** Python 3.11 + FastAPI
- **Base de datos:** PostgreSQL 15
- **Servidor web:** Nginx
- **Contenedores:** Docker + Docker Compose
- **Sincronización:** APScheduler (cada 60 min) → OneDrive → PostgreSQL

---

## 📁 Estructura del Proyecto

```
lotificaciones/
├── docker-compose.yml              # Config base (dev + prod)
├── docker-compose.override.yml     # Solo desarrollo (Windows)
├── .env.dev                        # Variables desarrollo
├── .env.prod                       # Variables producción
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py                 # Entry point FastAPI + scheduler
│       ├── database.py             # Conexión PostgreSQL
│       ├── core/
│       │   ├── config.py           # Settings desde .env
│       │   └── security.py        # JWT + auth
│       ├── models/
│       │   └── models.py           # SQLAlchemy models
│       ├── routers/
│       │   ├── auth.py             # Login, usuarios
│       │   ├── inventario.py       # API inventario
│       │   └── sync.py             # Trigger sync manual
│       └── sync/
│           └── sync_inventario.py  # Job sincronización Excel → DB
├── frontend/
│   ├── Dockerfile
│   ├── nginx-frontend.conf
│   └── html/
│       ├── index.html              # Login
│       ├── dashboard.html          # Dashboard principal
│       └── inventario.html         # Tabla de lotes con filtros
├── postgres/
│   └── init.sql                    # Schema completo de BD
└── nginx/
    └── nginx.conf                  # Reverse proxy
```

---

## 🚀 Inicio Rápido — Desarrollo Local (Windows)

### Requisitos previos
- Docker Desktop instalado y corriendo
- VS Code con extensión Docker (opcional pero recomendado)
- Los archivos Excel en tu OneDrive local sincronizados

### Paso 1 — Clonar y preparar
```bash
cd lotificaciones
cp .env.dev .env
```

### Paso 2 — Verificar ruta OneDrive en docker-compose.override.yml
Abrir `docker-compose.override.yml` y verificar que la ruta sea correcta:
```yaml
- "C:/Users/jlima/OneDrive - rvcuatro.com/...:/data/sources:ro"
```

### Paso 3 — Levantar el sistema
```bash
docker-compose up --build
```

### Paso 4 — Acceder
| Servicio     | URL                          |
|--------------|------------------------------|
| Sistema      | http://localhost:5004        |
| API Docs     | http://localhost:5004/docs   |
| pgAdmin      | http://localhost:5050        |

### Credenciales por defecto (CAMBIAR EN PRODUCCIÓN)
- **Sistema:** admin@rvcuatro.com / Admin2024!
- **pgAdmin:** admin@rvcuatro.com / AdminDev2024!

---

## 🖥️ Despliegue en Servidor Linux (Producción)

### Paso 1 — Verificar OneDrive en Linux
```bash
# Verificar dónde sincroniza OneDrive en el servidor Linux
ls ~/
# Buscar carpeta OneDrive - rvcuatro.com
find ~/ -name "VERSION FINAL INVENTARIOS*" 2>/dev/null
```

### Paso 2 — Preparar variables de producción
```bash
cp .env.prod .env
nano .env  # Editar: POSTGRES_PASSWORD, SECRET_KEY, ONEDRIVE_PATH
```

**Generar SECRET_KEY segura:**
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### Paso 3 — Verificar nombres de archivos (Linux es case-sensitive)
```bash
# Los archivos tienen nombres con espacios y caracteres especiales
# Verificar nombre exacto:
ls "/home/jlima/OneDrive - rvcuatro.com/Finanzas - Desarrollos - Planificación financiera/19. Lotes/Inventario/2. Tablero Lotificaciones Preliminar (5010)/"
```

### Paso 4 — Levantar en producción (sin override)
```bash
# En producción NO usar docker-compose.override.yml
docker-compose -f docker-compose.yml up -d --build
```

### Paso 5 — Verificar
```bash
docker-compose ps
docker-compose logs backend --tail=50
curl http://localhost:5004/api/health
```

---

## ⚙️ Comandos Útiles

```bash
# Ver logs en tiempo real
docker-compose logs -f backend

# Reiniciar solo el backend
docker-compose restart backend

# Ver estado de sincronización
curl -H "Authorization: Bearer TOKEN" http://localhost:5004/api/sync/estado

# Trigger sync manual desde terminal
curl -X POST -H "Authorization: Bearer TOKEN" http://localhost:5004/api/sync/inventario

# Conectar a PostgreSQL directamente
docker-compose exec postgres psql -U loti_user -d lotificaciones_dev

# Backup de base de datos
docker-compose exec postgres pg_dump -U loti_user lotificaciones_dev > backup_$(date +%Y%m%d).sql
```

---

## 🔐 Seguridad — Checklist Producción

- [ ] Cambiar password admin por defecto
- [ ] Generar SECRET_KEY nueva y aleatoria
- [ ] Cambiar passwords de PostgreSQL y pgAdmin
- [ ] Deshabilitar /docs en producción (descomentar en nginx.conf)
- [ ] Configurar pgAdmin solo acceso interno (sin exponer puerto 5050)
- [ ] Verificar que volumen OneDrive sea solo lectura (:ro)
- [ ] Configurar HTTPS con Let's Encrypt (próxima fase)

---

## 📊 Módulos del Sistema

| Módulo        | Estado      | Descripción                              |
|---------------|-------------|------------------------------------------|
| Inventario    | ✅ FASE 1    | Dashboard + tabla lotes + sync OneDrive  |
| Ingresos      | 🔄 FASE 2    | Flujos reales + OV proyectados           |
| Egresos       | ⏳ FASE 3    | Urbanización + administración            |
| Flujo de Caja | ⏳ FASE 3    | Consolidado real vs proyectado           |
| Impuestos     | ⏳ FASE 4    | ISR, IVA, IUSI por proyecto              |

---

## 🔄 Sincronización OneDrive

El sistema lee automáticamente los archivos Excel cada **60 minutos**.

**Archivos monitoreados:**
- `VERSION FINAL INVENTARIOS LOTES V2.xlsx` → tabla `lotes`
- `FLUJOS DE EFECTIVO.xlsx` → *(Fase 2)*
- `OV (CARTERA), DESIST Y METAS CONSOLIDADO SAP.xlsx` → *(Fase 2)*

**Ver historial de sincronización:**
- En la app: Menu → Sincronización (rol Gerente o Admin)
- En pgAdmin: `SELECT * FROM sync_log ORDER BY inicio DESC LIMIT 20;`
