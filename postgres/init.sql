-- ============================================================
-- SISTEMA LOTIFICACIONES — SCHEMA POSTGRESQL
-- Versión: 1.0 | Fase: Inventario
-- ============================================================

-- Extensiones
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- Para búsqueda de texto

-- ============================================================
-- CATÁLOGOS BASE
-- ============================================================

CREATE TABLE IF NOT EXISTS cat_estatus_lote (
    id          SERIAL PRIMARY KEY,
    codigo      VARCHAR(50) UNIQUE NOT NULL,
    descripcion VARCHAR(100) NOT NULL,
    color_hex   VARCHAR(7) DEFAULT '#808080', -- Para UI
    orden       INT DEFAULT 0
);

INSERT INTO cat_estatus_lote (codigo, descripcion, color_hex, orden) VALUES
    ('DISPONIBLE',            'Disponible',              '#22c55e', 1),
    ('RESERVADO',             'Reservado',               '#f59e0b', 2),
    ('VENTA',                 'En Venta',                '#3b82f6', 3),
    ('BLOQUEADO',             'Bloqueado',               '#ef4444', 4),
    ('CANJE',                 'Canje',                   '#8b5cf6', 5),
    ('VENTA_ADMINISTRATIVA',  'Venta Administrativa',    '#06b6d4', 6)
ON CONFLICT (codigo) DO NOTHING;

CREATE TABLE IF NOT EXISTS cat_forma_pago (
    id          SERIAL PRIMARY KEY,
    codigo      VARCHAR(50) UNIQUE NOT NULL,
    descripcion VARCHAR(100) NOT NULL
);

INSERT INTO cat_forma_pago (codigo, descripcion) VALUES
    ('CONTADO',             'Contado'),
    ('CREDITOSININTERES',   'Crédito Sin Interés'),
    ('CREDITOCONINTERES',   'Crédito Con Interés')
ON CONFLICT (codigo) DO NOTHING;

-- ============================================================
-- PROYECTOS / EMPRESAS
-- ============================================================

CREATE TABLE IF NOT EXISTS proyectos (
    id                  SERIAL PRIMARY KEY,
    id_sap              INT UNIQUE,                    -- ID del archivo fuente
    empresa_sap         VARCHAR(100) UNIQUE NOT NULL,  -- SBO_EFICIENCIA_URBANA
    nombre_sociedad     VARCHAR(150) NOT NULL,          -- Eficiencia Urbana, S.A.
    nombre_proyecto     VARCHAR(150) NOT NULL,          -- Hacienda Jumay
    activo              BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Insertar los 16 proyectos del archivo
INSERT INTO proyectos (id_sap, empresa_sap, nombre_sociedad, nombre_proyecto) VALUES
    (1,  'SBO_EFICIENCIA_URBANA', 'Eficiencia Urbana, S. A.',       'Hacienda Jumay'),
    (2,  'SBO_SER_GEN_CCC',       'Servicios Generales CCC, S. A.', 'La Ceiba'),
    (3,  'SBO_ROSSIO',            'Rossio, S.A.',                   'Hacienda el Sol'),
    (4,  'SBO_FRUGALEX',          'Frugalex, S.A.',                 'Oasis Zacapa'),
    (5,  'SBO_OTTAVIA',           'Ottavia, S.A.',                  'Cañadas de Jalapa'),
    (6,  'SBO_UTILICA',           'Utilica, S.A.',                  'Condado Jutiapa'),
    (7,  'SBO_TEZZOLI',           'Tezzoli, S.A.',                  'Club Campestre Jumay'),
    (8,  'SBO_URBIVA_2',          'Urviba, S.A.',                   'Club del Bosque'),
    (9,  'SBO_GARBATELLA',        'GARBATELLA, S.A',                'Club Residencial Progreso'),
    (10, 'SBO_CAPIPOS',           'Capipos, S.A.',                  'Arboleda Santa Elena'),
    (11, 'SBO_OVEST',             'Ovest, S.A.',                    'Hacienda Santa Lucia'),
    (12, 'SBO_CORCOLLE',          'Corcolle, S.A.',                 'Hacienda El Cafetal Fase I'),
    (13, 'SBO_LEOFRENI',          'Leofreni, S.A',                  'Hacienda El Cafetal Fase II'),
    (14, 'SBO_GIBRALEON',         'Gibraleon, S.A.',                'Hacienda El Cafetal Fase III'),
    (15, 'SBO_TALOCCI',           'Talocci, S. A.',                 'Hacienda El Cafetal Fase IV'),
    (16, 'SBO_VILET',             'Vilet, S.A',                     'Celajes De Tecpan')
ON CONFLICT (empresa_sap) DO UPDATE SET
    nombre_sociedad = EXCLUDED.nombre_sociedad,
    nombre_proyecto = EXCLUDED.nombre_proyecto,
    updated_at      = NOW();

-- ============================================================
-- INVENTARIO DE LOTES
-- ============================================================

CREATE TABLE IF NOT EXISTS lotes (
    id                          SERIAL PRIMARY KEY,
    proyecto_id                 INT NOT NULL REFERENCES proyectos(id),
    unidad_key                  VARCHAR(50) NOT NULL,       -- Clave única del lote (GA-060)
    unidad_actual               VARCHAR(50),
    manzana                     VARCHAR(100),
    metraje_inventario          NUMERIC(10,4),
    metraje_orden               NUMERIC(10,4),
    medida_orden                VARCHAR(50),

    -- Precios
    precio_sin_descuento        NUMERIC(14,2) DEFAULT 0,
    descuento                   NUMERIC(14,2) DEFAULT 0,
    precio_con_descuento        NUMERIC(14,2) DEFAULT 0,
    precio_final                NUMERIC(14,2) DEFAULT 0,
    precio_base_m2              NUMERIC(14,2),
    precio_esquina              NUMERIC(14,2) DEFAULT 0,
    valor_terreno               NUMERIC(14,2),
    total_intereses             NUMERIC(14,2) DEFAULT 0,
    cuota_mantenimiento         VARCHAR(50),
    es_esquina                  BOOLEAN DEFAULT FALSE,

    -- Estado
    estatus                     VARCHAR(50),               -- Normalizado
    estatus_raw                 VARCHAR(100),              -- Valor original del archivo
    status_promesa_compraventa  VARCHAR(100),
    status_informe_ponf         VARCHAR(100),
    forma_pago                  VARCHAR(50),

    -- Cliente (cuando está vendido/reservado)
    doc_num                     INT,
    card_code                   VARCHAR(50),
    card_name                   VARCHAR(200),
    telefono_cliente            VARCHAR(100),
    vendedor                    VARCHAR(200),

    -- Fechas
    fecha_venta                 DATE,
    fecha_solicitud_pcv         DATE,
    fecha_inicial_cobro         DATE,
    fecha_final_cobro           DATE,
    fecha_escrituracion         DATE,
    fecha_firma_pcv             DATE,
    fecha_vencimiento_pcv       DATE,
    plazo                       INT,

    -- Cartera
    pagado_capital              NUMERIC(14,2) DEFAULT 0,
    pagado_interes              NUMERIC(14,2) DEFAULT 0,
    pendiente_capital           NUMERIC(14,2) DEFAULT 0,
    pendiente_interes           NUMERIC(14,2) DEFAULT 0,
    cuotas_pagadas              INT DEFAULT 0,
    cuotas_pendientes           INT DEFAULT 0,
    saldo_cliente               NUMERIC(14,2) DEFAULT 0,

    -- Facturación
    facturacion_70              NUMERIC(14,2) DEFAULT 0,
    fecha_facturacion_70        DATE,
    doc_fac_sap_70              VARCHAR(50),
    facturacion_30              NUMERIC(14,2) DEFAULT 0,
    fecha_facturacion_30        DATE,
    doc_fac_sap_30              VARCHAR(50),

    -- Control de sincronización
    fuente                      VARCHAR(50) DEFAULT 'CONSBA', -- CONSBA o SBO_xxx
    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (proyecto_id, unidad_key)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_lotes_proyecto    ON lotes(proyecto_id);
CREATE INDEX IF NOT EXISTS idx_lotes_estatus     ON lotes(estatus);
CREATE INDEX IF NOT EXISTS idx_lotes_unidad_key  ON lotes(unidad_key);
CREATE INDEX IF NOT EXISTS idx_lotes_card_name   ON lotes USING gin(card_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_lotes_manzana     ON lotes(manzana);

-- ============================================================
-- USUARIOS Y ROLES
-- ============================================================

CREATE TABLE IF NOT EXISTS roles (
    id          SERIAL PRIMARY KEY,
    nombre      VARCHAR(50) UNIQUE NOT NULL,
    descripcion VARCHAR(200),
    nivel       INT DEFAULT 1  -- 1=viewer, 2=analista, 3=gerente, 4=admin
);

INSERT INTO roles (nombre, descripcion, nivel) VALUES
    ('ADMIN',    'Acceso total al sistema y configuración', 4),
    ('GERENTE',  'Ve todos los proyectos y reportes ejecutivos', 3),
    ('ANALISTA', 'Ve proyectos asignados con detalle financiero', 2),
    ('ASESOR',   'Ve solo su cartera de clientes y lotes asignados', 1),
    ('VIEWER',   'Solo lectura de inventario de proyectos asignados', 1)
ON CONFLICT (nombre) DO NOTHING;

CREATE TABLE IF NOT EXISTS usuarios (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email           VARCHAR(150) UNIQUE NOT NULL,
    nombre          VARCHAR(150) NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    rol_id          INT NOT NULL REFERENCES roles(id),
    activo          BOOLEAN DEFAULT TRUE,
    ultimo_acceso   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Tabla de permisos por proyecto (qué proyectos puede ver cada usuario)
CREATE TABLE IF NOT EXISTS usuario_proyectos (
    usuario_id  UUID NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    proyecto_id INT NOT NULL REFERENCES proyectos(id) ON DELETE CASCADE,
    PRIMARY KEY (usuario_id, proyecto_id)
);

-- ============================================================
-- SINCRONIZACIÓN Y AUDITORÍA
-- ============================================================

CREATE TABLE IF NOT EXISTS sync_log (
    id                  SERIAL PRIMARY KEY,
    archivo             VARCHAR(100) NOT NULL, -- INVENTARIO, FLUJOS, OV_CARTERA
    inicio              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fin                 TIMESTAMPTZ,
    estado              VARCHAR(20) DEFAULT 'EJECUTANDO', -- EJECUTANDO, EXITOSO, ERROR
    registros_leidos    INT DEFAULT 0,
    registros_insertados INT DEFAULT 0,
    registros_actualizados INT DEFAULT 0,
    registros_error     INT DEFAULT 0,
    mensaje_error       TEXT,
    detalles            JSONB
);

CREATE INDEX IF NOT EXISTS idx_sync_log_archivo ON sync_log(archivo);
CREATE INDEX IF NOT EXISTS idx_sync_log_inicio  ON sync_log(inicio DESC);

CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    tabla       VARCHAR(100) NOT NULL,
    registro_id VARCHAR(100) NOT NULL,
    accion      VARCHAR(20) NOT NULL,  -- INSERT, UPDATE, DELETE
    usuario_id  UUID REFERENCES usuarios(id),
    campo       VARCHAR(100),
    valor_antes TEXT,
    valor_nuevo TEXT,
    ip_origen   INET,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_tabla     ON audit_log(tabla);
CREATE INDEX IF NOT EXISTS idx_audit_registro  ON audit_log(registro_id);
CREATE INDEX IF NOT EXISTS idx_audit_created   ON audit_log(created_at DESC);

-- ============================================================
-- VISTAS ÚTILES PARA EL DASHBOARD
-- ============================================================

-- Vista: Resumen de inventario por proyecto
CREATE OR REPLACE VIEW v_resumen_inventario AS
SELECT
    p.id                                              AS proyecto_id,
    p.nombre_proyecto,
    p.nombre_sociedad,
    p.empresa_sap,
    COUNT(l.id)                                       AS total_lotes,
    COUNT(l.id) FILTER (WHERE l.estatus = 'DISPONIBLE')         AS disponibles,
    COUNT(l.id) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO')) AS vendidos_reservados,
    COUNT(l.id) FILTER (WHERE l.estatus = 'BLOQUEADO')          AS bloqueados,
    COUNT(l.id) FILTER (WHERE l.estatus = 'CANJE')              AS canjes,
    COALESCE(SUM(l.precio_final) FILTER (WHERE l.estatus = 'DISPONIBLE'), 0)       AS valor_disponible,
    COALESCE(SUM(l.precio_final) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO')), 0) AS valor_comprometido,
    COALESCE(SUM(l.precio_final), 0)                            AS valor_total_inventario,
    ROUND(
        COUNT(l.id) FILTER (WHERE l.estatus IN ('VENTA','RESERVADO'))::NUMERIC
        / NULLIF(COUNT(l.id), 0) * 100, 2
    )                                                           AS porcentaje_absorcion,
    MAX(l.updated_at)                                           AS ultima_actualizacion
FROM proyectos p
LEFT JOIN lotes l ON l.proyecto_id = p.id
WHERE p.activo = TRUE
GROUP BY p.id, p.nombre_proyecto, p.nombre_sociedad, p.empresa_sap;

-- Vista: Detalle de lotes con nombre de proyecto
CREATE OR REPLACE VIEW v_lotes_detalle AS
SELECT
    l.*,
    p.nombre_proyecto,
    p.nombre_sociedad,
    p.empresa_sap
FROM lotes l
JOIN proyectos p ON p.id = l.proyecto_id;

-- ============================================================
-- FUNCIÓN: Normalizar estatus del archivo fuente
-- ============================================================
CREATE OR REPLACE FUNCTION normalizar_estatus(estatus_raw TEXT)
RETURNS VARCHAR(50) AS $$
BEGIN
    CASE UPPER(TRIM(estatus_raw))
        WHEN 'DISPONIBLE'            THEN RETURN 'DISPONIBLE';
        WHEN 'DISPONIBLE '           THEN RETURN 'DISPONIBLE';
        WHEN 'VENTA'                 THEN RETURN 'VENTA';
        WHEN ' VENTA'                THEN RETURN 'VENTA';
        WHEN 'VENDIDO'               THEN RETURN 'VENTA';
        WHEN 'RESERVADO'             THEN RETURN 'RESERVADO';
        WHEN 'BLOQUEADO'             THEN RETURN 'BLOQUEADO';
        WHEN 'BLOQUEADA'             THEN RETURN 'BLOQUEADO';
        WHEN 'CANJE A'               THEN RETURN 'CANJE';
        WHEN 'CANJEA'                THEN RETURN 'CANJE';
        WHEN 'CANJE '                THEN RETURN 'CANJE';
        WHEN 'VENTA ADMON'           THEN RETURN 'VENTA_ADMINISTRATIVA';
        WHEN 'VENTA ADMI'            THEN RETURN 'VENTA_ADMINISTRATIVA';
        WHEN 'VENTA ADMINISTRATIVA'  THEN RETURN 'VENTA_ADMINISTRATIVA';
        ELSE RETURN 'DISPONIBLE';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================
-- TRIGGER: updated_at automático
-- ============================================================
CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_lotes_updated_at
    BEFORE UPDATE ON lotes
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER trg_proyectos_updated_at
    BEFORE UPDATE ON proyectos
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER trg_usuarios_updated_at
    BEFORE UPDATE ON usuarios
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- MÓDULO CARTERA — FASE 2
-- ============================================================

CREATE TABLE IF NOT EXISTS ov_cartera (
    id                      SERIAL PRIMARY KEY,
    empresa                 VARCHAR(100) NOT NULL,
    doc_entry               INTEGER NOT NULL,
    doc_num                 INTEGER,
    doc_date                DATE,
    tax_date                DATE,
    card_code               VARCHAR(50),
    card_name               VARCHAR(200),
    slp_code                INTEGER,
    slp_name                VARCHAR(150),
    referencia_manzana_lote VARCHAR(200),
    codigo_lote             VARCHAR(50),
    fecha_venta_lote        DATE,
    plazo                   VARCHAR(20),
    forma_pago              VARCHAR(50),
    status_ov               VARCHAR(50),
    line_num                INTEGER NOT NULL,
    item_code               VARCHAR(50),
    descripcion             VARCHAR(100),
    quantity                INTEGER DEFAULT 1,
    price                   NUMERIC(14,2) DEFAULT 0,
    disc_prcnt              NUMERIC(5,2) DEFAULT 0,
    line_total              NUMERIC(14,2) DEFAULT 0,
    g_total                 NUMERIC(14,2) DEFAULT 0,
    fecha_programada_cobro  DATE,
    line_status             VARCHAR(5),   -- O=abierta, C=cerrada
    tipo_linea              VARCHAR(5),   -- BB=capital, S=interes, N=excluir
    saldo_pendiente         NUMERIC(14,2) DEFAULT 0,
    -- Control
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (empresa, doc_entry, line_num)
);

CREATE INDEX IF NOT EXISTS idx_ovc_card_code      ON ov_cartera(card_code);
CREATE INDEX IF NOT EXISTS idx_ovc_empresa        ON ov_cartera(empresa);
CREATE INDEX IF NOT EXISTS idx_ovc_fecha_cobro    ON ov_cartera(fecha_programada_cobro);
CREATE INDEX IF NOT EXISTS idx_ovc_line_status    ON ov_cartera(line_status);
CREATE INDEX IF NOT EXISTS idx_ovc_tipo_linea     ON ov_cartera(tipo_linea);
CREATE INDEX IF NOT EXISTS idx_ovc_slp_code       ON ov_cartera(slp_code);

CREATE TABLE IF NOT EXISTS desistimientos (
    id                          SERIAL PRIMARY KEY,
    empresa                     VARCHAR(100),
    no_orden_venta              INTEGER,
    codigo_cliente              VARCHAR(50),
    nombre_cliente              VARCHAR(200),
    lote                        VARCHAR(200),
    media_orden                 VARCHAR(50),
    metraje_orden               NUMERIC(10,4),
    asesor_venta                VARCHAR(150),
    status_informe_ponf         VARCHAR(100),
    status_promesa_compraventa  VARCHAR(100),
    fecha_solicitud_pcv         DATE,
    fecha_venta                 DATE,
    fecha_inicio_cobro          DATE,
    fecha_desistimiento         DATE,
    plazo                       VARCHAR(20),
    precio_venta                NUMERIC(14,2),
    descuento                   NUMERIC(14,2),
    precio_con_descuento        NUMERIC(14,2),
    valor_cuota_anticipo        NUMERIC(14,2),
    valor_cuota_gastos_admin    NUMERIC(14,2),
    pendiente_tramite_anticipo  NUMERIC(14,2),
    pendiente_tramite_gastos    NUMERIC(14,2),
    cuotas_pagadas              INTEGER,
    motivo_desistimiento        TEXT,
    pagado_capital              NUMERIC(14,2),
    pagado_gastos_admin         NUMERIC(14,2),
    retenido_facturado          NUMERIC(14,2),
    reintegrado_cliente         NUMERIC(14,2),
    total_desistimiento         NUMERIC(14,2),
    no_cheque                   VARCHAR(50),
    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (empresa, no_orden_venta, lote)
);

CREATE INDEX IF NOT EXISTS idx_desist_empresa ON desistimientos(empresa);
CREATE INDEX IF NOT EXISTS idx_desist_cliente ON desistimientos(codigo_cliente);
CREATE INDEX IF NOT EXISTS idx_desist_fecha   ON desistimientos(fecha_desistimiento);

-- ============================================================
-- VISTAS DE CARTERA
-- ============================================================

-- Resumen por cliente (solo BB+S, excluye N)
CREATE OR REPLACE VIEW v_cartera_clientes AS
SELECT
    empresa,
    card_code,
    card_name,
    slp_name                                                            AS asesor,
    COUNT(DISTINCT doc_entry)                                           AS num_lotes,
    SUM(CASE WHEN tipo_linea='BB' AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS capital_pendiente,
    SUM(CASE WHEN tipo_linea='S'  AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS intereses_pendientes,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN saldo_pendiente ELSE 0 END) AS saldo_total,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
             AND fecha_programada_cobro < CURRENT_DATE THEN saldo_pendiente ELSE 0 END) AS monto_vencido,
    MIN(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
             AND fecha_programada_cobro < CURRENT_DATE THEN fecha_programada_cobro END) AS primera_fecha_vencida,
    MAX(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN fecha_programada_cobro END) AS ultima_fecha_cobro,
    CASE
        WHEN SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
                      AND fecha_programada_cobro < CURRENT_DATE THEN saldo_pendiente ELSE 0 END) > 0 THEN 'VENCIDO'
        ELSE 'AL_DIA'
    END AS estado_cartera
FROM ov_cartera
WHERE tipo_linea IN ('BB','S')
GROUP BY empresa, card_code, card_name, slp_name;

-- KPIs globales de cartera
CREATE OR REPLACE VIEW v_cartera_kpis AS
SELECT
    SUM(CASE WHEN tipo_linea='BB' AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS capital_total,
    SUM(CASE WHEN tipo_linea='S'  AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS intereses_total,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN saldo_pendiente ELSE 0 END) AS cartera_total,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
             AND fecha_programada_cobro < CURRENT_DATE THEN saldo_pendiente ELSE 0 END) AS mora_total,
    COUNT(DISTINCT CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN card_code END) AS clientes_activos,
    COUNT(DISTINCT CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
                        AND fecha_programada_cobro < CURRENT_DATE THEN card_code END) AS clientes_vencidos,
    -- Cobranza proyectada
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
             AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+30 THEN saldo_pendiente ELSE 0 END) AS cobro_30d,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
             AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+60 THEN saldo_pendiente ELSE 0 END) AS cobro_60d,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
             AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+90 THEN saldo_pendiente ELSE 0 END) AS cobro_90d,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
             AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE+365 THEN saldo_pendiente ELSE 0 END) AS cobro_365d
FROM ov_cartera;

-- Proyección mensual de cobros (próximos 12 meses)
CREATE OR REPLACE VIEW v_cobranza_mensual AS
SELECT
    DATE_TRUNC('month', fecha_programada_cobro)::DATE AS mes,
    empresa,
    SUM(CASE WHEN tipo_linea='BB' THEN saldo_pendiente ELSE 0 END) AS capital,
    SUM(CASE WHEN tipo_linea='S'  THEN saldo_pendiente ELSE 0 END) AS intereses,
    SUM(saldo_pendiente) AS total,
    COUNT(*) AS num_cuotas
FROM ov_cartera
WHERE line_status='O'
  AND tipo_linea IN ('BB','S')
  AND fecha_programada_cobro BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', fecha_programada_cobro), empresa
ORDER BY mes, empresa;

-- Aging (antigüedad de cartera vencida)
CREATE OR REPLACE VIEW v_cartera_aging AS
SELECT
    empresa,
    card_code,
    card_name,
    CASE
        WHEN CURRENT_DATE - fecha_programada_cobro BETWEEN 1  AND 30  THEN '1-30 días'
        WHEN CURRENT_DATE - fecha_programada_cobro BETWEEN 31 AND 60  THEN '31-60 días'
        WHEN CURRENT_DATE - fecha_programada_cobro BETWEEN 61 AND 90  THEN '61-90 días'
        WHEN CURRENT_DATE - fecha_programada_cobro BETWEEN 91 AND 180 THEN '91-180 días'
        WHEN CURRENT_DATE - fecha_programada_cobro > 180              THEN '+180 días'
    END AS rango_vencimiento,
    CURRENT_DATE - fecha_programada_cobro AS dias_vencido,
    tipo_linea,
    saldo_pendiente,
    fecha_programada_cobro
FROM ov_cartera
WHERE line_status='O'
  AND tipo_linea IN ('BB','S')
  AND fecha_programada_cobro < CURRENT_DATE
  AND saldo_pendiente > 0
ORDER BY dias_vencido DESC;
