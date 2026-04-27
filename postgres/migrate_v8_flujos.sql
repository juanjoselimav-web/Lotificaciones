-- ============================================================
-- MIGRACIÓN v8: Flujos de Efectivo (Histórico)
-- ============================================================

-- Tabla principal: transacciones individuales por sociedad
CREATE TABLE IF NOT EXISTS flujos_efectivo (
    id                      SERIAL PRIMARY KEY,
    sociedad                VARCHAR(100) NOT NULL,
    vertical                VARCHAR(50),
    belnr                   INTEGER,
    gjahr                   INTEGER,
    linea                   INTEGER DEFAULT 0,
    banco_codigo            VARCHAR(20),
    banco_nombre            VARCHAR(150),
    fecha_contable          DATE NOT NULL,
    anio                    INTEGER NOT NULL,
    mes                     INTEGER NOT NULL,        -- 1-12
    semana_iso              INTEGER NOT NULL,        -- semana ISO del año
    semana_label            VARCHAR(10),             -- ej: 'S42'
    cuenta_contrapartida    VARCHAR(20),
    cuenta_contrapartida_nombre VARCHAR(150),
    ubicacion_codigo        VARCHAR(20),
    ubicacion_nombre        VARCHAR(150),
    seccion                 VARCHAR(80),             -- de ESTRUCTURA RDI
    nombre_categoria        VARCHAR(150),            -- de ESTRUCTURA RDI
    monto_ingreso           NUMERIC(18,2) DEFAULT 0,
    monto_egreso            NUMERIC(18,2) DEFAULT 0,
    monto_aplicado          NUMERIC(18,2),
    tipo_transaccion        VARCHAR(30),
    modulo                  VARCHAR(20),             -- INGRESOS / EGRESOS
    cobro_num               INTEGER,
    cobro_fecha             DATE,
    cliente_codigo          VARCHAR(30),
    cliente_nombre          VARCHAR(200),
    cobro_comentario        TEXT,
    pago_num                INTEGER,
    pago_fecha              DATE,
    sn_codigo               VARCHAR(30),
    sn_nombre               VARCHAR(200),
    pago_comentario         TEXT,
    fuente                  VARCHAR(50) DEFAULT 'FLUJOS_EFECTIVO_XLSX',
    sincronizado_en         TIMESTAMP DEFAULT NOW(),
    UNIQUE (sociedad, belnr, gjahr, linea)
);

-- Tabla saldo inicial (sólo primer período; los siguientes se calculan)
CREATE TABLE IF NOT EXISTS flujos_saldo_inicial (
    id          SERIAL PRIMARY KEY,
    sociedad    VARCHAR(100) NOT NULL,
    anio        INTEGER NOT NULL,
    mes         INTEGER NOT NULL,
    semana_iso  INTEGER,
    semana_label VARCHAR(10),
    monto       NUMERIC(18,2) NOT NULL,
    UNIQUE (sociedad, anio, mes, semana_iso)
);

-- Índices de rendimiento
CREATE INDEX IF NOT EXISTS idx_flujos_sociedad_fecha   ON flujos_efectivo (sociedad, fecha_contable);
CREATE INDEX IF NOT EXISTS idx_flujos_sociedad_anio_mes ON flujos_efectivo (sociedad, anio, mes);
CREATE INDEX IF NOT EXISTS idx_flujos_seccion          ON flujos_efectivo (sociedad, seccion);

-- Vista agregada mensual (útil para el router)
CREATE OR REPLACE VIEW v_flujos_mensual AS
SELECT
    sociedad,
    anio,
    mes,
    TO_CHAR(DATE_TRUNC('month', fecha_contable), 'YYYY-MM') AS periodo,
    seccion,
    nombre_categoria,
    SUM(monto_ingreso) AS total_ingresos,
    SUM(monto_egreso)  AS total_egresos,
    COUNT(*)           AS num_transacciones
FROM flujos_efectivo
GROUP BY sociedad, anio, mes, DATE_TRUNC('month', fecha_contable), seccion, nombre_categoria;

-- Vista agregada semanal
CREATE OR REPLACE VIEW v_flujos_semanal AS
SELECT
    sociedad,
    anio,
    semana_iso,
    semana_label,
    seccion,
    nombre_categoria,
    SUM(monto_ingreso) AS total_ingresos,
    SUM(monto_egreso)  AS total_egresos,
    COUNT(*)           AS num_transacciones
FROM flujos_efectivo
GROUP BY sociedad, anio, semana_iso, semana_label, seccion, nombre_categoria;

-- Vista agregada anual
CREATE OR REPLACE VIEW v_flujos_anual AS
SELECT
    sociedad,
    anio,
    seccion,
    nombre_categoria,
    SUM(monto_ingreso) AS total_ingresos,
    SUM(monto_egreso)  AS total_egresos,
    COUNT(*)           AS num_transacciones
FROM flujos_efectivo
GROUP BY sociedad, anio, seccion, nombre_categoria;

COMMENT ON TABLE flujos_efectivo IS 'Transacciones históricas de flujo de efectivo por sociedad, sincronizadas desde Excel SAP';
COMMENT ON TABLE flujos_saldo_inicial IS 'Saldo inicial del primer período contable por sociedad';
