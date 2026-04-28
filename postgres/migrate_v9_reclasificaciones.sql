-- ============================================================
-- MIGRACIÓN v9: Reclasificaciones de Flujos de Efectivo
-- ============================================================

CREATE TABLE IF NOT EXISTS flujos_reclasificaciones (
    id                  SERIAL PRIMARY KEY,
    sociedad            VARCHAR(100) NOT NULL,
    cuenta              VARCHAR(20),
    cuenta_nombre       VARCHAR(150),
    monto               NUMERIC(18,2) NOT NULL,
    fecha_contable      DATE NOT NULL,
    anio                INTEGER NOT NULL,
    mes                 INTEGER NOT NULL,
    seccion_origen      VARCHAR(80) NOT NULL,
    nombre_origen       VARCHAR(150),
    seccion_destino     VARCHAR(80) NOT NULL,
    nombre_destino      VARCHAR(150),
    concepto            VARCHAR(150),
    sincronizado_en     TIMESTAMP DEFAULT NOW(),
    UNIQUE (sociedad, cuenta, fecha_contable, seccion_origen, seccion_destino, monto)
);

CREATE INDEX IF NOT EXISTS idx_reclasif_sociedad_fecha 
    ON flujos_reclasificaciones (sociedad, anio, mes);

COMMENT ON TABLE flujos_reclasificaciones IS 
    'Reclasificaciones contables entre secciones del flujo — no afectan flujo neto, solo redistribuyen entre buckets';
