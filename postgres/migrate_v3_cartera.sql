-- Migración v3: Crear tablas de cartera
-- Ejecutar en la BD existente

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
    line_status             VARCHAR(5),
    tipo_linea              VARCHAR(5),
    saldo_pendiente         NUMERIC(14,2) DEFAULT 0,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (empresa, doc_entry, line_num)
);

CREATE INDEX IF NOT EXISTS idx_ovc_card_code   ON ov_cartera(card_code);
CREATE INDEX IF NOT EXISTS idx_ovc_empresa     ON ov_cartera(empresa);
CREATE INDEX IF NOT EXISTS idx_ovc_fecha_cobro ON ov_cartera(fecha_programada_cobro);
CREATE INDEX IF NOT EXISTS idx_ovc_line_status ON ov_cartera(line_status);
CREATE INDEX IF NOT EXISTS idx_ovc_tipo_linea  ON ov_cartera(tipo_linea);

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

-- Vistas
CREATE OR REPLACE VIEW v_cartera_kpis AS
SELECT
    SUM(CASE WHEN tipo_linea='BB' AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS capital_total,
    SUM(CASE WHEN tipo_linea='S'  AND line_status='O' THEN saldo_pendiente ELSE 0 END) AS intereses_total,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN saldo_pendiente ELSE 0 END) AS cartera_total,
    SUM(CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
             AND fecha_programada_cobro < CURRENT_DATE THEN saldo_pendiente ELSE 0 END) AS mora_total,
    COUNT(DISTINCT CASE WHEN line_status='O' AND tipo_linea IN ('BB','S') THEN card_code END) AS clientes_activos,
    COUNT(DISTINCT CASE WHEN line_status='O' AND tipo_linea IN ('BB','S')
                        AND fecha_programada_cobro < CURRENT_DATE THEN card_code END) AS clientes_vencidos
FROM ov_cartera;
