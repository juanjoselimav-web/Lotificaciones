-- ============================================================
-- MIGRACIÓN v4: Módulo de Ventas
-- ============================================================

-- Tabla de vendedores con asignación de equipo
CREATE TABLE IF NOT EXISTS vendedores (
    id              SERIAL PRIMARY KEY,
    nombre          VARCHAR(200) NOT NULL UNIQUE,
    equipo          VARCHAR(50)  DEFAULT 'SIN_ASIGNAR', -- CONSERSA, RV4, SIN_ASIGNAR
    activo          BOOLEAN DEFAULT TRUE,
    es_sistema      BOOLEAN DEFAULT FALSE, -- TRUE para registros como 'Canje A', 'Bloqueado', etc.
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Seed: vendedores conocidos con equipo ya asignado
INSERT INTO vendedores (nombre, equipo, activo, es_sistema) VALUES
-- CONSERSA
('Andres Figueroa',                          'CONSERSA', true,  false),
('Rafael Morales',                           'CONSERSA', true,  false),
('Velinda Duque',                            'CONSERSA', true,  false),
('Nataly Aguilar',                           'CONSERSA', true,  false),
('Jeremy Torres',                            'CONSERSA', true,  false),
('Catalina Serrano',                         'CONSERSA', true,  false),
('Alejandro Estrada',                        'CONSERSA', true,  false),
('Albin Salazar',                            'CONSERSA', true,  false),
('Alejandro Alvarez',                        'CONSERSA', true,  false),
('Judith Raxcaco',                           'CONSERSA', true,  false),
-- RV4 (pendiente confirmar — marcados SIN_ASIGNAR por ahora)
('Diego Andres Pérez Ardón',                 'SIN_ASIGNAR', true,  false),
('Eunices Chinchilla',                       'SIN_ASIGNAR', true,  false),
('Heidy Marroquin',                          'SIN_ASIGNAR', true,  false),
('Jeniffer Marilin Hernandez Jimenez',       'SIN_ASIGNAR', true,  false),
('Mario René Rivera Amaya',                  'SIN_ASIGNAR', true,  false),
('Melisa Sotto',                             'SIN_ASIGNAR', true,  false),
('Veronica Rosmery Lopez Hernández',         'SIN_ASIGNAR', true,  false),
('Vilma Sagastume',                          'SIN_ASIGNAR', true,  false),
('Walker Cortez',                            'SIN_ASIGNAR', true,  false),
('Alexander Hernandez',                      'SIN_ASIGNAR', true,  false),
('Axel Garcia',                              'SIN_ASIGNAR', true,  false),
('Evelin Esquivel',                          'SIN_ASIGNAR', true,  false),
-- Registros de sistema (no son vendedores reales)
('-Ningún empleado del departamento de ventas-', 'SIN_ASIGNAR', false, true),
('Canje A',                                  'SIN_ASIGNAR', false, true),
('Bloqueado',                                'SIN_ASIGNAR', false, true),
('Bloqueo Municipal',                        'SIN_ASIGNAR', false, true),
('Apartado Proyecto Aptos',                  'SIN_ASIGNAR', false, true),
('0',                                        'SIN_ASIGNAR', false, true)
ON CONFLICT (nombre) DO NOTHING;

-- Tabla de metas mensuales por vendedor/proyecto
CREATE TABLE IF NOT EXISTS metas_ventas (
    id              SERIAL PRIMARY KEY,
    responsable     VARCHAR(200) NOT NULL,
    proyecto        VARCHAR(200) NOT NULL,
    meta_consersa   INTEGER DEFAULT 0,
    meta_rv4        INTEGER DEFAULT 0,
    mes             INTEGER NOT NULL DEFAULT 0, -- 0 = aplica todos los meses
    año             INTEGER NOT NULL DEFAULT 2026,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (responsable, proyecto, mes, año)
);

-- Seed metas 2026 (mismas todos los meses según archivo)
INSERT INTO metas_ventas (responsable, proyecto, meta_consersa, meta_rv4, mes, año) VALUES
('Andres Figueroa',    'Cañadas de Jalapa',           15, 15, 0, 2026),
('Andres Figueroa',    'Club Campestre Jumay',          8,  8, 0, 2026),
('Andres Figueroa',    'Hacienda Jumay',               15, 15, 0, 2026),
('Andres Figueroa',    'La Ceiba',                      7,  7, 0, 2026),
('Fernando Berduciendo','Arboleda Santa Elena',          5,  5, 0, 2026),
('Fernando Berduciendo','Club del Bosque',               5,  5, 0, 2026),
('Fernando Berduciendo','Hacienda El Cafetal Fase I',   10, 10, 0, 2026),
('Jordy Borrayo',      'Oasis Zacapa',                  8,  8, 0, 2026),
('Jordy Borrayo',      'Hacienda Santa Lucia',          8,  8, 0, 2026),
('Diego Fuentes',      'Hacienda el Sol',               5,  5, 0, 2026),
('Diego Fuentes',      'Condado Jutiapa',               8,  8, 0, 2026),
('Diego Fuentes',      'Club Residencial El Progreso',  7,  7, 0, 2026)
ON CONFLICT (responsable, proyecto, mes, año) DO NOTHING;

-- Índices
CREATE INDEX IF NOT EXISTS idx_vendedores_equipo  ON vendedores(equipo);
CREATE INDEX IF NOT EXISTS idx_metas_responsable  ON metas_ventas(responsable, año);

-- Vista: ventas mensuales brutas + netas
CREATE OR REPLACE VIEW v_ventas_mensual AS
SELECT
    DATE_TRUNC('month', l.fecha_venta)::DATE AS mes,
    p.nombre_proyecto AS proyecto,
    l.vendedor,
    l.forma_pago,
    l.plazo,
    COUNT(*) AS ventas_brutas,
    SUM(l.precio_final) AS valor_bruto,
    SUM(COALESCE(l.total_intereses, 0)) AS intereses_pactados
FROM lotes l
JOIN proyectos p ON p.id = l.proyecto_id
WHERE l.estatus IN ('VENTA','RESERVADO')
  AND l.fecha_venta IS NOT NULL
  AND l.vendedor NOT IN (
    '-Ningún empleado del departamento de ventas-',
    'Canje A','Bloqueado','Bloqueo Municipal','Apartado Proyecto Aptos'
  )
GROUP BY DATE_TRUNC('month', l.fecha_venta), p.nombre_proyecto, l.vendedor, l.forma_pago, l.plazo;

