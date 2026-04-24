DROP VIEW IF EXISTS v_lotes_detalle;
ALTER TABLE lotes ALTER COLUMN descuento TYPE NUMERIC(14,2);
CREATE OR REPLACE VIEW v_lotes_detalle AS
SELECT l.*, p.nombre_proyecto, p.nombre_sociedad, p.empresa_sap
FROM lotes l JOIN proyectos p ON p.id = l.proyecto_id;
