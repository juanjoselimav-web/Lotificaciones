-- migrate_v10_plazo_raw.sql
-- Agrega columna plazo_raw para guardar el valor original del campo Plazo del Excel
-- Esto permite identificar casos especiales: Ansak S.A., Canje A, Casa Modelo, etc.
-- La columna plazo (INT) se mantiene igual para no romper nada existente.

ALTER TABLE lotes ADD COLUMN IF NOT EXISTS plazo_raw VARCHAR(50);

-- Índice para búsquedas rápidas por tipo de plazo
CREATE INDEX IF NOT EXISTS idx_lotes_plazo_raw ON lotes(plazo_raw);

-- Comentario para documentar los valores especiales conocidos:
-- Excluidos de ventas: 'Canje A', 'Casa Modelo', 'Ansak, S.A.', 'Apartado Proyecto Aptos', 'Bloqueo Municipal'
-- Ventas con alerta:   'Final Proyecto', 'Final de Proyecto', 'Final proyecto', 'Venta Interna'
-- Ventas normales:     Números (12, 24, 36, 60, 120...), 'Contado'
