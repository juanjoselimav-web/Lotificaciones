-- Migración segura: agrega columna subseccion a flujos_reclasificaciones
-- Idempotente: no falla si ya existe
ALTER TABLE flujos_reclasificaciones
  ADD COLUMN IF NOT EXISTS subseccion TEXT;

-- Verificar resultado
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'flujos_reclasificaciones'
ORDER BY ordinal_position;
