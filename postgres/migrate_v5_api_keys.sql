-- Migración v5: Tabla de API Keys para API Pública

CREATE TABLE IF NOT EXISTS api_keys (
    id          SERIAL PRIMARY KEY,
    nombre      VARCHAR(100) NOT NULL,       -- Nombre del sistema cliente
    key_hash    VARCHAR(64) NOT NULL UNIQUE, -- SHA256 del API key
    permisos    VARCHAR(200) DEFAULT 'read', -- read | read,write
    activo      BOOLEAN DEFAULT TRUE,
    usos        INTEGER DEFAULT 0,
    ultimo_uso  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    notas       TEXT
);

-- Función para generar una API key (ejecutar manualmente)
-- SELECT encode(gen_random_bytes(32), 'hex') AS nueva_key;
-- Luego hashear: SELECT encode(sha256('TU_KEY_AQUI'::bytea), 'hex') AS hash;

-- Ejemplo: insertar una key para un sistema externo
-- INSERT INTO api_keys (nombre, key_hash, permisos, notas)
-- VALUES ('Sistema CRM', encode(sha256('tu-api-key-aqui'::bytea), 'hex'), 'read', 'Integración CRM 2026');
