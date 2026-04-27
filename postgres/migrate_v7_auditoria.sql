-- Migración v7: Tabla de auditoría de accesos
CREATE TABLE IF NOT EXISTS auditoria_accesos (
    id          SERIAL PRIMARY KEY,
    usuario_id  UUID REFERENCES usuarios(id) ON DELETE SET NULL,
    email       VARCHAR(150),
    nombre      VARCHAR(150),
    tipo        VARCHAR(20) NOT NULL,  -- LOGIN_OK | LOGIN_FAIL
    ip          VARCHAR(45),
    user_agent  VARCHAR(300),
    detalle     VARCHAR(200),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auditoria_created ON auditoria_accesos(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_auditoria_tipo ON auditoria_accesos(tipo);
