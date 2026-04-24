-- ============================================================
-- Migración v6: Nuevos roles y usuarios RV4
-- Contraseña temporal para todos: Rv4-2026!
-- ============================================================

-- 1. Nuevos roles
INSERT INTO roles (nombre, descripcion, nivel) VALUES
('DIRECTOR',       'Acceso completo a todos los módulos del sistema', 3),
('GERENTE_CONSBA', 'Inventario, Ventas y Cartera. Sin módulos financieros futuros', 2),
('GESTOR_CARTERA', 'Inventario, Ventas y Cartera', 2)
ON CONFLICT (nombre) DO NOTHING;

-- 2. ADMINISTRADORES (nivel 4)
INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'rgudiel@rvcuatro.com','Rosa Maria Gudiel Zepeda','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='ADMIN' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'admin@rv4.com','Administrador','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='ADMIN' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'atrinidad@rvcuatro.com','Alexander Adan Trinidad','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='ADMIN' ON CONFLICT (email) DO NOTHING;

-- 3. DIRECTORES (nivel 3)
INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'gcifuentes@rvcuatro.com','Gabriel Omar Cifuentes Beteta','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'e.barahona@consba.com.gt','Emanuel Barahona','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'ecoroy@rvcuatro.com','Erick Coroy','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'jroca@rvcuatro.com','Juan Pablo Roca','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'nymartinez@rvcuatro.com','Nelly Yasmin Martinez Estrada','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'cbenitez@rvcuatro.com','Carlos Benitez','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'gcastellanos@rvcuatro.com','Mayra Gabriela Castellano','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'j.barahona@consba.com.gt','Johnal Barahona','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 's.barahona@consba.com.gt','Santos Barahona','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'juans@rvcuatro.com','Juan Francisco Stahl','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'ggodoy@rvcuatro.com','Gerardo Godoy','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'tcarias@rvcuatro.com','Tulio Carias','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='DIRECTOR' ON CONFLICT (email) DO NOTHING;

-- 4. GERENTES CONSBA (nivel 2)
INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'gerenciacalidad@consba.com.gt','Gabriel Aragon','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='GERENTE_CONSBA' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'finanzas@consba.com.gt','Luis Lopez','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='GERENTE_CONSBA' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'gerenciageneral@consersa.gt','Rodrigo Illescas','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='GERENTE_CONSBA' ON CONFLICT (email) DO NOTHING;

INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'gerentefinanciero@consba.com.gt','Armando Vega','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='GERENTE_CONSBA' ON CONFLICT (email) DO NOTHING;

-- 5. VENDEDOR / ASESOR (nivel 1)
INSERT INTO usuarios (email, nombre, hashed_password, rol_id, activo)
SELECT 'gerencia@casaeficiente.com.gt','Gabriela Leal','$2b$12$ms6qSLK5sh0wzH9Wgcn2t.n4y/QaG/Ftz/XtTSt1K3HWoo7EyahHC',r.id,TRUE FROM roles r WHERE r.nombre='ASESOR' ON CONFLICT (email) DO NOTHING;

-- Verificar
SELECT u.nombre, u.email, r.nombre AS rol, r.nivel
FROM usuarios u JOIN roles r ON r.id = u.rol_id
ORDER BY r.nivel DESC, u.nombre;
