from sqlalchemy import Column, Integer, String, Boolean, Numeric, Date, DateTime, ForeignKey, BigInteger, Text
from sqlalchemy.dialects.postgresql import UUID, INET, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
import uuid


class Proyecto(Base):
    __tablename__ = "proyectos"

    id              = Column(Integer, primary_key=True)
    id_sap          = Column(Integer, unique=True)
    empresa_sap     = Column(String(100), unique=True, nullable=False)
    nombre_sociedad = Column(String(150), nullable=False)
    nombre_proyecto = Column(String(150), nullable=False)
    activo          = Column(Boolean, default=True)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())
    updated_at      = Column(DateTime(timezone=True), server_default=func.now())

    lotes = relationship("Lote", back_populates="proyecto")


class Lote(Base):
    __tablename__ = "lotes"

    id                         = Column(Integer, primary_key=True)
    proyecto_id                = Column(Integer, ForeignKey("proyectos.id"), nullable=False)
    unidad_key                 = Column(String(50), nullable=False)
    unidad_actual              = Column(String(50))
    manzana                    = Column(String(100))
    metraje_inventario         = Column(Numeric(10, 4))
    metraje_orden              = Column(Numeric(10, 4))
    medida_orden               = Column(String(50))

    precio_sin_descuento       = Column(Numeric(14, 2), default=0)
    descuento                  = Column(Numeric(5, 2), default=0)
    precio_con_descuento       = Column(Numeric(14, 2), default=0)
    precio_final               = Column(Numeric(14, 2), default=0)
    precio_base_m2             = Column(Numeric(14, 2))
    precio_esquina             = Column(Numeric(14, 2), default=0)
    valor_terreno              = Column(Numeric(14, 2))
    total_intereses            = Column(Numeric(14, 2), default=0)
    cuota_mantenimiento        = Column(String(20))
    es_esquina                 = Column(Boolean, default=False)

    estatus                    = Column(String(50))
    estatus_raw                = Column(String(100))
    status_promesa_compraventa = Column(String(100))
    status_informe_ponf        = Column(String(100))
    forma_pago                 = Column(String(50))

    doc_num                    = Column(Integer)
    card_code                  = Column(String(50))
    card_name                  = Column(String(200))
    telefono_cliente           = Column(String(100))
    vendedor                   = Column(String(200))

    fecha_venta                = Column(Date)
    fecha_solicitud_pcv        = Column(Date)
    fecha_inicial_cobro        = Column(Date)
    fecha_final_cobro          = Column(Date)
    fecha_escrituracion        = Column(Date)
    fecha_firma_pcv            = Column(Date)
    fecha_vencimiento_pcv      = Column(Date)
    plazo                      = Column(Integer)

    pagado_capital             = Column(Numeric(14, 2), default=0)
    pagado_interes             = Column(Numeric(14, 2), default=0)
    pendiente_capital          = Column(Numeric(14, 2), default=0)
    pendiente_interes          = Column(Numeric(14, 2), default=0)
    cuotas_pagadas             = Column(Integer, default=0)
    cuotas_pendientes          = Column(Integer, default=0)
    saldo_cliente              = Column(Numeric(14, 2), default=0)

    facturacion_70             = Column(Numeric(14, 2), default=0)
    fecha_facturacion_70       = Column(Date)
    doc_fac_sap_70             = Column(String(50))
    facturacion_30             = Column(Numeric(14, 2), default=0)
    fecha_facturacion_30       = Column(Date)
    doc_fac_sap_30             = Column(String(50))

    fuente                     = Column(String(20), default="CONSBA")
    created_at                 = Column(DateTime(timezone=True), server_default=func.now())
    updated_at                 = Column(DateTime(timezone=True), server_default=func.now())

    proyecto = relationship("Proyecto", back_populates="lotes")


class Rol(Base):
    __tablename__ = "roles"

    id          = Column(Integer, primary_key=True)
    nombre      = Column(String(50), unique=True, nullable=False)
    descripcion = Column(String(200))
    nivel       = Column(Integer, default=1)

    usuarios = relationship("Usuario", back_populates="rol")


class Usuario(Base):
    __tablename__ = "usuarios"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email           = Column(String(150), unique=True, nullable=False)
    nombre          = Column(String(150), nullable=False)
    hashed_password = Column(String(255), nullable=False)
    rol_id          = Column(Integer, ForeignKey("roles.id"), nullable=False)
    activo          = Column(Boolean, default=True)
    ultimo_acceso   = Column(DateTime(timezone=True))
    created_at      = Column(DateTime(timezone=True), server_default=func.now())
    updated_at      = Column(DateTime(timezone=True), server_default=func.now())

    rol = relationship("Rol", back_populates="usuarios")


class SyncLog(Base):
    __tablename__ = "sync_log"

    id                      = Column(Integer, primary_key=True)
    archivo                 = Column(String(100), nullable=False)
    inicio                  = Column(DateTime(timezone=True), server_default=func.now())
    fin                     = Column(DateTime(timezone=True))
    estado                  = Column(String(20), default="EJECUTANDO")
    registros_leidos        = Column(Integer, default=0)
    registros_insertados    = Column(Integer, default=0)
    registros_actualizados  = Column(Integer, default=0)
    registros_error         = Column(Integer, default=0)
    mensaje_error           = Column(Text)
    detalles                = Column(JSONB)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id          = Column(BigInteger, primary_key=True)
    tabla       = Column(String(100), nullable=False)
    registro_id = Column(String(100), nullable=False)
    accion      = Column(String(20), nullable=False)
    usuario_id  = Column(UUID(as_uuid=True), ForeignKey("usuarios.id"))
    campo       = Column(String(100))
    valor_antes = Column(Text)
    valor_nuevo = Column(Text)
    ip_origen   = Column(INET)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
