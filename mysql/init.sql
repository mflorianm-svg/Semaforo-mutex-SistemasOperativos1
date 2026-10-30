-- ──────────────────────────────────────────────────────────────
--  init.sql — Creación automática de la base de datos y tabla
--  Se ejecuta una sola vez al levantar el contenedor MySQL
-- ──────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS so_proyecto
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE so_proyecto;

-- Tabla de transacciones bancarias
CREATE TABLE IF NOT EXISTS transact (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    cuenta        VARCHAR(20)  NOT NULL,          
    tipo          VARCHAR(20)  NOT NULL,           -- DEPOSITO | RETIRO | TRANSFERENCIA
    monto         DECIMAL(10,2) NOT NULL,          -- monto de la operación

    estado        VARCHAR(20)  NOT NULL DEFAULT 'PENDIENTE',

    -- Metadatos del productor
    pid           INT          NOT NULL,
    creado_en     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    procesado_por VARCHAR(50)  DEFAULT NULL,
    procesado_en  DATETIME     DEFAULT NULL,

    INDEX idx_estado (estado),
    INDEX idx_creado (creado_en)
);
