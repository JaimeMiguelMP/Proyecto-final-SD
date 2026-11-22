PRAGMA foreign_keys = ON;

-- 1. Sucursales (nodos del clúster)
CREATE TABLE IF NOT EXISTS sucursal (
    id_sucursal   INTEGER PRIMARY KEY,
    alias         TEXT NOT NULL UNIQUE,
    ip            TEXT NOT NULL UNIQUE,
    capacidad_max INTEGER NOT NULL CHECK (capacidad_max > 0),
    estado        TEXT NOT NULL DEFAULT 'ACTIVA' CHECK (estado IN ('ACTIVA', 'CAIDA'))
);

-- 2. Catálogo general de artículos
CREATE TABLE IF NOT EXISTS articulo (
    id_articulo INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre      TEXT NOT NULL,
    categoria   TEXT NOT NULL,
    precio      REAL NOT NULL CHECK (precio > 0),
    version     INTEGER NOT NULL DEFAULT 1
);

-- 3. Unidades físicas distribuidas en sucursales
CREATE TABLE IF NOT EXISTS unidad (
    id_unidad          INTEGER PRIMARY KEY AUTOINCREMENT,
    id_articulo        INTEGER NOT NULL,
    serie              TEXT NOT NULL UNIQUE,
    id_sucursal_origen INTEGER NOT NULL,
    id_sucursal_actual INTEGER NOT NULL,
    estado             TEXT NOT NULL DEFAULT 'DISPONIBLE' CHECK (estado IN ('DISPONIBLE', 'VENDIDO')),
    version            INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (id_articulo) REFERENCES articulo(id_articulo),
    FOREIGN KEY (id_sucursal_origen) REFERENCES sucursal(id_sucursal),
    FOREIGN KEY (id_sucursal_actual) REFERENCES sucursal(id_sucursal)
);

-- 4. Lista distribuida de clientes
CREATE TABLE IF NOT EXISTS cliente (
    id_cliente     INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre         TEXT NOT NULL,
    correo         TEXT NOT NULL UNIQUE,
    telefono       TEXT NOT NULL,
    direccion      TEXT NOT NULL,
    version        INTEGER NOT NULL DEFAULT 1,
    actualizado_en TEXT NOT NULL
);

-- 5. Registro de ventas y guías de envío
-- Formato de guía: IDARTICULO-SERIE-SUCURSAL-IDCLIENTE
CREATE TABLE IF NOT EXISTS venta (
    id_venta    INTEGER PRIMARY KEY AUTOINCREMENT,
    id_cliente  INTEGER NOT NULL,
    id_unidad   INTEGER NOT NULL UNIQUE,
    codigo_guia TEXT NOT NULL UNIQUE,
    fecha_venta TEXT NOT NULL,
    FOREIGN KEY (id_cliente) REFERENCES cliente(id_cliente),
    FOREIGN KEY (id_unidad) REFERENCES unidad(id_unidad)
);

-- Datos iniciales de las 4 sucursales
INSERT OR IGNORE INTO sucursal (id_sucursal, alias, ip, capacidad_max, estado) VALUES
    (1, 'nodo1', '192.168.1.171', 10, 'ACTIVA'),
    (2, 'nodo2', '192.168.1.172', 10, 'ACTIVA'),
    (3, 'nodo3', '192.168.1.173', 10, 'ACTIVA'),
    (4, 'nodo4', '192.168.1.174', 10, 'ACTIVA');