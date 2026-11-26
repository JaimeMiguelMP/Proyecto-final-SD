from asignacion_inventario import asignar_sucursal, sucursales_con_espacio


def agregar_articulo(db, datos, fecha):
    series = datos["series"]
    if not isinstance(series, list) or not series or any(
        not isinstance(serie, str) or not serie.strip() for serie in series
    ):
        raise ValueError("Indica una lista de números de serie")
    espacio = sucursales_con_espacio(db)
    destinos = [asignar_sucursal(espacio) for _ in series]
    articulo = db.execute(
        "INSERT INTO articulo (nombre, categoria, precio) VALUES (?, ?, ?)",
        (datos["nombre"], datos["categoria"], float(datos["precio"])),
    ).lastrowid
    db.executemany("""
        INSERT INTO unidad (id_articulo, serie, id_sucursal_origen, id_sucursal_actual)
        VALUES (?, ?, ?, ?)
    """, ((articulo, serie, destino, destino) for serie, destino in zip(series, destinos)))
    return {"id_articulo": articulo, "sucursales": dict(zip(series, destinos))}


def consultar_inventario(db):
    return [dict(fila) for fila in db.execute("""
        SELECT a.id_articulo, a.nombre, a.categoria, a.precio,
               u.id_unidad, u.serie, u.id_sucursal_actual, u.version
        FROM articulo AS a JOIN unidad AS u USING (id_articulo)
        WHERE u.estado = 'DISPONIBLE' ORDER BY u.id_unidad
    """)]
