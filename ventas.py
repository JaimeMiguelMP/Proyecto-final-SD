def comprar_unidad(db, datos, fecha):
    unidad = int(datos["id_unidad"])
    cliente = int(datos["id_cliente"])
    fila = db.execute("""
        SELECT id_articulo, serie, id_sucursal_actual, version
        FROM unidad WHERE id_unidad = ?
    """, (unidad,)).fetchone()
    if fila is None:
        raise ValueError("La unidad no existe")
    version = int(datos.get("version_esperada", fila["version"]))
    actualizado = db.execute("""
        UPDATE unidad SET estado = 'VENDIDO', version = version + 1
        WHERE id_unidad = ? AND estado = 'DISPONIBLE' AND version = ?
          AND id_sucursal_actual IN (SELECT id_sucursal FROM sucursal WHERE estado = 'ACTIVA')
    """, (unidad, version))
    if actualizado.rowcount != 1:
        raise ValueError("La unidad ya no está disponible o cambió de versión")
    return registrar_venta(db, fila, unidad, cliente, fecha)


def registrar_venta(db, fila, unidad, cliente, fecha):
    guia = f'{fila["id_articulo"]}-{fila["serie"]}-{fila["id_sucursal_actual"]}-{cliente}'
    venta = db.execute("""
        INSERT INTO venta (id_cliente, id_unidad, codigo_guia, fecha_venta)
        VALUES (?, ?, ?, ?)
    """, (cliente, unidad, guia, fecha)).lastrowid
    return {"id_venta": venta, "id_unidad": unidad, "codigo_guia": guia}
