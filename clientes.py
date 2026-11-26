def consultar_clientes(db):
    return [dict(fila) for fila in db.execute("SELECT * FROM cliente ORDER BY id_cliente")]


def guardar_cliente(db, datos, fecha, actualizar=False):
    campos = tuple(datos[clave] for clave in ("nombre", "correo", "telefono", "direccion"))
    if actualizar:
        cliente = int(datos["id_cliente"])
        version = int(datos["version_esperada"])
        cursor = db.execute("""
            UPDATE cliente SET nombre = ?, correo = ?, telefono = ?, direccion = ?,
                actualizado_en = ?, version = version + 1
            WHERE id_cliente = ? AND version = ?
        """, (*campos, fecha, cliente, version))
        if cursor.rowcount != 1:
            raise ValueError("Cliente inexistente o versión desactualizada")
        return {"id_cliente": cliente, "version": version + 1}
    cliente = db.execute("""
        INSERT INTO cliente (nombre, correo, telefono, direccion, actualizado_en)
        VALUES (?, ?, ?, ?, ?)
    """, (*campos, fecha)).lastrowid
    return {"id_cliente": cliente, "version": 1}


def actualizar_cliente(db, datos, fecha):
    return guardar_cliente(db, datos, fecha, actualizar=True)
