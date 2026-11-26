from asignacion_inventario import asignar_sucursal, sucursales_con_espacio


def redistribuir_pendientes(db):
    unidades = db.execute("""
        SELECT u.id_unidad FROM unidad AS u JOIN sucursal AS s
        ON s.id_sucursal = u.id_sucursal_actual
        WHERE u.estado = 'DISPONIBLE' AND s.estado = 'CAIDA' ORDER BY u.id_unidad
    """).fetchall()
    espacio = sucursales_con_espacio(db)
    movidas = []
    for unidad, in unidades:
        if not any(libre > 0 for libre in espacio.values()):
            break
        destino = asignar_sucursal(espacio)
        db.execute("""
            UPDATE unidad SET id_sucursal_actual = ?, version = version + 1 WHERE id_unidad = ?
        """, (destino, unidad))
        movidas.append(unidad)
    return {"movidas": movidas, "pendientes": len(unidades) - len(movidas)}


def cambiar_estado_sucursal(db, datos, fecha):
    sucursal = int(datos["id_sucursal"])
    estado = datos["estado"]
    actualizado = db.execute(
        "UPDATE sucursal SET estado = ? WHERE id_sucursal = ?", (estado, sucursal),
    )
    if actualizado.rowcount != 1:
        raise ValueError("La sucursal no existe")
    return redistribuir_pendientes(db)


def redistribuir_caida(db, datos, fecha):
    return cambiar_estado_sucursal(db, {**datos, "estado": "CAIDA"}, fecha)


def recuperar_sucursal(db, datos, fecha):
    return cambiar_estado_sucursal(db, {**datos, "estado": "ACTIVA"}, fecha)
