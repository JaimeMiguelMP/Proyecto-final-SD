def sucursales_con_espacio(db):
    return {
        fila[0]: fila[1]
        for fila in db.execute("""
            SELECT s.id_sucursal, s.capacidad_max - COUNT(u.id_unidad)
            FROM sucursal AS s
            LEFT JOIN unidad AS u ON u.id_sucursal_actual = s.id_sucursal
                AND u.estado = 'DISPONIBLE'
            WHERE s.estado = 'ACTIVA'
            GROUP BY s.id_sucursal
        """)
    }


def asignar_sucursal(espacio):
    if not espacio:
        raise ValueError("No hay sucursales activas")
    destino = max(espacio, key=lambda nid: (espacio[nid], -nid))
    if espacio[destino] <= 0:
        raise ValueError("No hay espacio disponible en las sucursales")
    espacio[destino] -= 1
    return destino
