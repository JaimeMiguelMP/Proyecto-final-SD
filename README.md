# Inventario distribuido

Cada sucursal corre un proceso Python 3.11 o superior y guarda una réplica completa
en su propio SQLite. La ubicación física de cada unidad se guarda en
`id_sucursal_actual`. El directorio de miembros es fijo y debe ser el mismo en
todos los procesos. El protocolo está pensado para una red privada de nodos
confiables.

## Arranque

Configura los alias y las IP de `directory.json`. El puerto por defecto es TCP
5000 y la capacidad por defecto es de diez unidades disponibles por sucursal.
El archivo admite los campos opcionales `puerto` y `capacidad_max`. Para probar
todo en una máquina, usa `127.0.0.1` con un puerto distinto en cada entrada.

En cada sucursal, inicia su nodo desde el directorio del proyecto. Por ejemplo:

```powershell
python cluster.py --nodo 1
```

Usa `--nodo 2`, `--nodo 3` y `--nodo 4` para las otras sucursales.
`--config` permite elegir otro directorio JSON y `--db` otra ruta local de
SQLite. La ruta por defecto del ejemplo es `datos/nodo-1/inventario.db`.
Usa una base nueva en cada nodo al arrancar el clúster por primera vez y conserva
la misma ruta al reiniciarlo.

## Operaciones

En la consola de cualquier nodo:

```text
estado
CREAR_CLIENTE {"nombre":"Ana","correo":"ana@example.com","telefono":"123","direccion":"Centro"}
clientes
ACTUALIZAR_CLIENTE {"id_cliente":1,"version_esperada":1,"nombre":"Ana","correo":"ana@example.com","telefono":"123","direccion":"Norte"}
AGREGAR_ARTICULO {"nombre":"Libro","categoria":"Papel","precio":150,"series":["L001","L002"]}
inventario
COMPRAR_UNIDAD {"id_unidad":1,"id_cliente":1,"version_esperada":1,"solicitud":"compra-001"}
salir
```

Usa los IDs y versiones que devuelve tu ejecución. Si una escritura pierde su
respuesta, reintenta con los mismos datos y el mismo campo `solicitud`; el
registro persistente conserva su resultado. Usa un ID distinto para cada
operación nueva.

Los nodos eligen al candidato visible con el ID más alto. Una escritura necesita
tres votos persistentes con cuatro miembros. Si hay una partición, cada lado
puede observar un candidato distinto, pero solo un quórum puede elegir el valor
de una posición del registro. Las consultas son locales y pueden mostrar datos
atrasados durante una caída o antes de que termine la sincronización.

Cuando cae una sucursal, el maestro registra la caída y mueve las unidades
disponibles a las sucursales activas con más espacio. Si falta capacidad, las
unidades restantes siguen asignadas a la sucursal caída y no se pueden comprar
mientras siga caída. Al recuperar una sucursal, se actualiza su estado y se
vuelven a examinar las unidades pendientes.

## Prueba

```powershell
python -m unittest -v
```

Las pruebas inician cuatro nodos TCP en puertos locales temporales con una base
por nodo.
