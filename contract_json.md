# Contrato de comunicación entre nodos

Cada petición y respuesta es un objeto JSON codificado en UTF-8 y terminado
con un salto de línea. El puerto TCP por defecto es 5000. Las peticiones
contienen `tipo` y un objeto `datos`. Los participantes se configuran en
`directory.json`.

| Tipo | Datos | Respuesta |
|---|---|---|
| `HEARTBEAT` | Objeto vacío | `maestro_id` y última posición `aplicada` |
| `SOLICITUD_OPERACION` | Comando | `exito` y `resultado` o `error` |
| `PREPARAR` | `posicion`, `papeleta` | `ok` y, si existe, papeleta `aceptada` y `comando`; si se rechaza, `promesa` actual |
| `ACEPTAR` | `posicion`, `papeleta`, `comando` | `ok`; si se rechaza, `promesa` actual |
| `CONFIRMAR` | `entradas` con pares de posición y comando | `ok` |
| `SOLICITUD_SYNC` | `desde`, posición exclusiva | `entradas` con hasta 100 pares de posición y comando |

Un comando lleva `solicitud` (ID único), `operacion`, `fecha` (ISO 8601 UTC)
y `datos` (objeto). La papeleta es el par de número de ronda e ID de nodo.
El maestro obtiene una mayoría de aceptaciones durables antes de confirmar.
Los nodos aplican las entradas en orden y guardan los resultados en su SQLite.

- `AGREGAR_ARTICULO` requiere `nombre`, `categoria`, `precio` y una lista `series`.
- `CREAR_CLIENTE` requiere `nombre`, `correo`, `telefono` y `direccion`.
- `ACTUALIZAR_CLIENTE` usa los mismos datos y añade `id_cliente` y `version_esperada`.
- `COMPRAR_UNIDAD` requiere `id_unidad` e `id_cliente` y admite `version_esperada`.
- `REDISTRIBUIR_CAIDA` y `RECUPERAR_SUCURSAL` se originan en el maestro
  cuando cambia el estado observado de una sucursal. Sus datos llevan `id_sucursal`.
