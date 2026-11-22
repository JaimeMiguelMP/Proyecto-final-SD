# Contrato de Comunicación JSON (Puerto TCP 5000)

Todos los paquetes siguen la estructura base:
`{"tipo": "<TIPO>", "origen_id": <int>, "timestamp": <float>, "datos": { ... }}`

## 1. Infraestructura y Tolerancia a Fallas (Integrante 1)
- `HEARTBEAT`: Verificación periódica de vida.
  - Respuesta: `{"tipo": "HEARTBEAT_ACK", "origen_id": 2, "es_maestro": false}`
- `QUIEN_ES_MAESTRO`: Enviado por un nodo al arrancar o reconectarse para descubrir al líder actual.
  - Respuesta: `{"tipo": "MAESTRO_ACTUAL", "maestro_id": 4}`
- `ELECCION`: Mensaje del algoritmo Bully enviado a nodos con `id > mi_id`.
  - Respuesta: `{"tipo": "OK_ELECCION", "origen_id": 4}`
- `COORDINADOR`: Anuncio del ganador de la elección Bully a todo el clúster.
  - Datos: `{"maestro_id": 4}`
  - Respuesta: `{"tipo": "ACK"}`

## 2. Consenso por Mayoría (Quórum >= 3 de 4)
- `SOLICITUD_OPERACION`: Un nodo no-maestro reenvía una petición de escritura al maestro.
  - Datos: `{"operacion": "<OP>", "payload": {...}}`
  - Respuesta: `{"tipo": "RESULTADO_OPERACION", "exito": true, "detalle": "...", "resultado": {...}}`
- `PROPUESTA_CONSENSO`: El maestro consulta al resto del clúster antes de confirmar una escritura.
  - Datos: `{"tx_id": "tx-179...", "operacion": "<OP>", "payload": {...}}`
  - Respuesta: `{"tipo": "VOTO_OK", "tx_id": "..."}` o `{"tipo": "VOTO_RECHAZO", "motivo": "..."}`
- `COMMIT`: El maestro alcanzó mayoría y ordena aplicar el cambio en el SQLite local de cada nodo.
  - Datos: `{"tx_id": "tx-179...", "operacion": "<OP>", "payload": {...}}`
  - Respuesta: `{"tipo": "COMMIT_ACK", "tx_id": "..."}`

## 3. Sincronización de Nodos Recuperados
- `SOLICITUD_SYNC`: Un nodo que se levanta pide una copia completa de la BD al maestro.
  - Respuesta: `{"tipo": "SYNC_DATA", "tablas": {"sucursal": [...], "articulo": [...], "unidad": [...], "cliente": [...], "venta": [...]}}`

## 4. Operaciones de Negocio (`operacion` dentro de `payload`)
- `AGREGAR_ARTICULO`: Alta de artículo/unidades con sucursal asignada por el maestro.
- `CREAR_CLIENTE` / `ACTUALIZAR_CLIENTE`: Alta o edición con `version` para bloqueo optimista.
- `COMPRAR_UNIDAD`: Venta con `id_unidad`, `id_cliente`, `version_esperada`, `codigo_guia`, `fecha_venta`.
- `REDISTRIBUIR_CAIDA`: Reasignación de `id_sucursal_actual` de un nodo caído hacia nodos vivos.
- `DEVOLVER_ARTICULOS`: Retorno de unidades `DISPONIBLE` a su `id_sucursal_origen` tras reconexión.