import json
import socket
import sys
import threading
import time
import uuid
from datetime import datetime

PORT = 5000
CONFIG_FILE = "/home/jaime-mp/proyecto/directory.json"
CONSENSO_LOG = "/home/jaime-mp/proyecto/consenso.log"
HEARTBEAT_INTERVALO = 3.0
FALLOS_PARA_CAIDA = 2


def obtener_ip_local():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("192.168.1.254", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def obtener_timestamp():
    return datetime.now().timestamp()


class NodoCluster:
    def __init__(self, config_file=CONFIG_FILE, log_file=CONSENSO_LOG, callbacks=None):
        self.log_file = log_file
        self.io_lock = threading.Lock()
        self.estado_lock = threading.Lock()
        self.detener = threading.Event()

        # Hooks que conectarán los módulos db_manager.py y logica_inventario.py
        self.callbacks = callbacks or {}

        with open(config_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self.directorio = {int(k): v for k, v in raw.items()}

        self.mi_ip = obtener_ip_local()
        self.mi_id = self._detectar_mi_id()
        self.mi_alias = self.directorio[self.mi_id]["alias"]

        self.maestro_id = None
        self.en_eleccion = False
        self.fallos_consecutivos = {nid: 0 for nid in self.directorio if nid != self.mi_id}
        self.nodos_caidos = set()

    def _detectar_mi_id(self):
        for nid, info in self.directorio.items():
            if info["ip"] == self.mi_ip:
                return nid
        raise RuntimeError(f"La IP local {self.mi_ip} no aparece en {CONFIG_FILE}")

    @property
    def soy_maestro(self):
        return self.maestro_id == self.mi_id

    @property
    def quorum(self):
        return (len(self.directorio) // 2) + 1

    # ------------------------------------------------------------------
    # Utilidades de I/O y Transporte TCP
    # ------------------------------------------------------------------
    def log_consenso(self, evento):
        linea = f"[{obtener_timestamp():.6f}] [Nodo {self.mi_id} - {self.mi_alias}] {evento}"
        with self.io_lock:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(linea + "\n")

    def imprimir(self, texto):
        with self.io_lock:
            print(texto, flush=True)

    def enviar_paquete(self, destino_id, paquete, timeout=3.0):
        ip = self.directorio[destino_id]["ip"]
        paquete.setdefault("origen_id", self.mi_id)
        paquete.setdefault("timestamp", obtener_timestamp())
        payload = (json.dumps(paquete, ensure_ascii=False) + "\n").encode("utf-8")

        try:
            with socket.create_connection((ip, PORT), timeout=timeout) as sock:
                sock.settimeout(timeout)
                sock.sendall(payload)
                with sock.makefile("r", encoding="utf-8") as stream:
                    linea = stream.readline()
                    if not linea:
                        return None
                    return json.loads(linea)
        except (OSError, ValueError, json.JSONDecodeError):
            return None

    # ------------------------------------------------------------------
    # Arranque, Servidor TCP y Enrutamiento de Mensajes
    # ------------------------------------------------------------------
    def iniciar(self):
        threading.Thread(target=self._hilo_servidor, daemon=True).start()
        time.sleep(0.5)

        # Descubrir si ya existe un maestro activo para sincronizar BD antes de operar
        maestro_detectado = self._descubrir_maestro_existente()
        if maestro_detectado is not None:
            self.maestro_id = maestro_detectado
            self._sincronizar_con_maestro(maestro_detectado)
            # En Bully estricto, si mi ID es mayor al maestro actual, reclamo liderazgo
            if self.mi_id > maestro_detectado:
                self.iniciar_eleccion_bully()
        else:
            self.iniciar_eleccion_bully()

        threading.Thread(target=self._hilo_heartbeat, daemon=True).start()

    def detener_nodo(self):
        self.detener.set()

    def _hilo_servidor(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", PORT))
        server.listen(16)
        server.settimeout(1.0)
        self.imprimir(f"[*] {self.mi_alias} (ID {self.mi_id}, {self.mi_ip}) escuchando en TCP:{PORT}")

        try:
            while not self.detener.is_set():
                try:
                    conn, addr = server.accept()
                    threading.Thread(
                        target=self._atender_conexion, args=(conn, addr), daemon=True
                    ).start()
                except socket.timeout:
                    continue
        finally:
            server.close()

    def _atender_conexion(self, conn, addr):
        with conn:
            try:
                conn.settimeout(10.0)
                with conn.makefile("r", encoding="utf-8") as stream:
                    linea = stream.readline()
                if not linea:
                    return
                paquete = json.loads(linea)
                respuesta = self._procesar_paquete(paquete)
                conn.sendall((json.dumps(respuesta, ensure_ascii=False) + "\n").encode("utf-8"))
            except (OSError, ValueError, json.JSONDecodeError):
                pass

    def _procesar_paquete(self, paquete):
        tipo = paquete.get("tipo")
        origen = paquete.get("origen_id")
        datos = paquete.get("datos", {})

        if tipo == "HEARTBEAT":
            return {"tipo": "HEARTBEAT_ACK", "origen_id": self.mi_id, "es_maestro": self.soy_maestro}

        if tipo == "QUIEN_ES_MAESTRO":
            return {"tipo": "MAESTRO_ACTUAL", "origen_id": self.mi_id, "maestro_id": self.maestro_id}

        if tipo == "ELECCION":
            # Si recibo ELECCION de un nodo con ID menor, respondo OK e inicio mi propia elección
            if origen is not None and origen < self.mi_id:
                if not self.soy_maestro:
                    threading.Thread(target=self.iniciar_eleccion_bully, daemon=True).start()
                else:
                    threading.Thread(target=self._anunciar_coordinador, daemon=True).start()
            return {"tipo": "OK_ELECCION", "origen_id": self.mi_id}

        if tipo == "COORDINADOR":
            nuevo_maestro = datos.get("maestro_id")
            with self.estado_lock:
                self.maestro_id = nuevo_maestro
                self.en_eleccion = False
            self.log_consenso(f"NUEVO_MAESTRO reconocido: Nodo {nuevo_maestro}")
            self.imprimir(f"\n[*] Nuevo nodo maestro elegido: Nodo {nuevo_maestro}")
            return {"tipo": "ACK", "origen_id": self.mi_id}

        if tipo == "SOLICITUD_OPERACION":
            if not self.soy_maestro:
                return {"tipo": "RESULTADO_OPERACION", "exito": False, "detalle": "Este nodo no es maestro"}
            exito, detalle, res = self.coordinar_consenso(datos["operacion"], datos["payload"])
            return {"tipo": "RESULTADO_OPERACION", "exito": exito, "detalle": detalle, "resultado": res}

        if tipo == "PROPUESTA_CONSENSO":
            tx_id = datos.get("tx_id")
            operacion = datos.get("operacion")
            payload = datos.get("payload")
            validar_fn = self.callbacks.get("validar_operacion")
            valido, motivo = validar_fn(operacion, payload) if validar_fn else (True, "OK")
            if valido:
                self.log_consenso(f"VOTO_OK tx={tx_id} op={operacion}")
                return {"tipo": "VOTO_OK", "tx_id": tx_id, "origen_id": self.mi_id}
            self.log_consenso(f"VOTO_RECHAZO tx={tx_id} op={operacion} motivo={motivo}")
            return {"tipo": "VOTO_RECHAZO", "tx_id": tx_id, "motivo": motivo, "origen_id": self.mi_id}

        if tipo == "COMMIT":
            tx_id = datos.get("tx_id")
            operacion = datos.get("operacion")
            payload = datos.get("payload")
            aplicar_fn = self.callbacks.get("aplicar_operacion")
            if aplicar_fn:
                aplicar_fn(operacion, payload)
            self.log_consenso(f"COMMIT_APLICADO tx={tx_id} op={operacion}")
            return {"tipo": "COMMIT_ACK", "tx_id": tx_id, "origen_id": self.mi_id}

        if tipo == "SOLICITUD_SYNC":
            exportar_fn = self.callbacks.get("exportar_bd")
            tablas = exportar_fn() if exportar_fn else {}
            self.log_consenso(f"SYNC_ENVIADA hacia Nodo {origen}")
            return {"tipo": "SYNC_DATA", "tablas": tablas, "origen_id": self.mi_id}

        return {"tipo": "ERROR", "detalle": f"Tipo desconocido: {tipo}"}

    # ------------------------------------------------------------------
    # Algoritmo Bully (Elección de Maestro)
    # ------------------------------------------------------------------
    def _descubrir_maestro_existente(self):
        for nid in sorted(self.directorio.keys(), reverse=True):
            if nid == self.mi_id:
                continue
            resp = self.enviar_paquete(nid, {"tipo": "QUIEN_ES_MAESTRO"}, timeout=1.5)
            if resp and resp.get("maestro_id") is not None:
                return resp["maestro_id"]
        return None

    def _sincronizar_con_maestro(self, maestro_id):
        resp = self.enviar_paquete(maestro_id, {"tipo": "SOLICITUD_SYNC"}, timeout=5.0)
        if resp and resp.get("tipo") == "SYNC_DATA":
            importar_fn = self.callbacks.get("importar_bd")
            if importar_fn:
                importar_fn(resp.get("tablas", {}))
            self.log_consenso(f"SYNC_RECIBIDA desde Maestro Nodo {maestro_id}")

    def iniciar_eleccion_bully(self):
        with self.estado_lock:
            if self.en_eleccion:
                return
            self.en_eleccion = True

        self.log_consenso("ELECCION_BULLY iniciada")
        mayores = [nid for nid in self.directorio if nid > self.mi_id]
        alguien_respondio = False

        for nid in mayores:
            resp = self.enviar_paquete(nid, {"tipo": "ELECCION"}, timeout=2.0)
            if resp and resp.get("tipo") == "OK_ELECCION":
                alguien_respondio = True

        if not alguien_respondio:
            with self.estado_lock:
                self.maestro_id = self.mi_id
                self.en_eleccion = False
            self.log_consenso(f"ELEGIDO_MAESTRO Nodo {self.mi_id}")
            self.imprimir(f"\n[*] Asumiendo rol de MAESTRO (Nodo {self.mi_id})")
            self._anunciar_coordinador()
        else:
            # Esperar anuncio del nuevo coordinador; si no llega, reintentar
            time.sleep(3.0)
            with self.estado_lock:
                still_waiting = self.en_eleccion
                self.en_eleccion = False
            if still_waiting:
                self.iniciar_eleccion_bully()

    def _anunciar_coordinador(self):
        for nid in self.directorio:
            if nid != self.mi_id:
                self.enviar_paquete(
                    nid,
                    {"tipo": "COORDINADOR", "datos": {"maestro_id": self.mi_id}},
                    timeout=2.0,
                )

    # ------------------------------------------------------------------
    # Monitoreo de Salud (Heartbeats)
    # ------------------------------------------------------------------
    def _hilo_heartbeat(self):
        while not self.detener.is_set():
            time.sleep(HEARTBEAT_INTERVALO)
            if self.soy_maestro:
                self._verificar_sucursales()
            else:
                self._verificar_maestro()

    def _verificar_maestro(self):
        if self.maestro_id is None or self.maestro_id == self.mi_id:
            return
        resp = self.enviar_paquete(self.maestro_id, {"tipo": "HEARTBEAT"}, timeout=2.0)
        if resp and resp.get("tipo") == "HEARTBEAT_ACK":
            self.fallos_consecutivos[self.maestro_id] = 0
        else:
            self.fallos_consecutivos[self.maestro_id] += 1
            if self.fallos_consecutivos[self.maestro_id] >= FALLOS_PARA_CAIDA:
                caido = self.maestro_id
                self.fallos_consecutivos[caido] = 0
                self.nodos_caidos.add(caido)
                self.log_consenso(f"FALLA_DETECTADA Maestro Nodo {caido}")
                self.imprimir(f"\n[!] Maestro Nodo {caido} no responde. Iniciando elección Bully...")
                self.iniciar_eleccion_bully()
                # Si yo resulté elegido maestro, disparo la redistribución del maestro caído
                if self.soy_maestro:
                    cb_caida = self.callbacks.get("on_nodo_caido")
                    if cb_caida:
                        cb_caida(caido)

    def _verificar_sucursales(self):
        for nid in self.directorio:
            if nid == self.mi_id:
                continue
            resp = self.enviar_paquete(nid, {"tipo": "HEARTBEAT"}, timeout=2.0)
            if resp and resp.get("tipo") == "HEARTBEAT_ACK":
                self.fallos_consecutivos[nid] = 0
                if nid in self.nodos_caidos:
                    self.nodos_caidos.remove(nid)
                    self.log_consenso(f"NODO_RECUPERADO Nodo {nid}")
                    self.imprimir(f"\n[*] Nodo {nid} ha regresado al clúster.")
                    cb_rec = self.callbacks.get("on_nodo_recuperado")
                    if cb_rec:
                        cb_rec(nid)
            else:
                self.fallos_consecutivos[nid] += 1
                if self.fallos_consecutivos[nid] == FALLOS_PARA_CAIDA and nid not in self.nodos_caidos:
                    self.nodos_caidos.add(nid)
                    self.log_consenso(f"FALLA_DETECTADA Sucursal Nodo {nid}")
                    self.imprimir(f"\n[!] Sucursal Nodo {nid} caída. Iniciando redistribución...")
                    cb_caida = self.callbacks.get("on_nodo_caido")
                    if cb_caida:
                        cb_caida(nid)

    # ------------------------------------------------------------------
    # Consenso por Mayoría (API que llamarán CLI y Lógica de Inventario)
    # ------------------------------------------------------------------
    def solicitar_operacion(self, operacion, payload):
        """Punto de entrada único para cualquier escritura distribuida."""
        if self.soy_maestro:
            return self.coordinar_consenso(operacion, payload)

        if self.maestro_id is None:
            return False, "No hay maestro disponible en el clúster.", None

        resp = self.enviar_paquete(
            self.maestro_id,
            {"tipo": "SOLICITUD_OPERACION", "datos": {"operacion": operacion, "payload": payload}},
            timeout=8.0,
        )
        if not resp:
            return False, "Sin respuesta del maestro.", None
        return resp.get("exito", False), resp.get("detalle", ""), resp.get("resultado")

    def coordinar_consenso(self, operacion, payload):
        tx_id = f"tx-{uuid.uuid4().hex[:8]}"
        self.log_consenso(f"PROPUESTA_INICIADA tx={tx_id} op={operacion}")

        # 1. Validación local del maestro (ej. bloqueo optimista / espacio disponible)
        validar_fn = self.callbacks.get("validar_operacion")
        if validar_fn:
            ok_local, motivo = validar_fn(operacion, payload)
            if not ok_local:
                self.log_consenso(f"ABORT_LOCAL tx={tx_id} op={operacion} motivo={motivo}")
                return False, motivo, None

        # 2. Recolectar votos del clúster (el maestro ya cuenta como 1 voto a favor)
        votos_ok = 1
        participantes = []
        for nid in self.directorio:
            if nid == self.mi_id or nid in self.nodos_caidos:
                continue
            resp = self.enviar_paquete(
                nid,
                {
                    "tipo": "PROPUESTA_CONSENSO",
                    "datos": {"tx_id": tx_id, "operacion": operacion, "payload": payload},
                },
                timeout=3.0,
            )
            if resp and resp.get("tipo") == "VOTO_OK":
                votos_ok += 1
                participantes.append(nid)

        self.log_consenso(f"CONTEO_VOTOS tx={tx_id} votos={votos_ok}/{len(self.directorio)} quorum={self.quorum}")

        # 3. Decisión por mayoría
        if votos_ok < self.quorum:
            self.log_consenso(f"ABORT_SIN_QUORUM tx={tx_id} op={operacion}")
            return False, f"Consenso fallido: solo {votos_ok} votos (se requieren {self.quorum}).", None

        # 4. Aplicar COMMIT local y replicar a nodos activos
        aplicar_fn = self.callbacks.get("aplicar_operacion")
        resultado = aplicar_fn(operacion, payload) if aplicar_fn else None

        for nid in participantes:
            self.enviar_paquete(
                nid,
                {
                    "tipo": "COMMIT",
                    "datos": {"tx_id": tx_id, "operacion": operacion, "payload": payload},
                },
                timeout=3.0,
            )

        self.log_consenso(f"COMMIT_EXITOSO tx={tx_id} op={operacion}")
        return True, "Operación confirmada por consenso.", resultado


if __name__ == "__main__":
    # Prueba rápida independiente de elección Bully, heartbeats y consenso
    nodo = NodoCluster()
    nodo.iniciar()
    try:
        while True:
            cmd = input(f"[{nodo.mi_alias} | Maestro={nodo.maestro_id}] Comando (estado / probar / salir): ").strip().lower()
            if cmd == "estado":
                print(f"ID: {nodo.mi_id} | Maestro: {nodo.maestro_id} | Caídos: {sorted(nodo.nodos_caidos)}")
            elif cmd == "probar":
                ok, msg, _ = nodo.solicitar_operacion("PRUEBA_PING", {"nota": "verificacion_quorum"})
                print(f"Resultado consenso -> {ok}: {msg}")
            elif cmd == "salir":
                break
    except KeyboardInterrupt:
        pass
    finally:
        nodo.detener_nodo()
        sys.exit(0)