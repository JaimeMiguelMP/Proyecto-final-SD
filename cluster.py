import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import socketserver
import threading
import uuid

from clientes import actualizar_cliente, consultar_clientes, guardar_cliente
from consenso import Consenso
from db_manager import BaseLocal
from eleccion_maestro import EleccionMaestro
from inventario import agregar_articulo, consultar_inventario
from redistribucion import recuperar_sucursal, redistribuir_caida
from replicacion import Replicacion, codificar
from ventas import comprar_unidad


PORT = 5000
CONFIG_FILE = Path(__file__).with_name("directory.json")
HEARTBEAT_INTERVALO = 1.0
OPERACIONES = {
    "AGREGAR_ARTICULO": agregar_articulo,
    "CREAR_CLIENTE": guardar_cliente,
    "ACTUALIZAR_CLIENTE": actualizar_cliente,
    "COMPRAR_UNIDAD": comprar_unidad,
    "REDISTRIBUIR_CAIDA": redistribuir_caida,
    "RECUPERAR_SUCURSAL": recuperar_sucursal,
    "PRUEBA_PING": lambda db, datos, fecha: datos,
}
OPERACIONES_PUBLICAS = OPERACIONES.keys() - {"REDISTRIBUIR_CAIDA", "RECUPERAR_SUCURSAL"}
CONSULTAS = {"clientes": consultar_clientes, "inventario": consultar_inventario}


def obtener_ip_local():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(("192.168.1.254", 80))
            return sock.getsockname()[0]
        except OSError:
            return "127.0.0.1"


def error(detalle):
    return {"exito": False, "error": detalle}


class ServidorNodo(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class PeticionNodo(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.settimeout(10)
        try:
            linea = self.rfile.readline()
            if not linea:
                return
            paquete = json.loads(linea)
            respuesta = self.server.nodo.recibir(paquete["tipo"], paquete.get("datos", {}))
            self.wfile.write((codificar(respuesta) + "\n").encode("utf-8"))
        except (OSError, ValueError, TypeError, KeyError):
            return


class NodoCluster:
    def __init__(self, config_file=CONFIG_FILE, nodo_id=None, ruta_db=None):
        with open(config_file, encoding="utf-8") as archivo:
            self.directorio = {int(nid): info for nid, info in json.load(archivo).items()}
        if nodo_id is None:
            ip = obtener_ip_local()
            nodo_id = next((nid for nid, info in self.directorio.items() if info["ip"] == ip), None)
        if nodo_id not in self.directorio:
            raise ValueError("Indica con --nodo un ID presente en el directorio")

        self.mi_id = nodo_id
        self.detener = threading.Event()
        self.eleccion = EleccionMaestro(nodo_id, self.directorio)
        self.base = BaseLocal(ruta_db or Path("datos") / f"nodo-{nodo_id}" / "inventario.db",
                              self.directorio)
        self.replica = Replicacion(self.base, OPERACIONES)
        self.consenso = Consenso(self.base, self.replica, nodo_id, self.directorio, self.contactar)

    @property
    def maestro_id(self):
        return self.eleccion.maestro

    @property
    def soy_maestro(self):
        return self.maestro_id == self.mi_id

    def iniciar(self):
        config = self.directorio[self.mi_id]
        self.servidor = ServidorNodo((config["ip"], config.get("puerto", PORT)), PeticionNodo)
        self.servidor.nodo = self
        self.hilo_servidor = threading.Thread(target=self.servidor.serve_forever, daemon=True)
        self.hilo_monitoreo = threading.Thread(target=self._monitorear, daemon=True)
        self.hilo_servidor.start()
        self.hilo_monitoreo.start()

    def detener_nodo(self):
        self.detener.set()
        self.servidor.shutdown()
        self.servidor.server_close()
        self.hilo_servidor.join()
        self.hilo_monitoreo.join()

    def contactar(self, nid, tipo, datos=None):
        info = self.directorio[nid]
        paquete = {"tipo": tipo, "datos": datos or {}}
        try:
            with socket.create_connection((info["ip"], info.get("puerto", PORT)), timeout=0.7) as sock:
                sock.settimeout(30 if tipo == "SOLICITUD_OPERACION" else 1.5)
                sock.sendall((codificar(paquete) + "\n").encode("utf-8"))
                with sock.makefile("rb") as archivo:
                    linea = archivo.readline()
                return json.loads(linea) if linea else None
        except (OSError, ValueError):
            return None

    def recibir(self, tipo, datos):
        if tipo == "HEARTBEAT":
            return {"maestro_id": self.maestro_id, "aplicada": self.replica.aplicada()}
        if tipo in {"PREPARAR", "ACEPTAR"}:
            return self.consenso.recibir(tipo, datos)
        if tipo == "CONFIRMAR":
            self.replica.aprender(datos["entradas"])
            return {"ok": True}
        if tipo == "SOLICITUD_SYNC":
            return {"entradas": self.replica.desde(datos["desde"])}
        if tipo == "SOLICITUD_OPERACION":
            if datos["operacion"] not in OPERACIONES_PUBLICAS:
                return error("Operación desconocida")
            return self._coordinar(datos)
        return error(f"Tipo desconocido: {tipo}")

    def _sincronizar(self, nid, hasta):
        while not self.detener.is_set() and self.replica.aplicada() < hasta:
            desde = self.replica.aplicada()
            respuesta = self.contactar(nid, "SOLICITUD_SYNC", {"desde": desde})
            if not respuesta or not respuesta.get("entradas"):
                return
            self.replica.aprender(respuesta["entradas"])
            if self.replica.aplicada() <= desde:
                return

    def _monitorear(self):
        maestro_recuperado = False
        while not self.detener.is_set():
            alcanzables = {self.mi_id}
            for nid in self.directorio.keys() - {self.mi_id}:
                respuesta = self.contactar(nid, "HEARTBEAT")
                if respuesta is not None:
                    alcanzables.add(nid)
                    self._sincronizar(nid, respuesta["aplicada"])
            self.eleccion.observar(alcanzables)
            if self.soy_maestro and len(alcanzables) >= self.consenso.quorum:
                try:
                    if not maestro_recuperado:
                        self.consenso.coordinar()
                        maestro_recuperado = True
                    self._actualizar_sucursales(alcanzables)
                except (RuntimeError, OSError) as causa:
                    print(f"Monitoreo: {causa}", flush=True)
            else:
                maestro_recuperado = False
            self.detener.wait(HEARTBEAT_INTERVALO)

    def _actualizar_sucursales(self, alcanzables):
        with self.base.conexion() as db:
            estados = dict(db.execute("SELECT id_sucursal, estado FROM sucursal").fetchall())
        for nid in sorted(self.eleccion.caidos):
            if estados[nid] != "CAIDA":
                self._coordinar(self._crear_comando("REDISTRIBUIR_CAIDA", {"id_sucursal": nid}))
        for nid in sorted(alcanzables):
            if estados[nid] == "CAIDA":
                self._coordinar(self._crear_comando("RECUPERAR_SUCURSAL", {"id_sucursal": nid}))

    @staticmethod
    def _crear_comando(operacion, datos, solicitud=None):
        return {"operacion": operacion, "datos": datos, "solicitud": solicitud or uuid.uuid4().hex,
                "fecha": datetime.now(timezone.utc).isoformat(timespec="seconds")}

    def _coordinar(self, comando):
        if not self.soy_maestro:
            return error("El nodo dejó de ser maestro. Reintenta con el mismo ID de solicitud")
        try:
            return self.consenso.coordinar(comando)
        except RuntimeError as causa:
            return error(str(causa))

    def solicitar_operacion(self, operacion, payload, solicitud=None):
        if operacion not in OPERACIONES_PUBLICAS or not isinstance(payload, dict):
            return False, "Operación desconocida o datos inválidos", None

        comando = self._crear_comando(operacion, payload, solicitud)
        try:
            codificar(comando)
        except (TypeError, ValueError):
            return False, "Datos inválidos", None
        if self.soy_maestro:
            respuesta = self._coordinar(comando)
        elif self.maestro_id is not None:
            respuesta = self.contactar(self.maestro_id, "SOLICITUD_OPERACION", comando)
        else:
            respuesta = error("Aún no hay un maestro disponible")
        if respuesta is None:
            respuesta = error("Sin respuesta del maestro. El resultado puede estar pendiente")
        return respuesta["exito"], respuesta.get("error", "Operación confirmada"), respuesta.get("resultado")

    def consultar(self, nombre):
        with self.base.conexion() as db:
            return CONSULTAS[nombre](db)

    def estado(self):
        return {"nodo": self.mi_id, "maestro": self.maestro_id,
                "caidos": sorted(self.eleccion.caidos), "posicion_aplicada": self.replica.aplicada()}


def main():
    parser = argparse.ArgumentParser(description="Nodo del inventario distribuido")
    parser.add_argument("--nodo", type=int)
    parser.add_argument("--config", default=CONFIG_FILE, type=Path)
    parser.add_argument("--db", type=Path)
    args = parser.parse_args()
    nodo = NodoCluster(args.config, args.nodo, args.db)
    nodo.iniciar()
    print("Comandos: estado, clientes, inventario, probar, salir, u operación seguida de JSON.")
    try:
        while True:
            comando, _, argumentos = input(f"[nodo {nodo.mi_id}] ").strip().partition(" ")
            if comando == "salir":
                break
            if comando == "estado":
                respuesta = nodo.estado()
            elif comando in CONSULTAS:
                respuesta = nodo.consultar(comando)
            elif comando == "probar":
                respuesta = nodo.solicitar_operacion("PRUEBA_PING", {})
            elif comando in OPERACIONES_PUBLICAS:
                try:
                    datos = json.loads(argumentos)
                    solicitud = datos.pop("solicitud", None) if isinstance(datos, dict) else None
                    respuesta = nodo.solicitar_operacion(comando, datos, solicitud)
                except (TypeError, ValueError) as causa:
                    respuesta = error(str(causa))
            else:
                respuesta = error("Comando desconocido")
            print(codificar(respuesta), flush=True)
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        nodo.detener_nodo()


if __name__ == "__main__":
    main()
