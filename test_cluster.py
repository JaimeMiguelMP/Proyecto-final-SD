import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
import socket
import tempfile
import time
import unittest

from cluster import NodoCluster


class ClusterTest(unittest.TestCase):
    def setUp(self):
        temporal = self.enterContext(tempfile.TemporaryDirectory())
        self.ruta = Path(temporal)
        self.nodos = {}
        self.addCleanup(self.cerrar)
        with ExitStack() as pila:
            sockets = [pila.enter_context(socket.socket()) for _ in range(4)]
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            directorio = {
                nid: {"alias": f"nodo{nid}", "ip": "127.0.0.1", "puerto": sock.getsockname()[1]}
                for nid, sock in enumerate(sockets, 1)
            }
        self.config = self.ruta / "directory.json"
        self.config.write_text(json.dumps(directorio), encoding="utf-8")
        for nid in reversed(directorio):
            self.arrancar(nid)
        self.esperar(lambda: all(nodo.maestro_id == 4 for nodo in self.nodos.values()))

    def arrancar(self, nid):
        nodo = NodoCluster(self.config, nid, self.ruta / f"nodo-{nid}.db")
        nodo.iniciar()
        self.nodos[nid] = nodo

    def detener(self, nid):
        self.nodos.pop(nid).detener_nodo()

    def cerrar(self):
        with ThreadPoolExecutor() as ejecutor:
            list(ejecutor.map(self.detener, list(self.nodos)))

    def esperar(self, condicion, segundos=15):
        limite = time.monotonic() + segundos
        while time.monotonic() < limite:
            if condicion():
                return
            time.sleep(0.05)
        self.fail("El clúster no alcanzó el estado esperado")

    def ejecutar(self, nid, operacion, datos, solicitud=None):
        exito, detalle, resultado = self.nodos[nid].solicitar_operacion(
            operacion, datos, solicitud,
        )
        self.assertTrue(exito, detalle)
        return resultado

    def test_inventario_ventas_y_recuperacion(self):
        cliente = {"nombre": "Ana", "correo": "ana@example.com",
                   "telefono": "123", "direccion": "Centro"}
        alta = self.ejecutar(1, "CREAR_CLIENTE", cliente, "cliente-ana")
        self.assertEqual(alta, self.ejecutar(2, "CREAR_CLIENTE", cliente, "cliente-ana"))
        cid = alta["id_cliente"]
        actualizado = {**cliente, "direccion": "Norte", "id_cliente": cid, "version_esperada": 1}
        self.ejecutar(2, "ACTUALIZAR_CLIENTE", actualizado)
        self.assertEqual(self.nodos[3].consultar("clientes")[0]["direccion"], "Norte")

        alta = self.ejecutar(2, "AGREGAR_ARTICULO", {
            "nombre": "Libro", "categoria": "Papel", "precio": 100, "series": ["A", "B", "C", "D"],
        })
        self.assertEqual(alta["sucursales"], {"A": 1, "B": 2, "C": 3, "D": 4})
        unidad = self.nodos[1].consultar("inventario")[0]
        compra = {"id_unidad": unidad["id_unidad"], "id_cliente": cid}
        fallo = self.nodos[1].solicitar_operacion("COMPRAR_UNIDAD", {**compra, "id_cliente": 999})
        self.assertFalse(fallo[0])
        self.assertEqual(len(self.nodos[3].consultar("inventario")), 4)
        with ThreadPoolExecutor() as pool:
            resultados = list(pool.map(
                lambda nid: self.nodos[nid].solicitar_operacion("COMPRAR_UNIDAD", compra), [1, 2],
            ))
        exitos = [resultado for ok, _, resultado in resultados if ok]
        self.assertEqual(len(exitos), 1)
        self.assertEqual(exitos[0]["codigo_guia"], f'{alta["id_articulo"]}-A-1-{cid}')
        self.assertEqual(len(self.nodos[3].consultar("inventario")), 3)

        self.detener(4)
        self.esperar(lambda: all(nodo.maestro_id == 3 for nodo in self.nodos.values()))
        self.esperar(lambda: all(
            fila["id_sucursal_actual"] != 4 for fila in self.nodos[3].consultar("inventario")
        ))
        self.ejecutar(1, "AGREGAR_ARTICULO", {
            "nombre": "Agenda", "categoria": "Papel", "precio": 50, "series": ["E"],
        })
        self.arrancar(4)
        self.esperar(lambda: self.nodos[4].consultar("inventario")
                     == self.nodos[3].consultar("inventario"))
        self.esperar(lambda: all(nodo.maestro_id == 4 for nodo in self.nodos.values()))

        self.detener(3)
        self.detener(4)
        self.esperar(lambda: all(nodo.maestro_id == 2 for nodo in self.nodos.values()))
        exito, detalle, _ = self.nodos[1].solicitar_operacion("PRUEBA_PING", {})
        self.assertFalse(exito)
        self.assertIn("quórum", detalle)

    def test_recupera_operacion_aceptada_antes_de_confirmar(self):
        maestro = self.nodos[4]
        comando = maestro._crear_comando("PRUEBA_PING", {"valor": 10}, "pendiente")
        # Se corta la ejecución después de persistir ACEPTAR en tres nodos.
        with maestro.consenso.escrituras:
            posicion = maestro.replica.aplicada() + 1
            datos = {"posicion": posicion, "papeleta": maestro.consenso._nueva_papeleta()}
            self.assertGreaterEqual(len(maestro.consenso._fase("PREPARAR", datos)), 3)
            self.assertTrue(maestro.consenso.recibir("ACEPTAR", {**datos, "comando": comando})["ok"])
            for nid in (1, 2):
                self.assertTrue(maestro.contactar(nid, "ACEPTAR", {**datos, "comando": comando})["ok"])
        self.assertIsNone(maestro.replica.resultado("pendiente"))
        self.detener(4)
        self.esperar(lambda: self.nodos[3].maestro_id == 3)
        self.esperar(lambda: self.nodos[3].replica.resultado("pendiente") is not None)
        self.assertEqual(self.nodos[3].replica.resultado("pendiente")["resultado"], {"valor": 10})


if __name__ == "__main__":
    unittest.main()
