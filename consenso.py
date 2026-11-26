import json
import threading
import time

from replicacion import codificar


class Consenso:
    """Paxos por posición para un conjunto fijo de nodos con almacenamiento persistente."""

    def __init__(self, base, replica, nodo, miembros, contactar):
        self.base = base
        self.replica = replica
        self.nodo = nodo
        self.miembros = tuple(miembros)
        self.quorum = len(self.miembros) // 2 + 1
        self.contactar = contactar
        self.escrituras = threading.Lock()
        self.ronda = 0
        with base.conexion() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS votos (
                    posicion INTEGER PRIMARY KEY,
                    promesa TEXT NOT NULL,
                    aceptada TEXT,
                    comando TEXT
                );
                CREATE TABLE IF NOT EXISTS ronda_local (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    ronda TEXT NOT NULL
                );
                INSERT OR IGNORE INTO ronda_local VALUES (1, '0');
            """)

    def recibir(self, tipo, datos):
        posicion = datos["posicion"]
        papeleta = tuple(datos["papeleta"])
        with self.base.transaccion() as db:
            fila = db.execute("SELECT * FROM votos WHERE posicion = ?", (posicion,)).fetchone()
            if fila and tuple(json.loads(fila["promesa"])) > papeleta:
                return {"ok": False, "promesa": json.loads(fila["promesa"])}

            serializada = codificar(papeleta)
            if tipo == "PREPARAR":
                db.execute("""
                    INSERT INTO votos (posicion, promesa) VALUES (?, ?)
                    ON CONFLICT(posicion) DO UPDATE SET promesa = excluded.promesa
                """, (posicion, serializada))
                if fila and fila["aceptada"] is not None:
                    return {"ok": True, "aceptada": json.loads(fila["aceptada"]),
                            "comando": json.loads(fila["comando"])}
                return {"ok": True}

            comando = codificar(datos["comando"])
            if fila and fila["aceptada"] == serializada and fila["comando"] != comando:
                return {"ok": False, "promesa": list(papeleta)}
            db.execute("""
                INSERT INTO votos VALUES (?, ?, ?, ?)
                ON CONFLICT(posicion) DO UPDATE SET promesa = excluded.promesa,
                    aceptada = excluded.aceptada, comando = excluded.comando
            """, (posicion, serializada, serializada, comando))
            return {"ok": True}

    def _nueva_papeleta(self):
        with self.base.transaccion() as db:
            anterior = int(db.execute("SELECT ronda FROM ronda_local").fetchone()[0])
            self.ronda = max(time.time_ns(), anterior + 1, self.ronda + 1)
            db.execute("UPDATE ronda_local SET ronda = ?", (str(self.ronda),))
        return [self.ronda, self.nodo]

    def _fase(self, tipo, datos):
        respuestas = []
        for nid in self.miembros:
            if nid == self.nodo:
                respuesta = self.recibir(tipo, datos)
            else:
                respuesta = self.contactar(nid, tipo, datos)
            if respuesta and respuesta.get("ok"):
                respuestas.append(respuesta)
            elif respuesta and "promesa" in respuesta:
                self.ronda = max(self.ronda, respuesta["promesa"][0])
        return respuestas

    def _resolver_posicion(self, posicion, propuesta):
        for _ in range(2):
            datos = {"posicion": posicion, "papeleta": self._nueva_papeleta()}
            promesas = self._fase("PREPARAR", datos)
            if len(promesas) < self.quorum:
                continue
            anteriores = [voto for voto in promesas if "aceptada" in voto]
            if anteriores:
                propuesta_elegida = max(anteriores, key=lambda voto: voto["aceptada"])["comando"]
            else:
                propuesta_elegida = propuesta
            if propuesta_elegida is None:
                return False

            votos = self._fase("ACEPTAR", {**datos, "comando": propuesta_elegida})
            if len(votos) < self.quorum:
                continue
            entradas = [[posicion, propuesta_elegida]]
            self.replica.aprender(entradas)
            for nid in self.miembros:
                if nid != self.nodo:
                    self.contactar(nid, "CONFIRMAR", {"entradas": entradas})
            return True
        raise RuntimeError("No se alcanzó el quórum. El resultado puede estar pendiente")

    def coordinar(self, comando=None):
        with self.escrituras:
            while comando is None or self.replica.resultado(comando["solicitud"]) is None:
                if not self._resolver_posicion(self.replica.aplicada() + 1, comando):
                    break
            if comando is not None:
                return self.replica.resultado(comando["solicitud"])
