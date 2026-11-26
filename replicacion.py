import json
import sqlite3


def codificar(valor):
    return json.dumps(valor, ensure_ascii=False, sort_keys=True, allow_nan=False)


class Replicacion:
    def __init__(self, base, operaciones):
        self.base = base
        self.operaciones = operaciones
        with self.base.conexion() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS registro_confirmado (
                    posicion INTEGER PRIMARY KEY,
                    comando TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS solicitudes_aplicadas (
                    solicitud TEXT PRIMARY KEY,
                    respuesta TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS progreso_replica (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    posicion INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO progreso_replica VALUES (1, 0);
            """)

    def aplicada(self):
        with self.base.conexion() as db:
            return db.execute("SELECT posicion FROM progreso_replica").fetchone()[0]

    def resultado(self, solicitud):
        with self.base.conexion() as db:
            fila = db.execute("""
                SELECT respuesta FROM solicitudes_aplicadas WHERE solicitud = ?
            """, (solicitud,)).fetchone()
        return json.loads(fila[0]) if fila else None

    def desde(self, posicion, limite=100):
        with self.base.conexion() as db:
            return [[fila[0], json.loads(fila[1])] for fila in db.execute("""
                SELECT posicion, comando FROM registro_confirmado
                WHERE posicion > ? ORDER BY posicion LIMIT ?
            """, (posicion, limite))]

    def aprender(self, entradas):
        with self.base.transaccion() as db:
            for posicion, comando in entradas:
                serializado = codificar(comando)
                db.execute("INSERT OR IGNORE INTO registro_confirmado VALUES (?, ?)",
                           (posicion, serializado))
                existente = db.execute("""
                    SELECT comando FROM registro_confirmado WHERE posicion = ?
                """, (posicion,)).fetchone()[0]
                if existente != serializado:
                    raise RuntimeError(f"Dos operaciones confirmadas en posición {posicion}")

            aplicada = db.execute("SELECT posicion FROM progreso_replica").fetchone()[0]
            while True:
                fila = db.execute("""
                    SELECT comando FROM registro_confirmado WHERE posicion = ?
                """, (aplicada + 1,)).fetchone()
                if fila is None:
                    break
                self._aplicar(db, json.loads(fila[0]))
                aplicada += 1
            db.execute("UPDATE progreso_replica SET posicion = ?", (aplicada,))

    def _aplicar(self, db, comando):
        solicitud = comando["solicitud"]
        if db.execute("SELECT 1 FROM solicitudes_aplicadas WHERE solicitud = ?",
                      (solicitud,)).fetchone():
            return

        db.execute("SAVEPOINT operacion")
        try:
            funcion = self.operaciones[comando["operacion"]]
            resultado = funcion(db, comando["datos"], comando["fecha"])
            respuesta = {"exito": True, "resultado": resultado}
        except (ValueError, TypeError, KeyError, OverflowError, sqlite3.IntegrityError) as error:
            db.execute("ROLLBACK TO operacion")
            respuesta = {"exito": False, "error": str(error)}
        db.execute("RELEASE operacion")
        db.execute("INSERT INTO solicitudes_aplicadas VALUES (?, ?)",
                   (solicitud, codificar(respuesta)))
