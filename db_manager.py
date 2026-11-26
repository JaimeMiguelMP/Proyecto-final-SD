from contextlib import contextmanager
from pathlib import Path
import sqlite3

from init_db import DEFAULT_SCHEMA


class BaseLocal:
    def __init__(self, ruta, directorio):
        self.ruta = Path(ruta)
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        with self.conexion() as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(Path(DEFAULT_SCHEMA).read_text(encoding="utf-8"))
        with self.transaccion() as db:
            ids = set(directorio)
            db.executemany("DELETE FROM sucursal WHERE id_sucursal = ?",
                           ((fila[0],) for fila in db.execute("SELECT id_sucursal FROM sucursal")
                            if fila[0] not in ids))
            # Un directorio idéntico en cada réplica configura el mismo estado inicial.
            db.execute("UPDATE sucursal SET alias = '@' || id_sucursal, ip = '@' || id_sucursal")
            db.executemany("""
                INSERT INTO sucursal (id_sucursal, alias, ip, capacidad_max)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id_sucursal) DO UPDATE SET
                    alias = excluded.alias, ip = excluded.ip, capacidad_max = excluded.capacidad_max
            """, ((nid, info["alias"], info["ip"] if "puerto" not in info
                   else f'{info["ip"]}:{info["puerto"]}', info.get("capacidad_max", 10))
                  for nid, info in sorted(directorio.items())))

    @contextmanager
    def conexion(self):
        db = sqlite3.connect(self.ruta, isolation_level=None, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys = ON")
            db.execute("PRAGMA synchronous = FULL")
            yield db
        finally:
            db.close()

    @contextmanager
    def transaccion(self):
        with self.conexion() as db:
            db.execute("BEGIN IMMEDIATE")
            with db:
                yield db
