import os
import sqlite3
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(BASE_DIR, "inventario.db")
DEFAULT_SCHEMA = os.path.join(BASE_DIR, "schema.sql")


def inicializar_bd(db_path=DEFAULT_DB, schema_path=DEFAULT_SCHEMA, reiniciar=False):
    if reiniciar and os.path.exists(db_path):
        os.remove(db_path)

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()

    print(f"[*] Base de datos lista en: {db_path}")


if __name__ == "__main__":
    reiniciar_flag = "--reset" in sys.argv
    inicializar_bd(reiniciar=reiniciar_flag)