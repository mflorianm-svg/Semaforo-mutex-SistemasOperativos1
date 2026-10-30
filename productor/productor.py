"""
productor.py
────────────────────────────────────────────────────────────────
Genera transacciones bancarias aleatorias y las inserta en MySQL
con estado PENDIENTE cada 1-2 segundos.

No usa semáforo porque solo inserta filas nuevas (no modifica
filas existentes, por lo tanto no hay condición de carrera).
"""

import os
import random
import time
import mysql.connector
from datetime import datetime

# ── Configuración desde variables de entorno ────────────────
DB_CONFIG = {
    "host":     os.environ.get("MYSQL_HOST",     "mysql"),
    "database": os.environ.get("MYSQL_DATABASE", "so_proyecto"),
    "user":     os.environ.get("MYSQL_USER",     "so_user"),
    "password": os.environ.get("MYSQL_PASSWORD", "so_pass1234"),
}

PID = os.getpid()   # PID del proceso actual

# Catálogos para generar datos aleatorios
TIPOS_TRANSACCION = ["DEPOSITO", "RETIRO", "TRANSFERENCIA"]
PREFIJOS_CUENTA   = ["GT", "MX", "US", "CO"]


def ts() -> str:
    """Retorna timestamp legible HH:MM:SS para logs."""
    return datetime.now().strftime("%H:%M:%S")


def generar_datos() -> dict:
    """Genera una transacción bancaria con datos aleatorios."""
    prefijo  = random.choice(PREFIJOS_CUENTA)
    numero   = random.randint(10_000_000, 99_999_999)
    cuenta   = f"{prefijo}-{numero}"
    tipo     = random.choice(TIPOS_TRANSACCION)
    monto    = round(random.uniform(10.0, 9_999.99), 2)
    return {"cuenta": cuenta, "tipo": tipo, "monto": monto}


def conectar_con_reintentos(max_reintentos: int = 15, espera: int = 3):
    """
    Intenta conectar a MySQL con reintentos.
    MySQL puede tardar unos segundos en estar listo aunque el
    healthcheck ya pasó.
    """
    for intento in range(1, max_reintentos + 1):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            print(f"[{ts()}] Productor (PID {PID}) → Conexión a MySQL exitosa", flush=True)
            return conn
        except mysql.connector.Error as err:
            print(
                f"[{ts()}] Productor → Intento {intento}/{max_reintentos} "
                f"fallido: {err}. Reintentando en {espera}s...",
                flush=True,
            )
            time.sleep(espera)
    raise RuntimeError("No se pudo conectar a MySQL después de varios intentos.")


def insertar_transaccion(conn, datos: dict) -> int:
    """Inserta una fila con estado PENDIENTE y retorna el id generado."""
    sql = """
        INSERT INTO transact (cuenta, tipo, monto, estado, pid)
        VALUES (%s, %s, %s, 'PENDIENTE', %s)
    """
    cursor = conn.cursor()
    cursor.execute(sql, (datos["cuenta"], datos["tipo"], datos["monto"], PID))
    conn.commit()
    new_id = cursor.lastrowid
    cursor.close()
    return new_id


def main():
    print(f"[{ts()}] ╔══════════════════════════════════════════╗", flush=True)
    print(f"[{ts()}] ║  PRODUCTOR iniciado  —  PID: {PID:<10}  ║", flush=True)
    print(f"[{ts()}] ╚══════════════════════════════════════════╝", flush=True)

    conn = conectar_con_reintentos()
    contador = 0

    while True:
        try:
            datos = generar_datos()
            new_id = insertar_transaccion(conn, datos)
            contador += 1

            print(
                f"[{ts()}] Productor → INSERT  id={new_id:>4}  "
                f"cuenta={datos['cuenta']}  tipo={datos['tipo']:<14}  "
                f"monto=Q{datos['monto']:>9.2f}  "
                f"[total insertadas: {contador}]",
                flush=True,
            )

        except mysql.connector.Error as err:
            # Si la conexión se cayó, intentar reconectar
            print(f"[{ts()}] Productor → ERROR de DB: {err}. Reconectando...", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            conn = conectar_con_reintentos()

        # Espera aleatoria entre 1 y 2 segundos antes de la siguiente inserción
        pausa = random.uniform(1.0, 2.0)
        time.sleep(pausa)


if __name__ == "__main__":
    main()
