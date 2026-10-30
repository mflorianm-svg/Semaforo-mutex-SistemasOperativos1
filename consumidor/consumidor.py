"""
consumidor.py
────────────────────────────────────────────────────────────────
Procesa transacciones bancarias de MySQL con estado PENDIENTE.

Sincronización:
  - Escenario A (USE_SEMAFORO=true):
      Usa un semáforo distribuido en Redis (SET NX EX) para garantizar
      que solo un consumidor a la vez acceda a la sección crítica.
      La liberación se hace con un script Lua para que sea atómica
      (verifica dueño y borra en una sola operación).

  - Escenario B (USE_SEMAFORO=false):
      No adquiere ni libera el semáforo. Ambos consumidores entran
      directamente a la sección crítica → demuestra condición de carrera.
"""

import os
import random
import time
import uuid
import mysql.connector
import redis
from datetime import datetime

# ── Configuración desde variables de entorno ─────────────────
CONSUMER_ID  = os.environ.get("CONSUMER_ID",    "consumidor")
USE_SEMAFORO = os.environ.get("USE_SEMAFORO",   "true").lower() == "true"
SEMAFORO_KEY = os.environ.get("SEMAFORO_KEY",   "semaforo:transacciones")
SEMAFORO_TTL = int(os.environ.get("SEMAFORO_TTL", "10"))

DB_CONFIG = {
    "host":     os.environ.get("MYSQL_HOST",     "mysql"),
    "database": os.environ.get("MYSQL_DATABASE", "so_proyecto"),
    "user":     os.environ.get("MYSQL_USER",     "so_user"),
    "password": os.environ.get("MYSQL_PASSWORD", "so_pass1234"),
}

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# ── Script Lua para liberación atómica del semáforo ──────────
#   Comprueba que el valor almacenado en Redis sea igual al
#   id_unico que pasamos. Solo si coincide, elimina la clave.
#   Esto evita que un consumidor libere el semáforo de otro.
LUA_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""


def ts() -> str:
    """Retorna timestamp legible HH:MM:SS para logs."""
    return datetime.now().strftime("%H:%M:%S")


# ── Conexión a MySQL con reintentos ──────────────────────────

def conectar_mysql(max_reintentos: int = 15, espera: int = 3):
    for intento in range(1, max_reintentos + 1):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            print(
                f"[{ts()}] {CONSUMER_ID} → Conexión a MySQL exitosa",
                flush=True,
            )
            return conn
        except mysql.connector.Error as err:
            print(
                f"[{ts()}] {CONSUMER_ID} → MySQL intento {intento}/{max_reintentos}: "
                f"{err}. Reintentando en {espera}s...",
                flush=True,
            )
            time.sleep(espera)
    raise RuntimeError(f"{CONSUMER_ID}: No se pudo conectar a MySQL.")


def conectar_redis(max_reintentos: int = 10, espera: int = 2):
    for intento in range(1, max_reintentos + 1):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print(
                f"[{ts()}] {CONSUMER_ID} → Conexión a Redis exitosa "
                f"({REDIS_HOST}:{REDIS_PORT})",
                flush=True,
            )
            return r
        except redis.exceptions.ConnectionError as err:
            print(
                f"[{ts()}] {CONSUMER_ID} → Redis intento {intento}/{max_reintentos}: "
                f"{err}. Reintentando en {espera}s...",
                flush=True,
            )
            time.sleep(espera)
    raise RuntimeError(f"{CONSUMER_ID}: No se pudo conectar a Redis.")


# ── Semáforo distribuido ─────────────────────────────────────

def adquirir_semaforo(r, id_unico: str) -> bool:
    """
    Intenta adquirir el semáforo.
    SET semaforo:transacciones <id_unico> NX EX <TTL>
    Retorna True si lo adquirió, False si está ocupado.
    """
    resultado = r.set(SEMAFORO_KEY, id_unico, nx=True, ex=SEMAFORO_TTL)
    return resultado is True


def liberar_semaforo(r, id_unico: str) -> bool:
    """
    Libera el semáforo SOLO si este consumidor sigue siendo el dueño.
    Usa script Lua para que la verificación + borrado sea atómica.
    Retorna True si liberó, False si ya no era dueño (expiró o alguien más lo tomó).
    """
    resultado = r.eval(LUA_RELEASE, 1, SEMAFORO_KEY, id_unico)
    return resultado == 1


# ── Lógica de la sección crítica ─────────────────────────────

def obtener_transaccion_pendiente(conn):
    """
    Selecciona la transacción PENDIENTE más antigua.
    Retorna la fila como dict o None si no hay pendientes.
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT id, cuenta, tipo, monto, creado_en
        FROM   transact
        WHERE  estado = 'PENDIENTE'
        ORDER  BY creado_en ASC, id ASC
        LIMIT  1
        """
    )
    fila = cursor.fetchone()
    cursor.close()
    return fila


def marcar_como_procesado(conn, id_transaccion: int, procesado_por: str):
    """Actualiza el estado de la transacción a PROCESADO."""
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE transact
        SET    estado        = 'PROCESADO',
               procesado_por = %s,
               procesado_en  = NOW()
        WHERE  id = %s
          AND  estado = 'PENDIENTE'
        """,
        (procesado_por, id_transaccion),
    )
    conn.commit()
    filas_afectadas = cursor.rowcount
    cursor.close()
    return filas_afectadas  # 0 si otro consumidor ya la procesó (race condition visible en Escenario B)


# ── Ciclo principal ───────────────────────────────────────────

def ciclo_consumidor(conn, r):
    """
    Ciclo infinito del consumidor:
      1. Adquirir semáforo (o entrar directo si USE_SEMAFORO=false)
      2. Seleccionar transacción PENDIENTE más antigua
      3. Simular procesamiento (sleep 1-3s)
      4. Marcar como PROCESADO
      5. Liberar semáforo
    """
    id_unico = f"{CONSUMER_ID}-{uuid.uuid4().hex[:8]}"  # token único para este ciclo

    # ── Paso 1: Adquirir semáforo ─────────────────────────
    if USE_SEMAFORO:
        adquirido = adquirir_semaforo(r, id_unico)
        if not adquirido:
            print(
                f"[{ts()}] {CONSUMER_ID} → ESPERANDO semáforo "
                f"(ocupado por otro consumidor)...",
                flush=True,
            )
            time.sleep(0.5)
            return  # reintentar en el próximo ciclo
        print(
            f"[{ts()}] {CONSUMER_ID} → ADQUIRIÓ semáforo  "
            f"[token={id_unico}]",
            flush=True,
        )
    else:
        print(
            f"[{ts()}] {CONSUMER_ID} → [SIN SEMÁFORO] Entra directo a sección crítica",
            flush=True,
        )

    # ── Paso 2: Seleccionar transacción pendiente ─────────
    try:
        transaccion = obtener_transaccion_pendiente(conn)

        if transaccion is None:
            print(
                f"[{ts()}] {CONSUMER_ID} → No hay transacciones PENDIENTES. Esperando...",
                flush=True,
            )
            if USE_SEMAFORO:
                liberar_semaforo(r, id_unico)
                print(
                    f"[{ts()}] {CONSUMER_ID} → LIBERÓ semáforo (sin trabajo)",
                    flush=True,
                )
            time.sleep(1.0)
            return

        tid = transaccion["id"]
        print(
            f"[{ts()}] {CONSUMER_ID} → PROCESANDO id={tid:<4}  "
            f"cuenta={transaccion['cuenta']}  "
            f"tipo={transaccion['tipo']:<14}  "
            f"monto=Q{float(transaccion['monto']):>9.2f}",
            flush=True,
        )

        # ── Paso 3: Simular procesamiento ─────────────────
        demora = random.uniform(1.0, 3.0)
        time.sleep(demora)

        # ── Paso 4: Marcar como PROCESADO ─────────────────
        filas = marcar_como_procesado(conn, tid, CONSUMER_ID)
        if filas > 0:
            print(
                f"[{ts()}] {CONSUMER_ID} → COMPLETÓ   id={tid:<4}  "
                f"(demora={demora:.2f}s)",
                flush=True,
            )
        else:
            # En Escenario B, esto revela la condición de carrera:
            # la fila ya fue procesada por el otro consumidor
            print(
                f"[{ts()}] {CONSUMER_ID} → ⚠ CONFLICTO id={tid:<4}  "
                f"ya fue procesada por otro consumidor (RACE CONDITION)",
                flush=True,
            )

    except mysql.connector.Error as err:
        print(f"[{ts()}] {CONSUMER_ID} → ERROR MySQL en sección crítica: {err}", flush=True)

    finally:
        # ── Paso 5: Liberar semáforo siempre al salir ─────
        if USE_SEMAFORO:
            liberado = liberar_semaforo(r, id_unico)
            if liberado:
                print(
                    f"[{ts()}] {CONSUMER_ID} → LIBERÓ semáforo  "
                    f"[token={id_unico}]",
                    flush=True,
                )
            else:
                print(
                    f"[{ts()}] {CONSUMER_ID} → EXPIRÓ semáforo antes de liberarlo "
                    f"(TTL agotado)",
                    flush=True,
                )
            # Pausa breve para dar oportunidad al otro consumidor de adquirir
            time.sleep(0.3)


def main():
    
    if USE_SEMAFORO:
        modo = "CON semáforo (Escenario A)"
    else: 
        modo = "SIN semáforo (Escenario B)"
    print(f"[{ts()}] ╔══════════════════════════════════════════╗", flush=True)
    print(f"[{ts()}] ║  {CONSUMER_ID:<10} iniciado                   ║", flush=True)
    print(f"[{ts()}] ║  Modo: {modo:<35}║", flush=True)
    print(f"[{ts()}] ╚══════════════════════════════════════════╝", flush=True)

    conn = conectar_mysql()
    r    = conectar_redis() if USE_SEMAFORO else None

    while True:
        try:
            ciclo_consumidor(conn, r)
        except mysql.connector.Error as err:
            print(f"[{ts()}] {CONSUMER_ID} → Error de conexión MySQL: {err}. Reconectando...", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            conn = conectar_mysql()
        except Exception as err:
            print(f"[{ts()}] {CONSUMER_ID} → Error inesperado: {err}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    
    print(USE_SEMAFORO," ",SEMAFORO_KEY,"------------------------")
    main()
