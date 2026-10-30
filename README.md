# Productor-Consumidor con Semáforo Redis + MySQL

Simulación de un sistema de transacciones bancarias que demuestra el patrón productor-consumidor
con sincronización mediante un semáforo distribuido en Redis.

---

## Descripción

Un productor genera transacciones bancarias aleatorias y las inserta en MySQL con estado `PENDIENTE`.
Dos consumidores compiten por procesarlas y cambiar su estado a `PROCESADO`.

Para evitar que ambos consumidores procesen la misma transacción al mismo tiempo, el acceso a la
sección crítica se controla con un semáforo distribuido en Redis usando el patrón `SET NX EX`.

---

## Arquitectura

```
┌─────────────┐     INSERT PENDIENTE     ┌───────────────────┐
│  Productor  │ ────────────────────────▶│      MySQL        │
│  (Python)   │                          │  tabla: transact  │
└─────────────┘                          └───────────────────┘
                                                ▲      ▲
┌─────────────┐  SET NX EX  ┌──────────┐       │      │
│ Consumidor1 │────────────▶│  Redis   │       │      │
│  (Python)   │             │ Semáforo │ UPDATE│      │
└─────────────┘             └──────────┘───────┘      │
┌─────────────┐  BLOQUEADO hasta que C1 libere         │
│ Consumidor2 │──────────────────────────── PROCESADO ─┘
│  (Python)   │
└─────────────┘
```

### Contenedores

| Contenedor   | Imagen              | Función                                      |
|--------------|---------------------|----------------------------------------------|
| `mysql`      | `mysql:8`           | Base de datos con la tabla transaccional     |
| `redis`      | `redis:7`           | Almacena el semáforo distribuido             |
| `productor`  | Dockerfile propio   | Inserta transacciones PENDIENTE cada 1-2s    |
| `consumidor1`| Dockerfile propio   | Procesa transacciones (con o sin semáforo)   |
| `consumidor2`| Misma imagen que C1 | Procesa transacciones (con o sin semáforo)   |

---

## Estructura del proyecto

```
.
├── .env                     # Variables de entorno (credenciales, configuración)
├── .gitignore
├── docker-compose.yml       # Definición de los 5 contenedores
├── README.md
├── mysql/
│   └── init.sql             # Creación automática de la BD y tabla transact
├── productor/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── productor.py         # Lógica del productor
├── consumidor/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumidor.py        # Lógica del consumidor + semáforo Redis
└── evidencias/              # Logs y capturas (no se sube al repo)
```

---

## Requisitos previos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- Puertos `3306` (MySQL) y `6379` (Redis) libres en tu máquina

---

## Variables de entorno

Crear un archivo `.env` en la raíz del proyecto con el siguiente contenido:

```env
# MySQL
MYSQL_ROOT_PASSWORD=root1234
MYSQL_DATABASE=so_proyecto
MYSQL_USER=so_user
MYSQL_PASSWORD=so_pass1234

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Semáforo
SEMAFORO_KEY=semaforo:transacciones
SEMAFORO_TTL=10
```

> El archivo `.env` está en `.gitignore` y no se sube al repositorio.

---

## Despliegue

### Levantar el sistema completo

```bash
docker compose up -d --build
```

Esto construye las imágenes, descarga MySQL y Redis, y levanta los 5 contenedores en el orden
correcto. MySQL y Redis deben estar `healthy` antes de que arranquen los demás servicios.

### Ver estado de los contenedores

```bash
docker ps
```

### Ver logs en tiempo real

```bash
# Todos los contenedores
docker compose logs -f

# Solo el productor
docker logs -f productor

# Solo los consumidores
docker compose logs -f consumidor1 consumidor2
```

### Detener el sistema

```bash
# Solo detener
docker compose down

# Detener y eliminar volumen de MySQL (borra todos los datos)
docker compose down -v
```

---

## Escenario A — Con semáforo (comportamiento correcto)

Es el comportamiento por defecto. En `docker-compose.yml` ambos consumidores tienen:

```yaml
USE_SEMAFORO: "true"
```

**Qué se observa en los logs:**

```
[10:15:01] consumidor1 → ADQUIRIÓ semáforo  [token=consumidor1-a1b2c3d4]
[10:15:01] consumidor1 → PROCESANDO id=  12  cuenta=GT-12345678  tipo=DEPOSITO  monto=Q  500.00
[10:15:01] consumidor2 → ESPERANDO semáforo (ocupado por otro consumidor)...
[10:15:03] consumidor1 → COMPLETÓ   id=  12  (demora=2.10s)
[10:15:03] consumidor1 → LIBERÓ semáforo  [token=consumidor1-a1b2c3d4]
[10:15:03] consumidor2 → ADQUIRIÓ semáforo  [token=consumidor2-e5f6g7h8]
[10:15:03] consumidor2 → PROCESANDO id=  13  ...
```

**Verificación en MySQL — ninguna transacción procesada dos veces:**

```sql
-- Debe retornar 0 filas
SELECT id, COUNT(*) AS veces
FROM transact
WHERE estado = 'PROCESADO'
GROUP BY id HAVING COUNT(*) > 1;
```

**Ver estado del semáforo en Redis:**

```bash
docker exec redis redis-cli GET semaforo:transacciones
# Ocupado: muestra el token del consumidor dueño
# Libre:   (nil)
```

---

## Escenario B — Sin semáforo (condición de carrera)

### 1. Cambiar la configuración

En `docker-compose.yml`, cambiar `USE_SEMAFORO` a `"false"` en ambos consumidores:

```yaml
CONSUMER_ID:  consumidor1
USE_SEMAFORO: "false"    # ← cambiar

CONSUMER_ID:  consumidor2
USE_SEMAFORO: "false"    # ← cambiar
```

### 2. Limpiar datos anteriores y reiniciar

```bash
docker exec mysql mysql -uso_user -pso_pass1234 so_proyecto -e "DELETE FROM transact;"

docker compose up -d consumidor1 consumidor2
```

> No se necesita `--build` porque solo cambió una variable de entorno, no el código.

### 3. Observar la condición de carrera

```bash
docker compose logs -f consumidor1 consumidor2
```

Busca líneas con `⚠ CONFLICTO`:

```
[10:20:05] consumidor1 → [SIN SEMÁFORO] Entra directo a sección crítica
[10:20:05] consumidor2 → [SIN SEMÁFORO] Entra directo a sección crítica
[10:20:05] consumidor1 → PROCESANDO id=  5  ...
[10:20:05] consumidor2 → PROCESANDO id=  5  ...   ← misma transacción
[10:20:07] consumidor2 → COMPLETÓ   id=  5
[10:20:08] consumidor1 → ⚠ CONFLICTO id=  5  ya fue procesada por otro consumidor
```

### 4. Restaurar Escenario A

Volver a `USE_SEMAFORO: "true"` y reiniciar:

```bash
docker compose up -d consumidor1 consumidor2
```

---

## Tabla transact

```sql
CREATE TABLE transact (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    cuenta        VARCHAR(20),    -- número de cuenta (ej: GT-12345678)
    tipo          VARCHAR(20),    -- DEPOSITO | RETIRO | TRANSFERENCIA
    monto         DECIMAL(10,2),  -- monto de la operación
    estado        VARCHAR(20),    -- PENDIENTE | PROCESADO
    pid           INT,            -- PID del proceso productor
    creado_en     DATETIME,       -- timestamp de inserción (automático)
    procesado_por VARCHAR(50),    -- ID del consumidor que procesó
    procesado_en  DATETIME        -- timestamp de procesamiento
);
```

---

## Consultas útiles

Conectarse a MySQL desde la terminal:

```bash
docker exec -it mysql mysql -uso_user -pso_pass1234 so_proyecto
```

O desde MySQL Workbench con `127.0.0.1:3306`, usuario `so_user`, contraseña `so_pass1234`.

```sql
-- Resumen general
SELECT
  COUNT(*)                  AS total,
  SUM(estado='PENDIENTE')   AS pendientes,
  SUM(estado='PROCESADO')   AS procesados
FROM transact;

-- Cuántas transacciones procesó cada consumidor
SELECT procesado_por, COUNT(*) AS procesadas
FROM transact
WHERE estado = 'PROCESADO'
GROUP BY procesado_por;

-- Últimas 10 procesadas
SELECT id, cuenta, tipo, monto, procesado_por, procesado_en
FROM transact
WHERE estado = 'PROCESADO'
ORDER BY procesado_en DESC
LIMIT 10;

-- Verificar cero duplicados (Escenario A)
SELECT id, COUNT(*) AS veces
FROM transact
WHERE estado = 'PROCESADO'
GROUP BY id HAVING COUNT(*) > 1;
```

---

## Implementación del semáforo

**Adquirir** — atómica por diseño de Redis:
```
SET semaforo:transacciones <token_unico> NX EX 10
```
- `NX`: solo setea si la clave no existe (exclusión mutua)
- `EX 10`: expira en 10 segundos (evita deadlock si el proceso muere)

**Liberar** — script Lua para garantizar atomicidad:
```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
```
El script verifica que el token sea del consumidor dueño antes de borrar,
evitando que un consumidor libere el semáforo de otro.
