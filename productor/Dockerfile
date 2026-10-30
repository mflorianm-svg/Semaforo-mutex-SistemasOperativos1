# ── Productor ────────────────────────────────────────────────
FROM python:3.11-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias primero (aprovecha caché de capas)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY productor.py .

# El productor arranca directamente
CMD ["python", "-u", "productor.py"]
