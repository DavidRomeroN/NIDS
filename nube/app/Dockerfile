FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# tzdata: sin esto TZ=America/Lima cae a UTC en silencio y el horario de
# clases del productor de incidentes queda corrido 5 horas.
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

CMD ["sleep", "infinity"]
