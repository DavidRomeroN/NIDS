FROM python:3.10-slim-trixie

# default-jre-headless (OpenJDK 21 en trixie) con JAVA_HOME genérico: la misma
# imagen sirve en amd64 (laptop) y arm64 (Raspberry Pi o VM ARM).
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir pyspark==4.2.0 numpy==2.2.6

WORKDIR /jobs

CMD ["sleep", "infinity"]
