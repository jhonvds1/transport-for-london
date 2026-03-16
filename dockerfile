FROM python:3.11-slim-bookworm

# instalar java
RUN apt-get update && apt-get install -y openjdk-17-jdk

# definir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# definir diretório de trabalho
WORKDIR /app

# instalar PySpark
RUN pip install pyspark

# copiar requirements e instalar libs
COPY requirements.txt .
RUN pip install -r requirements.txt

# copiar dados (opcional se você não montar via volume)
COPY data data

# copiar src (opcional, se não for montar via volume)
COPY src src

# comando padrão (pode ser sobrescrito no docker run)
CMD ["python", "-m", "src.main"]