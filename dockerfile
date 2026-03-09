FROM python:3.10-slim-bookworm

# instalar java
RUN apt-get update && apt-get install -y openjdk-17-jdk

# definir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# instalar pyspark
RUN pip install pyspark

WORKDIR /app

COPY data/raw data/raw

COPY src src

VOLUME ["src/transform"]

CMD ["python", "-m", "src.transform.transform"]
