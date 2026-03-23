FROM python:3.11-slim-bookworm

# evitar prompts interativos
ENV DEBIAN_FRONTEND=noninteractive

# instalar dependências do sistema + Java
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    gcc \
    && apt-get clean

# definir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# definir diretório de trabalho
WORKDIR /app

# atualizar pip (IMPORTANTE)
RUN pip install --upgrade pip

# copiar requirements e instalar libs
COPY requirements.txt .
RUN pip install -r requirements.txt

# copiar código
COPY data data
COPY src src

# comando padrão
CMD ["python", "-m", "src.main"]