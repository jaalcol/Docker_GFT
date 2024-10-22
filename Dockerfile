# Usa Ubuntu 22.04 como base
FROM ubuntu:22.04

# Establece el directorio de trabajo
WORKDIR /app

# Actualiza los paquetes del sistema e instala las dependencias necesarias
RUN apt-get update && \
    apt-get install -y \
    wget \
    curl \
    tar \
    openjdk-11-jdk \
    python3.10 \
    python3-pip

# Configura JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Descarga y extrae Apache Flink 1.17.1
RUN wget https://archive.apache.org/dist/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz && \
    tar -xvzf flink-1.17.1-bin-scala_2.12.tgz && \
    mv flink-1.17.1 /opt/flink && \
    rm flink-1.17.1-bin-scala_2.12.tgz

# Establece las variables de entorno para Flink
ENV FLINK_HOME=/opt/flink
ENV PATH="$FLINK_HOME/bin:$PATH"

# Instala PyFlink
RUN python3 -m pip install apache-flink==1.17.1

# Copia el archivo de configuración de Flink modificado
COPY flink-conf.yaml $FLINK_HOME/conf/

# Copia el contenido de la aplicación al contenedor
COPY . /app

# Define el comando por defecto
CMD ["/bin/bash"]
# Comando por defecto para mantener el contenedor en ejecución
#CMD ["/bin/bash", "-c", "/opt/flink/bin/jobmanager.sh start-foreground & /opt/flink/bin/taskmanager.sh start-foreground"]