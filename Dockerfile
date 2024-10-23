FROM flink:1.17.1
 
RUN apt-get update -y \
    && apt-get install -y python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python

# Instala PyFlink
RUN pip install apache-flink==1.17.1 kafka-python --upgrade

# Instalar el cliente de Elasticsearch para Python
RUN pip install elasticsearch

# Instalar libreria rich
RUN pip install rich

# Copiar jars de kafka
COPY ./jars/flink-connector-kafka-3.1.0-1.17.jar /opt/flink/lib/
COPY ./jars/kafka-clients-3.8.0.jar /opt/flink/lib/

WORKDIR /app