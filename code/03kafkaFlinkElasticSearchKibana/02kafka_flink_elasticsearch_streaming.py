from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from elasticsearch import Elasticsearch

def test_elasticsearch_connection():
    # Definir los parámetros de conexión
    es_host = 'http://elasticsearch:9200'
    index_name = 'kafka_flink_elasticsearch_streaming'
    
    try:
        # Conectar a Elasticsearch
        es = Elasticsearch([es_host])
        
        # Verificar si el clúster está disponible
        if es.ping():
            print("Conexión exitosa a Elasticsearch")
            
            # Verificar si el índice existe
            if es.indices.exists(index=index_name):
                print(f"El índice '{index_name}' ya existe.")
            else:
                # Crear el índice si no existe
                es.indices.create(index=index_name)
                print(f"El índice '{index_name}' ha sido creado.")
        else:
            print("No se pudo conectar a Elasticsearch")
    
    except Exception as e:
        print(f"Error al intentar conectarse a Elasticsearch: {e}")

test_elasticsearch_connection()

# Create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Specify connector and format jars
t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///tmp/scripts/jars/flink-sql-connector-kafka-1.17.1.jar;"  # mind -> ;
    "file:///tmp/scripts/jars/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar"
)

# Define source table DDL
source_ddl = """
    CREATE TABLE source_table(
        id_str VARCHAR,
        username VARCHAR,
        tweet VARCHAR,
        location VARCHAR,
        created_at VARCHAR,
        retweet_count BIGINT,
        followers_count BIGINT,
        lang VARCHAR,
        coordinates VARCHAR
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'tweets-sim',
        'properties.bootstrap.servers' = 'docker-kafka-1:29092',
        'properties.group.id' = 'test_3',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
"""

# Define sink table DDL
sink_ddl = """
    CREATE TABLE sink_table(
        id_str VARCHAR,
        username VARCHAR,
        tweet VARCHAR,
        location VARCHAR,
        created_at VARCHAR,
        retweet_count BIGINT,
        followers_count BIGINT,
        lang VARCHAR,
        coordinates VARCHAR
    ) WITH (        
        'connector' = 'elasticsearch-7',
        'index' = 'kafka_flink_elasticsearch_streaming',
        'hosts' = 'http://elasticsearch:9200',
        'format' = 'json'
    )
"""

# Execute DDL statements to create tables
t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

# Retrieve the source table
source_table = t_env.from_path('source_table')

print("Source Table Schema:")
source_table.print_schema()

# Process the data
result_table = source_table.select(
    col("id_str"),
    col("username"),
    col("tweet"),
    col("location"),
    col("created_at"),
    col("retweet_count"),
    col("followers_count"),
    col("lang"),
    col("coordinates")
)

# Retrieve the sink table
sink_table = t_env.from_path('sink_table')
print("Sink Table Schema:")
sink_table.print_schema()

# Insertar los datos procesados en la tabla sink
try:
    result_table.execute_insert('sink_table').wait()
    print("Datos insertados correctamente en Elasticsearch.")
except Exception as e:
    print(f"Error al insertar datos en la tabla sink: {e}")
