from pyflink.table import TableEnvironment, EnvironmentSettings
from kafka import KafkaAdminClient
import json


def check_kafka_connection(bootstrap_servers, topic):
    try:
        # Crear un cliente de administración para comprobar los tópicos
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        
        # Obtener la lista de tópicos disponibles
        topics = admin_client.list_topics()

        if topic not in topics:
            print(f"El tópico '{topic}' no existe.")
            return False
        else:
            print(f"Conexión exitosa a Kafka. El tópico '{topic}' existe.")
            return True
    except Exception as e:  # Cambiado a Exception
        print(f"Error al conectarse a Kafka: {e}")
        return False

# Specify Kafka connection details
bootstrap_servers = 'docker-kafka-1:29092'
topic = 'tweets-sim'

# Check the Kafka connection and topic existence before proceeding
check_kafka_connection(bootstrap_servers, topic)

# Create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Specify connector and format jars
# t_env.get_config().get_configuration().set_string(
#     "pipeline.jars",
#     "file:///home/angel/flink/lib/flink-sql-connector-kafka-1.17.1.jar"
# )

"""

1. t_env.get_config()

- refers to the table execution environment (Table Environment), which is the context in which SQL or table operations 
 are configured and executed in Flink.

- get_config() retrieves the configuration of the table execution environment, which can be modified to adjust specific execution parameters.

2. get_configuration().set_string()

- get_configuration() returns the underlying configuration object that holds all the parameters and settings for Flink.

- set_string() is a method that allows you to set a configuration parameter using a string value.

3. "pipeline.jars"

- is the configuration key that specifies additional libraries or JAR files to be included in the execution pipeline. 
This key tells Flink which additional libraries to load during the job's execution. In this case, it is the Kafka connector.

4. "file:///home/angel/flink/lib/flink-sql-connector-kafka-1.17.1.jar"

- This is the value being assigned to the "pipeline.jars" key. In this case, it's a file path that points to the Kafka connector for Flink
to consume or produce data using SQL.

"""


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

# Execute DDL statement to create the source table
t_env.execute_sql(source_ddl)

# Retrieve the source table
source_table = t_env.from_path('source_table')

print("Source Table Schema:")
source_table.print_schema()

# Define a SQL query to select all columns from the source table
sql_query = "SELECT * FROM source_table"

# Execute the query and retrieve the result table
result_table = t_env.sql_query(sql_query)

# Print the result table to the console
result_table.execute().print() #no wait
