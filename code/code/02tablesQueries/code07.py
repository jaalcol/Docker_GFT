# query transformation using sql (index divided by 2)
from pyflink.table import EnvironmentSettings, TableEnvironment

# Initialize the streaming table environment.
# This enables the execution of continuous queries on unbounded and bounded data.
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Create a source table to simulate real-time data generation using the DataGen connector.
# This table generates a sequence of IDs and corresponding data values within specified ranges.
table_env.execute_sql("""
    CREATE TABLE random_source (
        id BIGINT, 
        data TINYINT
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '8',
        'fields.data.kind' = 'sequence',
        'fields.data.start' = '4',
        'fields.data.end' = '11'
    )
""")

# Retrieve the "random_source" table from the table environment
table_source = table_env.from_path("random_source")

# Execute the table operation and display the results
table_source.execute().print()

# Create a sink table where the results of the streaming computation will be sent.
# The 'print' connector outputs the results to the standard output (console), suitable for debugging and testing.
table_env.execute_sql("""
    CREATE TABLE print_sink (
        id_res BIGINT, 
        data_sum TINYINT 
    ) WITH (
        'connector' = 'print'
    )
""")


# Define and execute an SQL query to insert data into the print sink.
# The query applies a transformation to the ID field, dividing it by two,
# then filters to retain only the records with modified IDs greater than one.
# It also calculates the sum of data associated with these IDs,
# showcasing how to use simple SQL transformations and aggregations in PyFlink.
table_env.execute_sql("""
    INSERT INTO print_sink
        SELECT id_res, SUM(data) AS data_sum FROM 
            (SELECT id / 2 AS id_res, data FROM random_source)
        WHERE id_res > 1
        GROUP BY id_res
""").wait()  # The 'wait' call ensures that the main program waits for the completion of the SQL job.

# 1 4 -> 1/2 = 0 no
# 2 5 -> 2/2 = 0 no
# 3 6 -> 3/2 = 1 no
# 4 7 -> 4/2 = 2 si 7
# 5 8 -> 5/2 = 2 si 7 + 8 = 15 OK
# 6 9 -> 6/2 = 3 si 9
# 7 10 -> 7/2 = 3 si 9 + 10 = 19 OK
# 8 11 -> 8/2 = 4 si 11 OK

# 2> +I[4, 11]
# ^subtask id (paralell task)
# +I insert, -U update before, +U add update, -D delete
