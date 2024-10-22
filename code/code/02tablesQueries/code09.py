#table API & SQL
# use SQL Table in Table API

from pyflink.table import TableEnvironment, EnvironmentSettings

# Initialize the Table environment in streaming mode
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Create a source table using the SQL DDL
# This table generates a sequence of numbers for 'id' and 'data' columns
table_env.execute_sql("""
    CREATE TABLE sql_source (
        id BIGINT, 
        data TINYINT 
    ) WITH (
        'connector' = 'datagen',          -- Using the data generator connector
        'fields.id.kind' = 'sequence',    -- 'id' is a sequence
        'fields.id.start' = '1',          -- Starting at 1
        'fields.id.end' = '4',            -- Ending at 4 for 'id'
        'fields.data.kind' = 'sequence',  -- 'data' is also a sequence
        'fields.data.start' = '4',        -- Starting at 4
        'fields.data.end' = '7'           -- Ending at 7 for 'data'
    )
""")

# Convert the SQL table to a Table API table
# This is a handle to the table that we can use with the Table API
table = table_env.from_path("sql_source")

# Alternatively, you could create the table from a SQL query
# This would allow for more complex transformations using SQL syntax
# table = table_env.sql_query("SELECT * FROM sql_source")

# Execute and print the table contents to the console
# This will output the generated sequence of numbers to standard out
table.execute().print()
