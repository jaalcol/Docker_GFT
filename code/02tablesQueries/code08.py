#table API & SQL
# use Table Object in SQL (create_temporary_view)
from pyflink.table import TableEnvironment, EnvironmentSettings

# Initialize streaming Table environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Create a print sink table for result output
table_env.execute_sql("""
    CREATE TABLE table_sink (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'  -- Print API output to console
    )
""")  # Note: Using SQL style comment here "--"

# Generate Table API table with predefined elements
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])

# Register the Table API table as a view for SQL querying
table_env.create_temporary_view('table_api_table', table)

# Insert data from the Table API table into the print sink table
table_env.execute_sql("INSERT INTO table_sink SELECT * FROM table_api_table").wait()
# Execution is synchronous with 'wait()', ensuring completion before proceeding

