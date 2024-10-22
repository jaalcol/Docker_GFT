# creating table with TableDescriptor
# Importing necessary modules from PyFlink
from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes

# Setting the environment for streaming mode
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Defining the table 'random_source' using TableDescriptor
table_env.create_temporary_table(
    'random_source',
    # Specifying the connector as 'datagen'
    TableDescriptor.for_connector('datagen')
        # Building the schema for the table
        .schema(Schema.new_builder()
                # Defining the 'id' column with BIGINT data type
                .column('id', DataTypes.BIGINT())
                # Defining the 'data' column with TINYINT data type
                .column('data', DataTypes.TINYINT())
                .build())
        # Setting sequence options for 'id' column to generate values from 1 to 3
        .option('fields.id.kind', 'sequence')
        .option('fields.id.start', '1')
        .option('fields.id.end', '3')
        # Setting sequence options for 'data' column to generate values from 4 to 6
        .option('fields.data.kind', 'sequence')
        .option('fields.data.start', '4')
        .option('fields.data.end', '6')
        .build())

# Fetching the created table into a table object
table = table_env.from_path("random_source")

# Executing and printing the table's data
table.execute().print()
