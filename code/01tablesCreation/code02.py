# Using list object to create tables

# Import necessary modules from pyflink.table
from pyflink.table import EnvironmentSettings, TableEnvironment

# Set the environment settings for batch processing mode
env_settings = EnvironmentSettings.in_batch_mode()

# Create a table environment using the specified batch settings
table_env = TableEnvironment.create(env_settings)

# Create a table with two rows of elements
# Each row has two columns: an integer and a string
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
# angel - print the table
table.execute().print()
# by default, the type of the "id" column is BIGINT
print('By default the type of the "id" column is %s.' % table.get_schema().get_field_data_type("id"))

from pyflink.table import DataTypes
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
                                DataTypes.ROW([DataTypes.FIELD("id", DataTypes.TINYINT()),
                                               DataTypes.FIELD("data", DataTypes.STRING())]))
# now the type of the "id" column is set as TINYINT
print('Now the type of the "id" column is %s.' % table.get_schema().get_field_data_type("id"))