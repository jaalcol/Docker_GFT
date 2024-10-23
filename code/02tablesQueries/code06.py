# udf (User-Defined Function) for map
# Import the necessary modules for table environment and UDF creation.
# got to 90pandas before continue
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import DataTypes
from pyflink.table.udf import udf
import pandas as pd

# Initialize a batch processing environment. Batch mode is selected for finite, bounded data processing.
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# Create a table 'orders' with sample data. The data represents orders with customer names, countries, and revenues.
orders = table_env.from_elements(
    [('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
    ['name', 'country', 'revenue']
)
orders.execute().print()
# Define a User-Defined Function (UDF) to process each row of the table.
# This UDF takes the 'name' and 'revenue' fields, and multiplies the 'revenue' by 10.
map_function = udf(
    lambda x: pd.concat([x.name, x.revenue * 10], axis=1),
    result_type=DataTypes.ROW(
        [DataTypes.FIELD("name", DataTypes.STRING()),
         DataTypes.FIELD("revenue", DataTypes.BIGINT())]
    ),
    func_type="pandas"  # Use Pandas for vectorized operations, efficient for batch processing.
)

# Apply the map function to the 'orders' table, transforming each row according to the UDF logic.
# Finally, execute the mapping operation and print the results.
orders.map(map_function).execute().print()
