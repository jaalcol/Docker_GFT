# group_by using api table
# Import the necessary modules for Environment Settings and Table Environment
from pyflink.table import EnvironmentSettings, TableEnvironment
# Import the column selection function
from pyflink.table.expressions import col

# Set the environment to batch mode for batch processing of queries
env_settings = EnvironmentSettings.in_batch_mode()
# Initialize the TableEnvironment for query execution
table_env = TableEnvironment.create(env_settings)

# Create a table from a collection of tuples, defining the structure of the table with name, country, and revenue
orders = table_env.from_elements(
    [('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
    ['name', 'country', 'revenue']
)
orders.execute().print()

# Perform a series of transformations to compute the total revenue from customers in France
# 1. Select the columns needed for the calculation
# 2. Filter the rows where the country is France
# 3. Group the results by customer name
# 4. Select the name and the sum of the revenue, renaming the sum column to 'rev_sum'
revenue = orders \
    .select(col("name"), col("country"), col("revenue")) \
    .where(col("country") == 'FRANCE') \
    .group_by(col("name")) \
    .select(col("name"), col("revenue").sum.alias('rev_sum'))

# Execute the query and print the results
revenue.execute().print()
