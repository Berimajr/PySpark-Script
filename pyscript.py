from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define a schema for the DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Create a SparkSession object
spark = SparkSession.builder.appName("MyPySparkJob").getOrCreate()

# Create a DataFrame with some sample data
data = [("John", 25, "New York"),
        ("Jane", 30, "San Francisco"),
        ("Bob", 35, "Chicago")]

df = spark.createDataFrame(data, schema)

# Perform some transformations on the data
df = df.filter(df.Age > 30).groupBy(df.City).count()

# Show the results
df.show()

# Stop the SparkSession
spark.stop()

