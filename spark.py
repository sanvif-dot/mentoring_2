from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a SparkSession
spark = SparkSession.builder \
    .appName("NYC Taxi Pickup Locations") \
    .getOrCreate()

# Read the data from the parquet file
df = spark.read.parquet("green_trips.parquet")

# Register the dataframe as a temporary view
df.createOrReplaceTempView("green_trips")

# Define the SQL query to get the top 3 pickup locations by month
query = """
    SELECT EXTRACT(MONTH FROM lpep_pickup_datetime) AS month, PULocationID AS pickup_location_id, COUNT(*) AS pickups_count
    FROM green_trips
    GROUP BY month, pickup_location_id
    ORDER BY month, pickups_count DESC
    """

# Execute the SQL query
result = spark.sql(query)

# Calculate the top 3 pickup locations for each month
top_3_locations = result.groupBy("month").agg(
    F.collect_list("pickup_location_id").alias("top_3_pickup_locations")
)

# Display the result
top_3_locations.show()
