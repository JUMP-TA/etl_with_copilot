
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def extract_data(spark, file_path):
    # Extract data from a CSV file
    return spark.read.csv(file_path, header=True, inferSchema=True)

def transform_data(df):
    # Perform a simple transformation: filter and select specific columns
    return df.filter(col("age") > 35).select("name", "age", "city")

def load_data(df, output_path):
    # Load transformed data into a new CSV file
    df.write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Simple ETL").getOrCreate()

    # Define file paths
    input_file = "data/input.csv"
    output_file = "data/output"

    # Execute ETL process
    data = extract_data(spark, input_file)
    transformed_data = transform_data(data)
    load_data(transformed_data, output_file)

    # Stop Spark session
    spark.stop()
