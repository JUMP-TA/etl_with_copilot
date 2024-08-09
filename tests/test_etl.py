
import pytest
from pyspark.sql import SparkSession

# Add src directory to the system path
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../src'))

from etl import extract_data, transform_data, load_data

@pytest.fixture(scope="module")
def spark():
    # Setup: Create a Spark session
    spark = SparkSession.builder.appName("Test ETL").getOrCreate()
    yield spark
    # Teardown: Stop the Spark session
    spark.stop()

def test_extract_data(spark):
    # Create a test CSV file
    test_file = "/tmp/test_input.csv"
    with open(test_file, "w") as f:
        f.write("name,age,city\nJohn Doe,45,New York\nJane Doe,30,Los Angeles\nAlice,40,Chicago\n")
    
    # Test the extract_data function
    df = extract_data(spark, test_file)
    
    # Define the expected output
    expected_data = [("John Doe", 45, "New York"),
                     ("Jane Doe", 30, "Los Angeles"),
                     ("Alice", 40, "Chicago")]
    expected_df = spark.createDataFrame(expected_data, ["name", "age", "city"])
    
    # Check if the result matches the expected output
    assert df.collect() == expected_df.collect()

    # Clean up test file
    os.remove(test_file)

def test_transform_data(spark):
    # Create a DataFrame with test data
    test_data = [("John Doe", 45, "New York"),
                 ("Jane Doe", 30, "Los Angeles"),
                 ("Alice", 40, "Chicago")]
    df = spark.createDataFrame(test_data, ["name", "age", "city"])
    
    # Apply the transform_data function
    result_df = transform_data(df)
    
    # Define the expected output
    expected_data = [("John Doe", 45, "New York"),
                     ("Alice", 40, "Chicago")]
    expected_df = spark.createDataFrame(expected_data, ["name", "age", "city"])
    
    # Check if the result matches the expected output
    assert result_df.collect() == expected_df.collect()

def test_load_data(spark):
    # Create a DataFrame with test data
    test_data = [("John Doe", 45, "New York"),
                 ("Alice", 40, "Chicago")]
    df = spark.createDataFrame(test_data, ["name", "age", "city"])
    
    # Define the output path
    output_path = "/tmp/test_output"
    
    # Apply the load_data function
    load_data(df, output_path)
    
    # Read the output data back
    result_df = spark.read.csv(output_path, header=True, inferSchema=True)
    
    # Define the expected output
    expected_df = df
    
    # Check if the result matches the expected output
    assert result_df.collect() == expected_df.collect()
    
    # Clean up output directory
    os.system(f"rm -r {output_path}")
