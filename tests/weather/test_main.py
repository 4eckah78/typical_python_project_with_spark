from weather.main import get_hottest_temp, read_file
import os
from pyspark.sql import SparkSession


def test_main():
    os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-20"

    spark = SparkSession.builder.master("local[*]").appName("PySpark").getOrCreate()
    df = read_file("GlobalLandTemperaturesByMajorCity.csv", spark)
    assert df is not None
    assert get_hottest_temp(df, 1985) == ("Riyadh", 8)
