import argparse
import os
from typing import Tuple

import pandas as pd
import pyspark as ps
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def read_file(filename: str, spark: ps.sql.SparkSession) -> ps.sql.DataFrame:
    with open(filename) as f:
        pandas_df = pd.read_csv(f)
        df = spark.createDataFrame(pandas_df)
    return df


def get_hottest_temp(df: ps.sql.DataFrame, year: int) -> Tuple[str, int]:
    group = df.filter(F.year('dt') == year).orderBy(
        'AverageTemperature', ascending=False
    )
    city = group.select('City').collect()[0][0]
    month = group.select(F.month('dt')).collect()[0][0]
    return city, month


if __name__ == "__main__":

    os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-20"

    spark = SparkSession.builder.master("local[*]").appName("PySpark").getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="This is the name of a file with weather data")
    parser.add_argument("year", default=8000, help="This is a port")
    args = parser.parse_args()
    filename = args.filename
    year = int(args.year)

    df = read_file(filename, spark)
    city, month = get_hottest_temp(df, year)
    print(f"The hottest temperature in {year} was in {city} in {month}")
