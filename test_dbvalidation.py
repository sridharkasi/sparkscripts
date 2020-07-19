import sys
from pyspark.sql import *
# from pyspark.sql import SparkSession

from CommonFunc.utils import *
import pyspark.sql

if __name__ == "__main__":
# def test_script():
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()


    dbtable="./Datafile/Datatable.csv"
    survey_raw_df = load_survey_df(spark, dbtable)
    partitioned_survey_df = survey_raw_df.repartition(2)
    partitioned_survey_df.show()
    count_df = count_by_country(partitioned_survey_df)
    count_df.show()
    spark.stop()