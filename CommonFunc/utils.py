import configparser

from pyspark import SparkConf

def load_survey_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)
def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()
def count_country(survey_df):
    return survey_df\
        .select ("Country").distinct().collect() \

def countRec(survey_df):
    return survey_df\
        # .select ("Age").count \
def nullcheck(survey_df):
    return survey_df.filter("state isNull") \

def Dupcheck(survey_df):
    return survey_df \
        .groupBy("Age", "Gender", "Country").count().filter("count > 1")

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf