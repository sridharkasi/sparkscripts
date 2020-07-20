import sys
from pyspark.sql import *
# from pyspark.sql import SparkSession

from CommonFunc.utils import *
import pytest

# if __name__ == "__main__":
@pytest.fixture
def test_script():
    conf = get_spark_app_config()
    global spark, dbtable, survey_raw_df, partitioned_survey_df
    # global dbtable
    # global survey_raw_df
    # global partitioned_survey_df
    dbtable = "./Datafile/Datatable.csv"
    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()
    survey_raw_df = load_survey_df(spark, dbtable)
    partitioned_survey_df = survey_raw_df.repartition(2)
    yield
    spark.stop()
    # dbtable="./Datafile/Datatable.csv"
    # survey_raw_df = load_survey_df(spark, dbtable)
def test_TotalCount(test_script):
    getcount = countRec(partitioned_survey_df)
    print ("Total Record Count: "+str(getcount.count()))
    print("Expected Count [9] Vs Actual Count [" + str(getcount.count())+"]")
    assert (int(getcount.count())== 9)
def test_ColumnCount(test_script):
    getcount = countRec(partitioned_survey_df)
    print("Total Column Count: " + str(len(getcount.columns)))
    print("Expected Count [27] Vs Actual Count [" + str(len(getcount.columns)) + "]")
    print(getcount.printSchema())
    assert (len(getcount.columns) == 27)
def test_GroupCountry_Age_below40(test_script):
    count_df = count_by_country(partitioned_survey_df)
    records = count_df.count()
    print("Total Records Found: "+ str(records))
    count_df.show()
def test_duplicatecheck(test_script):
    dup = Dupcheck(partitioned_survey_df)
    records = dup.count()
    # print("Total Number of Duplicate Records Found: ["+str(records)+"]")
    print(str(records) + " - Duplicate Records Found")
    dup.show()
    assert (int(dup.count()) < 1)

