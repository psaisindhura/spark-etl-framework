#from transformations import calculate_hash, filter_and_add_column
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
import pytest
from pyspark.sql import SparkSession



@pytest.fixture
def spark():
    return SparkSession.builder.master("local[2]").getOrCreate()


def test_filter_and_add_column(spark):
    data = [("A", 10), ("B", 3), ("C", 15), ("D", 8)]
    df = spark.createDataFrame(data, ["id", "value"])

    #assert 
