from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, concat,md5
from pyspark.sql.types import StringType
import unittest
import hashlib
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def calculate_hash(spark, df):
    """
    Adds a new column with hashing of all the df columns.
    """
    return df.withColumn("all_hash",md5(concat(*df.columns)))

def join_df(spark, df1, df2, join_column, join_type='inner'):
    """
    Join the df1 and df2 on inner join using join_column and return the new dataframe 
    """

def filter_and_add_column(df: DataFrame, threshold: int) -> DataFrame:
    """
    Filter rows where 'value' is greater than threshold and add a new column 'value_squared'.
    
    Args:
        df: Input DataFrame
        threshold: Minimum value to keep in the 'value' column
        
    Returns:
        Transformed DataFrame
    """
    return