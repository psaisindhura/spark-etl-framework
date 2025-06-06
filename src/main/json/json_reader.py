from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import types as T
import pyspark.sql.functions as F

def create_spark_session_local(app_name: str,config:dict = None) -> SparkSession:

    spark= SparkSession.builder.appName(app_name) 
    if config:
        for key,value in  config.items():
            spark= spark.config(key, value)
     
    return spark.getOrCreate()

def read_json_file(spark, file_path: str):
    try:
        df = spark.read.option("multiline","true").json(file_path)
        return df
    except Exception as e:
        return None

def flatten(df):
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])
    
    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode(col_name))
    
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])
        
        
    for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))

    return df

def transform_json_data(df):
    """
    Transform the DataFrame by adding a new column with a constant value.
    
    Args:
        df: Input DataFrame
    
    Returns:
        Transformed DataFrame
    """
    return df.withColumn("brewing_country_company", col("brewing.country.company")) \
              .withColumn("brewing_country_id", col("brewing.country.id")) \
                .withColumn("coffee_country_company", col("coffee.country.company")) \
                .withColumn("coffee_country_id",col("coffee.country.id")) \
                .drop("brewing", "coffee") 

def save_df_as_parquet(df, path):
    df.write.partitionBy("brewing_country_company").parquet(path, mode="overwrite")
    
if __name__ == "__main__":

   spark = create_spark_session_local("JsonReader")
   df = read_json_file(spark, "/root/code/spark-etl/files/coffee.json")
   flattened_df = flatten(df)
   ##transformed_df = transform_json_data(df)
   flattened_df.show(truncate=False)
   flattened_df.printSchema()
   #save_df_as_parquet(transformed_df, "/root/code/spark-etl/output/files/coffee.parquet")

