from pyspark.sql import SparkSession
from typing import List, Tuple, Optional

class FileWriter:
    def __init__(self):
        # Initialize with a Spark session
        self.spark=SparkSession.builder.appName(name="SparkFileWriter").getOrCreate()            


    def _write_csv(self, file_path: str, data: List[Tuple], columns: List[str]):        
        df=self.spark.createDataFrame(data, columns)
        df.coalesce(1).write.csv(file_path, header=True, mode="overwrite")
        return df
        
    def _write_json(self, file_path: str, data: List[Tuple], columns: List[str]):        
        df=self.spark.createDataFrame(data, columns)
        df.write.json(file_path, mode="overwrite")
        return df
        
    def _write_parquet(self, file_path: str, data: List[Tuple], columns: List[str]):        
        df=self.spark.createDataFrame(data, columns)
        df.write.parquet(file_path, mode="overwrite")
        return df
        
    def _write_orc(self, file_path: str, data: List[Tuple], columns: List[str]):        
        df=self.spark.createDataFrame(data, columns)
        df.write.orc(file_path,mode="overwrite")
        return df
        
    def _write_text(self, file_path: str, data: List[Tuple]):        
        df=self.spark.createDataFrame(data)
        df.write.text(file_path,mode="overwrite")
        return df


