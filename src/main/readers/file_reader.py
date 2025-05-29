from pyspark.sql import SparkSession

class FileReader:
    def __init__(self):
        # Initialize with a Spark session
         self.spark=SparkSession.builder.appName(name="SparkETLFramwork").getOrCreate()


    def read_file(self, file_path: str):
        """
        Reads a file of specified type into a Spark DataFrame.

        Parameters:
        - file_path: str - path to the file
        - file_type: str - type of file (e.g., 'csv', 'json', 'parquet', etc.)
        - options: dict - additional options for reading the file

        Returns:
        - DataFrame: Spark DataFrame containing the file data
        """
        if file_path is not  None:
            filePath = file_path.split(".")
            file_type = filePath[1]

        if file_type == "csv":
            df=self._read_csv(file_path)

        elif file_type == "json":
            df=self._read_json(file_path)

        elif file_type == "parquet":
            df=self._read_parquet(file_path)
             
        elif file_type == "orc":
             df=self._read_orc(file_path)
        elif file_type == "txt":
            df=self._read_text(file_path)        

        return df

    def _read_csv(self, file_path: str):
        # Logic to read CSV file
        return self.spark.read.csv(file_path,header=True,inferSchema=True)

    def _read_json(self, file_path: str):
        # Logic to read JSON file
        return self.spark.read.option("multiline", "true").json(file_path)     
    
    def _read_parquet(self, file_path: str):
        # Logic to read Parquet file
        return self.spark.read.parquet(file_path)    

    def _read_orc(self, file_path: str):
        # Logic to read ORC file
        return self.spark.read.orc(file_path)       

    def _read_text(self, file_path: str):
        # Logic to read text file
        return self.spark.read.text(file_path)    
        
        
        
