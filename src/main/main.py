#main file to run the jobs
from readers.file_reader import FileReader
from writers.file_writer import FileWriter


if __name__ == "__main__":
    # filereader = FileReader()
    # df = filereader.read_file("/root/business.csv")
    # df.show()
    data = [("Alice", 30), ("Bob", 25), ("Cathy", 27)]
    columns = ["Name", "Age"]
    filewriter = FileWriter()
    df = filewriter._write_csv(
        "code/spark-etl/spark-etl-framework/src/main/writers/datawriter",
        data,
        columns
    )
    df.show()
