from pyspark.sql import SparkSession
import datetime

def save_csv_to_hdfs(file_path, output_path, partition_date):
    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("SaveCSVtoHDFS").getOrCreate()
    
    # Đọc dữ liệu từ file CSV
    df = spark.read.csv(file_path, header=True)
    
    # Tạo đường dẫn đến thư mục đầu ra với partitioning
    output_partitioned_path = f"{output_path}/year={partition_date.year}/month={partition_date.month}/day={partition_date.day}"
    
    # Ghi dữ liệu partitioned vào HDFS
    df.write.partitionBy("year", "month", "day").csv(output_partitioned_path, mode="overwrite")

# Gọi hàm để lưu trữ dữ liệu partitioned trên HDFS
file_path = "/home/data/Superstore.csv"  # Đường dẫn đến file CSV
output_path = "hdfs://localhost:9000/datalake/Superstore"  # Đường dẫn đến thư mục đầu ra trên HDFS
partition_date = datetime.date(2022, 6, 21)  # Ngày tháng năm partition

save_csv_to_hdfs(file_path, output_path, partition_date)
