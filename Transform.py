from pyspark.sql import SparkSession, SaveMode
from pyspark.sql.functions import sum

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("ReadFromHDFSAndSaveToHive").enableHiveSupport().getOrCreate()

# Đọc dữ liệu từ HDFS và tạo DataFrame
df = spark.read.csv("hdfs://localhost:9000/datalake/Superstore.csv", header=True, inferSchema=True).drop("year", "month", "day")

# Tạo DataFrame mới với kết quả tổng sale
result_df = df.groupBy("Order Date", "City", "Category", "Segment").agg(sum("Sales"), sum("Profit"))

# Lưu DataFrame vào Hive
result_df.write.format("hive").mode(SaveMode.Append).saveAsTable("reports.sum_sale_profit")
