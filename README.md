# simple-etl
- Công nghệ đã dùng: Vmware, Hadoop, Pyspark, Hive
- Flie Extract.py thực hiện việc lấy data từ local lên hdfs
- File Transform thực hiện đọc dữ liệu từ hdfs vào spark data-frame để tính tổng sales và profit bằng cách nhóm các thuộc tính "Order Date", "City", "Category", "Segment" sau đó lưu data-frame này vào hive
