from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Tạo SparkSession
    spark = SparkSession.builder.appName("PerformanceTest").getOrCreate()
    
    # Tạo một RDD lớn
    data = range(1, 10000001)  # Tập dữ liệu gồm 10 triệu số
    rdd = spark.sparkContext.parallelize(data, numSlices=100)
    
    # Tính tổng
    total = rdd.sum()
    
    print(f"Total Sum: {total}")
    
    # Đếm số phần tử
    count = rdd.count()
    
    print(f"Total Count: {count}")
    
    # Dừng SparkSession
    spark.stop()
