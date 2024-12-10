from utils.generate_docker_compose import generate_docker_compose as change_worker
from pyspark.sql import SparkSession

NUM_EXECUTORS = 3 
EXECUTOR_MEMORY = "1G"
EXECUTOR_CORES = 3

spark = SparkSession.builder \
    .appName("Custom Spark Cluster") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.instances", NUM_EXECUTORS) \
    .config("spark.executor.memory", EXECUTOR_MEMORY) \
    .config("spark.executor.cores", EXECUTOR_CORES) \
    .getOrCreate()

data = [("John", 28), ("Doe", 35), ("Alice", 30)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df.show()

spark.stop()