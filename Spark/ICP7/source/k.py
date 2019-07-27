from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import os
os.environ["SPARK_HOME"] = "C:\\Users\\Niteesha\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-2.4.3-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="C:\\winutils"
# Create spark session

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Detecting-Malicious-URL App").getOrCreate()

spark = SparkSession.builder.appName("ICP 14").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define input path


# Load data and select feature and label columns
data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("C:\\Users\\Niteesha\\Desktop\\diab.csv")
data = data.select("procs","p_id","meds")

# Create vector assembler for feature columns
assembler = VectorAssembler(inputCols=data.columns, outputCol="features")
data = assembler.transform(data)

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)

model = kmeans.fit(data)

# Make predictions
predictions = model.transform(data)

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)