from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# Load training data
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Auto').getOrCreate()

data = spark.read.csv("C:\Users\saidivya\Desktop\adult.csv",
                           header=True, inferSchema="true")



from pyspark.ml.feature import StringIndexer
# Convert target into numerical categories
labelIndexer = StringIndexer(inputCol="Salary", outputCol="label")

from pyspark.ml.feature import VectorAssembler


featureAssembler = VectorAssembler(inputCols=["Age", "sex", "capital-gain","capital-loss","hours-per-week","marital","pos","role","race","gender","num1","num2","num3","state","val"],
                                   outputCol='features')



#output = featureAssembler.transform(data)


splits = data.randomSplit([0.7, 0.3])
train= splits[0]
test = splits[1]



# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

pipeline = Pipeline(stages=[labelIndexer, featureAssembler, nb])

# Run stages in pipeline and train model
model = pipeline.fit(train)

# select example rows to display.
predictions = model.transform(test)
predictions.printSchema()
predictions.show()

# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="capital-gain", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))