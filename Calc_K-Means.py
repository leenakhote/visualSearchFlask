from pyspark import SparkConf, SparkContext  #from pacakge import classes/function
sc = SparkContext.getOrCreate()
from numpy import array
from math import sqrt
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors, VectorUDT
from pyspark.mllib.linalg import Vector as MLLibVector, Vectors as MLLibVectors
from pyspark.mllib.clustering import KMeans, KMeansModel



data = sc.textFile("/home/siddhesh/train_no_1.csv")
parsed_data = data.map(lambda x : x.split("/n")).flatMap(lambda words : (word.split(",") for word in words))\
.map(lambda x : [elem.strip('"') for elem in x]).map(lambda x: (MLLibVectors.dense(x[3:])))

print parsed_data



# Build the model (cluster the data)
clusters = KMeans.train(parsed_data, 2, maxIterations=20, initializationMode="random")


# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    # print "PREDICT :",clusters.predict(point)
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))


#
WSSSE = parsed_data.map(lambda point: error(point)).collect()                       #.reduce(lambda x, y: x + y)
# print("Within Set Sum of Squared Error = " + str(WSSSE))

# print clusters.centers
