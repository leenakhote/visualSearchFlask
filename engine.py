from pyspark.mllib.recommendation import ALS

from pyspark import SparkConf, SparkContext  #from pacakge import classes/function
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Row
from pyspark.sql.session import SparkSession
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT
import pickle
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors, VectorUDT
from pyspark.sql.functions import col, split
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Normalizer
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType
import numpy as np
import csv
from pyspark.sql import Row
from pyspark.ml.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import requests
import json
import pandas
import shlex, subprocess

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cosineSimilarity(a,b):
    #print a ,b
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

class RecommendationEngine:
    """A visual Search APPlication
    """

    def getNormalizer(self,dataFrameFeatures,outputColName):
        #define Normalizer to get normailize freatures
        normalized = Normalizer(inputCol="features", outputCol=outputColName, p=2.0)
        #Get Normalize feature
        normData = normalized.transform(dataFrameFeatures);
        return normData

    def getCosineSimilarity(self,testDataFrameFeatures):
        print "****************************** inside cosine "
        #Create Table for temporary View for Train DAta
        trainDataFrameFeaturesNorm = self.trainNormalizeFeatures.createOrReplaceTempView("trainDataFrameFeatures")
        #Create Table for temporary View for TEst DAta
        df =  self.trainNormalizeFeatures.withColumn("coSim", udf(cosineSimilarity, FloatType())( col("TrainNormFeatures"),array([lit(v) for v in testDataFrameFeatures['testDataImgFeatures']])))
        print "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ"
        resultss = df.sort(df.coSim.desc())
        res = resultss.createOrReplaceTempView("test")
        sqlDF_train = self.sqlContext.sql("select index,url,productId,cosim from test order by cosim desc limit 10")         #.toDF("index","url", "productId", "cosim").coalesce(1).write.mode("overwrite").format("json").save("op.json")
        top_match = sqlDF_train.toPandas().to_dict('records')
        #print top_match
        return top_match
        # print result

    def getTestData(self,imgUrl):
        testDataFeature = requests.post('http://192.168.1.150:8011/visualsearch/extractproductfeature/',
                                      data=json.dumps({'imgUrl': imgUrl})).content

        testDataFeature = json.loads(testDataFeature)
        #print testDataFeature
        testData = {}
        testData['imgUrl'] = imgUrl
        testData['testDataImgFeatures'] = (testDataFeature['imgFeatures'])
        return testData

    def getVisualSearch(self,imgUrl):
        print("inside visual search")
        logger.info("inside visualsearch")
        dataframe_features = self.getTestData(imgUrl)

        a=[json.dumps(dataframe_features)]
        jsonRDD = self.sc.parallelize(a)
        df = self.sparkSession.read.json(jsonRDD)
        df.show()
        #df.printSchema()
        to_vector = udf(lambda a: DenseVector(a), VectorUDT())
        data = df.select("imgUrl", to_vector("testDataImgFeatures").alias("features"))
        #data.printSchema()
        #data.show()
        testNormalizeFeatures = self.getNormalizer(data,"TestNormFeatures")
        # testNormalizeFeatures.show()
        final_result = self.getCosineSimilarity(dataframe_features)
        #print final_result
        return final_result

    def formatFeaturesDF(self,featuresRawRDD, outputColName):

        #Convert RDD Data into DataFrame
        dataframe_features = featuresRawRDD.map(lambda x: (x[0], x[1], x[2],DenseVector((x[3]).split(',')))).toDF(["index", "url", "productId", "features"])

        trainNormalizeFeatures = self.getNormalizer(dataframe_features,outputColName)
        return trainNormalizeFeatures


    def __init__(self, sc, feature_dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        self.sc = sc

        logger.info("Starting up the Recommendation Engine: ")
        #self.sc = SparkContext.getOrCreate()
        self.sparkSession = SparkSession(self.sc)
        self.sqlContext = SQLContext(self.sc)

        logger.info("Loading Features data...")
        featuresRawRDD = self.sc.textFile(feature_dataset_path)
        logger.info("Loading Features RDD data...")

        #Map Read CSV file and Map to RDD
        featuresRdd = featuresRawRDD.mapPartitions(lambda x: csv.reader(x))
        #featuresss = featuresRdd.map(lambda x : x.encode("ascii", "ignore"))
        # print featuresRdd.collect()
        self.trainNormalizeFeatures = self.formatFeaturesDF(featuresRdd,"TrainNormFeatures")
        print self.trainNormalizeFeatures.show()
