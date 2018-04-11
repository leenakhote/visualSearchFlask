# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.sql import SQLContext
# from pyspark.sql.functions import desc

import pickle
import csv
import numpy as np

def get_db(ip,db,col):
    from pymongo import MongoClient
    client = MongoClient(ip,27017)
    db = client[db]
    return db[col]

def add_product(curser,data):
    return curser.insert(data)

def get_product(curser):
    return curser.find_one()

def get_allproduct(curser):
    return curser.find({})

def writeToCsv(filePath,csvData):
    '''
    csvData takes a list and filepath of the csv as input
    note : pass the filename only dont pass the filename with extension as extension is appended inside the function
    '''
    with open(filePath+".csv", 'a') as csvfile:
       spamwriter = csv.writer(csvfile, delimiter='\n',
                                quoting=csv.QUOTE_MINIMAL)
       spamwriter.writerow(csvData)
       csvfile.flush()


def writeToCsvNew(filePath,csvData):
    with open(filePath+".csv", 'w') as csvfile:
        fieldnames = ['index', 'imgUrl', 'product_id', 'features']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # writer.writeheader()
        writer.writerows(csvData)
        print("writing complete")


if __name__ == '__main__':


    #Define DB and collection name
    ip = 'mongodb://styfiadmin:tGaKMCEZyxBK3Cad@styfimongodbclusterone-shard-00-00-1tkzd.mongodb.net:27017,styfimongodbclusterone-shard-00-01-1tkzd.mongodb.net:27017,styfimongodbclusterone-shard-00-02-1tkzd.mongodb.net:27017/ip_new?ssl=true&replicaSet=styfimongodbclusterone-shard-0&authSource=admin'
    #ip = 'localhost' # Image processing Server
    dbName = 'ip_new'
    colName = 'product'
    curser = get_db(ip,dbName,colName)

    products = get_allproduct(curser)

    # features = []
    # image = []
    data = []
    count = 0
    index = 0

    for product in products:
        #
        # imgUrl = product['image_url'][0]
        # feature = product['features']
        # map(lambda x: float(x),feature)
        # # print type(np.array(feature))
        # image.append(imgUrl)
        # features.append(np.array(feature))
        #np.savetxt(features, features, delimiter=",")

        data_object = {}
        data_object["index"] = index+1
        index = index+1
        data_object["imgUrl"] = product['image_url'][0]
        data_object["product_id"] = product['product_id']
        # #y =  np.asarray(product['features'])
        # list1 = product['features']
        # str1 = ''.join(str(e) for e in list1)

        data_object["features"] = str(product['features']).strip('[]')
        #np.savetxt(y, features, delimiter=",")
        #data_object["features"] = str1
        data.append(data_object)

        count = count + 1
        # print feature , " --->> ",count
        if count > 10:
            break
        # pass
    #print data

    writeToCsvNew("/home/leena/dev/visualsearch/train_no",data)
    # a = np.asarray(image)
    #np.savetxt("/home/leena/dev/visualsearch/train.csv", features, delimiter=",")
    # print y
    # np.savetxt("abc_image.csv", a, fmt='%.18e',delimiter=",")
    # writeToCsv("abc_feature",features)
    #writeToCsv("abc_image_test",a)
    # with open('imgFeatures_10.pickle', 'wb') as handle:
    #     pickle.dump(data, handle)
