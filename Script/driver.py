import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SparkSession
def get_class_path():
    return os.path.dirname(os.path.realpath(__file__))

conf = (SparkConf().setMaster("yarn-client").setAppName("Arpu-Upward"))
conf.set("spark.executor.instances", "55")
conf.set("spark.executor.memory", "3g")
sc = SparkContext.getOrCreate(conf=conf)
lib_files = ['driver.py','config.py']

class_path = get_class_path()
for f in lib_files:
    sc.addPyFile(os.path.join(class_path, f))

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
if __name__ == '__main__':
    pass