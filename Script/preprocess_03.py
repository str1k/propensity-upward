from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SparkSession
from pyspark.sql import SQLContext, HiveContext
import pyspark.sql.functions as func
import pandas as pd
import numpy as np
import re
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import when
import ds_config
import logging
if __name__ == "__main__":
	logging.getLogger("py4j").setLevel(logging.ERROR)
	conf = SparkConf().setAppName("preprocess_03")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ).load(ds_config.preprocess_01_output_01)
	gsdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ds_config.gs_customer_prof_before_delim).load(ds_config.gs_customer_prof_before)