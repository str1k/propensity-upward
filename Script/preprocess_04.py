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
	conf = SparkConf().setAppName("preprocess_04")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(ds_config.preprocess_02_output_01)
	gsdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(ds_config.gs_customer_prof_after)
	gsdf.registerTempTable("GS_SUMMARY")
	selected = spark.sql("SELECT analytic_id, foreigner_flag, mobile_region, service_month, urbanflag, billing_region from GS_SUMMARY")
	present_df = df.join(selected, ["analytic_id"], "left_outer")
	present_df.registerTempTable("present_df")
	service_month = sqlContext.sql("SELECT analytic_id, service_month from present_df")
	means = service_month.agg( *[func.mean(c).alias(c) for c in service_month.columns if c != 'analytic_id']).toPandas().to_dict('records')[0]
	means['foreigner_flag'] = 'N'
	means['mobile_region'] = 'NA'
	means['urbanflag'] = 'Y'
	means['billing_region'] = 'NA'
	means['service_month'] = 12
	present_df_eliminateGSna = present_df.fillna(means)
	present_df_eliminateGSna.registerTempTable("present_df_eliminateGSna")
	present_df_eliminateGSna.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_04_output_01)