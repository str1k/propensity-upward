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
    conf = SparkConf().setAppName("preprocess_01")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ds_config.cvm_non_voice_arpu_delimiter).load(ds_config.cvm_non_voice_arpu_source)
    df.registerTempTable("rawTable")
    missing_df = spark.sql("SELECT mobile_segment,distinct_out_number_m1,distinct_out_number_m2,distinct_out_number_m3,distinct_out_number_m4,distinct_out_number_m5,distinct_out_number_m6 from rawTable")

    hs_price3 = spark.sql("SELECT analytic_id, hs_release_price_baht_m3 from rawTable")
    hs_price6 = spark.sql("SELECT analytic_id, hs_release_price_baht_m6 from rawTable")

    means = hs_price3.agg( *[func.mean(c).alias(c) for c in hs_price3.columns if c != 'analytic_id']).toPandas().to_dict('records')[0]
    means2 = hs_price6.agg( *[func.mean(c).alias(c) for c in hs_price6.columns if c != 'analytic_id']).toPandas().to_dict('records')[0]

    means['mobile_segment'] = 'NA'
    means['distinct_out_number_m1'] = 0
    means['distinct_out_number_m2'] = 0
    means['distinct_out_number_m3'] = 0
    means['distinct_out_number_m4'] = 0
    means['distinct_out_number_m5'] = 0
    means['distinct_out_number_m6'] = 0
    means['hs_release_price_baht_m6'] = means2['hs_release_price_baht_m6']
    no_NA_df = df.fillna(means)
    no_NA_df.registerTempTable("noNATable")
    no_NA_df.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_01_output_01)
