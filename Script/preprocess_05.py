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
conf = SparkConf().setAppName("preprocess_05")
      sc = SparkContext(conf=conf)
      sqlContext = SQLContext(sc)
      df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(ds_config.preprocess_04_output_01)
      parsedDf = df.drop('sub_id','register_date','avg_arpu_before','percent_change','most_region_usage_voice_m1','most_region_usage_data_m1',\
                  'most_region_usage_m1','most_province_usage_voice_m1','most_province_usage_data_m1','most_province_usage_m1',\
                  'most_region_usage_voice_m3','most_region_usage_data_m3','most_region_usage_m3','most_province_usage_voice_m3',\
                  'most_province_usage_data_m3','most_province_usage_m3','hs_brand_name_m1','hs_model_m1','hs_support_3g2100_flag_m1',\
                  'hs_support_lte_flag_m1','hs_release_date_m1','hs_release_price_baht_m1','hs_first_eff_date_m1','hs_early_adopt_m1',\
                  'hs_brand_name_m3','hs_model_m3','hs_support_3g2100_flag_m3','hs_support_lte_flag_m3','hs_release_date_m3','hs_release_price_baht_m3',\
                  'hs_first_eff_date_m3', 'hs_early_adopt_m3', 'hs_brand_name_m6', 'hs_model_m6', 'hs_release_date_m6', 'hs_first_eff_date_m6',\
                  'no_of_device_before','main_package_id_m1','main_package_m1','main_package_price_m1','main_package_service_m1',\
                  'ontop_package_flag_m1','ontop_package_id_m1','ontop_package_m1','ontop_package_price_m1','main_package_id_m2',\
                  'main_package_m2','main_package_price_m2','main_package_service_m2','ontop_package_flag_m2','ontop_package_id_m2',\
                  'ontop_package_m2','ontop_package_price_m2','main_package_id_m3','main_package_m3','main_package_price_m3','main_package_service_m3',\
                  'ontop_package_flag_m3','ontop_package_id_m3','ontop_package_m3','ontop_package_price_m3','main_package_id_m4',\
                  'main_package_m4','main_package_service_m4','ontop_package_id_m4','ontop_package_m4','main_package_id_m5','main_package_m5',\
                  'main_package_service_m5','ontop_package_id_m5','ontop_package_m5','main_package_id_m6','main_package_m6','main_package_service_m6',\
                  'ontop_package_id_m6','ontop_package_m6','vou_payperuse_m1','mou_offnet_m1','vou_payperuse_m2','mou_offnet_m2',\
                  'vou_payperuse_m3','mou_offnet_m3','no_of_ontop_active_m1','no_of_ontop_active_m2','no_of_ontop_active_m3',\
                  'streaming_level_m1','streaming_level_m2','streaming_level_m3','total_volumn_mb_wifi_m1','total_volumn_mb_wifi_m2',\
                  'total_volumn_mb_wifi_m3','data_3g_usage_mb_m1','data_3g_usage_mb_m2','data_3g_usage_mb_m3','data_4g_usage_mb_m1',\
                  'data_4g_usage_mb_m2','data_4g_usage_mb_m3','total_voice_main_package_rev_m1','total_voice_main_package_rev_m2',\
                  'total_voice_main_package_rev_m3','total_gprs_main_package_rev_m1','total_gprs_main_package_rev_m2',\
                  'total_gprs_main_package_rev_m3','total_voice_ontop_package_rev_m1','total_voice_ontop_package_rev_m2',\
                  'total_voice_ontop_package_rev_m3','total_gprs_ontop_package_rev_m1','total_gprs_ontop_package_rev_m2',\
                  'total_gprs_ontop_package_rev_m3','total_main_rev_mth_m1','total_main_rev_mth_m2','total_main_rev_mth_m3',\
                  'total_ontop_rev_mth_m1','total_ontop_rev_mth_m2','total_ontop_rev_mth_m3','bill_discount_before',\
                  'bill_discount_after','mainpack_abnormal_flag','data_traffic_subs_mb_m1','mou_ic_total_m1','mou_og_intl_m1',\
                  'mou_og_total_m1','num_of_days_data_used_m1','sms_og_total_m1','data_traffic_subs_mb_m2',\
                  'mou_ic_total_m2','mou_og_intl_m2','mou_og_total_m2','num_of_days_data_used_m2','sms_og_total_m2',\
                  'data_traffic_subs_mb_m3','mou_ic_total_m3','mou_og_intl_m3','mou_og_total_m3','num_of_days_data_used_m3',\
                  'sms_og_total_m3','distinct_out_number_m1','distinct_out_number_m2','distinct_out_number_m3','promotion_price_discount_m3',\
                  'promotion_price_discount_m6','voice_charging_type_m3','voice_charging_type_m6','promotion_type_m3','promotion_type_m6',\
                  'most_region_usage_voice_m6','most_region_usage_data_m6','most_province_usage_voice_m6','most_province_usage_data_m6')
      df.registerTempTable("present_df")
      ontop_package_price_m4 = sqlContext.sql("SELECT analytic_id, ontop_package_price_m4 from present_df")
      means = ontop_package_price_m4.agg( *[func.mean(c).alias(c) for c in ontop_package_price_m4.columns if c != 'analytic_id']).toPandas().to_dict('records')[0]
      means['ontop_package_price_m4'] = 0
      means['ontop_package_price_m5'] = 0
      means['ontop_package_price_m6'] = 0
      parsedDf = parsedDf.fillna(means)
      parsedDf.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_05_output_01)
      sc.stop()