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
    conf = SparkConf().setAppName("preprocess_06")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(ds_config.preprocess_04_output_01)
    parsedDf = df.drop('sub_id','register_date','avg_arpu_after','most_region_usage_voice_m1','most_region_usage_data_m1',\
                  'most_region_usage_m1','most_province_usage_voice_m1','most_province_usage_data_m1','most_province_usage_m1',\
                  'most_region_usage_voice_m6','most_region_usage_data_m6','most_region_usage_m6','most_province_usage_voice_m6',\
                  'most_province_usage_data_m6','most_province_usage_m6','hs_brand_name_m1','hs_model_m1','hs_support_3g2100_flag_m1',\
                  'hs_support_lte_flag_m1','hs_release_date_m1','hs_release_price_baht_m1','hs_first_eff_date_m1','hs_early_adopt_m1',\
                  'hs_brand_name_m6','hs_model_m6','hs_support_3g2100_flag_m6','hs_support_lte_flag_m6','hs_release_date_m6','hs_release_price_baht_m6',\
                  'hs_first_eff_date_m6', 'hs_early_adopt_m6', 'hs_brand_name_m3', 'hs_model_m3', 'hs_release_date_m3', 'hs_first_eff_date_m3',\
                  'no_of_device_after','main_package_id_m4','main_package_m4','main_package_price_m4','main_package_service_m4',\
                  'ontop_package_flag_m4','ontop_package_id_m4','ontop_package_m4','ontop_package_price_m4','main_package_id_m5',\
                  'main_package_m5','main_package_price_m5','main_package_service_m5','ontop_package_flag_m5','ontop_package_id_m5',\
                  'ontop_package_m5','ontop_package_price_m5','main_package_id_m6','main_package_m6','main_package_price_m6','main_package_service_m6',\
                  'ontop_package_flag_m6','ontop_package_id_m6','ontop_package_m6','ontop_package_price_m6','main_package_id_m1',\
                  'main_package_m1','main_package_service_m1','ontop_package_id_m1','ontop_package_m1','main_package_id_m2','main_package_m2',\
                  'main_package_service_m2','ontop_package_id_m2','ontop_package_m2','main_package_id_m3','main_package_m3','main_package_service_m3',\
                  'ontop_package_id_m3','ontop_package_m3','vou_payperuse_m4','mou_offnet_m4','vou_payperuse_m5','mou_offnet_m5',\
                  'vou_payperuse_m6','mou_offnet_m6','no_of_ontop_active_m4','no_of_ontop_active_m5','no_of_ontop_active_m6',\
                  'streaming_level_m4','streaming_level_m5','streaming_level_m6','total_volumn_mb_wifi_m4','total_volumn_mb_wifi_m5',\
                  'total_volumn_mb_wifi_m6','data_3g_usage_mb_m4','data_3g_usage_mb_m5','data_3g_usage_mb_m6','data_4g_usage_mb_m4',\
                  'data_4g_usage_mb_m5','data_4g_usage_mb_m6','total_voice_main_package_rev_m4','total_voice_main_package_rev_m5',\
                  'total_voice_main_package_rev_m6','total_gprs_main_package_rev_m4','total_gprs_main_package_rev_m5',\
                  'total_gprs_main_package_rev_m6','total_voice_ontop_package_rev_m4','total_voice_ontop_package_rev_m5',\
                  'total_voice_ontop_package_rev_m6','total_gprs_ontop_package_rev_m4','total_gprs_ontop_package_rev_m5',\
                  'total_gprs_ontop_package_rev_m6','total_main_rev_mth_m4','total_main_rev_mth_m5','total_main_rev_mth_m6',\
                  'total_ontop_rev_mth_m4','total_ontop_rev_mth_m5','total_ontop_rev_mth_m6','bill_discount_before',\
                  'bill_discount_after','data_traffic_subs_mb_m4','mou_ic_total_m4','mou_og_intl_m4',\
                  'mou_og_total_m4','num_of_days_data_used_m4','sms_og_total_m4','data_traffic_subs_mb_m5',\
                  'mou_ic_total_m5','mou_og_intl_m5','mou_og_total_m5','num_of_days_data_used_m5','sms_og_total_m5',\
                  'data_traffic_subs_mb_m6','mou_ic_total_m6','mou_og_intl_m6','mou_og_total_m6','num_of_days_data_used_m6',\
                  'sms_og_total_m6','distinct_out_number_m4','distinct_out_number_m5','distinct_out_number_m6','promotion_price_discount_m3',\
                  'promotion_price_discount_m6','voice_charging_type_m3','voice_charging_type_m6','promotion_type_m3','promotion_type_m6')
    df.registerTempTable("training_df")
    ontop_package_price_m1 = sqlContext.sql("SELECT analytic_id, ontop_package_price_m1 from training_df")
    means = ontop_package_price_m1.agg( *[func.mean(c).alias(c) for c in ontop_package_price_m1.columns if c != 'analytic_id']).toPandas().to_dict('records')[0]
    means['ontop_package_price_m1'] = 0
    means['ontop_package_price_m2'] = 0
    means['ontop_package_price_m3'] = 0
    parsedDf = parsedDf.fillna(means)
    parsedDf.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_06_output_01)
    sc.stop()