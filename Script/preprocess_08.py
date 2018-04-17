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
def onehotenc(t_df, column):
    categories = t_df.select(column).distinct().rdd.flatMap(lambda x : x).collect()
    categories.sort()
    for category in categories:
        function = udf(lambda item: 1 if item == category else 0, IntegerType())
        new_column_name = column+'_'+str(category)
        t_df = t_df.withColumn(new_column_name, function(col(column)))
    t_df = t_df.drop(column)
    return t_df

if __name__ == "__main__":
    logging.getLogger("py4j").setLevel(logging.ERROR)
    conf = SparkConf().setAppName("preprocess_08")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(ds_config.preprocess_06_output_01)
    df.registerTempTable("dftb")
    ma_age = sqlContext.sql("SELECT analytic_id, ma_age from dftb")
    means = ma_age.agg( *[func.mean(c).alias(c) for c in ma_age.columns if c != 'analytic_id']).toPandas().to_dict('records')[0]
    parsedDF = df.drop('multisim_user_flag_before','most_region_usage_voice_m3','most_region_usage_data_m3',\
                   'most_province_usage_voice_m3','most_province_usage_data_m3','bundle_handset_yn_m6',\
                   'onnet_offnet_yn_m6','bundle_service_yn_m6',\
                  'voice_yn_m6','data_yn_m6','sms_yn_m6','mms_yn_m6','awifi_yn_m6','swifi_yn_m6','other_app_yn_m6',\
                  'data_unlimited_yn_m6','data_throttling_yn_m6','data_throttling_speed_kbps_m6','data_network_m6',\
                  'charge_by_second_yn_m6','voice_quota_minute_m6','data_quota_volume_mb_m6','voice_pay_per_use_m6',\
                  'ais_call_inpack_hours_m6','promotion_price_discount_flag_m3')
    from pyspark.sql.functions import *
    from pyspark.sql.types import IntegerType
    parsedDF = parsedDF.withColumn('ma_age', regexp_replace('ma_age', 'NA', str(means['ma_age'])))
    parsedDF = parsedDF.withColumn('hs_early_adopt_m3', regexp_replace('hs_early_adopt_m3', 'NA', '0'))
    parsedDF = parsedDF.withColumn('voice_pay_per_use_m3', regexp_replace('voice_pay_per_use_m3', 'NA', '1.5'))
    parsedDF = parsedDF.withColumn('data_quota_volume_mb_m3', regexp_replace('data_quota_volume_mb_m3', 'NA', '-1'))
    parsedDF = parsedDF.withColumn('voice_quota_minute_m3', regexp_replace('voice_quota_minute_m3', 'NA', '-1'))
    parsedDF = parsedDF.withColumn('foreigner_flag', regexp_replace('foreigner_flag', 'Y', '1'))
    parsedDF = parsedDF.withColumn('foreigner_flag', regexp_replace('foreigner_flag', 'N', '0'))
    parsedDF = parsedDF.withColumn('urbanflag', regexp_replace('urbanflag', 'Y', '1'))
    parsedDF = parsedDF.withColumn('urbanflag', regexp_replace('urbanflag', 'N', '0'))
    parsedDF = parsedDF.selectExpr('analytic_id','mobile_segment as mobile_segment_p','avg_arpu_before as avg_arpu_p',\
                              'ma_gender as gender_p','ma_age as age_p','most_region_usage_m3 as most_region_usage_p3',\
                              'most_province_usage_m3 as most_province_usage_p3','hs_support_3g2100_flag_m3 as hs_support_3g2100_flag_p3',\
                              'hs_support_lte_flag_m3 as hs_support_lte_flag_p3','hs_release_price_baht_m3 as hs_release_price_baht_p3',\
                              'hs_early_adopt_m3 as hs_early_adopt_p3', 'no_of_device_before as no_of_device_p','main_package_price_m1 as main_package_price_p1',\
                              'main_package_price_m2 as main_package_price_p2','main_package_price_m3 as main_package_price_p3',\
                              'ontop_package_flag_m1 as ontop_package_flag_p1','ontop_package_flag_m2 as ontop_package_flag_p2',\
                              'ontop_package_flag_m3 as ontop_package_flag_p3','ontop_package_price_m1 as ontop_package_price_p1',\
                              'ontop_package_price_m2 as ontop_package_price_p2','ontop_package_price_m3 as ontop_package_price_p3',\
                              'vou_payperuse_m1 as vou_payperuse_p1','vou_payperuse_m2 as vou_payperuse_p2','vou_payperuse_m3 as vou_payperuse_p3',\
                              'mou_offnet_m1 as mou_offnet_p1','mou_offnet_m2 as mou_offnet_p2','mou_offnet_m3 as mou_offnet_p3',\
                              'no_of_ontop_active_m1 as no_of_ontop_active_p1','no_of_ontop_active_m2 as no_of_ontop_active_p2',\
                              'no_of_ontop_active_m3 as no_of_ontop_active_p3','streaming_level_m1 as streaming_level_p1',\
                              'streaming_level_m2 as streaming_level_p2','streaming_level_m3 as streaming_level_p3',\
                              'total_volumn_mb_wifi_m1 as total_volumn_mb_wifi_p1','total_volumn_mb_wifi_m2 as total_volumn_mb_wifi_p2',\
                              'total_volumn_mb_wifi_m3 as total_volumn_mb_wifi_p3','data_3g_usage_mb_m1 as data_3g_usage_mb_p1',\
                              'data_3g_usage_mb_m2 as data_3g_usage_mb_p2','data_3g_usage_mb_m3 as data_3g_usage_mb_p3',\
                              'data_4g_usage_mb_m1 as data_4g_usage_mb_p1','data_4g_usage_mb_m2 as data_4g_usage_mb_p2',\
                              'data_4g_usage_mb_m3 as data_4g_usage_mb_p3','total_voice_main_package_rev_m1 as total_voice_main_package_rev_p1',\
                              'total_voice_main_package_rev_m2 as total_voice_main_package_rev_p2','total_voice_main_package_rev_m3 as total_voice_main_package_rev_p3',\
                              'total_gprs_main_package_rev_m1 as total_gprs_main_package_rev_p1','total_gprs_main_package_rev_m2 as total_gprs_main_package_rev_p2',\
                              'total_gprs_main_package_rev_m3 as total_gprs_main_package_rev_p3','total_voice_ontop_package_rev_m1 as total_voice_ontop_package_rev_p1',\
                              'total_voice_ontop_package_rev_m2 as total_voice_ontop_package_rev_p2','total_voice_ontop_package_rev_m3 as total_voice_ontop_package_rev_p3',\
                              'total_gprs_ontop_package_rev_m1 as total_gprs_ontop_package_rev_p1','total_gprs_ontop_package_rev_m2 as total_gprs_ontop_package_rev_p2',\
                              'total_gprs_ontop_package_rev_m3 as total_gprs_ontop_package_rev_p3','total_main_rev_mth_m1 as total_main_rev_mth_p1',\
                              'total_main_rev_mth_m2 as total_main_rev_mth_p2', 'total_main_rev_mth_m3 as total_main_rev_mth_p3',\
                              'total_ontop_rev_mth_m1 as total_ontop_rev_mth_p1','total_ontop_rev_mth_m2 as total_ontop_rev_mth_p2',\
                              'total_ontop_rev_mth_m3 as total_ontop_rev_mth_p3','data_traffic_subs_mb_m1 as data_traffic_subs_mb_p1',\
                              'data_traffic_subs_mb_m2 as data_traffic_subs_mb_p2','data_traffic_subs_mb_m3 as data_traffic_subs_mb_p3',\
                              'mou_ic_total_m1 as mou_ic_total_p1','mou_ic_total_m2 as mou_ic_total_p2','mou_ic_total_m3 as mou_ic_total_p3',\
                              'mou_og_intl_m1 as mou_og_intl_p1','mou_og_intl_m2 as mou_og_intl_p2','mou_og_intl_m3 as mou_og_intl_p3',\
                              'mou_og_roaming_m1 as mou_og_roaming_p1','mou_og_roaming_m2 as mou_og_roaming_p2','mou_og_roaming_m3 as mou_og_roaming_p3',\
                              'mou_og_total_m1 as mou_og_total_p1','mou_og_total_m2 as mou_og_total_p2','mou_og_total_m3 as mou_og_total_p3',\
                              'num_of_days_data_used_m1 as num_of_days_data_used_p1','num_of_days_data_used_m2 as num_of_days_data_used_p2',\
                              'num_of_days_data_used_m3 as num_of_days_data_used_p3','sms_og_total_m1 as sms_og_total_p1','sms_og_total_m2 as sms_og_total_p2',\
                              'sms_og_total_m3 as sms_og_total_p3','distinct_out_number_m1 as distinct_out_number_p1',\
                              'distinct_out_number_m2 as distinct_out_number_p2','distinct_out_number_m3 as distinct_out_number_p3',\
                              'bundle_handset_yn_m3 as bundle_handset_yn_p3','onnet_offnet_yn_m3 as onnet_offnet_yn_p3',\
                              'bundle_service_yn_m3 as bundle_service_yn_p3','voice_yn_m3 as voice_yn_p3','data_yn_m3 as data_yn_p3',\
                              'sms_yn_m3 as sms_yn_p3','mms_yn_m3 as mms_yn_p3','awifi_yn_m3 as awifi_yn_p3','swifi_yn_m3 as swifi_yn_p3',\
                              'other_app_yn_m3 as other_app_yn_p3','data_unlimited_yn_m3 as data_unlimited_yn_p3','data_throttling_yn_m3 as data_throttling_yn_p3',\
                              'data_throttling_speed_kbps_m3 as data_throttling_speed_kbps_p3','data_network_m3 as data_network_p3',\
                              'charge_by_second_yn_m3 as charge_by_second_yn_p3','voice_quota_minute_m3 as voice_quota_minute_p3',\
                              'data_quota_volume_mb_m3 as data_quota_volume_mb_p3','voice_pay_per_use_m3 as voice_pay_per_use_p3',\
                               'ais_call_inpack_hours_m3 as ais_call_inpack_hours_p3','foreigner_flag as foreigner_flag_p',\
                              'mobile_region as mobile_region_p','service_month as service_month_p','urbanflag as urbanflag_p','billing_region as billing_region_p',\
                              'percent_change as label', 'mainpack_abnormal_flag')
    parsedDF = parsedDF.withColumn('label', when(parsedDF['label']>= 20, 1).otherwise(0))
    parsedDF.registerTempTable("parsedDF")
    parsedDF = sqlContext.sql("SELECT *,((ontop_package_price_p1+ontop_package_price_p2+ontop_package_price_p3)/3) as avg_ontop_package_price_p from parsedDF")
    parsedDF.registerTempTable("parsedDF")

    parsedDF = sqlContext.sql("SELECT *,((vou_payperuse_p1+vou_payperuse_p2+vou_payperuse_p3)/3) as avg_vou_payperuse_p from parsedDF")
    parsedDF.registerTempTable("parsedDF")  

    parsedDF = sqlContext.sql("SELECT *,((mou_offnet_p1+mou_offnet_p2+mou_offnet_p3)/3) as avg_mou_offnet_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((no_of_ontop_active_p1+no_of_ontop_active_p2+no_of_ontop_active_p3)/3) as avg_no_of_ontop_active_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((streaming_level_p1+streaming_level_p2+streaming_level_p3)/3) as avg_streaming_level_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((total_volumn_mb_wifi_p1+total_volumn_mb_wifi_p2+total_volumn_mb_wifi_p3)/3) as avg_total_volumn_mb_wifi_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((data_3g_usage_mb_p1+data_3g_usage_mb_p2+data_3g_usage_mb_p3)/3) as avg_data_3g_usage_mb_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((data_4g_usage_mb_p1+data_4g_usage_mb_p2+data_4g_usage_mb_p3)/3) as avg_data_4g_usage_mb_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((total_voice_main_package_rev_p1+total_voice_main_package_rev_p2+total_voice_main_package_rev_p1)/3) as avg_total_voice_main_package_rev_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((total_gprs_main_package_rev_p1+total_gprs_main_package_rev_p2+total_gprs_main_package_rev_p3)/3) as avg_total_gprs_main_package_rev_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((total_voice_ontop_package_rev_p1+total_voice_ontop_package_rev_p2+total_voice_ontop_package_rev_p3)/3) as avg_total_voice_ontop_package_rev_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((total_gprs_ontop_package_rev_p1+total_gprs_ontop_package_rev_p2+total_gprs_ontop_package_rev_p3)/3) as avg_total_gprs_ontop_package_rev_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((total_main_rev_mth_p1+total_main_rev_mth_p2+total_main_rev_mth_p3)/3) as avg_total_main_rev_mth_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((total_ontop_rev_mth_p1+total_ontop_rev_mth_p2+total_ontop_rev_mth_p3)/3) as avg_total_ontop_rev_mth_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((data_traffic_subs_mb_p1+data_traffic_subs_mb_p2+data_traffic_subs_mb_p3)/3) as avg_data_traffic_subs_mb_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((mou_ic_total_p1+mou_ic_total_p2+mou_ic_total_p3)/3) as avg_mou_ic_total_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((mou_og_intl_p1+mou_og_intl_p2+mou_og_intl_p3)/3) as avg_mou_og_intl_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    #parsedDF = sqlContext.sql("SELECT *,((mou_og_roaming_p1+mou_og_roaming_p2+mou_og_roaming_p3)/3) as avg_mou_og_roaming_p from parsedDF")
    #parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((mou_og_total_p1+mou_og_total_p2+mou_og_total_p3)/3) as avg_mou_og_total_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((num_of_days_data_used_p1+num_of_days_data_used_p2+num_of_days_data_used_p3)/3) as avg_num_of_days_data_used_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((sms_og_total_p1+sms_og_total_p2+sms_og_total_p3)/3) as avg_sms_og_total_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT *,((distinct_out_number_p1+distinct_out_number_p2+distinct_out_number_p3)/3) as avg_distinct_out_number_p from parsedDF")
    parsedDF.registerTempTable("parsedDF") 

    parsedDF = sqlContext.sql("SELECT * FROM parsedDF WHERE mainpack_abnormal_flag = 0")
    parsedDF = parsedDF.drop("mainpack_abnormal_flag")
    parsedDF.registerTempTable("parsedDF")

    parsedDF = onehotenc(parsedDF, "mobile_segment_p")
    parsedDF = onehotenc(parsedDF, "gender_p")
    parsedDF = onehotenc(parsedDF, "most_region_usage_p3")
    parsedDF = onehotenc(parsedDF, "most_province_usage_p3")
    parsedDF = onehotenc(parsedDF, "bundle_handset_yn_p3")
    parsedDF = onehotenc(parsedDF, "onnet_offnet_yn_p3")
    parsedDF = onehotenc(parsedDF, "bundle_service_yn_p3")
    parsedDF = onehotenc(parsedDF, "voice_yn_p3")
    parsedDF = onehotenc(parsedDF, "data_yn_p3")
    parsedDF = onehotenc(parsedDF, "sms_yn_p3")
    parsedDF = onehotenc(parsedDF, "mms_yn_p3")
    parsedDF = onehotenc(parsedDF, "awifi_yn_p3")
    parsedDF = onehotenc(parsedDF, "swifi_yn_p3")
    parsedDF = onehotenc(parsedDF, "other_app_yn_p3")
    parsedDF = onehotenc(parsedDF, "data_unlimited_yn_p3")
    parsedDF = onehotenc(parsedDF, "data_throttling_yn_p3")
    parsedDF = onehotenc(parsedDF, "data_network_p3")
    parsedDF = onehotenc(parsedDF, "charge_by_second_yn_p3")
    parsedDF = onehotenc(parsedDF, "mobile_region_p")
    parsedDF = onehotenc(parsedDF, "billing_region_p")
    parsedDF.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_08_output_01)
    validate, train = parsedDF.randomSplit([0.7, 0.3]) 
    train.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_08_output_02)
    validate.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_08_output_03)
    maindf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(ds_config.preprocess_03_output_01)
    fittedDF = parsedDF.join(maindf, ["analytic_id"], "inner")
    fittedDF.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_08_output_04)
    sc.stop()