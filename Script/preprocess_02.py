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
    conf = SparkConf().setAppName("preprocess_02")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
	df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(ds_config.preprocess_01_output_01)
	tarif = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", '|').load(ds_config.master_tarif)
	#drop column
	tarif = tarif.drop("promotion_cd","source_type","operator_name","charge_type","billing_system","promotion_name","promotion_description")
	tarif = tarif.drop("promotion_sale_start_data","promotion_sale_end_date","promotion_duration","promotion_duration_type")
	tarif = tarif.drop("promotion_price_amount","promotion_discount_start_bill","promotion_discount_end_bill")
	tarif = tarif.drop("promotion_price_type_code","promotion_location_yn","promotion_location_area","peak_offpeak_yn")
	tarif = tarif.drop("sms_quota_trans","mms_quota_trans")
	tarif = tarif.drop("voice_rate_in_pack","data_volume_rate_in_pack","data_minute_rate_in_pack","sms_trans_rate_in_pack","mms_trans_rate_in_pack")
	tarif = tarif.drop("mms_pay_per_use","data_volume_pay_per_use","data_minute_pay_per_use","sms_pay_per_use")
	
	#replace value
	tarif = tarif.withColumn('data_quota_volume_mb', regexp_replace('data_quota_volume_mb', 'Unlimited', '999999'))
	tarif.registerTempTable("selectedtarif")
	#create new feature
	parsedtarif = sqlContext.sql("SELECT *,(ais_call_inpack_start_hour-ais_call_inpack_end_hour) as ais_call_inpack_hours from selectedtarif")
	parsedtarif = parsedtarif.withColumn('ais_call_inpack_hours',abs(parsedtarif.ais_call_inpack_hours))
	parsedtarif = parsedtarif.drop("ais_call_inpack_start_hour","ais_call_inpack_end_hour")
	parsedtarifM3 = parsedtarif.selectExpr("promotion_id as main_package_id_m3","promotion_price_discount as promotion_price_discount_m3", "bundle_handset_yn as bundle_handset_yn_m3",\
                                      "onnet_offnet_yn as onnet_offnet_yn_m3", "bundle_service_yn as bundle_service_yn_m3", "voice_yn as voice_yn_m3",\
                                      "data_yn as data_yn_m3", "sms_yn as sms_yn_m3", "mms_yn as mms_yn_m3", "awifi_yn as awifi_yn_m3", "swifi_yn as swifi_yn_m3",\
                                      "other_app_yn as other_app_yn_m3", "voice_charging_type as voice_charging_type_m3", "voice_first_minute_rate as voice_first_minute_rate_m3",\
                                      "voice_other_minute_rate as voice_other_minute_rate_m3", "data_unlimited_yn as data_unlimited_yn_m3", "data_throttling_yn as data_throttling_yn_m3",\
                                      "data_throttling_speed_kbps as data_throttling_speed_kbps_m3", "data_speed_mb as data_speed_mb_m3","data_network as data_network_m3",\
                                      "charge_by_second_yn as charge_by_second_yn_m3", "voice_quota_minute as voice_quota_minute_m3", "data_quota_volume_mb as data_quota_volume_mb_m3",\
                                      "data_quota_minute as data_quota_minute_m3", "voice_pay_per_use as voice_pay_per_use_m3", "ais_call_inpack_hours as ais_call_inpack_hours_m3",\
                                      "promotion_type as promotion_type_m3")
	joinedtarifm3 = df.join(parsedtarifM3, ["main_package_id_m3"], "left_outer")
	joinedtarifm3.registerTempTable("joinedtarifm3")
	joinedtarifm3m6 = joinedtarifm3.join(parsedtarifM6, ["main_package_id_m6"], "left_outer")
	joinedtarifm3m6.repartition(1).write.option("sep","|").option("header","true").csv("/preprocessed_cvm/joinedtarifm3m6_"+ datestamp)
	tmp = sqlContext.sql("SELECT promotion_price_discount_m3,bundle_handset_yn_m3,onnet_offnet_yn_m3,bundle_service_yn_m3 from joinedtarifm3")
	#tmp.agg(*[ (1 - (func.count(c) / func.count('*'))).alias(c) for c in tmp.columns]).show()
	joinedtarifm3m6.select("promotion_price_discount_m3").show()

	joinedtarifm3m6 = joinedtarifm3m6.withColumn('promotion_price_discount_flag_m3', when(joinedtarifm3m6['promotion_price_discount_m3']> 0, 1).otherwise(0))
	joinedtarifm3m6 = joinedtarifm3m6.drop("voice_first_minute_rate_m3","voice_other_minute_rate_m3")
	joinedtarifm3m6 = joinedtarifm3m6.drop("voice_first_minute_rate_m6","voice_other_minute_rate_m6")
	joinedtarifm3m6 = joinedtarifm3m6.drop("data_speed_mb_m3")
	joinedtarifm3m6 = joinedtarifm3m6.drop("data_speed_mb_m6")
	joinedtarifm3m6 = joinedtarifm3m6.drop("data_quota_minute_m3")
	joinedtarifm3m6 = joinedtarifm3m6.drop("data_quota_minute_m6")
	ais_call_inpack_hours_m3 = spark.sql("SELECT analytic_id, ais_call_inpack_hours_m3 from joinedtarifm3")
	means = ais_call_inpack_hours_m3.agg( *[func.mean(c).alias(c) for c in ais_call_inpack_hours_m3.columns if c != 'analytic_id']).toPandas().to_dict('records')[0]
	means['bundle_handset_yn_m3'] = 'NA'
	means['onnet_offnet_yn_m3'] = 'NA'
	means['bundle_service_yn_m3'] = 'NA'
	means['voice_yn_m3'] = 'NA'
	means['data_yn_m3'] = 'NA'
	means['sms_yn_m3'] = 'NA'
	means['mms_yn_m3'] = 'NA'
	means['awifi_yn_m3'] = 'NA'
	means['swifi_yn_m3'] = 'NA'
	means['other_app_yn_m3'] = 'NA'
	means['voice_charging_type_m3'] = 'NA'
	means['data_unlimited_yn_m3'] = 'NA'
	means['data_throttling_yn_m3'] = 'NA'
	means['data_throttling_speed_kbps_m3'] = 0
	means['data_network_m3'] = 'NA'
	means['charge_by_second_yn_m3'] = 'NA'
	means['voice_quota_minute_m3'] = 'NA'
	means['data_quota_volume_mb_m3'] = 'NA'
	means['voice_pay_per_use_m3'] = 'NA'
	means['ais_call_inpack_hours_m3'] = 0

	means['bundle_handset_yn_m6'] = 'NA'
	means['onnet_offnet_yn_m6'] = 'NA'
	means['bundle_service_yn_m6'] = 'NA'
	means['voice_yn_m6'] = 'NA'
	means['data_yn_m6'] = 'NA'
	means['sms_yn_m6'] = 'NA'
	means['mms_yn_m6'] = 'NA'
	means['awifi_yn_m6'] = 'NA'
	means['swifi_yn_m6'] = 'NA'
	means['other_app_yn_m6'] = 'NA'
	means['voice_charging_type_m6'] = 'NA'
	means['data_unlimited_yn_m6'] = 'NA'
	means['data_throttling_yn_m6'] = 'NA'
	means['data_throttling_speed_kbps_m6'] = 0
	means['data_network_m6'] = 'NA'
	means['charge_by_second_yn_m6'] = 'NA'
	means['voice_quota_minute_m6'] = 'NA'
	means['data_quota_volume_mb_m6'] = 'NA'
	means['voice_pay_per_use_m6'] = 'NA'
	means['ais_call_inpack_hours_m6'] = 0

	joinedtarifm3m6_elimNA = joinedtarifm3m6.fillna(means)
	joinedtarifm3m6_elimNA.registerTempTable("joinedtarifm3elimNA")

	joinedtarifm3m6_elimNA.repartition(1).write.option("sep","|").option("header","true").csv(ds_config.preprocess_02_output_01)