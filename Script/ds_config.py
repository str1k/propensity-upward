date_stamp = 'test_20180309'

cvm_non_voice_arpu_source = 'wasb://ds-cvm-hd-rs-devprod-02-2017-09-25t08-15-40-207z@natds201708cvm1sa01.blob.core.windows.net/data_cvm/NON_VOICE/cvm_non_voice_arpu_upward_jan18_aug17_20180226.dat'
cvm_non_voice_arpu_delimiter = '|'

master_tarif = '/data_cvm/NON_VOICE/master_package_tariff_20180305.txt'

gs_customer_prof_before = 'wasb://ds-cvm-hd-rs-devprod-02-2017-09-25t08-15-40-207z@natds201708cvm1sa01.blob.core.windows.net/data_cvm/POSTPAID/GS_SUMMARY_CUSTOMER_PROF_201710.dat'
gs_customer_prof_before_delim = '|'

preprocess_01_output_01 = '/preprocessed_cvm/mainset_elim_null_' + date_stamp

preprocess_02_output_01 = '/preprocessed_cvm/processed_ARPU_Tarif_'+ date_stamp
preprocess_02_output_02 = '/preprocessed_cvm/joinedtarifm3m6_'+ date_stamp

preprocess_03_output_01 = '/preprocessed_cvm/ARPUTraining_' + date_stamp