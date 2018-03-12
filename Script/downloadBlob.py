from azure.storage.blob import BlockBlobService
data_dir = '/home/strikermx/data_dir'
block_blob_service = BlockBlobService(account_name='natds201801cvmarpu', account_key='melH7xjBqGc0yCtz4eL+v8rfR+Lx/cbTqlZ7Jz+adMNpTEIDdAU0L0nd2yUaMkimqU0gM0XixAwk8CRhuKoduw==')
block_blob_service.set_container_acl('ds-cvm-hdi-arpu-upward-2018-01-12t03-00-38-195z', public_access=PublicAccess.Container)
block_blob_service.get_blob_to_path('ds-cvm-hdi-arpu-upward-2018-01-12t03-00-38-195z', argv[1], data_dir +'/' + argv[2])