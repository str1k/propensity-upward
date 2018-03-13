from azure.storage.blob import BlockBlobService
import sys
import os
data_dir = '/home/strikermx/data_dir/model_' + sys.argv[2]
block_blob_service = BlockBlobService(account_name='natds201801cvmarpu', account_key='melH7xjBqGc0yCtz4eL+v8rfR+Lx/cbTqlZ7Jz+adMNpTEIDdAU0L0nd2yUaMkimqU0gM0XixAwk8CRhuKoduw==')
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
generator = block_blob_service.list_blobs('ds-cvm-hdi-arpu-upward-2018-01-12t03-00-38-195z')
for blob in generator:
    if sys.argv[1] + sys.argv[2] +'/part' in blob.name:
        print(blob.name)
        block_blob_service.get_blob_to_path('ds-cvm-hdi-arpu-upward-2018-01-12t03-00-38-195z', blob.name ,'/home/strikermx/data_dir/model_'+sys.argv[2]+'/training_'+sys.argv[2]+'.dat')

