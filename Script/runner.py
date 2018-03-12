import subprocess
import sys
import time
import pexpect
import ds_config

def ssh_command (user, host, password, command):
    """This runs a command on the remote host."""
    print " I am logging into", host
    ssh_newkey = 'Are you sure you want to continue connecting'
    child = pexpect.spawn('ssh -l %s %s %s'%(user, host, command))
    i = child.expect([pexpect.TIMEOUT, ssh_newkey, 'password: '])
    if i == 0: # Timeout
        print('ERROR!')
        print('SSH could not login. Here is what SSH said:')
        print(child.before, child.after)
        return None
    if i == 1: # SSH does not have the public key. Just accept it.
        child.sendline ('yes')
        child.expect ('password: ')
        i = child.expect([pexpect.TIMEOUT, 'password: '])
        if i == 0: # Timeout
            print('9ERROR!')
            print('SSH could not login. Here is what SSH said:')
            print(child.before, child.after)
            return None       
    child.sendline(password)
    return child

if __name__ == "__main__":
	#sys.argv[1] preprocess or train or generate
	if sys.argv[1] == 'preprocess':
		t_start = time.time()
		t_01start = time.time()
		print subprocess.Popen(" spark-submit --master yarn-client --queue default --num-executors 55 \
			--executor-memory 3G --executor-cores 1 --driver-memory 10G preprocess_01.py", shell=True, stdout=subprocess.PIPE).stdout.read()
		t_01end = time.time()
		print("Preprocess 01 complete in "+ str(t_01end - t_01start) + " seconds")
		t_02start = time.time()
		print subprocess.Popen(" spark-submit --master yarn-client --queue default --num-executors 55 \
			--executor-memory 3G --executor-cores 1 --driver-memory 10G preprocess_02.py", shell=True, stdout=subprocess.PIPE).stdout.read()
		t_02end = time.time()
		print("Preprocess 02 complete in "+ str(t_02end - t_02start) + " seconds")
		t_03start = time.time()
		print subprocess.Popen(" spark-submit --master yarn-client --queue default --num-executors 55 \
			--executor-memory 3G --executor-cores 1 --driver-memory 10G preprocess_03.py", shell=True, stdout=subprocess.PIPE).stdout.read()
		t_03end = time.time()
		print("Preprocess 03 complete in "+ str(t_03end - t_03start) + " seconds")
		t_04start = time.time()
		print subprocess.Popen(" spark-submit --master yarn-client --queue default --num-executors 55 \
			--executor-memory 3G --executor-cores 1 --driver-memory 10G preprocess_04.py", shell=True, stdout=subprocess.PIPE).stdout.read()
		t_04end = time.time()
		print("Preprocess 04 complete in "+ str(t_04end - t_04start) + " seconds")
		t_05start = time.time()
		print subprocess.Popen(" spark-submit --master yarn-client --queue default --num-executors 55 \
			--executor-memory 3G --executor-cores 1 --driver-memory 10G preprocess_05.py", shell=True, stdout=subprocess.PIPE).stdout.read()
		t_05end = time.time()
		print("Preprocess 05 complete in "+ str(t_05end - t_05start) + " seconds")
		t_06start = time.time()
		print subprocess.Popen(" spark-submit --master yarn-client --queue default --num-executors 55 \
			--executor-memory 3G --executor-cores 1 --driver-memory 10G preprocess_06.py", shell=True, stdout=subprocess.PIPE).stdout.read()
		t_06end = time.time()
		print("Preprocess 06 complete in "+ str(t_06end - t_06start) + " seconds")
		t_07start = time.time()
		print subprocess.Popen(" spark-submit --master yarn-client --queue default --num-executors 55 \
			--executor-memory 3G --executor-cores 1 --driver-memory 10G preprocess_07.py", shell=True, stdout=subprocess.PIPE).stdout.read()
		t_07end = time.time()
		print("Preprocess 07 complete in "+ str(t_07end - t_07start) + " seconds")
		t_08start = time.time()
		print subprocess.Popen(" spark-submit --master yarn-client --queue default --num-executors 55 \
			--executor-memory 3G --executor-cores 1 --driver-memory 10G preprocess_08.py", shell=True, stdout=subprocess.PIPE).stdout.read()
		t_08end = time.time()
		print("Preprocess 08 complete in "+ str(t_08end - t_08start) + " seconds")
		t_end = time.time()
		print("Preprocess tasks complete in "+ str(t_end - t_start) + " seconds")
	elif ys.argv[1] == 'train':
		child = ssh_command ('strikermx', 'dtdsjust.southeastasia.cloudapp.azure.com', 'ni1909900377702,', 'python /home/strikermx/propensity-upward/Script/downloadBlob.py ' + ds_config.preprocess_08_output_02 + ' '+ ds_config.date_stamp )
		child = ssh_command ('strikermx', 'dtdsjust.southeastasia.cloudapp.azure.com', 'ni1909900377702,', 'python /home/strikermx/propensity-upward/Script/train_decisionTree.py ' + ds_config.date_stamp)

