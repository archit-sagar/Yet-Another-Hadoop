import sys
import json
import os
from hashlib import sha256

try:
    fdata=open(sys.argv[1],"r")
    config=json.load(fdata)
    fdata.close()
except:
    print("Error reading config file!")
    sys.exit()

#checking if all the required fields are there in the configuration file
try:
    if 'block_size' not in config.keys():
        config['block_size'] = 512
        print("Block size was not specified. Using default value of 512 bytes.")
    if 'replication_factor' not in config.keys():
        config['replication_factor'] = 3
        print("Replication factor was not specified. Using default value of 3.")
    if 'num_datanodes' not in config.keys():
        config['num_datanodes'] = 4
        print("Number of datanodes not specified. Using default value of 4.")
    if 'datanode_size' not in config.keys():
        config['datanode_size'] = 30
        print('Size of datanode was not specified. Using default value of 30.')
    if 'sync_period' not in config.keys():
        config['sync_period'] = 10
        print('Sync period was not specified. Using default value of 10.')
    if 'fs_path' not in config.keys():
        config['fs_path'] = "user/home/"
        print("FS Path was not specified. Using default value of 'user/home/'")
    if 'python_command' not in config.keys():
        config['python_command'] = 'python'
        print("Python command was not found. Using default value 'python'.")
except:
    exit()

if 'path_to_datanodes' not in config.keys():
    sys.exit('Path to datanodes not specified. Exiting...')
if 'path_to_namenodes' not in config.keys():
    sys.exit('Path to namenodes not specified. Exiting...')
if 'datanode_log_path' not in config.keys():
    sys.exit('Datanode log path not specified. Exiting...')
if 'namenode_log_path' not in config.keys():
    sys.exit('Namenode log path not specified. Exiting...')
if 'namenode_checkpoints' not in config.keys():
    sys.exit('Namenode checkpoints path not specified. Exiting...')
if 'dfs_setup_config' not in config.keys():
    sys.exit('DFS Setup config file path not specified. Exiting...')


directory=config["path_to_datanodes"]
if not os.path.isdir(directory):
    sys.exit('Path to datanodes was not a directory. Exiting...')
if not os.path.isdir(config['datanode_log_path']):
    sys.exit('Datanode log path was not a directory. Exiting...')
if not os.path.isdir(config['namenode_checkpoints']):
    sys.exit('Namenode checkpoints was not a directory. Exiting...')
if not os.path.isfile(config['namenode_log_path']):
    sys.exit('Namenode log path was not a file. Exiting...')
if not os.path.isdir(config["path_to_namenodes"]):
    sys.exit('Path to namenodes was not a directory. Exiting...')
if config['num_datanodes']<config['replication_factor']:
    sys.exit("Number of datanodes were not sufficient. Exiting...")

try:
    for i in range(config["num_datanodes"]):
        path=os.path.join(directory,str(i))
        os.mkdir(path)
except:
        print("Datanode already present, using existing datanodes...")

print("Datanodes are created")

# creating ports.json file for namenode, to store namenode port
f = open(os.path.join(config["path_to_namenodes"], "ports.json"), 'w')
f.write(json.dumps({"port": 0},indent=4))
f.close()

directory=config["datanode_log_path"]
for i in range(config["num_datanodes"]):
    path=os.path.join(directory,str(i)+".txt")
    dn_log=open(path,'w')
    dn_log.close()


dfs_info=open(config["dfs_setup_config"],"w")
dfs_info.write(json.dumps(config,indent=4))
dfs_info.close()

#creating hash for this configuration, to verify during loading the dfs
content = str(config).strip() #hash must be of setup_config. Since, we have same file for both, config works fine
hashVal = sha256(content.encode())
hexHash = hashVal.hexdigest()

with open(os.path.join(config["path_to_datanodes"], "hash.txt"), 'w') as f:
    f.write(hexHash)

with open(os.path.join(config["path_to_namenodes"], "hash.txt"), 'w') as f:
    f.write(hexHash)

with open(os.path.join(config["namenode_checkpoints"], "hash.txt"), 'w') as f:
    f.write(hexHash)

print("Configurations saved")

print("Configuration complete")