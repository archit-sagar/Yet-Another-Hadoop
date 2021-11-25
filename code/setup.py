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
    exit()


directory=config["path_to_datanodes"]
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