#loads the dfs
    #Verifies if provided config is valid
    #creates the namenode and datanode processes

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

hexHash = sha256(str(config).strip().encode()).hexdigest()

valid_conf = 0

with open(os.path.join(config["path_to_datanodes"], "hash.txt"), 'r') as f:
    if f.readline() == hexHash:
        valid_conf +=1

with open(os.path.join(config["path_to_namenodes"], "hash.txt"), 'r') as f:
    if f.readline() == hexHash:
        valid_conf +=1

with open(os.path.join(config["namenode_checkpoints"], "hash.txt"), 'r') as f:
    if f.readline() == hexHash:
        valid_conf +=1

if valid_conf == 3:
    print("configuration valid. Proceeding further")
else:
    print("Invalid configuration. Please setup and try again")

#next steps: