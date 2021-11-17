import sys
import json
import os

try:
    fdata=open(sys.argv[1],"r")
    config=json.load(fdata)
    fdata.close()
except:
    print("Error reading config file!")
    exit()


directory=config["path_to_datanodes"]
try:
    #creating ports.json file in datanodes folder. Here, datanode ports are stored when they start
    open(os.path.join(directory, "ports.json"), 'w').close()
    for i in range(config["num_datanodes"]):
        path=os.path.join(directory,str(i))
        os.mkdir(path)
except:
        print("Datanode already present, using existing datanodes...")

print("Datanodes are created")

directory=config["datanode_log_path"]
for i in range(config["num_datanodes"]):
    path=os.path.join(directory,str(i)+".txt")
    dn_log=open(path,'w')
    dn_log.close()


dfs_info=open(config["dfs_setup_config"],"w")
dfs_info.write(json.dumps(config,indent=4))
dfs_info.close()

print("Configurations saved")

print("Configuration complete")