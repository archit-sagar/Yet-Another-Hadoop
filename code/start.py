#loads the dfs
    #Verifies if provided config is valid
    #creates the namenode and datanode processes

import sys
import json
import os
from hashlib import sha256
import socket
import subprocess
import time
import rpyc

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
def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port

namenodeProgramPath = 'namenode.py'
datanodeProgramPath = 'datanode.py'

namenodePort = get_free_tcp_port()
namenode = subprocess.Popen(args=["python", namenodeProgramPath, namenodePort])
with  open(os.path.join(config["path_to_namenodes"], "ports.json"), 'w') as f:
    f.write(json.dumps({"port": namenodePort},indent=4))

print("NAMENODE started at port", namenodePort)
time.sleep(1)

#connect to namenode, set config
try:
    con=rpyc.connect("localhost", namenodePort)
    res = con.root.set_config(config)
    if res:
        print("NAMENODE READY")
        con.close()
    else:
        raise Exception
except:
    print("NAMENODE FAILED")
    namenode.terminate()
    exit()

datanodePortDetails = {}
datanodes = [None] * config["num_datanodes"]

for i in range(config["num_datanodes"]):
    freePort = get_free_tcp_port()
    datanodePortDetails[i] = freePort
    #python datanode.py its_port its_path its_log_path namenode_port
    datanodes[i] = subprocess.Popen(args=["python", datanodeProgramPath, freePort, os.path.join(config["path_to_datanodes"], i),  os.path.join(config["datanode_log_path"], str(i)+".txt"), namenodePort])
    print("DATANODE", i, "started at port", freePort)
    time.sleep(0.5) #so that the port gets used before starting next datanode


try:
    for i in range(config['num_datanodes']):
        port = datanodePortDetails[i]
        con=rpyc.connect("localhost", port)
        res = con.root.isReady()
        if res:
            print("DATANODE", i, "READY")
            con.close()
        else:
            raise Exception
except:
    print("DATANODE", i,"FAILED")
    #terminating datanodes
    for i in range(config['num_datanodes']):
        datanodes[i].terminate()
    #ternimating namenode
    namenode.terminate()
    exit()

try:
    namenode.wait()
    for datanode in datanodes:
        datanode.wait()
except:
    print("Exiting...")
    namenode.terminate()
    for datanode in datanodes:
        datanode.terminate()

