import sys
import rpyc
from rpyc.utils.server import ThreadedServer
import json
import os
import pickle
import logging

#python namenode.py its_port config_path
myPort = int(sys.argv[1])
fdata=open(sys.argv[2],"r")
config=json.load(fdata)
fdata.close()


#setting up logger
logging.basicConfig(filename=config['namenode_log_path'], level=logging.DEBUG, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger()
logger.info("Namenode Started")

fs_image = {}

#for each datanode, maintains num of available blocks {datanode_id: availableNum}
datanodeDetails = {}
datanodePorts = {}

#loading saved fs_image back to memory
checkpointFilePath = os.path.join(config['namenode_checkpoints'], "checkpointFile")
if os.path.exists(checkpointFilePath):
    with open(checkpointFilePath, 'rb') as f:
        fs_image = pickle.load(f)
    logger.info("fs_image loaded from checkpoint")

class NameNodeService(rpyc.Service):
    def exposed_isReady(self):
        return True
    def exposed_registerDatanode(self, id, portNum, availableNum):
        datanodeDetails[id] = availableNum
        datanodePorts[id] = portNum
        logger.debug("Datanode %s registered", id)
        return True


if __name__ == "__main__":
    t = ThreadedServer(NameNodeService, port=myPort)
    logger.info("Namenode ThreadedServer started on port %s", myPort)
    t.start()



