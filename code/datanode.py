import sys
import rpyc
from rpyc.utils.server import ThreadedServer
import json
import os
import logging

#python datanode.py datanode_id its_port config_path
myId = int(sys.argv[1])
myPort = int(sys.argv[2])
fdata=open(sys.argv[3],"r")
config=json.load(fdata)
fdata.close()
with  open(os.path.join(config["path_to_namenodes"], "ports.json"), 'r') as f:
    namenodePort = json.load(f)["port"]

#setting up logger
logging.basicConfig(filename=os.path.join(config['datanode_log_path'], str(myId)+".txt"), level=logging.DEBUG, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger()
logger.info("Datanode Started")

myDatanodePath = os.path.join(config["path_to_datanodes"], str(myId))
#maintaining list of blocks isn't necessary. Might be removed later
blockList = os.listdir(myDatanodePath) #assuming folder contains only blocks
logger.debug(blockList)

availableBlocksNum = config["datanode_size"] - len(blockList)
logger.info("Available blocks: %s", availableBlocksNum)

con=rpyc.connect("localhost", namenodePort)
res = con.root.registerDatanode(myId, myPort, availableBlocksNum)
if res:
    logger.info("Registered with Namenode")
con.close()

class DataNodeService(rpyc.Service):
    def exposed_isReady(self):
        return True


if __name__ == "__main__":
    t = ThreadedServer(DataNodeService, port=myPort)
    logger.info("Datanode ThreadedServer started on port %s", myPort)
    t.start()