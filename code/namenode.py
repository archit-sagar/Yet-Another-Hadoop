import sys
import rpyc
from rpyc.utils.server import ThreadedServer
import json
import os
import pickle
import logging
import signal
import uuid
import random
import threading
import time

#python namenode.py its_port config_path
myPort = int(sys.argv[1])
fdata=open(sys.argv[2],"r")
config=json.load(fdata)
fdata.close()


#setting up logger
import logging.config
logging.basicConfig(filename=config['namenode_log_path'], level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('')
logger.info("Namenode Started")

fs_image = {
    'folders': {

    },
    'files': {

    }
}

datanode_state=[]
datanode_blocks={}
for i in range(config["num_datanodes"]):
    datanode_blocks[i]=[]
    datanode_state.append(False)


tempBlockDetails = {}

#for each datanode, maintains num of available blocks {datanode_id: availableNum}
datanodeDetails = {}
datanodePorts = {}

heart_beat_condition=False
running=True


#loading saved fs_image back to memory
checkpointFilePath = os.path.join(config['namenode_checkpoints'], "checkpointFile")
if os.path.exists(checkpointFilePath):
    try:
        with open(checkpointFilePath, 'rb') as f:
            full_metadata = pickle.load(f)
            fs_image=full_metadata[0]
            datanode_blocks=full_metadata[1]
        logger.info("fs_image loaded from checkpoint")
        logger.info(fs_image)
        logger.info("block details loaded from checkpoint")
        logger.info(datanode_blocks)
    except:
        logger.error("Error loading fs_image checkpoint")

class NameNodeService(rpyc.Service):
    def exposed_isReady(self):
        return True
    def exposed_registerDatanode(self, id, portNum, availableNum):
        datanodeDetails[id] = availableNum
        datanodePorts[id] = portNum
        datanode_state[id]=True
        logger.info("Datanode %s registered", id)
        return True

    def exposed_returnPorts(self, id):
        return datanodePorts[id]

    def getFolder(self, absoluteFolderPath): #returns folder dict if exists, else return false
        curFolder = fs_image
        splitPath = list(filter(lambda x: x, absoluteFolderPath.split("/")))
        for folderName in splitPath:
            if not folderName.isalnum():
                return False
            if folderName in curFolder["folders"].keys():
                curFolder = curFolder["folders"][folderName]
            else:
                return False
        return curFolder
    

    def exposed_isFolderExists(self, absoluteFolderPath): #path: separated by / ex: a/b/c
        if self.getFolder(absoluteFolderPath):
            return True
        else: return False

    def getFile(self, absoluteFilePath): #returns file if exists, else return false
        splitPath = list(filter(lambda x: x, absoluteFilePath.split("/")))
        fileName = splitPath[-1]
        folderPath = splitPath[:-1]
        if self.exposed_isFolderExists(str("/").join(folderPath)):
            curFolder = self.getFolder(str("/").join(folderPath))
            if fileName in curFolder["files"].keys():
                return curFolder['files'][fileName]
            else:
                return False
        return False

    def exposed_getFile(self,absoluteFilePath):
        return self.getFile(absoluteFilePath)
    
    def exposed_isFileExists(self, absoluteFilePath): #path: a/b/c/file.txt
        if self.getFile(absoluteFilePath):
            return True
        else: return False

    def exposed_addFolder(self, absoluteFolderPath): #adds a new folder for absoluteFolderPath, return true or false
        splitPath = list(filter(lambda x: x, absoluteFolderPath.split("/")))
        folderName = splitPath[-1]
        if not folderName.isalnum():
            return False
        parentFolderPath = str("/").join(splitPath[:-1])
        folder = self.getFolder(parentFolderPath)
        if folder == False:
            return False
        if folderName in folder['folders'].keys():
            return False
        #adding entry
        folder["folders"][folderName] = {
            "folders": {},
            "files": {},
            "metadata": {
                'createdTime': time.time()
            }
        }
        logger.info("Added new Folder {}".format(absoluteFolderPath))
        return True

    def exposed_addFileEntry(self, absoluteFilePath, meta): #adds file entry, return true. Else return false
        meta = pickle.loads(meta) #objects must be passed as pickled, to preserve their type
        if self.exposed_isFileExists(absoluteFilePath):
            return False
        splitPath = list(filter(lambda x: x, absoluteFilePath.split("/")))
        fileName = splitPath[-1]
        folderPath = splitPath[:-1]
        folder = self.getFolder(str("/").join(folderPath))
        #adding entry
        folder['files'][fileName] = {
            "metadata": meta,
            "blocks": []
        }
        logger.info("Added new file {} ".format(absoluteFilePath))
        return True

    def exposed_allocateBlocks(self):
        randId = uuid.uuid1().int
        #for now, allocating datanodes randomly
        #as of now, I'm not checking if space available or not, will add it later
        n = config['replication_factor']
        rowToSend = [randId] #with port numbers of datanodes
        rowToStore = [randId] #with id of datanodes
        available = {i for i in datanodeDetails.keys()} #later may add other variable called as active datanodes and use it
        for i in range(n):
            ch = random.choice(list(available))
            rowToStore.append(ch)
            available.remove(ch)
            rowToSend.append(datanodePorts[ch])
            # datanodeDetails[ch] -= 1
            #not changing, availableNum as of now, will do later
        tempBlockDetails[randId] = rowToStore
        return rowToSend
        
    def exposed_commitBlocks(self, block_id, status, absoluteFilePath=''):
        #might improve later
        if not status:
            tempBlockDetails.pop(block_id)
            return
        row = tempBlockDetails.pop(block_id)
        file = self.getFile(absoluteFilePath)
        file['blocks'].append(row)
        for i in range(1,len(row)):
            datanode_blocks[row[i]].append(block_id)
        return

    def exposed_start_heartbeat(self):
        global heart_beat_condition
        heart_beat_condition=True

    def exposed_stop_heartbeat(self):
        global heart_beat_condition
        heart_beat_condition=False

    def exposed_find_datanodes_for_block(self,blockid):
        contact_datanodes=[]
        for i in range(config["num_datanodes"]):
            if blockid in datanode_blocks[i]:
                contact_datanodes.append(datanodePorts[i])
        return contact_datanodes

def writeCheckPoints(signal, frame): #writes fs_image into checkpointFile
    full_metadata=[fs_image,datanode_blocks]
    with open(checkpointFilePath, 'wb') as f:
        pickle.dump(full_metadata, f)
        logger.info("Fs_image written into Checkpoint")
    global running
    running=False
    time.sleep(config["sync_period"])
    sys.exit()

def sending_heartbeat():
    while(running):
        if heart_beat_condition:
            for i in range(config["num_datanodes"]):
                try:
                    con = rpyc.connect('localhost',datanodePorts[i])
                    con.root.heartbeat_recieve(datanode_blocks[i])
                    con.close()
                    datanode_state[i] = True
                except:
                    datanode_state[i]=False
        time.sleep(config["sync_period"])


if __name__ == "__main__":
    signal.signal(signal.SIGINT, writeCheckPoints)
    t = ThreadedServer(NameNodeService, port=myPort)
    heartbeat_thread = threading.Thread(target=sending_heartbeat)
    heartbeat_thread.start()
    logger.info("Namenode ThreadedServer started on port %s", myPort)
    t.start()



