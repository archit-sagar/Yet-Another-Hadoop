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
import math

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

datanode_blocks={}
for i in range(config["num_datanodes"]):
    datanode_blocks[i]=[]


tempBlockDetails = {}

#for each datanode, maintains num of available blocks {datanode_id: availableNum}
datanodeDetails = {}
datanodePorts = {}

heart_beat_condition=False

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
    
    def exposed_getContents(self, absoluteFolderPath):
        folder=self.getFolder(absoluteFolderPath)
<<<<<<< HEAD
        name=[]
        for i in folder['folders']:
            name.append(('folder', i, folder['folders'][i]['metadata']))
        for i in folder['files']:
            name.append(('file', i, folder['files'][i]['metadata']))
        return name
=======
        folder_names=[]
        if ('folders' in folder.keys()):
            for i in folder['folders']:
                folder_time= time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(folder['folders'][i]['metadata']['createdTime']))
                folder_names.append(('folder',i,folder_time))
        if ('files' in folder.keys()):
            for i in folder['files']:
                file_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(folder['files'][i]['metadata']['createdTime']))
                folder_names.append(('files',i,folder['files'][i]['metadata']['size'],file_time))
        return folder_names
>>>>>>> origin/latest-branch

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
            return (False, 1) #file already exists
        n = config['replication_factor']
        logger.info(datanodeDetails)
        if len(datanodeDetails.keys()) < n:
            return (False, 2) #Min datanodes unavailable, try later
        nonZeroDatanodes = {k:v for (k,v) in datanodeDetails.items() if v>0}
        if len(nonZeroDatanodes.keys()) < n:
            return (False,3) #No space available
        reqBlocks = math.ceil(meta['size']/config['block_size'])
        minBlocksPerNode = math.ceil((reqBlocks*n)/len(nonZeroDatanodes.keys()))
        #checks if all available nodes have minimum blocks available
        for v in nonZeroDatanodes.values():
            if v < minBlocksPerNode:
                return (False,3) #no space available
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
        return (True,1)

    def exposed_allocateBlocks(self):
        #for now, allocating datanodes randomly
        #this checking will be unnesessary in most cases
        nonZeroDatanodes = {k:v for (k,v) in datanodeDetails.items() if v>0}
        n = config['replication_factor']
        if len(nonZeroDatanodes.keys()) < n:
            return False #Min datanodes unavailable, try later
        randId = uuid.uuid1().int
        rowToSend = [randId] #with port numbers of datanodes
        rowToStore = [randId] #with id of datanodes
        #nodes are choosen based on available blocks (more available blocks, gets choosen first)
        #shuffle because, to avoid same node being primary
        choosenDnodes = [i for (i,j) in sorted(nonZeroDatanodes.items(), key=lambda item: item[1], reverse=True)[:n]]
        random.shuffle(choosenDnodes)
        logger.info('allocated nodes {}'.format(choosenDnodes))
        for i in choosenDnodes:
            rowToStore.append(i)
            rowToSend.append(datanodePorts[i])
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
            datanodeDetails[row[i]] -= 1
        return

    def folderRemovable(self,folder):
        if folder == False:
            return 2
        if (folder['folders']=={}) and (folder["files"]=={}):
            return 1
        else:
            return 3
    
    def exposed_removeFolder(self, absoluteFolderPath): #removes a folder for absoluteFolderPath, return true is successful deleteion or else false
        folder = self.getFolder(absoluteFolderPath)
        cond=self.folderRemovable(folder)
        if cond==1:
            splitPath = list(filter(lambda x: x, absoluteFolderPath.split("/")))
            folderName = splitPath[-1]
            parentFolderPath = str("/").join(splitPath[:-1])
            parentFolder = self.getFolder(parentFolderPath)
            parentFolder["folders"].pop(folderName)
            return 1
        else:
            return cond

    def exposed_removeFile(self, absoluteFilePath): #removes a folder for absoluteFolderPath, return true is successful deleteion or else false
        fileData=self.getFile(absoluteFilePath)
        blocks=fileData['blocks']
        try:
            for i in blocks:
                blockID=i[0]
                dns=i[1:]
                for i in dns:  
                    datanode_blocks[i].remove(blockID)
            splitPath = list(filter(lambda x: x, absoluteFilePath.split("/")))
            fileName = splitPath[-1]
            parentFolderPath = str("/").join(splitPath[:-1])
            parentFolder = self.getFolder(parentFolderPath)
            parentFolder["files"].pop(fileName)
            return True
        except:
            return False

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
    sys.exit()

def sending_heartbeat():
    while(True):
        if heart_beat_condition:
            for i in range(config["num_datanodes"]):
                try:
                    con = rpyc.connect('localhost',datanodePorts[i])
                    con.root.heartbeat_recieve(datanode_blocks[i])
                    con.close()
                    datanodeDetails[i] = config['datanode_size'] - len(datanode_blocks[i]) #availableNum
                except:
                    if i in datanodeDetails:
                        datanodeDetails.pop(i)
        time.sleep(config["sync_period"])


if __name__ == "__main__":
    signal.signal(signal.SIGINT, writeCheckPoints)
    t = ThreadedServer(NameNodeService, port=myPort)
    heartbeat_thread = threading.Thread(target=sending_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()
    logger.info("Namenode ThreadedServer started on port %s", myPort)
    t.start()



