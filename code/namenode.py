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

fs_image = {
    'folders': {

    },
    'files': {

    }
}

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

    def getFolder(self, absoluteFolderPath): #returns folder dict if exists, else return false
        curFolder = fs_image
        splitPath = absoluteFolderPath.split("/")
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
    
    def exposed_isFileExists(self, absoluteFilePath): #path: a/b/c/file.txt
        splitPath = absoluteFilePath.split("/")
        fileName = splitPath[-1]
        folderPath = splitPath[:-1]
        if self.exposed_isFolderExists(str("/").join(folderPath)):
            curFolder = fs_image
            for folderName in folderPath:
                curFolder = curFolder["folders"][folderName]
            if fileName in curFolder["files"].keys():
                return True
            else:
                return False
        return False

    def exposed_addFolder(self, absoluteFolderPath, folderName): #adds a new folder at absoluteFolderPath, return true or false
        folder = self.getFolder(absoluteFolderPath)
        if folder == False:
            return False
        if folderName in folder['folders'].keys():
            return False
        #adding entry
        folder["folders"][folderName] = {
            "folders": {},
            "files": {}
        }
        logger.info("Added new Folder {} at {}".format(folderName, absoluteFolderPath))
        return True

    def exposed_addFileEntry(self, absoluteFilePath, meta): #adds file entry, return true. Else return false
        if self.exposed_isFileExists(absoluteFilePath):
            return False
        splitPath = absoluteFilePath.split("/")
        fileName = splitPath[-1]
        folderPath = splitPath[:-1]
        folder = self.getFolder(str("/").join(folderPath))
        #adding entry
        folder[fileName] = {
            "metadata": meta.copy(),
            "blocks": []
        }
        logger.info("Added new file {} ".format(absoluteFilePath))
        return True

if __name__ == "__main__":
    t = ThreadedServer(NameNodeService, port=myPort)
    logger.info("Namenode ThreadedServer started on port %s", myPort)
    t.start()



