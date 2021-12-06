import time
import rpyc
import json
import argparse
import os
import uuid
import subprocess
import logging


parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', required=True)
parser.add_argument('-o', '--output', required=True)
parser.add_argument('-c', '--config', required=True)
parser.add_argument('-m', '--mapper', required=True)
parser.add_argument('-r', '--reducer', required=True)

args = parser.parse_args()
inPath = args.input
outPath = args.output
configPath = args.config
mapperText = args.mapper
mapperPath = mapperText.split('.py')[0]+'.py'
mapperArgs = [i for i in mapperText.split('.py')[1].split(' ') if i]
reducerText = args.reducer
reducerPath = reducerText.split('.py')[0]+'.py'
reducerArgs = [i for i in reducerText.split('.py')[1].split(' ') if i]

#validating argument values
def printError(msg):
    print("Error: {}".format(msg))

if not os.path.isfile(configPath):
    printError("Config file not found")
    exit()

if  not os.path.isdir(outPath):
    printError("Output directory not found")
    exit()

if not os.path.isfile(mapperPath.split(' ')[0]):
    printError("Mapper program not found")
    exit()

if not os.path.isfile(reducerPath.split(' ')[0]):
    printError("Reducer program not found")
    exit()

fdata=open(configPath,"r")
config=json.load(fdata)
fdata.close()

logging.basicConfig(filename=os.path.join(outPath, "output.log"), level=logging.DEBUG, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger()
logger.info("Mapred Started")

def getActualPath(path):
    basePath = config["fs_path"].rstrip("/") #/myDFS/ -> /myDFS
    if path.startswith(basePath):
        return path.replace(basePath, '').strip('/')
    else:
        return False

actualFilePath = getActualPath(inPath)
if not actualFilePath:
    printError("Invalid input path")
    logger.error("Invalid input path")
    exit()

with  open(os.path.join(config["path_to_namenodes"], "ports.json"), 'r') as f:
    namenodePort = json.load(f)["port"] #port (int)


try:
    namenode = rpyc.connect("localhost", namenodePort)
    if not namenode.root.isReady():
        raise Exception
    logger.info(f"connected to namenode at port:{namenodePort}")
except:
    print("Namenode connection failed")
    logger.error("Namenode connection failed")
    exit()

#check if input file exists
if not namenode.root.isFileExists(actualFilePath):
    printError("Input file not found")
    logger.error("Input file not found")
    exit()

def storeFileToTempFile(tempFilePath, absFilePath):
    logger.info("Attempting to store hdfs file into local temp file")
    with open(tempFilePath,'w') as f:
        fileContent=namenode.root.getFile(absFilePath)
        blocks=fileContent['blocks']
        for i in blocks:
            blockID=i[0]
            dn1=i[1:]
            readStatus = False
            res = ''
            for p in dn1:
                try:
                    con=rpyc.connect('localhost', namenode.root.returnPorts(p))
                    res=con.root.read(blockID)
                    con.close()
                    if res != False:
                        readStatus = True
                        break
                except:
                    continue
            if readStatus:
                f.write(res)
            else:
                logger.error("File storing failed")
                return False
        f.close()
        return True


#store the file from dfs to local in a temp file
tempFileName = 'temp' + uuid.uuid1().hex
tempFilePath = os.path.join(outPath, tempFileName)
logger.info(f"temp file path: {tempFilePath}")
try:
    startTime = time.time()
    if not storeFileToTempFile(tempFilePath, actualFilePath):
        printError("Failed reading file")
        exit()

    logger.info("creating mapper process")
    mapper = subprocess.Popen(args=[config['python_command'], mapperPath, *mapperArgs], stdout=subprocess.PIPE, stdin=open(tempFilePath, 'r'))
    out, err = mapper.communicate()
    if err or mapper.returncode:
        printError(err)
        logger.error(f"mapper process failed. {err}")
        raise Exception
    outText = out.decode('utf-8')

    #sorting the map results
    outText = str('\n').join(sorted(outText.splitlines()))
    #writing map results into temp file
    with open(tempFilePath, 'w') as f:
        f.write(outText)
        f.close()
    
    logger.info("mapper output written successfully to temp file")
    mapper.terminate()

    logger.info('creating reducer process')
    reducer = subprocess.Popen(args=[config['python_command'], reducerPath, *reducerArgs], stdout=subprocess.PIPE, stdin=open(tempFilePath, 'r'))
    out, err = reducer.communicate()
    if err or reducer.returncode:
        logger.error(f"reducer process failed. {err}")
        raise Exception
    outText = out
    

    #writing reducer output to output.txt in destination
    outputFilePath = os.path.join(outPath, 'output.txt')
    with open(outputFilePath, 'wb') as f:
        f.write(outText)

    endTime = time.time()
    exeTime = endTime - startTime

    logger.info(f"reducer output written successfully into output file {outputFilePath}")
    print("Task Executed successfully in {:0.2f}s. Output: {}".format(exeTime,outputFilePath))

except Exception as e:
    printError("Task Failed")
    logger.error("Task Failed due to internal failures")
    print(e)
finally:
    mapper.terminate()
    reducer.terminate()
    os.unlink(tempFilePath)
    logger.info(f"Exiting... Removed temp file {tempFilePath}")
    exit()
    

