import sys
import rpyc
import json
import os
import time
import pickle

fdata=open(sys.argv[1],"r")
config=json.load(fdata)
fdata.close()

basePath = config["fs_path"].rstrip("/") #/myDFS/ -> /myDFS
actualPath = ''

with  open(os.path.join(config["path_to_namenodes"], "ports.json"), 'r') as f:
    namenodePort = json.load(f)["port"] #port (int)


try:
    namenode = rpyc.connect("localhost", namenodePort)
    if not namenode.root.isReady():
        raise Exception
except:
    print("Namenode connection failed")
    exit()

def printError(msg):
    print("Error: {}".format(msg))

def handleDots(path): #.. -> go back. hello/../home -> /home
    path = path.strip('/')
    splitPath = list(filter(lambda x: x, path.split("/")))
    while '..' in splitPath:
        i = splitPath.index('..')
        if i == 0:
            return False
        splitPath.pop(i-1) #removes previous to ..
        splitPath.pop(i-1) #removes ..
    for name in splitPath:
        if not name.isalnum():
            return False
    return str('/').join(splitPath)

def getAbsolutePath(path):
    if path.startswith(basePath):
        path = path.replace(basePath, '')
    if path.startswith('/'):
        return handleDots(path)
    else:
        fullPath = str('/').join(actualPath.strip("/").split("/") + path.strip("/").split("/"))
        return handleDots(fullPath)
    

def cdCommand(args): #it validates if folder exists, if true then changes the actualPath variable
    global actualPath
    try:
        path = args[0]
        if not path:
            raise Exception
    except:
        printError("Path is required")
        return
    absPath = getAbsolutePath(path)
    if absPath == False:
        printError("Path invalid")
        return
    res = namenode.root.isFolderExists(absPath)
    if res:
        actualPath = absPath
    else:
        printError("Path doesn't exists")
    
def mkdirCommand(args): #creates a dir if not already exists
    try:
        path = args[0]
        if not path:
            raise Exception
    except:
        printError("Folder path is required")
        return
    absPath = getAbsolutePath(path)
    if absPath == False:
        printError("Path invalid")
        return
    if namenode.root.isFolderExists(absPath):
        printError("Folder already exists")
        return
    res = namenode.root.addFolder(absPath)
    if res:
        print('New Folder created')
    else:
        printError("Folder not created")

def putCommand(args): #reads file from source and puts it to destination
    try:
        sourcePath = args[0]
        if not sourcePath:
            raise Exception
    except:
        printError("Source and Destination are required")
        return
    try:
        destPath = args[1]
    except:
        destPath = ''
    
    if not os.path.isfile(sourcePath):
        printError("Source file not found")
        return
    absoluteDestPath = getAbsolutePath(destPath)
    if absoluteDestPath == False:
        printError("Invalid Destination Path")
        return
    if not namenode.root.isFolderExists(absoluteDestPath):
        printError("Destination folder doesn't exists")
        return
    fileName = sourcePath.strip('/').split('/')[-1] #getting last value
    fileSize = os.path.getsize(sourcePath)
    fileTime = time.time() #epoch time, while file is added
    absoluteFilePath = absoluteDestPath + '/' + fileName
    metaData = {
        'size': fileSize,
        'createdTime': fileTime
    }
    res = namenode.root.addFileEntry(absoluteFilePath, pickle.dumps(metaData))
    if not res:
        printError("File already exists in given destination")
        return
    print("File entry added")
    #now start reding file, get allocated blocks, put data into blocks
    with open(sourcePath, 'r') as f:
        blockCount = 0
        startTime = time.time()
        while f.tell() < fileSize: #f.tell() returns current read position
            data = f.read(config['block_size'])
            writeStatus = False
            for x in range(5): #if block storage fails, try 5 more times
                row = namenode.root.allocateBlocks() #[block_id, dn1, dn2, dn3]
                blockId = row[0]
                dn1 = row[1]
                nextDns = row[2:]
                try:
                    con = rpyc.connect('localhost', dn1)
                    res = con.root.recursiveWrite(blockId, data, nextDns)
                    con.close()
                    if res:
                        namenode.root.commitBlocks(blockId, True, absoluteFilePath)
                        writeStatus = True
                        blockCount+=1
                        break
                    else:
                        namenode.root.commitBlocks(blockId, False)
                except Exception as e:
                    print(e)
                    namenode.root.commitBlocks(blockId, False)
            if not writeStatus:
                #failed to write even after 5 attempts
                #remove the half written file using rm command
                #show error
                printError("Write Failed in between")
                return
        endTime = time.time()
        repFactor = config['replication_factor']
        print("{} * {} = {} Blocks written in {:0.2f}s".format(blockCount, repFactor, blockCount*repFactor,endTime-startTime))

def catCommand(args):
    path=args[0]
    fileLoc=path.split('/')
    filename=fileLoc[-1]
    filePath=fileLoc[0:-1]
    absPath = getAbsolutePath(str('/').join(filePath))
    absPath=absPath+'/'+filename
    if absPath == False:
        printError("Path invalid")
        return
    res = namenode.root.isFileExists(absPath)
    if not res:
        printError("File doesn't exists")
        return
    try:
        if(args[1]=='>'):
            destFile=args[2]
            with open(destFile,'w') as f:
                fileContent=namenode.root.getFile(absPath)
                blocks=fileContent['blocks']
                for i in blocks:
                    blockID=i[0]
                    dn1=i[1:]
                    for i in dn1:
                        con=rpyc.connect('localhost', namenode.root.returnPorts(i))
                        res=con.root.read(blockID)
                        con.close()
                        if res:
                            break
                    f.write(res)
            f.close()
            return
    except:
        print("Printing to terminal...")
    fileContent=namenode.root.getFile(absPath)
    blocks=fileContent['blocks']
    for i in blocks:
        blockID=i[0]
        dn1=i[1:]
        for i in dn1:
            con=rpyc.connect('localhost', namenode.root.returnPorts(i))
            res=con.root.read(blockID)
            con.close()
            if res:
                break
        print(res,end='')   
    print()         
    

def rmCommand(args): #deletes the specified file
    pass

def rmdirCommand(args): #deletes the specified folder
    pass

def exitCommand(args):
    print('exiting...')
    exit()

funcs = {
    'exit': exitCommand,
    'cd': cdCommand,
    'mkdir': mkdirCommand,
    'put': putCommand,
    'cat': catCommand,
    'rm': rmCommand,
    'rmdir':rmdirCommand
}

def default(args):
    printError("Command not found")

def commandHandler(command, args): #command is string, args is list
    func = funcs.get(command, default)
    func(args)

def parseCommand(s):
    splits = s.split(" ")
    cmd = splits[0]
    args = splits[1:]
    return cmd, args

while True:
    print("user@{}/{}$:".format(basePath, actualPath), end=' ')
    inpLine = input()
    cmd, args = parseCommand(inpLine)
    if not cmd:
        continue
    commandHandler(cmd, args)
    