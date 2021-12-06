import sys
import rpyc
import json
import os
import time
import pickle
from tabulate import tabulate
import math

fdata=open(sys.argv[1],"r")
config=json.load(fdata)
fdata.close()

basePath = config["fs_path"].rstrip("/") #/myDFS/ -> /myDFS
actualPath = ''

def get_namenode():
    with  open(os.path.join(config["path_to_namenodes"], "ports.json"), 'r') as f:
        namenodePort = json.load(f)["port"] #port (int)


    try:
        namenode = rpyc.connect("localhost", namenodePort)
        if not namenode.root.isReady():
            raise Exception
        return namenode
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
    res = get_namenode().root.isFolderExists(absPath)
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
    if get_namenode().root.isFolderExists(absPath):
        printError("Folder already exists")
        return
    res = get_namenode().root.addFolder(absPath)
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
    if not get_namenode().root.isFolderExists(absoluteDestPath):
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
    res,mes = get_namenode().root.addFileEntry(absoluteFilePath, pickle.dumps(metaData))
    if not res:
        if mes == 1:
            printError("File already exists in given destination")
        elif mes == 2:
            printError("Datanodes unavailable, try later")
        elif mes == 3:
            printError("No space available")
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
                row = get_namenode().root.allocateBlocks() #[block_id, dn1, dn2, dn3]
                if row == False:
                   break
                blockId = row[0]
                dn1 = row[1]
                nextDns = row[2:]
                try:
                    con = rpyc.connect('localhost', dn1)
                    res = con.root.recursiveWrite(blockId, data, nextDns)
                    con.close()
                    if res:
                        get_namenode().root.commitBlocks(blockId, True, absoluteFilePath)
                        writeStatus = True
                        blockCount+=1
                        break
                    else:
                        get_namenode().root.commitBlocks(blockId, False)
                except Exception as e:
                    print(e)
                    get_namenode().root.commitBlocks(blockId, False)
            if not writeStatus:
                #failed to write even after 5 attempts or in between while allocating
                #remove the half written file using rm command
                get_namenode().root.removeFile(absoluteFilePath)
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
    res = get_namenode().root.isFileExists(absPath)
    if not res:
        printError("File doesn't exists")
        return
    try:
        if(args[1]=='>'):
            destFile=args[2]
            with open(destFile,'w') as f:
                fileContent=get_namenode().root.getFile(absPath)
                blocks=fileContent['blocks']
                for i in blocks:
                    blockID=i[0]
                    dn1=i[1:]
                    readStatus = False
                    for i in dn1:
                        try:
                            con=rpyc.connect('localhost', get_namenode().root.returnPorts(i))
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
                        print()
                        printError("Read failed. Datanode unavailable")
            return
    except:
        # print("Printing to terminal...")
        fileContent=get_namenode().root.getFile(absPath)
        blocks=fileContent['blocks']
        for i in blocks:
            blockID=i[0]
            dn1=i[1:]
            readStatus = False
            for i in dn1:
                try:
                    con=rpyc.connect('localhost', get_namenode().root.returnPorts(i))
                    res=con.root.read(blockID)
                    con.close()
                    if res != False:
                        readStatus = True
                        break
                except:
                    continue
            if readStatus:
                print(res,end='') 
            else:
                print()
                printError("Read failed. Datanode unavailable")
                return
        print()
    
        
def sizeConvert(size):
    if size==0:
        return "0 B"
    sizeNames = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024,i)
    s = round(size/p,2)
    return ("{0} {1}".format(s, sizeNames[i]))

def mapper(fnames):
    contents=[]
    for i in fnames:
        if (i[0]=='folder'):
            contents.append((i[1],i[2],'<DIR>'))
        if (i[0]=='files'):
            contents.append((i[1],i[3],sizeConvert(i[2])))
    return contents

def lsCommand(args):
    fnames=list(get_namenode().root.exposed_getContents(actualPath))
    filedetails=mapper(fnames)
    try:
        if args[0]=='-d':
                print(tabulate(filedetails,headers=['Name','CreatedTime','Size']))
    except:
        for i in fnames:
            print(i[1])
    print()

        

def rmCommand(args): #deletes the specified file
    try:
        path = args[0]
        if not path:
            raise Exception
    except:
        printError("File to be deleted needs to be specified")
        return
    fileLoc=path.split('/')
    filename=fileLoc[-1]
    parentFolderLoc=fileLoc[0:-1]
    parentFolderPath = getAbsolutePath(str('/').join(parentFolderLoc))
    if parentFolderPath==False:
        printError("Path invalid")
        return
    filePath=parentFolderPath+'/'+filename
    check = get_namenode().root.isFileExists(filePath)
    if not check:
        printError("File doesn't exists")
        return
    res=get_namenode().root.removeFile(filePath)
    if res:
        print('File successfully deleted')
    else:
        printError("File deletion unsuccessful")

def rmdirCommand(args): #deletes the specified folder
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
    if get_namenode().root.isFolderExists(absPath):
        res = get_namenode().root.removeFolder(absPath)
    else:
        printError("Folder does not exist")
        return
    
    if res==1:
        print('Folder successfully deleted')
    else:
        if res==2:
            print('Couldnt access folder')
        if res==3:
            print('Folder is not empty')
        printError("Folder not removed")

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
    'rmdir':rmdirCommand,
    'ls': lsCommand
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
    