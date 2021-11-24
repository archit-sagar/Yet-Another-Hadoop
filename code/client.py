import sys
import rpyc
import json
import os

fdata=open(sys.argv[1],"r")
config=json.load(fdata)
fdata.close()

basePath = config["fs_path"].rstrip("/") #/myDFS/ -> /myDFS
actualPath = ''

with  open(os.path.join(config["path_to_namenodes"], "ports.json"), 'r') as f:
    namenodePort = json.load(f)["port"] #port (int)

with  open(os.path.join(config["path_to_datanodes"], "ports.json"), 'r') as f:
    datanodePorts = json.load(f) #dict

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
    path = args[0]
    if not path:
        printError("Path is required")
        return
    absPath = getAbsolutePath(path)
    print(absPath)
    if absPath == False:
        printError("Path invalid")
        return
    res = namenode.root.isFolderExists(absPath)
    if res:
        actualPath = absPath
    else:
        printError("Path doesn't exists")
    
def mkdirCommand(args): #creates a dir if not already exists
    path = args[0]
    if not path:
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

    

def exitCommand(args):
    exit()

funcs = {
    'exit': exitCommand,
    'cd': cdCommand,
    'mkdir': mkdirCommand
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
    