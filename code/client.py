import sys
import rpyc
import json
import os

fdata=open(sys.argv[1],"r")
config=json.load(fdata)
fdata.close()

basePath = config["fs_path"] #/myDFS/hello
actualPath = ''

while True:
    print("{}/{}".format(basePath, actualPath))