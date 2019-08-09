from digitizer import parse
import sys,os,ROOT,time
from directory import findFile
from multiprocessing import Pool
from nicholas import findFile,organizeRootFiles,getFinished,getFiles
import math

def getRootRows(rootFile):
    sensorData=rootFile.Get('wfm')
    return sensorData.GetEntries()

def getBinaryRows(original):
    fileSize=os.path.getsize(original)
    rowSize=1030*4
    rows=fileSize/rowSize
    if rows-int(rows) > 0:
        #print("Note remainder",rows-int(rows),rows)
        pass
    return math.floor(rows)


    finished=[f.replace('.root','') for f in rootFiles if f not in unfinished]
    print("All of these files are finished:")
    print("  {:>10s} - {:^14s} - {} ".format("N Events","Run Name","Path Location"))    
    for f in finished:
        rootPath=os.path.join(outputPath,f+'.root')
        rootFile=ROOT.TFile(rootPath)
        print("  {:>10d} - {:^14s} - {} ".format(getRootRows(rootFile),f,rootPath))
    return finished,unfinished

if __name__=='__main__':
    getFinished(getFiles())


