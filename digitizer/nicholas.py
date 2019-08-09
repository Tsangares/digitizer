'''
A strange parser for the caen digitizer bianry data.
Its main pupose for the caen x-ray DAQ.
'''
from digitizer import parse
import sys,os,ROOT,time,argparse
from directory import findFile
from multiprocessing import Pool
from itertools import repeat
import math

def getRootRows(rootFile):
    sensorData=rootFile.Get('wfm')
    return sensorData.GetEntries()

def getBinaryRows(original):
    fileSize=os.path.getsize(original)
    rowSize=1030*4
    rows=fileSize/rowSize
    return math.floor(rows)

def getFinished(files,folderName):
    rootFiles=getRootFiles(folderName)
    folderPath,outputPath=getPaths(folderName)
    unfinished=[]
    finished=[]
    print("{:18s}:               {:10s} : {:10s}".format("","rootRows", "binaryRows"))
    for name in getGroups():
        rootPath=[f for f in rootFiles if name == f.replace('.root','')]
        if len(rootPath) == 0:
            print("{:18s}: Omitted".format(name))
            unfinished.append(name)
            continue
        rootPath=rootPath[0]
        originalFolder=files[name]
        rootFile=ROOT.TFile(os.path.join(outputPath,rootPath))
        try:
            binaryRows=getBinaryRows(originalFolder[0])
            rootRows=getRootRows(rootFile)
        except Exception as e:
            print("{:18s}: Error Corrupt {:10d} : {:10d}".format(rootPath,rootRows,binaryRows))
        if binaryRows > rootRows:
            print("{:18s}: Error TooFew  {:10d} : {:10d}".format(rootPath,rootRows,binaryRows))
            unfinished.append(name)
        elif  binaryRows < rootRows:
            print("{:18s}: Error TooMany {:10d} : {:10d}".format(rootPath,rootRows,binaryRows))
            unfinished.append(name)
        else:
            print("{:18s}: Done          {:10d} : {:10d}".format(rootPath,rootRows,binaryRows))
            finished.append(name)
    return finished,unfinished

def getChannel(filename):
    try:
        return int(filename.split('wave_')[1].split('.dat')[0])
    except:
        return None

def organizeRootFiles(files):
    output={}
    for f in files:
        folderName=f.split('/')[-3]
        try: output[folderName]
        except: output[folderName]=[]
        output[folderName].append((f,getChannel(f)))
    for key,vals in output.items():
        vals=sorted(vals,key=lambda a: a[1])
        output[key]=[v[0] for v in vals]
    return output

frequencies=[5000,2500,1000,750]
def getTimeAxis(binary=None):
    if binary is not None:
        try:
            config=os.path.join("/".join(binary.split("/")[:-2]),'meta/config.txt')
            freq=None
            with open(config,'r') as f:
                line=[l for l in f.read().split('\n') if 'DRS4_FREQUENCY' in l and '#' not in l][0]
                drs=int(line.split(' ')[-1])
                freq=frequencies[drs]
        except:
            freq=1
    else:
        freq=1    
    axis=[]
    for t in range(2**10):
        axis.append(float(t)/freq)
    return axis

class Page:
    def __init__(self,filename,pageSize=1000):
        self.filename=filename
        self.pageSize=pageSize
        self.index=0
        self.total=0
        self.complete=False
    def next(self,step=True):
        myOffset=self.index*self.pageSize
        data=parse(self.filename,limit=self.pageSize,offset=myOffset,verbose=False)
        if step:
            self.index+=1
            self.total+=len(data)
            if len(data) < self.pageSize:
                self.complete=True
        return data

    def getProgress(self):
        return self.len()/float(self.metaLength())
    
    def metaLength(self):
        return getBinaryRows(self.filename)
    def totalLength(self):
        return len(parse(self.filename,limit=-1,verbose=True))
    def len(self):
        return self.total
    
    def completed(self):
        if self.complete: return True
        return len(self.next(step=False))==0

    def __repr__(self):
        return "{1} channel {0}".format(getChannel(self.filename),self.filename.split('/')[-3])
    def __str__(self):
        return "{1} channel {0}".format(getChannel(self.filename),self.filename.split('/')[-3])
    

def caenToRoot(group,binaries,outputPath,debug=False,index=0,pages=None,filesize=1_000_000_000):

    rootFolderPath=os.path.join(outputPath,group)
    rootFilePath=os.path.join(rootFolderPath,f'{group}_{index:03d}.root')
    if not os.path.exists(rootFolderPath): os.makedirs(rootFolderPath)
    
    rootFile=ROOT.TFile(rootFilePath,'RECREATE')
    rootTree=ROOT.TTree('wfm', f'Waveforms from argonne')
    print(f'Writing to {rootFilePath}')

    if pages is None:
        pages={getChannel(binary): Page(binary) for binary in binaries}
    vectors={channel: ROOT.std.vector('double')() for channel,page in pages.items()}
    for channel,vector in vectors.items():
        rootTree.Branch(f'w{channel}',vector)
        
    timeVector=ROOT.std.vector('double')()
    indexVector=ROOT.std.vector('double')()
    rootTree.Branch(f't',timeVector)
    rootTree.Branch(f'index',indexVector)
    axis=getTimeAxis(binaries[0])
    index_axis=getTimeAxis()
    if debug:
        print("=== Debug: Calculating Number of Events ===")
        for chan,page in pages.items():
            meta=page.metaLength()
            total=page.totalLength()
            print("{:15s} - Meta {:10d} - Total {:10d}".format(str(page),meta,total))
    while not pages[0].completed():
        startTime=time.time()
        dataset={channel: page.next() for channel,page in pages.items()}
        events=len(dataset[0])
        channels=[chan for chan,_ in dataset.items()]
        for eventIndex in range(events):
            eventSize=len(dataset[0][eventIndex])
            
            for t in axis: timeVector.push_back(t)
            for i in index_axis: indexVector.push_back(i)
            for pointIndex in range(eventSize):
                for chan in channels:
                    w=dataset[chan][eventIndex][pointIndex]
                    vectors[chan].push_back(w)
                    
            rootTree.Fill()
            for chan,vector in vectors.items():
                vector.clear()
            timeVector.clear()
            indexVector.clear()
        endTime=time.time()
        rootFileSize=os.path.getsize(rootFilePath)
        MB=rootFileSize//10**6
        progress=pages[0].getProgress()
        print(f"{group}: Processed {pages[0].len()} events on {len(channels)} channels ({progress*100:.02f}%) in {endTime-startTime:.02f}s, a total of {MB} MB")
        if rootFileSize > filesize: break
    
    rootFile.Write()
    rootFile.Close()
    if not pages[0].completed():
        index+=1
        caenToRoot(group,binaries,outputPath,debug=debug,index=index,pages=pages,filesize=filesize)
    else:
        print("============== Completed {} ==============".format(group))

def getGroups():
    return [group for group,_ in getFiles().items()]
    
def getFiles():
    condition=lambda a: '.dat' in a and 'wave_' in a
    cwd=os.getcwd()
    folderName='argonne_july_28_2019'
    if len(sys.argv)==2: folderName=sys.argv[1]
    folderPath=os.path.join(cwd,folderName)
    outputPath=os.path.join(cwd,'argonne_july_28_2019_root')
    try: os.mkdir(outputPath)
    except: pass
    files=findFile(folderPath,condition)
    files=organizeRootFiles(files)
    return files

def getRootFiles(folderName):
    rootFiles=[]
    _,rootFolder=getPaths(folderName)
    if os.path.isdir(rootFolder):
        rootFiles=[f for f in os.listdir(rootFolder) if '.root' in f and ('run' in f or '1564' in f or 'beam' in f)]
    return rootFiles

def getPaths(folderName,path=None):
    if path is None:
        path=os.getcwd()
    folderPath=os.path.join(cwd,folderName)
    outputPath=os.path.join(cwd,f'{folderName}_root')
    return folderPath,outputPath

if __name__=='__main__':
    #Condition finds files with these attributes 
    condition=lambda a: '.dat' in a and 'wave_' in a
    cwd=os.getcwd()
    DESC="Command line utility to parse caen data into root."
    parser=argparse.ArgumentParser(description=DESC)
    parser.add_argument('folderName',type=str,nargs='?',help="The name of the folder with all of the caen binary data.",default='argonne_july_28_2019')
    #parser.add_argument('fileSize',type=int,nargs='?',help="The chunked size of root files.",default=1_000_000_000) #Not going into the function
    args=parser.parse_args()
    folderName=args.folderName
    folderPath,outputPath=getPaths(folderName)
    try: os.mkdir(outputPath)
    except: pass
    files=findFile(folderPath,condition)
    files=organizeRootFiles(files)
    #finished,unfinished=getFinished(files,folderName)
    #print(f'finished:unfinished <=> {len(finished)}:{len(unfinished)}\n')
    #print("======= Preliminaries Completed =======")
    
    def filter(group):
        #if len(sys.argv) == 1: return False
        if 'water_run_' in group: return False
        return True
        if 'beam_run37' == group:
            return True
        return False
    #files = {group: _f for group,_f in files.items() if group in unfinished and filter(group)}
    groups=[]
    binaries=[]
    for group,binary in files.items():
        groups.append(group)
        binaries.append(binary)
    with Pool(10) as p:
        p.starmap(caenToRoot,zip(groups,binaries,repeat(outputPath)))

