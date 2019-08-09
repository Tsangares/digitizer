from digitizer import parse
import sys,os,ROOT,time
from directory import findFile
from multiprocessing import Pool
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

def getFinished():
    condition=lambda a: '.dat' in a and 'wave_' in a
    cwd=os.getcwd()
    folderName='argonne_july_28_2019'
    folderPath=os.path.join(cwd,folderName)
    outputPath=os.path.join(cwd,'root')
    files=findFile(folderPath,condition)
    files=organizeRootFiles(files)
    unfinished=[]
    rootFiles=os.listdir(outputPath)
    for rootPath in rootFiles:
        name=rootPath.replace('.root','')
        #print(f"On {name}")
        originalFolder=files[name]
        rootFile=ROOT.TFile(os.path.join(outputPath,rootPath))
        try:
            binaryRows=getBinaryRows(originalFolder[0])
            rootRows=getRootRows(rootFile)
        except Exception as e:
            print(e)
            print(rootPath)
        if binaryRows != rootRows:
            print(f"{rootPath}: Unfinished, length {rootRows}; {binaryRows}")
            unfinished.append(name)
    finished=[f.replace('.root','') for f in rootFiles if f not in unfinished]
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
def getTimeAxis(binary):
    config=os.path.join("/".join(binary.split("/")[:-2]),'meta/config.txt')
    freq=None
    with open(config,'r') as f:
        line=[l for l in f.read().split('\n') if 'DRS4_FREQUENCY' in l and '#' not in l][0]
        drs=int(line.split(' ')[-1])
        freq=frequencies[drs]
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
    def next(self,step=True):
        data=parse(self.filename,limit=self.pageSize,offset=self.index*self.pageSize)
        if step:
            self.index+=1
            self.total+=len(data)
        return data

    def len(self):
        return self.total
    
    def completed(self):
        return len(self.next(step=False))==0

    def __repr__(self):
        return "{1} channel {0}".format(getChannel(self.filename),self.filename.split('/')[-3])
    

def caenToRoot(group,binaries):
    rootFilePath=os.path.join(outputPath,f'{group}.root')
    rootFile=ROOT.TFile(rootFilePath,'RECREATE')
    rootTree=ROOT.TTree('wfm', f'Waveforms from argonne')
    if not os.path.exists(rootFilePath): os.makedirs(rootFilePath)
    print(f'Writing to {rootFilePath}')
    
    pages={getChannel(binary): Page(binary) for binary in binaries}
    vectors={channel: ROOT.std.vector('double')() for channel,page in pages.items()}
    for channel,vector in vectors.items():
        rootTree.Branch(f'w{channel}',vector)
        
    timeVector=ROOT.std.vector('double')()
    rootTree.Branch(f't',timeVector)
    axis=getTimeAxis(binaries[0])
    
    while not pages[0].completed():
        startTime=time.time()
        dataset={channel: page.next() for channel,page in pages.items()}
        events=len(dataset[0])
        channels=[chan for chan,_ in dataset.items()]
        for eventIndex in range(events):
            eventSize=len(dataset[0][eventIndex])
            
            for t in axis: timeVector.push_back(t)
            for pointIndex in range(eventSize):
                for chan in channels:
                    w=dataset[chan][eventIndex][pointIndex]
                    vectors[chan].push_back(w)
                    
            rootTree.Fill()
            for chan,vector in vectors.items():
                vector.clear()
            timeVector.clear()
        endTime=time.time()
        print(f"{group}: Processed {pages[0].len()} events on {len(channels)} channels in {endTime-startTime:.02f}s")
        
    rootFile.Write()
    rootFile.Close()
if __name__=='__main__':
    condition=lambda a: '.dat' in a and 'wave_' in a
    cwd=os.getcwd()
    folderName='argonne_july_28_2019'
    if len(sys.argv)==2: folderName=sys.argv[1]
    folderPath=os.path.join(cwd,folderName)
    outputPath=os.path.join(cwd,'root')
    try: os.mkdir(outputPath)
    except: pass
    files=findFile(folderPath,condition)
    files=organizeRootFiles(files)
    finished,unfinished=getFinished()
    files = {group: _f for group,_f in files.items() if group in unfinished}
    if False:
        choice=list(files.items())[7]
        caenToRoot(choice[0],choice[1])
        quit()
        
    with Pool(10) as p:
        p.starmap(caenToRoot,list(files.items()))

