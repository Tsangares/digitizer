from digitizer import parse
import sys,os,ROOT,time
from directory import findFile

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

def getConfig(filename):
    pass

def getTimeAxis(binary):
    axis=[]
    for t in range(2**10):
        axis.append(float(t))
    return axis
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
    for group,binaries in files.items():
        start=time.time()
        rootFilePath=os.path.join(outputPath,f'{group}.root')
        rootFile=ROOT.TFile(rootFilePath,'RECREATE')
        rootTree=ROOT.TTree('wfm', f'Waveforms from argonne')
        vector=ROOT.std.vector('double')()
        if not os.path.exists(rootFilePath): os.makedirs(rootFilePath)
        print(f'Writing to {rootFilePath}')
        
        nEvents=0
        for binary in binaries:
            waveforms=parse(binary)
            nEvents=len(waveforms)
            channel=getChannel(binary)
            print(f'Parsed {len(waveforms)} events on channel {channel}')
            rootTree.Branch(f'w{channel}',vector)

            for waveform in waveforms:
                for w in waveform:
                    vector.push_back(w)
                rootTree.Fill()
                vector.clear()

        rootTree.Branch(f't',vector)
        filename=binaries[0].split('/')[-1]
        axis=getTimeAxis(binaries[0].replace(f'/{filename}',''))
        for _ in range(nEvents):
            for t in axis:
                vector.push_back(t)
            rootTree.Fill()
            vector.clear()
            
        rootFile.Write()
        rootFile.Close()
        end=time.time()
        print("Finished writing in {:.02f}s".format(end-start))
    print("done")
