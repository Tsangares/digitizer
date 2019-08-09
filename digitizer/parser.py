#!/usr/bin/env python

from numpy import fromfile,dtype

def parseBinary(filename,limit=-1,offset=0,truncate=0,verbose=False):
    with open(filename, mode='rb') as f:
        lenEvent=1030 #Length of an event
        lenHeader=6   #Length of the header info
        myOffset=offset*lenEvent*4
        if limit==-1:
            myCount=-1
        else:
            myCount=limit*lenEvent
        if verbose:
            print("Seeked to position:",f.seek(myOffset))
        else:
            f.seek(myOffset)
        rawBinary=fromfile(f,dtype=dtype("<f"),count=myCount)
        if verbose:
            print(filename)
            print("{:10s} | {:10s} | {:10s}".format("length","count","offset"))
            print("{:10d} | {:10d} | {:10d}".format(len(rawBinary),myCount,myOffset))
        nEvents=len(rawBinary)//lenEvent
        #Seperate traces
        traces=[ [float(adc) for adc in rawBinary[i*lenEvent+lenHeader:(i+1)*lenEvent-truncate]] for i in range(nEvents) ]
        return traces
    
def extractChannel(filename):
    try:
        return int(filename[-7:-4].replace('e','').replace('_',''))
    except:
        print("Could not parse the channel number")
        return None
        
if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Parse and view the output data from the Caen Digitizer DT5742.')
    parser.add_argument('filename', type=str, help='The path to a single data file produced by the digitizer.')
    parser.add_argument('-e','--event', metavar='event', type=int, help='The index of the event you want returned. If you do not enable this it will return all the events.',default=0)
    args = parser.parse_args()
    print("Opening",args.filename)
    events=parseBinary(args.filename)
    chan=extractChannel(args.filename)
    print('This is channel {} and there are {} events.'.format(chan,len(events)))
    
    
