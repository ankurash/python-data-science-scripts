import pandas as pd
import os
import csv
import multiprocessing as mp
import time

matchingLabelsToRemove = ['Unnamed:','facebook.com','youtube.com','twitter.com','instagram.com','snapchat.com','google.com','soundcloud']

#--- Function to return a list of uncommon elements between two lists
def ListDiff(list1, list2): 
    return(list(set(list1) - set(list2))) 

#--- Remove columns with labals containing the given string patterns
def RemoveMatchingColumnLabelsFromCSV(InputCSVFile, OutputDir):
    global matchingLabelsToRemove
    dat = pd.read_csv(InputCSVFile)
    colnamesBefore = dat.columns
    
    try:
        for pattern in matchingLabelsToRemove:
            #--- for labels containing the pattern
            for c in dat.columns:
                if c.find(pattern)>=0:
                    dat.drop([c], axis='columns', inplace=True)
            #--- for labels starting with the pattern
            #cols = [c for c in dat.columns if c.find(url)]
            #dat=dat[cols]
    except Exception as e:
        print("ERROR: error in removing url!!!")
        print(e)
    
    colnamesAfter = dat.columns        # Columns in new data
    removedElements = ListDiff(colnamesBefore, colnamesAfter)
    print(len(removedElements),"COLUMNS REMOVED FROM ",InputCSVFile,":\n",removedElements)
    
    #--- Writing data to output file
    OutputCSVFile = os.path.join(OutputDir,"Clean_" + InputCSVFile)    
    outfile = open(OutputCSVFile, "w+")
    dat.to_csv(outfile, sep=',')

#--- main
def main():
    inputdir = os.getcwd()
    OutputDir = os.path.join(inputdir,"OUTPUT")
    if not os.path.exists(OutputDir):
        os.mkdir(OutputDir)

    #Change runForSingleFile to True and sampleFile to the filename of the single file, if you want to run for single file
    runForSingleFile = False
    sampleFile = "Adjacency_7thNov_6thDec2017.csv"

    starttime = time.time()
    if(runForSingleFile == True):
        RemoveMatchingColumnLabelsFromCSV(sampleFile, OutputDir)
    else:
        try:
            processes = [mp.Process(target=RemoveMatchingColumnLabelsFromCSV, args=(sampleFile, OutputDir)) for sampleFile in os.listdir(inputdir) if sampleFile.lower().endswith(".csv")]
            # Run processes
            for p in processes:
                p.start()
            # Exit the completed processes
            for p in processes:
                p.join()
        except:
            print('multiprocessing exception')

    print('That took {} seconds'.format(time.time() - starttime))

main()