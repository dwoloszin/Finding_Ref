import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime



def change_columnsName(df):
    for i in df.columns:
        df.rename(columns={i:i + '_' + df.name},inplace=True)
    return df

def ImportDF(pathName,folderName):
    pathImportSI =pathName+folderName
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []
    lastData = datetime.fromtimestamp(getmtime(all_filesSI[0])).strftime('%Y%m%d')
    for filename in all_filesSI:
        fileData = datetime.fromtimestamp(getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="UTF-8",header=0, error_bad_lines=False,dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000 )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS 
        li.append(df)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()

    return frameSI
def ImportDF2(pathImportSI):
    #pathImportSI = os.getcwd() + '/export/'+folderName
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []
    lastData = datetime.fromtimestamp(getmtime(all_filesSI[0])).strftime('%Y%m%d')
    for filename in all_filesSI:
        fileData = datetime.fromtimestamp(getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="UTF-8",header=0, error_bad_lines=False,dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000 )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS 
        li.append(df)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()

    return frameSI 

def ImportDF4(pathImportSI,index1):
    #pathImportSI = os.getcwd() + '/export/'+folderName
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []
    lastData = datetime.fromtimestamp(getmtime(all_filesSI[0])).strftime('%Y%m%d')
    for filename in all_filesSI:
        fileData = datetime.fromtimestamp(getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="UTF-8",header=0, error_bad_lines=False,dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000 )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS 
        li.append(df)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    try:
      frameSI = frameSI.groupby(index1).agg('first')
      frameSI = frameSI.reset_index()
    except:
       pass
    frameSI = frameSI.drop_duplicates()
    return frameSI 


#Corrigir qdo não tem 2 arquivos
def ImportDF3(pathImportSI,index1):
    #pathImportSI = os.getcwd() + '/export/'+folderName
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []
    lastData = datetime.fromtimestamp(getmtime(all_filesSI[0])).strftime('%Y%m%d')
    for filename in all_filesSI:
        fileData = datetime.fromtimestamp(getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="UTF-8",header=0, error_bad_lines=False,dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000 )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS
        li.append(df)
    firstLoop = True
    Count = 2
    for frame in li:
      if firstLoop:
        f1 = li[0]
        f2 = li[1]
        cols_to_use = f2.columns.difference(f1.columns)
        cols_to_use = cols_to_use.tolist()
        cols_to_use.append(index1)
        Merged = pd.merge(f1,f2[cols_to_use], how='outer',left_on=[index1],right_on=[index1])#outer #left
        #Merged = pd.merge(f1,f2, how='outer',left_on=[index1],right_on=[index1])#outer #left
      
        firstLoop = False
      else:
        if len(li)>2:
          f2 = li[Count]
          Merged = pd.merge(Merged,f2, how='outer',left_on=[index1],right_on=[index1])#outer #left   
          
    Merged = Merged.drop_duplicates()


    return Merged 

