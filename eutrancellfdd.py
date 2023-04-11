import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import ShortName
import TratarSync


def eutrancellfdd(TEC):
  inicio = timeit.default_timer()
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  this_function_name = inspect.currentframe().f_code.co_name
  print(this_function_name, ' Processing...')
  pathToImport = script_dir + '/export/'+TEC+'/'+this_function_name
  pathToSave = script_dir + '/export/'+'PROCESS'+'/'+this_function_name +'/'
  if not os.path.exists(pathToSave):
    os.makedirs(pathToSave) 
  if os.path.exists(pathToImport):  
    #Frame = ImportDF.ImportDF3(pathToImport,'EUtranCellFDDId')
    Frame = ImportDF.ImportDF4(pathToImport,['EUtranCellFDDId'])
    Frame.drop_duplicates(inplace=True)
    Frame['TEC'] = TEC
    Frame = tratarArchive(Frame)
    Frame = TratarSync.processArchive(Frame,['EUtranCellFDDId'])
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):
  #Frame['PCI'] = (Frame['physicalLayerCellIdGroup'].astype(int)*3) + (Frame['physicalLayerSubCellId'].astype(int))

  
  try:
    Frame.loc[(Frame['NodeId'].isna()) & (Frame['earfcndl'].isin(['1700','1525','1575'])),['NodeId']] = Frame['EUtranCellFDDId'].str[:-2]
    Frame.loc[(Frame['NodeId'].isna()) & (~Frame['earfcndl'].isin(['1700','1525','1575'])),['NodeId']] = Frame['EUtranCellFDDId'].str[:-1]
    Frame.loc[Frame['TableName_eutrancellfdd'].isna(),['TableName_eutrancellfdd']] = 'eutrancellfdd'
    Frame['SITE'] = Frame['NodeId']
    Frame.loc[(Frame['NodeId'].str[-2:-1].isin(['-','_'])),['SITE']] = Frame['NodeId'].str[:-2]
    Frame['additionalUpperLayerIndList2'] = Frame['additionalUpperLayerIndList'].str.split(',').str[1]

  
    Frame['BW DL'] = Frame['dlChannelBandwidth'].astype(float)/1000
    Frame['BW DL'] = Frame['BW DL'].astype(int)
    Frame['BW DL'] = Frame['BW DL'].astype(str) + ' M'
    
    Frame['FREQ CELL'] = Frame['freqBand'].map({'1':'2100','3':'1800','7':'2600','28':'700'})
    
    Frame['Tecnologia'] = '4G'
    Frame['PCI'] = (Frame['physicalLayerCellIdGroup'].astype(int)*3) + (Frame['physicalLayerSubCellId'].astype(int))
    Frame['PCI'] = Frame['PCI'].astype(int)
    Frame['VENDOR'] = 'ERICSSON'
    Frame = ShortName.tratarShortNumber(Frame,'SITE')
  except:
    pass
  droplist = ['ENodeBFunctionId','dlChannelBandwidth','freqBand','physicalLayerCellIdGroup','physicalLayerSubCellId']
  Frame.drop(droplist, axis=1, inplace=True,errors='ignore')  
    
  return Frame




script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
frame_tableList.drop(frame_tableList[frame_tableList['TableList'] != 'eutrancellfdd'].index, inplace=True)
TEC_List = frame_tableList['TEC'].tolist()

for i in TEC_List:
  eutrancellfdd(i) 
