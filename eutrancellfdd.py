import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import ShortName



def eutrancellfdd(TEC):
  inicio = timeit.default_timer()
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  this_function_name = inspect.currentframe().f_code.co_name
  print(this_function_name, ' Processing...')
  pathToImport = script_dir + '/export/'+TEC+'/'+this_function_name
  pathToSave = script_dir + '/export/'+'PROCESS'+'/'+this_function_name +'/'
  if not os.path.exists(pathToSave):
    os.makedirs(pathToSave) 
  Frame = ImportDF.ImportDF2(pathToImport)
  Frame.drop_duplicates(inplace=True)
  Frame['TEC'] = TEC
  Frame = tratarArchive(Frame)
  Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):
  
  try:
    Frame['SITE'] = Frame['NodeId']
    Frame.loc[(Frame['NodeId'].str[-2:-1].isin(['-','_'])),['SITE']] = Frame['NodeId'].str[:-2]
    Frame['additionalUpperLayerIndList2'] = Frame['additionalUpperLayerIndList'].str.split(',').str[1]

  
    Frame['BW DL'] = Frame['dlChannelBandwidth'].astype(float)/1000
    Frame['BW DL'] = Frame['BW DL'].astype(int)
    Frame['BW DL'] = Frame['BW DL'].astype(str) + ' M'
    
    Frame['FREQ CELL'] = Frame['freqBand'].map({'1':'2100','3':'1800','7':'2600','28':'700'})
    
    Frame['Tecnologia'] = '4G'
    Frame = Frame.loc[Frame['CELL'] != '0']
    Frame['PCI'] = Frame['physicalLayerCellIdGroup'].astype(int)*3 + Frame['physicalLayerSubCellId'].astype(int)
    Frame['PCI'] = Frame['PCI'].astype(int)
    Frame['VENDOR'] = 'ERICSSON'
    Frame = ShortName.tratarShortNumber(Frame,'SITE')
  except:
    pass
   
    
  return Frame


