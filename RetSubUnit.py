import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import TratarSync



script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
def RetSubUnit(TEC):
  inicio = timeit.default_timer()
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  this_function_name = inspect.currentframe().f_code.co_name
  print(this_function_name, ' Processing...')
  pathToImport = script_dir + '/export/'+TEC+'/'+this_function_name
  pathToSave = script_dir + '/export/'+'PROCESS'+'/'+this_function_name +'/'
  if not os.path.exists(pathToSave):
    os.makedirs(pathToSave) 
  if os.path.exists(pathToImport):
    Frame = ImportDF.ImportDF2(pathToImport)
    Frame.drop_duplicates(inplace=True)
    Frame['TEC'] = TEC
    Frame = tratarArchive(Frame)
    Frame = TratarSync.processArchive(Frame,['NodeId','AntennaUnitGroupId'])
    droplist = ['syncStatus','EquipmentId','AntennaNearUnitId','RetSubUnitId']
    Frame.drop(droplist, errors='ignore', axis=1,inplace=True)
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print (f'{this_function_name} duracao: %.2f' % ((fim - inicio)/60) + ' min')




def tratarArchive(Frame):
  
  
  try:
    Frame.loc[Frame['NodeId'].str[:3] == '4S-',['NodeId']] = '4G-' + Frame['NodeId'].str[3:]# Corrigir SKY q mudou de ID
    Frame['Ref'] = Frame['NodeId'].astype(str) + Frame['AntennaUnitGroupId'].astype(str) 
  except:
    pass
   
   
  return Frame



script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
frame_tableList.drop(frame_tableList[frame_tableList['TableList'] != 'RetSubUnit'].index, inplace=True)
TEC_List = frame_tableList['TEC'].tolist()

for i in TEC_List:
  RetSubUnit(i) 