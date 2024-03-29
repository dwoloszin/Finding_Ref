import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import numpy as np
import TratarSync



def RfBranch(TEC):
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
    droplist = ['NodeId','syncStatus','EquipmentId','RfBranchId','reservedBy','rfPortRef','RfPort','AuxPlugInUnit']
    Frame.drop(droplist, errors='ignore', axis=1,inplace=True)
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print (f'{this_function_name} duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(frameSI):
  try:
    frameSI = SplitValues.processArchive3_1(frameSI,'rfPortRef','FieldReplaceableUnit=')
    frameSI = SplitValues.processArchive3_1(frameSI,'rfPortRef','RfPort=')
    frameSI = SplitValues.processArchive3_1(frameSI,'rfPortRef','AuxPlugInUnit=')
    frameSI.loc[frameSI['NodeId'].str[:3] == '4S-',['NodeId']] = '4G-' + frameSI['NodeId'].str[3:]# Corrigir SKY q mudou de ID
    frameSI.loc[~frameSI['AuxPlugInUnit'].isna(),['FieldReplaceableUnit']] = frameSI['AuxPlugInUnit']
    #frameSI['Ref'] = frameSI['NodeId'].astype(str) + frameSI['AntennaUnitGroupId'].astype(str)
  except:
    pass
  frameSI['Ref'] = frameSI['NodeId'].astype(str) + frameSI['AntennaUnitGroupId'].astype(str)
  frameSI['Ref2'] = frameSI['NodeId'].astype(str) + frameSI['FieldReplaceableUnit'].astype(str)
  return frameSI




script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
frame_tableList.drop(frame_tableList[frame_tableList['TableList'] != 'RfBranch'].index, inplace=True)
TEC_List = frame_tableList['TEC'].tolist()

for i in TEC_List:
  RfBranch(i)