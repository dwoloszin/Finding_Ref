import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import numpy as np
import TratarSync 


script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
def SectorEquipmentFunction(TEC):
  inicio = timeit.default_timer()
  this_function_name = inspect.currentframe().f_code.co_name
  print(this_function_name, ' Processing...')
  pathToImport = script_dir + '/export/'+TEC+'/'+this_function_name
  pathToSave = script_dir + '/export/'+'PROCESS'+'/'+this_function_name +'/'
  if not os.path.exists(pathToSave):
    os.makedirs(pathToSave)
  if os.path.exists(pathToImport):  
    #Frame = ImportDF.ImportDF2(pathToImport)
    Frame = ImportDF.ImportDF4(pathToImport,['NodeId','SectorEquipmentFunctionId'])
    Frame.drop_duplicates(inplace=True)
    Frame['TEC'] = TEC
    Frame = tratarArchive(Frame)
    Frame = TratarSync.processArchive(Frame,['EUtranCellFDD'])
    try:
      Frame['CELL2'] = Frame['EUtranCellFDD']
      Frame.loc[Frame['CELL2'].str[-4:-2] == '26',['CELL2'] ] = Frame['EUtranCellFDD'].astype(str) + '|4C-' + Frame['EUtranCellFDD'].str[3:]
      splitValue = 'CELL2'
      Frame = (Frame.set_index(Frame.columns.drop(splitValue,1).tolist())[splitValue].str.split('|', expand=True).stack().reset_index().rename(columns={0:splitValue}).loc[:, Frame.columns] )
      Frame['EUtranCellFDD'] = Frame['CELL2']
      Frame.drop('CELL2', axis=1,inplace=True)

    except:
      pass
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')


  fim = timeit.default_timer()
  print (f'{this_function_name} duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):
  
  
  try:
    Frame = SplitValues.processArchive3(Frame,'reservedBy','SectorCarrier=')
    Frame = SplitValues.processArchive3(Frame,'rfBranchRef','RfBranch=')
    Frame = SplitValues.processArchive3(Frame,'rfBranchRef','FieldReplaceableUnit=')
    
    Frame['FieldReplaceableUnit'] = Frame['FieldReplaceableUnit'].str.split(',').str[0]
    splitValue = 'SectorCarrier'
    Frame = (Frame.set_index(Frame.columns.drop(splitValue,1).tolist())[splitValue].str.split('|', expand=True).stack().reset_index().rename(columns={0:splitValue}).loc[:, Frame.columns] )
    Frame['SITE'] = Frame['NodeId']
    Frame.loc[(Frame['NodeId'].str[-2:-1] == '-'),['SITE']] = Frame['NodeId'].str[:-2]
    Frame['EUtranCellFDD'] = Frame['SITE'].astype(str) + Frame['SectorCarrier']
    Frame.loc[(np.array(list(map(len,Frame['SectorCarrier'].astype(str).values))) > 2),['EUtranCellFDD']] = Frame['SITE'].astype(str) + '-'+Frame['SectorCarrier'].astype(str)
    Frame.loc[Frame['TEC'] == '5G',['EUtranCellFDD']] = Frame['SectorCarrier']
    Frame['Ref'] = Frame['NodeId'].astype(str) + Frame['SectorEquipmentFunctionId'].astype(str)
    Frame.loc[Frame['SectorCarrier'].str[:3] == '5G-',['TEC']] = '5G'
    Frame.loc[Frame['SectorCarrier'].str[:3] == '5G-',['EUtranCellFDD']] = Frame['SectorCarrier']
    
  except:
    pass  
     
  return Frame


script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
frame_tableList.drop(frame_tableList[frame_tableList['TableList'] != 'SectorEquipmentFunction'].index, inplace=True)
TEC_List = frame_tableList['TEC'].tolist()

for i in TEC_List:
  SectorEquipmentFunction(i) 
  
