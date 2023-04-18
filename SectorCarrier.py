import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import numpy as np
import TratarSync


def SectorCarrier(TEC):
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
    Frame = TratarSync.processArchive(Frame,['EUtranCellFDD'])
    droplist = ['NodeId','syncStatus','ENodeBFunctionId','SectorCarrierId','reservedBy','sectorFunctionRef','SITE']
    Frame.drop(droplist, errors='ignore', axis=1,inplace=True)
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print (f'{this_function_name} duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(frameSI):
  try:
    frameSI = SplitValues.processArchive3(frameSI,'reservedBy','EUtranCellFDD=')
    frameSI = SplitValues.processArchive3(frameSI,'reservedBy','UlCompGroup=')
    frameSI = SplitValues.processArchive3(frameSI,'sectorFunctionRef','SectorEquipmentFunction=')
    frameSI['SITE'] = frameSI['NodeId']
    frameSI.loc[(frameSI['NodeId'].str[-2:-1] == '-'),['SITE']] = frameSI['NodeId'].str[:-2]
    frameSI.loc[(frameSI['EUtranCellFDD'].isna()),['EUtranCellFDD']] = frameSI['SITE'].astype(str)+frameSI['SectorCarrierId'].astype(str)
    frameSI.loc[(frameSI['EUtranCellFDD'].isna())&
                (np.array(list(map(len,frameSI['SectorCarrierId'].astype(str).values))) > 2),['EUtranCellFDD']] = frameSI['SITE'].astype(str) + '-'+frameSI['SectorCarrierId'].astype(str)
    frameSI['Ref'] = frameSI['NodeId'].astype(str) + frameSI['SectorEquipmentFunction'].astype(str)
    #splitValue = 'EUtranCellFDD'
    #frameSI = (frameSI.set_index(frameSI.columns.drop(splitValue,1).tolist())[splitValue].str.split('|', expand=True).stack().reset_index().rename(columns={0:splitValue}).loc[:, frameSI.columns] )

    frameSI['EUtranCellFDD'] = frameSI['EUtranCellFDD'].str.replace('|', '',regex=True)
    frameSI['UlCompGroup'] = frameSI['UlCompGroup'].str.replace('\D', '', regex=True) #removing all caracter besides numbers
    #frameSI['UlCompGroup'] = frameSI['UlCompGroup'].str.replace('\d+', '', regex=True) #removing all numbers besides caracters

  except:
    pass
  return frameSI
  


script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
frame_tableList.drop(frame_tableList[frame_tableList['TableList'] != 'SectorCarrier'].index, inplace=True)
TEC_List = frame_tableList['TEC'].tolist()

for i in TEC_List:
  SectorCarrier(i)