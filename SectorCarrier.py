import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import numpy as np



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
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(frameSI):
  df = frameSI.astype(str)
  #remover 5G
  listData = []
  frameSI = frameSI.loc[~(frameSI['NodeId'].str[:2] == '5G')]
  li = []
  for index, row in df.iterrows(): 
    #print(row['rfPortRef'])  
    if 'MeContext=' in row['sectorFunctionRef']:
      string0 = row['reservedBy'].replace('\t',';')
      stringA = row['sectorFunctionRef'].replace('\t',';')
      SectorEqui = string0.partition('UlCompGroup=')[2].split(',')[0]
      SectorEqui = SectorEqui[:-1]  
      AuxPlugInUnit = stringA.partition('SectorEquipmentFunction=')[2].split(',')[0]
      siteid = stringA.partition(',MeContext=')[2].split(',')[0]
      MeContext = string0.partition('EUtranCellFDD=')[2].split(',')[0]
      stringA = stringA.split(';')
      stringB = AuxPlugInUnit+';'+siteid+';'+MeContext+';'+SectorEqui+';'
      
      for i in stringA:
        if i != '':
          stringB += i + ';'
      #print(stringB[:-1].split(';'))
      listData.append(stringB[:-1].split(';'))

  #df = pd.DataFrame (listData, columns = ['AuxPlugInUnit','FieldReplaceableUnit','NodeId','EquipmentId','AntennaUnitGroupId','RfBranchId','reservedBy','rfPortRef'])      
  df = pd.DataFrame (listData, columns = ['SectorEquipmentFunction','MeContext','EUtranCellFDD','Freq','ref'])      
  li.append(df)
  frameSI2 = pd.concat(li, axis=0, ignore_index=True)

  
  frameSI2['Ref_2'] = frameSI2['MeContext'].astype(str) + frameSI2['SectorEquipmentFunction'].astype(str)
  frameSI2.loc[frameSI2['EUtranCellFDD'].str[-1:] == ']',['EUtranCellFDD']] = frameSI2['EUtranCellFDD'].str[:-1]

  frameSI2 = frameSI2.drop(['SectorEquipmentFunction','MeContext','ref','Freq'],1)
  frameSI2 = frameSI2.loc[(np.array(list(map(len,frameSI2['EUtranCellFDD'].values))) >= 1)]
  frameSI2 = frameSI2.drop_duplicates()
  return frameSI2



SectorCarrier('4G')