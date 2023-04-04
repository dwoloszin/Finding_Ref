import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import numpy as np



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
    Frame = tratarArchive(Frame)
    Frame['TEC'] = TEC
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(frameSI):
  df = frameSI.astype(str)
  listData = []
  li = []
  #frameSI = frameSI.loc[~(frameSI['NodeId'].str[:2] == '5G')]
  for index, row in df.iterrows(): 
    if 'MeContext=' in row['rfPortRef']:
      string0 = row['reservedBy'].replace('\t',';')
      stringA = row['rfPortRef'].replace('\t',';')
      SectorEqui = string0.partition('SectorEquipmentFunction=')[2].split(',')[0]
      SectorEqui = SectorEqui[:-1]  
      AuxPlugInUnit = stringA.partition('AuxPlugInUnit=')[2].split(',')[0]
      FieldReplaceableUnit = stringA.partition('FieldReplaceableUnit=')[2].split(',')[0]
      MeContext = stringA.partition('MeContext=')[2].split(',')[0]
      stringA = stringA.split(';')
      stringB = AuxPlugInUnit+';'+FieldReplaceableUnit+';'+MeContext+';'+SectorEqui+';'
      
      for i in stringA:
        if i != '':
          stringB += i + ';'
      #print(stringB[:-1].split(';'))
      listData.append(stringB[:-1].split(';'))

  #df = pd.DataFrame (listData, columns = ['AuxPlugInUnit','FieldReplaceableUnit','NodeId','EquipmentId','AntennaUnitGroupId','RfBranchId','reservedBy','rfPortRef'])      
  df = pd.DataFrame (listData, columns = ['RRU1','RRU2','MeContext','AntennaUnitGroupId','teste'])      
  li.append(df)
  frameSI2 = pd.concat(li, axis=0, ignore_index=True)
  #frameSI2.loc[frameSI2['RRU1'].isna(),['RRU1']] = frameSI2['RRU2']
  #frameSI2['RRU1'] = np.where(frameSI2['RRU1'].isnull(), frameSI2['RRU2'], frameSI2['RRU1'])
  frameSI2.loc[(np.array(list(map(len,frameSI2['RRU1'].values))) <= 1),['RRU1']] = frameSI2['RRU2']
  
  frameSI2['Ref1'] = frameSI2['MeContext'].astype(str) + frameSI2['RRU1'].astype(str)
  frameSI2['Ref2'] = frameSI2['MeContext'].astype(str) + frameSI2['AntennaUnitGroupId'].astype(str)

  frameSI2 = frameSI2.drop(['RRU1','RRU2','MeContext','teste'],1)
  frameSI2 = frameSI2.drop_duplicates()
  frameSI2 = frameSI2.loc[(np.array(list(map(len,frameSI2['AntennaUnitGroupId'].values))) >= 1)]
  return frameSI2



RfBranch('4G')