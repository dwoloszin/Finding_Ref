import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import TratarSync



def AuxPlugInUnit(TEC):
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
    Frame = TratarSync.processArchive(Frame,['NodeId','AuxPlugInUnitId','serialNumber'])
    droplist = ['syncStatus','productData']
    Frame.drop(droplist, errors='ignore', axis=1,inplace=True)
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
    FieldReplaceableUnit = Frame.copy()
    FieldReplaceableUnit.rename(columns={'TableName_AuxPlugInUnit': 'TableName_FieldReplaceableUnit',
                                         'AuxPlugInUnitId':'FieldReplaceableUnitId'}, inplace=True)
    pathToSave2 = script_dir + '/export/'+'PROCESS'+'/'+'FieldReplaceableUnit' +'/'
    FieldReplaceableUnit.to_csv(pathToSave2 + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):
  try:
    Frame = SplitValues.processArchive3(Frame,'productData','productionDate=')
    Frame = SplitValues.processArchive3(Frame,'productData','serialNumber=')
    Frame = SplitValues.processArchive3(Frame,'productData','productName=')
    Frame = SplitValues.processArchive3(Frame,'productData','productNumber=')
    Frame = SplitValues.processArchive3(Frame,'productData','productRevision=')
    listCorrection = ['productionDate','serialNumber','productName','productNumber','productRevision']
    for i in listCorrection:
      Frame[i] = Frame[i].str.replace('|', '',regex=True)
  except:
    pass
  droplist = ['EquipmentId','RbsSubrackId','RbsSlotId']
  Frame.drop(droplist, axis=1, inplace=True,errors='ignore')
  Frame['Ref'] = Frame['NodeId'].astype(str) + Frame['AuxPlugInUnitId'].astype(str)
  return Frame



script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
frame_tableList.drop(frame_tableList[frame_tableList['TableList'] != 'AuxPlugInUnit'].index, inplace=True)
TEC_List = frame_tableList['TEC'].tolist()

for i in TEC_List:
  AuxPlugInUnit(i)


