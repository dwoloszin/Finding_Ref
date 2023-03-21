import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues


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
    Frame = ImportDF.ImportDF2(pathToImport)
    Frame.drop_duplicates(inplace=True)
    Frame['TEC'] = TEC
    Frame = tratarArchive(Frame)
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')


  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):
  Frame = SplitValues.processArchive3(Frame,'reservedBy','SectorCarrier=')
  Frame = SplitValues.processArchive3(Frame,'rfBranchRef','RfBranch=')
  Frame = SplitValues.processArchive3(Frame,'rfBranchRef','FieldReplaceableUnit=')
  Frame['FieldReplaceableUnit'] = Frame['FieldReplaceableUnit'].str.split(',').str[0]

  '''
  try:
    Frame = SplitValues.processArchive(Frame,'sectorEquipmentFunctionRef')
  except:
    pass  
  '''   
  return Frame

TEC = ['4G','5G']
for i in TEC:
  SectorEquipmentFunction(i)