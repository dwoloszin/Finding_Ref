import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import TratarSync




def UeMeasControl(TEC):
  inicio = timeit.default_timer()
  this_function_name = inspect.currentframe().f_code.co_name
  print(this_function_name, ' Processing...')
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  pathToImport = script_dir + '/export/'+TEC+'/'+this_function_name
  pathToSave = script_dir + '/export/'+'PROCESS'+'/'+this_function_name +'/'
  if not os.path.exists(pathToSave):
    os.makedirs(pathToSave)
  if os.path.exists(pathToImport):  
    #Frame = ImportDF.ImportDF2(pathToImport)
    #Frame = ImportDF.ImportDF3(pathToImport,'EUtranCellFDDId')
    Frame = ImportDF.ImportDF4(pathToImport,['EUtranCellFDDId'])
    Frame.drop_duplicates(inplace=True)
    Frame['TEC'] = TEC
    Frame = tratarArchive(Frame)
    Frame = TratarSync.processArchive(Frame,['EUtranCellFDDId'])
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')


  fim = timeit.default_timer()
  print (f'{this_function_name} duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):
  '''
  try:
    Frame = SplitValues.processArchive4(Frame,'productData','productionDate=')

  except:
    pass   
  '''
  return Frame





script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
frame_tableList.drop(frame_tableList[frame_tableList['TableList'] != 'UeMeasControl'].index, inplace=True)
TEC_List = frame_tableList['TEC'].tolist()

for i in TEC_List:
  UeMeasControl(i) 