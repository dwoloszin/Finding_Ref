import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import SplitValues
import GroupBy
import TratarSync



def FeatureState(TEC):
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
    Frame = TratarSync.processArchive(Frame,['NodeId'])
    Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):
  try:
    Frame = Frame.loc[:,~Frame.T.duplicated(keep='first')]
    Frame = GroupBy.processArchive(Frame,'NodeId')
    columns_to_split = ['FeatureStateId', 'featureState','serviceState']
    for col in columns_to_split:
      x = Frame[col].values[:1][0].split('|')
      y = Frame[columns_to_split[0]].values[:1][0].split('|')
      #print(x,len(x))
      columList = []
      for i in range(len(x)):
        columList.append(col+'_'+y[i])
      Frame[columList] = Frame[col].str.split('|', expand=True)
    Frame = Frame.loc[:, ~Frame.columns.str.startswith(columns_to_split[0]+'_')]      
  except:
    pass
   
  droplist = ['SystemFunctionsId','LmId']
  Frame.drop(droplist, axis=1, inplace=True,errors='ignore')    
  
  return Frame




script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
frame_tableList.drop(frame_tableList[frame_tableList['TableList'] != 'FeatureState'].index, inplace=True)
TEC_List = frame_tableList['TEC'].tolist()

for i in TEC_List:
  FeatureState(i) 
