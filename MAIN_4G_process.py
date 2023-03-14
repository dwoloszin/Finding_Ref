import os
import sys
import timeit
import shutil
import ImportDF
import pandas as pd
import inspect
import SplitValues



def AuxPlugInUnit():
  inicio = timeit.default_timer()
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  this_function_name = inspect.currentframe().f_code.co_name
  TEC = '4G'
  pathToImport = script_dir + '/export/'+TEC+'/'+this_function_name
  pathToSave = script_dir + '/export/'+'PROCESS'+'/'+this_function_name +'/'
  if not os.path.exists(pathToSave):
    os.makedirs(pathToSave) 
  Frame = ImportDF.ImportDF2(pathToImport)
  Frame.drop_duplicates(inplace=True)
  Frame['TEC'] = TEC
  Frame = tratarArchive(Frame)
  Frame.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  fim = timeit.default_timer()
  print ('duracao FINAL: %.2f' % ((fim - inicio)/60) + ' min')



def tratarArchive(Frame):
  #frameSI['BW DL'] = frameSI['bSChannelBwDL'].astype(str) + ' M'
  Frame = SplitValues.processArchive(Frame,'productData')

  return Frame  