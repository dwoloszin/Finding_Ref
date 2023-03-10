import ENM
import os
import sys
import pandas as pd
import timeit
import shutil



def processArchive(site,table,parameterList,TEC):
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  if len(site)>0:
    cmd = f'cmedit get {site} {table}.({parameterList}) -t -s'
  else:
    cmd = f'cmedit get * {table}.({parameterList}) -t -s'
  pathToSave = cmd.split('.(')[0].split(' ')[-1]
  print (f'\nprocessing: {pathToSave}')
  inicio = timeit.default_timer()
  print(cmd)
  frameSIList = ENM.processArchiveReturn(cmd)
  frameCount = 0
  #if os.path.exists(script_dir + '/export/'+TEC+'/'):
  #  shutil.rmtree(script_dir + '/export/'+TEC+'/', ignore_errors=False, onerror=None)

  for Frame in frameSIList:
    ArchiveName1 = pathToSave + '_' + str(frameCount)
    Frame = Frame.astype(str)
    #Frame['TableName'] = pathToSave
    Frame.insert(loc=0, column='TableName_'+pathToSave, value=pathToSave)
    Frame = Frame.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    csv_path = os.path.join(script_dir, 'export/'+TEC+'/'+ pathToSave +'/')
    if not os.path.exists(csv_path):
      os.makedirs(csv_path)    
    Frame = tratarArchive(Frame)
    #Frame.head(1).to_csv(csv_path + ArchiveName1 + '.csv',index=False,header=True,sep=';')
    Frame.to_csv(csv_path + TEC+'_' + ArchiveName1 + '.csv',index=False,header=True,sep=';')
    frameCount +=1
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')


def tratarArchive(frameSI):
  #frameSI['BW DL'] = frameSI['bSChannelBwDL'].astype(str) + ' M'

  return frameSI