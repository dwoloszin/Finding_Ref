import os
import sys
import timeit
import ImportDF
import pandas as pd
import inspect
import GetFolderList




script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
def DUMP(TEC):
  inicio = timeit.default_timer()
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  this_function_name = inspect.currentframe().f_code.co_name
  print(this_function_name, ' Processing...')
  pathToImport = script_dir + '/export/PROCESS'
  dirlist1 = GetFolderList.processArchive2(pathToImport)
  frameList = []
  folderNameList = []
  for i in dirlist1:
    folderName = i.split('\\')[-1]
    Frame = ImportDF.ImportDF2(i)
    Frame = Frame.loc[Frame['TEC'] == TEC]
    Frame.drop_duplicates(inplace=True)
    Frame.name = folderName
    Frame = change_columnsName(Frame)
    frameList.append(Frame)
    if folderName not in folderNameList: 
      folderNameList.append(folderName)
  
  pathToImport2 = script_dir + '/import/Relation'
  print(pathToImport2)
  FrameRelation = ImportDF.ImportDF2(pathToImport2)
  FrameRelation = FrameRelation.loc[FrameRelation['TEC'] == TEC]
  FrameRelation['Frame1'] = FrameRelation['Frame1']+'_'+FrameRelation['Frame2']
  #FrameRelation['Relation1'] = FrameRelation['Relation1']+'_'+FrameRelation['Frame1'].str.split('_').str[0]
  #print(FrameRelation)
  
  keys = FrameRelation['Frame1'].tolist()
  values = FrameRelation['Relation1'].tolist()
  y1 = FrameRelation['Frame2'].tolist()
  y2 = FrameRelation['Relation2'].tolist()
  keys.extend(y1)
  values.extend(y2)
  ListPair2 = dict(zip(keys, values))#key, value
  ListPair = FrameRelation.set_index('Frame1').to_dict()['Frame2']

  #Merged = pd.merge(getFrame(frameList,'eutrancellfdd'),getFrame(frameList,'ReportConfigB1GUtra'), how='left',left_on=['EUtranCellFDDId_eutrancellfdd'],right_on=['EUtranCellFDDId_ReportConfigB1GUtra'])
  #Merged.name = 'Merged'
  firstLoop = True
  for key, value in ListPair.items():
    if firstLoop:
      f1 = getFrame(frameList,key.split('_')[0])
      f2 = getFrame(frameList,value)
      f1l = ListPair2.get(key)
      f2l = ListPair2.get(value)
      n2 = f2l +'_'+ f2.name
      Merged = pd.merge(f1,f2, how='outer',left_on=[f1l],right_on=[n2])#outer #left
      #Merged = pd.merge(getFrame(frameList,'eutrancellfdd'),getFrame(frameList,'ReportConfigB1GUtra'), how='left',left_on=['EUtranCellFDDId_eutrancellfdd'],right_on=['EUtranCellFDDId_ReportConfigB1GUtra'])
      Merged.name = 'Merged'
      firstLoop = False
    else:
      print(key,value)
      f2 = getFrame(frameList,value)

      f2l = ListPair2.get(value)
      print('F2: ',f2.name)
      n2 = f2l +'_'+ f2.name

      #print(Merged.head(2))
      #print(Merged.head(2))
      print('Merged_'+str(f2.name))
      f1l = ListPair2.get('Merged_'+str(f2.name))
      #print(f1l)

      Merged = pd.merge(Merged,f2, how='left',left_on=[f1l],right_on=[n2])
      Merged.name = 'Merged_'+f2.name
      print(Merged.name)
      print('OK')
  pathToSave = script_dir + '/export/'+this_function_name +'/'
  if not os.path.exists(pathToSave):
    os.makedirs(pathToSave) 
  Merged.drop_duplicates(inplace=True,ignore_index=True)
  #ref to aggregate
  #Merged.groupby(['A'], group_keys=False).apply(lambda x: x.loc[x.B.idxmax()])
  Merged.to_csv(pathToSave + TEC+'_' + this_function_name + '.csv',index=False,header=True,sep=';')
  
  fim = timeit.default_timer()
  print ('duracao: %.2f' % ((fim - inicio)/60) + ' min')

def tratarArchive(Frame):  
  '''
  try:
    Frame = SplitValues.processArchive(Frame,'productData')
  except:
    pass
  '''  
  return Frame






def getFrame(frames_list,desired_frame_name):
  for frame in frames_list:
      if frame.name == desired_frame_name:
          return frame
def change_columnsName(df):
  for i in df.columns:
      df.rename(columns={i:i + '_' + df.name},inplace=True)
  return df


