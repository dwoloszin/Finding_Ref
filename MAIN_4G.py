import os
import sys
import timeit
import shutil
import ENM_GetData
import ImportDF
import pandas as pd
import subprocess
import numpy as np
import subprocess




def processArchive(Content):
  inicio = timeit.default_timer()
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  TEC = '4G'
  pathToSaving = script_dir + '/export/'+TEC+'/'
  site = 'SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP;SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP;SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP,SubNetwork=DUs;SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP,SubNetwork=RadioNodes'
  #get everything from table
  #site = ''
  frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
  frame_tableList.drop(frame_tableList[frame_tableList['TEC'] != TEC].index, inplace=True)
  frame_tableList['parameterList'] = [x.strip('()').split(',') for x in frame_tableList['parameterList'].astype(str)]
  frame_tableList = frame_tableList.sort_values(by=['TableList'], ascending=[True])
  tableList = frame_tableList['TableList'].tolist()
  parameterList = frame_tableList['parameterList'].tolist()
  CustomCMDList = frame_tableList['CustomCMD'].tolist()
  #dropList = frame_tableList['dropList'].tolist()[0].str.split(',')
  dropList = frame_tableList['keepList'].tolist()
  UPDATE_List = frame_tableList['UPDATE'].tolist()

  dropList2 = []
  for i in dropList:
    if str(i) == 'nan':
      a = []
    else:
      a = i.split(',') 
    dropList2.append(a)

 

  
  if Content == 'TESTE':
    #setup for get everthing from a site 4G-SPVA07*
    #setup for get everthing from table ''
    site = ''
    site = '4G-SPIB42*' 
    
    parameterList = []
    for i in range(len(tableList)):
      parameterList.append(['*'])
    print(parameterList)
    #setup for get everthing from a site


  count = 0
  cmd_List = []
  for i in parameterList: 
    s = ','.join(str(x) for x in i)
    cmd2 = []
    cmd2.append("python")
    cmd2.append("-c")
    cmd2.append(f"from ENM_GetData import processArchive; processArchive('{site}','{tableList[count]}','{s}','{TEC}','{CustomCMDList[count]}','{UPDATE_List[count]}')")
    cmd_List.append(cmd2)
    #ENM_GetData.processArchive(site,tableList[count],s,TEC,CustomCMDList[count],UPDATE_List[count])
    count +=1
    
  processes = []
  for cmd in cmd_List:
    processes.append(subprocess.Popen(cmd, shell=True))
  # Wait for all processes to complete
  for process in processes:
      process.wait()




  listOfHeader = []
  for folderName in tableList:
    try:
      FrameSI = ImportDF.ImportDF(pathToSaving,folderName)
      listOfHeader.append(list(FrameSI.columns.values))
    except:
      pass

  with open(TEC+"_file.txt", "w") as f:
    for list1 in listOfHeader:
      for i in list1:
        f.write(str(i) +"\n")

 





  fim = timeit.default_timer()
  print ('duracao FINAL 4G: %.2f' % ((fim - inicio)/60) + ' min')



processArchive('UPDATE')  