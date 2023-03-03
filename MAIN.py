import os
import sys
import timeit
import shutil
import ENM_GetData
import ImportDF
import pandas as pd
inicio = timeit.default_timer()
script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
#site = '4G-SPVA07*'
site = 'SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE_NR,SubNetwork=TSP;SubNetwork=ONRM_ROOT_MO_R,SubNetwork=LTE,SubNetwork=TSP'


frame_tableList = ImportDF.ImportDF2(script_dir+'/import/TableList/')
#verificar se funciona
#frame_tableList = frame_tableList.sort_values(by=[['TableList']], ascending=[False])
tableList = frame_tableList['TableList'].tolist()

'''
tableList = ['eutrancellfdd','SectorCarrier','SectorEquipmentFunction','ReportConfigB1GUtra',
              'ReportConfigB1NR','FieldReplaceableUnit','Slot','AuxPlugInUnit','PlugInUnit',
              'enodebfunction','UePolicyOptimization','UeMeasControl','RfBranch']
'''

if os.path.exists(script_dir + '/export/'):
  shutil.rmtree(script_dir + '/export/', ignore_errors=False, onerror=None)
             
for i in tableList:
  ENM_GetData.processArchive(site,i)


listOfHeader = []
for folderName in tableList:
  try:
    FrameSI = ImportDF.ImportDF(folderName)
    listOfHeader.append(list(FrameSI.columns.values))
  except:
    pass

with open("file.txt", "w") as f:
  for list1 in listOfHeader:
    for i in list1:
      f.write(str(i) +"\n")

  

























fim = timeit.default_timer()
print ('duracao FINAL: %.2f' % ((fim - inicio)/60) + ' min')