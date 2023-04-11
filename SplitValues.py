import pandas as pd


def processArchive(frameSI,columnNAME):
  datalist = frameSI.at[0,columnNAME].split(',')
  datalist2 = []
  for i in datalist:
    x = i.split('=')[0].replace('{','')
    #datalist2.append(columnNAME + '_' + x)
    datalist2.append(x)
  frameSI[datalist2] = pd.DataFrame([ x.split(',') for x in frameSI[columnNAME].astype(str).tolist() ])
  frameSI = frameSI.astype(str)
  
  for i in datalist2:
    frameSI[i] = [x.split('=')[-1].replace('}','') for x in frameSI[i]]

  return frameSI

def processArchive2(frameSI,columnNAME):
  datalist = frameSI.at[0,columnNAME].split(',')
  datalist2 = []
  for i in datalist:
    x = i.split('=')[0].replace('{','')
    datalist2.append(columnNAME + '_' + x)
  frameSI[datalist2] = pd.DataFrame([ x.split(',') for x in frameSI[columnNAME].astype(str).tolist() ])
  frameSI = frameSI.astype(str)
  
  for i in datalist2:
    frameSI[i] = [x.split('=')[-1].replace('}','') for x in frameSI[i]]

  return frameSI

def processArchive3(frameSI,columnNAME,valueSearch):
  for index, row in frameSI.iterrows():
    listData = []
    li = []
    stringB = ''
    if valueSearch in str(row[columnNAME]):
      string0 = row[columnNAME].replace(', ',';').split(';')
      for i in string0:
        SectorEqui = i.partition(valueSearch)[2].split(']')[0]
        li.append(SectorEqui)
      for h in li:
        #print(h)
        #if h != '' and h not in stringB: 
        stringB += h + '|'  
      frameSI.at[index,valueSearch[:-1]] = stringB[:-1]
  return frameSI


def processArchive3_1(frameSI,columnNAME,valueSearch):
  for index, row in frameSI.iterrows():
    listData = []
    li = []
    stringB = ''
    if valueSearch in str(row[columnNAME]):
      string0 = row[columnNAME].replace(',',';').split(';')
      for i in string0:
        SectorEqui = i.partition(valueSearch)[2].split(']')[0]
        li.append(SectorEqui)
      for h in li:
        if h != '' and h not in stringB:
          stringB += h + '|'    
      frameSI.at[index,valueSearch[:-1]] = stringB[:-1]
  return frameSI



def processArchive4(frameSI,columnNAME,valueSearch):
  for index, row in frameSI.iterrows():
    listData = []
    li = []
    stringB = ''
    if valueSearch in str(row[columnNAME]):
      string0 = row[columnNAME].replace(', ',';').split(';')
      for i in string0:
        SectorEqui = i.partition(valueSearch)[2].split('}')[0]
        li.append(SectorEqui)
      for h in li:
        if h != '' and h not in stringB:
          stringB += h + '|'    
      frameSI.at[index,valueSearch[:-1]] = stringB[:-1]
  return frameSI


