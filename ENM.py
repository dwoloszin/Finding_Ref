import time
import enmscripting
import credentials
import pandas as pd
import util

credentials = util.getCredentials()

def processArchiveReturn(cmd):
  Login = True
  ok = False
  while Login:
    try:
      session = enmscripting.open('https://enmcvo.internal.timbrasil.com.br',credentials['ENM_login'],credentials['ENM_passwd'])
      command = cmd
      response = session.command().execute(command,timeout_seconds=6000)
      listaDF = []
      for table in response.get_output().groups():
        listData = []
        for row in table:
          headers = list()
          for cell in table[0]:
            headers.append(cell.labels()[0])
          listData.append(row)
        df = pd.DataFrame(listData, columns = headers)
        df = df.drop_duplicates()
        df = df.reset_index(drop=True) # reset the index
        listaDF.append(df)  
      enmscripting.close(session)
      Login = False
      ok = True
    except:
      times = 5
      print ("Login Failed, trying again in {} seconds...".format(times))
      time.sleep(times)
      pass
    if ok:
      return listaDF



ENM_list = ['https://enmcvo.internal.timbrasil.com.br','https://enmcvob.internal.timbrasil.com.br']
def processArchiveReturn2(cmd):
  Login = True
  ok = False
  listaDF = []
  errorLog = []
  while Login:
    for ENM in ENM_list:
      try:
        session = enmscripting.open(ENM,credentials['ENM_login'],credentials['ENM_passwd'])
        command = cmd
        response = session.command().execute(command,timeout_seconds=6000)
        for table in response.get_output().groups():
          listData = []
          for row in table:
            headers = list()
            for cell in table[0]:
              headers.append(cell.labels()[0])
            listData.append(row)
          df = pd.DataFrame(listData, columns = headers)
          df['Server'] = ENM
          df = df.drop_duplicates()
          df = df.reset_index(drop=True) # reset the index
          listaDF.append(df)  
        enmscripting.close(session)
      except:
        times = 5
        print (f"{ENM} :Login Failed, trying again in {times} seconds...")
        time.sleep(times)
      pass
    Login = False
    ok = True
    if ok:
      return listaDF




