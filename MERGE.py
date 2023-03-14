import os
import ImportDF
import Dell_Content


def processArchive(pathToSave,this_function_name):
  Frame = ImportDF.ImportDF2(pathToSave)
  if os.path.exists(pathToSave):
    Dell_Content.processArchive(pathToSave)
  Frame.to_csv(pathToSave + this_function_name + '.csv',index=False,header=True,sep=';')
