import os


def processArchive(rootdir):
    dirList = []
    for it in os.scandir(rootdir):
      if it.is_dir():
        name2 = it.path.split(f"\\")[-1]
        if name2 not in dirList:
          dirList.append(name2)
    return dirList

def processArchive2(rootdir):
    dirList = []
    for it in os.scandir(rootdir):
      if it.is_dir():
        name2 = it.path
        if name2 not in dirList:
          dirList.append(name2)
    return dirList

