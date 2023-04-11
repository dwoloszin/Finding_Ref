
def processArchive(Frame,listColumnref):
    syncList = {'SYNCHRONIZED':0,'UNSYNCHRONIZED':2}
    Frame['OrderSync'] = 1
    try:
      for key, value in syncList.items():
        Frame.loc[Frame['syncStatus'] == key,['OrderSync']] = value
      listColumnref.insert(0,'OrderSync')  
      Frame.sort_values(by=listColumnref, ascending=True,inplace=True)
      Frame.drop_duplicates(subset=listColumnref[1:] , keep='first',inplace=True)
      Frame.drop('OrderSync', axis=1,inplace=True)
    except:
       pass
    return Frame