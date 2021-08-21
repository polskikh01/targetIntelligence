import pandas
import numpy as np
import matplotlib.pyplot as plt
import time
import io
from datetime import datetime

''' КОЛОНКИ ПАРКЕТА '''
#['AddedOnDate', 'Mac', 'RadarId', 'Face', 'AdmTypeBit', 'AddedOnTick', 'FrameOid']
#['OidColumn', 'PlayerUUID', 'BillingDate', 'MediaItemName', 'Duration', 'BookingId', 'LogFileName', 'Src', 'MediaItemId', 'advertisementId', 'MacCountAll', 'MacCountStaticOnly', 'DateTimeInTick', 'DateTimeOutTick']
colPlayerLog = ['advertisementId', 'MacCountAll','DateTimeInTick']
colCrowd = ['AddedOnDate', 'Mac', 'RadarId', 'Face', 'AdmTypeBit', 'AddedOnTick', 'FrameOid']

''' СЛОВАРИ ДЛЯ ЗАПИСИ OTS ПО ДНЯМ МЕСЯЦА '''
datesOfOTS = [{1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,
              19:0,20:0,21:0,22:0,23:0,24:0,25:0,26:0,27:0,28:0,29:0,30:0,31:0},
              {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,
              19:0,20:0,21:0,22:0,23:0,24:0,25:0,26:0,27:0,28:0,29:0,30:0,31:0},
              {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,
              19:0,20:0,21:0,22:0,23:0,24:0,25:0,26:0,27:0,28:0,29:0,30:0,31:0},
              {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,
              19:0,20:0,21:0,22:0,23:0,24:0,25:0,26:0,27:0,28:0,29:0,30:0,31:0},
              {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,
              19:0,20:0,21:0,22:0,23:0,24:0,25:0,26:0,27:0,28:0,29:0,30:0,31:0},
              {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,
              19:0,20:0,21:0,22:0,23:0,24:0,25:0,26:0,27:0,28:0,29:0,30:0,31:0},
              {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,
              19:0,20:0,21:0,22:0,23:0,24:0,25:0,26:0,27:0,28:0,29:0,30:0,31:0},
              {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,
              19:0,20:0,21:0,22:0,23:0,24:0,25:0,26:0,27:0,28:0,29:0,30:0,31:0}]

''' ПРИТЯГИВАЕМ ИЗ ТЕКСТОВОГО ФАЙЛА ПУТИ КО ВСЕМ ФАЙЛАМ ДАТАСЕТА '''

dirsPlayerLog = []
f = open("paths.txt", "r")
line = f.readline()
while line:
    line = f.readline()
    dirsPlayerLog.append(line.rstrip())
f.close()

del dirsPlayerLog[-1]

adsIndexes = [41517, 42473, 44054, 42181, 40085, 41081, 41007, 41061, 55713, 56432, 66607, 64856, 66395, 57487, 58384, 56954, 58492, 57470]

a = -1
for j in adsIndexes:
    for i in range(0, len(dirsPlayerLog)):
        test2 = pandas.read_parquet(dirsPlayerLog[i], engine = "fastparquet", columns = colPlayerLog)
        df = test2.loc[test2['advertisementId'] == j] #['MacCountAll'].sum()

        if i%20==0:
            a+=1
            
        for index, row in df.iterrows():
            datesOfOTS[a][int(datetime.utcfromtimestamp(int(row['DateTimeInTick'])/1000).strftime('%d'))] += int(row['MacCountAll'])
    print("advertisementId = "+ str(j))
    print(datesOfOTS)
    a = -1
    for i in range(0,len(datesOfOTS)):
        for key, value in datesOfOTS[i].items():
            datesOfOTS[i][key] = 0





'''

test2 = pandas.read_parquet(dirsPlayerLog[102], engine = "fastparquet", columns = colPlayerLog)
print(test2)

'''
