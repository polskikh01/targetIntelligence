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

adsIndexes = [15715,31555,31915,29030,24631,32871,33115,24821,24639,25427,25051,75294,78950,59080,61039,52648,50565,51169,54988,54861]

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
    
    for i in range(0,len(datesOfOTS)):
        for key, value in datesOfOTS[i].items():
            doo[i][key] = 0















#for i in range(0, len(dirsPlayerLog)):
    #print("Билборд №"+str(i+1))
    #test2 = pandas.read_parquet(dirsPlayerLog[i], engine = "fastparquet", columns = colPlayerLog)
    #datesOfOTS[int(datetime.utcfromtimestamp(int(test2.loc[test2['advertisementId'] == 15715 ]['DateTimeInTick'])/1000).strftime('%d'))] += test2.loc[test2['advertisementId'] == 15715 & test2['DateTimeInTick'] == 15715]['MacCountAll'].sum()
    #print(test2.loc[test2['advertisementId'] == 15715]['MacCountAll'].sum())

#test2 = pandas.read_parquet(dirsPlayerLog[0], engine = "fastparquet", columns = colPlayerLog)
#print(test2.loc[test2['advertisementId'] == 15715])
#print(datetime.utcfromtimestamp(int(test2.loc[0]['DateTimeInTick'])/1000).strftime('%Y-%m-%d %H:%M:%S'))


'''

t0= time.perf_counter()

for i in range(0, len(dirsPlayerLog)):
    playerLogParquet = pandas.read_parquet(dirsPlayerLog[i], engine = "fastparquet", columns = colPlayerLog)

    for index, row in playerLogParquet.iterrows():
        if row['advertisementId'] == 25329:
            datesOfOTS[i][int(datetime.utcfromtimestamp(int(row['DateTimeInTick'])/1000).strftime('%d'))] += int(row['MacCountAll'])

print(datesOfOTS)
t1 = time.perf_counter() - t0
print("Время выполнения скрипта: ", t1 - t0)
'''

#визуализация
'''
plt.bar(list(datesOfOTS.keys()), datesOfOTS.values(), color='g')
plt.xlabel('Days of Month')
plt.ylabel('OTS')
plt.title('Histogram of OTS per days of mouth')
plt.xlim(0, 31)
plt.ylim(0, 100000)
plt.grid(True)
plt.show()
'''
